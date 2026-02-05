# bot.py – Telegram-бот для покупки Stars через DAO Lama

import asyncio
import base64
import html
import os
import random
import sys
import time
import urllib.parse
import uuid


from typing import Dict, Optional, Tuple, Set


from collections import defaultdict


import ssl
import certifi


ssl._create_default_https_context = ssl.create_default_context

from aiogram import Bot, Dispatcher, F, types
from aiogram.types import BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeDefault
from aiogram.types import ChatMemberUpdated
from aiogram.dispatcher.event.bases import SkipHandler


from aiogram.enums import ParseMode


from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError


from aiogram.filters import Command, CommandStart


from aiogram.fsm.state import State, StatesGroup


from aiogram.fsm.context import FSMContext


from aiogram.fsm.storage.memory import MemoryStorage


from bs4 import BeautifulSoup


from dotenv import load_dotenv


from loguru import logger
import requests


import db_selector as db


from functools import wraps

def check_blocked(func):
    """Декоратор для проверки блокировки пользователя"""
    @wraps(func)
    async def wrapper(update, *args, **kwargs):
        # Получаем user_id из разных типов событий
        user_id = None
        
        if hasattr(update, 'from_user'):  # Message
            user_id = update.from_user.id
        elif hasattr(update, 'message') and update.message:  # CallbackQuery
            user_id = update.message.from_user.id
            
        # Проверяем блокировку
        if user_id and db.is_user_blocked(user_id):
            text = "❌ Ваш аккаунт заблокирован.\n\nЕсли вы считаете это ошибкой, обратитесь в поддержку."
            
            if hasattr(update, 'answer'):  # Message
                await update.answer(text)
            elif hasattr(update, 'message'):  # CallbackQuery
                await update.answer()
                await update.message.answer(text)
            return
            
        return await func(update, *args, **kwargs)
    return wrapper
import admin
import casino
import daolama_api as dao
import dao_wallet as ton


from xr_pay import create_invoice, check_invoice, usd_to_token


from locales import get_text, get_user_lang, set_user_lang, get_language_keyboard



try:
    import crypto_pay



    CRYPTOPAY_ENABLED = bool(os.getenv("CRYPTOPAY_TOKEN"))
except ImportError:


    CRYPTOPAY_ENABLED = False
    logger.warning("CryptoPay module not found, disabling CryptoPay payments")

load_dotenv()

import base64
BOT_TOKEN_ENCRYPTED = os.getenv("BOT_TOKEN_ENCRYPTED")
if BOT_TOKEN_ENCRYPTED:
    BOT_TOKEN = base64.b64decode(BOT_TOKEN_ENCRYPTED).decode('utf-8')
else:
    BOT_TOKEN = os.getenv("BOT_TOKEN")  # Fallback для старого формата
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
TON_WALLET_ADDRESS = os.getenv("TON_WALLET_ADDRESS")

if not (BOT_TOKEN and TON_WALLET_ADDRESS):
    sys.exit("❌ Missing BOT_TOKEN or TON_WALLET_ADDRESS")

logger.remove()
logger.add("bot.log", rotation="10 MB", level="INFO")
logger.add(sys.stderr, level="DEBUG",
           format="{time:HH:mm:ss} | {level} | {message}")

PROCESSING_PURCHASES: Set[str] = set()
USER_LAST_ACTION: Dict[int, float] = {}
PURCHASE_COOLDOWN = 3.0

USER_RATE_LIMITS: Dict[int, list] = defaultdict(list)
RATE_LIMIT_ACTIONS = 10
RATE_LIMIT_WINDOW = 60

COMPLETED_PURCHASES: Dict[str, float] = {}
PURCHASE_CACHE_TTL = 3600

_rate_cache = {"ts": 0, "usd": 0.0, "rub": 0.0}

PENDING_PAYMENTS: Dict[str, Tuple[int, float]] = {}
USER_PAYMENTS: Dict[int, str] = {}
PAYMENT_TIMEOUT = 1800
MIN_DEPOSIT = 0.1

_session = None


def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.verify = certifi.where()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        _session.mount('http://', adapter)
        _session.mount('https://', adapter)
    return _session


def check_rate_limit(user_id: int) -> bool:
    now = time.time()

    USER_RATE_LIMITS[user_id] = [
        ts for ts in USER_RATE_LIMITS[user_id]
        if now - ts < RATE_LIMIT_WINDOW
    ]

    if len(USER_RATE_LIMITS[user_id]) >= RATE_LIMIT_ACTIONS:
        return False

    USER_RATE_LIMITS[user_id].append(now)
    return True


def cleanup_old_purchases():
    now = time.time()
    to_remove = []

    for purchase_id, timestamp in list(COMPLETED_PURCHASES.items()):
        if now - timestamp > PURCHASE_CACHE_TTL:
            to_remove.append(purchase_id)

    for purchase_id in to_remove:
        del COMPLETED_PURCHASES[purchase_id]


def ton_rates() -> tuple[float, float]:
    now = time.time()
    if now - _rate_cache["ts"] > 300 or _rate_cache["usd"] == 0:
        try:
            r = get_session().get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "the-open-network", "vs_currencies": "usd,rub"},
                timeout=10
            )
            r.raise_for_status()
            data = r.json()
            _rate_cache.update({
                "ts": now,
                "usd": float(data.get("the-open-network", {}).get("usd", 0)),
                "rub": float(data.get("the-open-network", {}).get("rub", 0))
            })
            logger.debug(f"Rates updated: {_rate_cache}")
        except Exception as e:
            logger.error(f"Failed to update rates: {e}")
            if _rate_cache["usd"] == 0:
                _rate_cache.update({"usd": 6.5, "rub": 650})
    return _rate_cache["usd"], _rate_cache["rub"]


def _fee(val: float | None = None) -> float:
    if val is not None:
        db.set_fee_percent(max(0, float(val)))
        logger.info(f"Fee updated to {val}%")
    return db.get_fee_percent()


def price_one(*, with_fee=False, retry_count=3) -> float | None:
    base = None

    for attempt in range(retry_count):
        try:
            if hasattr(dao, 'stars_price'):
                base = dao.stars_price(50) / 50
                break
        except dao.DAOLamaError as exc:
            logger.warning(f"DAO Lama price error (attempt {attempt + 1}/{retry_count}): {exc}")
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)
        except Exception as exc:
            logger.error(f"Unexpected error getting price: {exc}")
            break

    if base is None:
        try:
            page = get_session().get(
                "https://fragment.com/stars/buy?quantity=50",
                timeout=20
            ).text
            val = BeautifulSoup(page, "html.parser").select_one(
                'input[value="50"] + .tm-form-radio-label .tm-value')
            base = float(val.text.replace(",", ".")) / 50 if val else None
        except Exception as e:
            logger.error(f"Fallback price scraping failed: {e}")
            base = 0.0046

    if base is None:
        return None
    return round(base * (1 + _fee() / 100), 6) if with_fee else round(base, 6)


def get_wallet_balance() -> float:
    try:
        r = get_session().get(
            f"https://tonapi.io/v2/accounts/{TON_WALLET_ADDRESS}",
            headers={"accept": "application/json"},
            timeout=15
        )
        r.raise_for_status()
        return int(r.json().get("balance", 0)) / 1e9
    except Exception as exc:
        logger.error(f"TON API balance error: {exc}")
        try:
            r = get_session().get(
                f"https://toncenter.com/api/v2/getAddressInformation",
                params={"address": TON_WALLET_ADDRESS},
                headers={"X-API-Key": os.getenv("TONCENTER_API_KEY", "")},
                timeout=15
            )
            data = r.json()
            if data.get("ok"):
                return int(data.get("result", {}).get("balance", 0)) / 1e9
        except Exception as e:
            logger.error(f"TonCenter API balance error: {e}")
        return 0.0


def generate_payment_code(user_id: int) -> str:
    timestamp = int(time.time() * 1000) % 1000000
    random_part = random.randint(100, 999)
    return f"{user_id % 10000:04d}{timestamp:06d}{random_part:03d}"

def create_tonkeeper_link(address: str, amount: float, comment: str) -> str:
    """Создает deeplink для Tonkeeper"""
    # Конвертируем в нанотоны (1 TON = 10^9 нанотонов)
    amount_nano = int(amount * 1_000_000_000)
    # Создаем ссылку для Tonkeeper
    ton_link = f"https://app.tonkeeper.com/transfer/{address}?amount={amount_nano}&text={urllib.parse.quote(str(comment))}"
    return ton_link


def decode_comment(comment: str) -> Optional[str]:
    if not comment:
        return None

    comment = str(comment).strip()

    if len(comment) % 4 == 0 and all(
            c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=' for c in comment):
        try:
            decoded = base64.b64decode(comment).decode('utf-8')
            if decoded and (decoded.isdigit() or any(c.isalnum() for c in decoded)):
                return decoded.strip()
        except Exception:
            pass

    return comment


def cleanup_expired_payments():
    current_time = time.time()
    expired_codes = []

    for code, (user_id, timestamp) in list(PENDING_PAYMENTS.items()):
        if current_time - timestamp > PAYMENT_TIMEOUT:
            expired_codes.append(code)
            if user_id in USER_PAYMENTS and USER_PAYMENTS[user_id] == code:
                del USER_PAYMENTS[user_id]

    for code in expired_codes:
        del PENDING_PAYMENTS[code]
        logger.debug(f"Removed expired payment code {code}")


def get_display_currency(user_id: int) -> tuple[str, str]:
    lang = get_user_lang(user_id)
    if lang == 'en':
        return 'USD', '$'
    else:
        return 'RUB', '₽'


def format_price_for_user(user_id: int, ton_amount: float) -> tuple[float, str]:
    usd_rate, rub_rate = ton_rates()
    lang = get_user_lang(user_id)

    if lang == 'en':
        return ton_amount * usd_rate, 'USD'
    else:
        return ton_amount * rub_rate, 'RUB'


bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())


def _bal_tuple():
    return get_wallet_balance(), db.get_internal()


admin.setup(
    dp, _bal_tuple, _fee, db.add_internal,
    ADMIN_ID, lambda: db._users(), db.update_user_stat, bot
)

casino.setup_casino(dp, db, bot)


def kb_main(user_id: int) -> types.InlineKeyboardMarkup:
    lang = get_user_lang(user_id)
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_buy_stars'), callback_data="buy")],
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_topup'), callback_data="topup")],
        [
            types.InlineKeyboardButton(text="🎰 Games" if lang == 'en' else "🎰 Игры", callback_data="casino"),
            types.InlineKeyboardButton(text=get_text(user_id, 'btn_balance'), callback_data="bal")
        ],
        [
            types.InlineKeyboardButton(text=get_text(user_id, 'btn_price'), callback_data="price"),
            types.InlineKeyboardButton(text=get_text(user_id, 'btn_language'), callback_data="language")
        ],
        [
            types.InlineKeyboardButton(text="ℹ️ Info" if lang == 'en' else "ℹ️ Инфо", callback_data="info"),
            types.InlineKeyboardButton(text="💰 Earn" if lang == 'en' else "💰 Заработать", callback_data="partner")
        ],
        [types.InlineKeyboardButton(text="📋 Tasks" if lang == 'en' else "📋 Задания", callback_data="tasks")],
    ])


def kb_back(user_id: int, lbl_key: str = "btn_back") -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=get_text(user_id, lbl_key), callback_data="menu")]
    ])

def kb_info(user_id: int) -> types.InlineKeyboardMarkup:
    support_text = "👨‍💻 Support" if get_user_lang(user_id) == 'en' else "👨‍💻 Поддержка"
    channel_text = "📢 Channel" if get_user_lang(user_id) == 'en' else "📢 Канал"
    chat_text = "💬 Chat" if get_user_lang(user_id) == 'en' else "💬 Чат"
    back_text = "⬅️ Back" if get_user_lang(user_id) == 'en' else "⬅️ Назад"
    
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=support_text, url="https://t.me/YOUR_SUPPORT_BOT")],
        [types.InlineKeyboardButton(text=channel_text, url="https://t.me/YOUR_CHANNEL")],
        [types.InlineKeyboardButton(text=chat_text, url="https://t.me/YOUR_CHAT")],
        [types.InlineKeyboardButton(text=back_text, callback_data="menu")]
    ])

def kb_topup(user_id: int) -> types.InlineKeyboardMarkup:
    buttons = [
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_ton'), callback_data="topup_ton")],
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_xrocket'), callback_data="topup_xrocket")]
    ]

    if CRYPTOPAY_ENABLED:
        buttons.append([
            types.InlineKeyboardButton(text=get_text(user_id, 'btn_cryptopay'), callback_data="topup_crypto"),
        ])

    buttons.append([types.InlineKeyboardButton(text=get_text(user_id, 'btn_back'), callback_data="menu")])

    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_tokens(user_id: int) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [
            types.InlineKeyboardButton(text="USDT", callback_data="token_USDT"),
            types.InlineKeyboardButton(text="TON", callback_data="token_TONCOIN"),
        ],
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_back'), callback_data="menu")],
    ])


def kb_buy_mode(user_id: int) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_self'), callback_data="buy_self")],
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_friend'), callback_data="buy_friend")],
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_back'), callback_data="menu")],
    ])


def kb_stars_amount(user_id: int, show_prices: bool = True) -> types.InlineKeyboardMarkup:
    buttons = []

    price_per_star = None
    if show_prices:
        price_per_star = price_one(with_fee=True)

    amounts = [100, 500, 1000, 5000, 10000]

    for amount in amounts:
        if show_prices and price_per_star:
            if get_user_lang(user_id) == 'en':
                usd_rate, _ = ton_rates()
                price = int(amount * price_per_star * usd_rate)
                text = get_text(user_id, f'btn_stars_{amount}_price', price=price)
            else:
                _, rub_rate = ton_rates()
                price = int(amount * price_per_star * rub_rate)
                text = get_text(user_id, f'btn_stars_{amount}_price', price=price)
        else:
            text = get_text(user_id, f'btn_stars_{amount}')

        buttons.append([types.InlineKeyboardButton(
            text=text,
            callback_data=f"stars_amount_{amount}"
        )])

    buttons.append([types.InlineKeyboardButton(
        text=get_text(user_id, 'btn_stars_custom'),
        callback_data="stars_custom"
    )])

    buttons.append([types.InlineKeyboardButton(
        text=get_text(user_id, 'btn_back'),
        callback_data="menu"
    )])

    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_crypto_currencies(user_id: int) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [
            types.InlineKeyboardButton(text="💵 USDT", callback_data="crypto_USDT"),
            types.InlineKeyboardButton(text="💎 TON", callback_data="crypto_TON"),
        ],
        [
            types.InlineKeyboardButton(text="₿ BTC", callback_data="crypto_BTC"),
            types.InlineKeyboardButton(text="Ξ ETH", callback_data="crypto_ETH"),
        ],
        [
            types.InlineKeyboardButton(text="💲 USDC", callback_data="crypto_USDC"),
            types.InlineKeyboardButton(text="🔶 BNB", callback_data="crypto_BNB"),
        ],
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_back'), callback_data="menu")],
    ])


async def ensure_user_registered(message_or_callback):
    user = None
    if hasattr(message_or_callback, 'from_user'):
        user = message_or_callback.from_user
    elif hasattr(message_or_callback, 'message') and hasattr(message_or_callback.message, 'from_user'):
        user = message_or_callback.message.from_user

    if not user:
        return

    user_id = user.id
    username = user.username or user.first_name or f"User_{user_id}"
    db.ensure_user(user_id, username)


class Buy(StatesGroup):
    mode = State()
    user = State()
    amount_selection = State()
    qty = State()
    confirming = State()


class TopUpXR(StatesGroup):
    token = State()
    usd = State()
    wait = State()


class TopUpTON(StatesGroup):
    waiting = State()
    amount = State()


class TopUpCrypto(StatesGroup):
    currency = State()
    amount = State()
    wait = State()


class PartnerWithdraw(StatesGroup):
    amount = State()
    wallet = State()
    confirm = State()


async def safe_edit(msg: types.Message, text: str,
                    reply_markup: types.InlineKeyboardMarkup | None = None):
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        error_msg = str(exc)
        if "message is not modified" in error_msg:
            logger.debug("Message not modified, skipping")
        elif "there is no text in the message to edit" in error_msg:
            try:
                await msg.delete()
            except:
                pass
            await msg.answer(text, reply_markup=reply_markup)
        elif "message can't be edited" in error_msg:
            await msg.answer(text, reply_markup=reply_markup)
        elif "message to edit not found" in error_msg:
            await msg.answer(text, reply_markup=reply_markup)
        else:
            logger.error(f"Failed to edit message: {error_msg}")
    except Exception as e:
        logger.error(f"Unexpected error in safe_edit: {e}")
        try:
            await msg.answer(text, reply_markup=reply_markup)
        except Exception:
            pass



# === MIDDLEWARE ДЛЯ БЛОКИРОВКИ ===
from aiogram import BaseMiddleware
from aiogram.dispatcher.event.bases import SkipHandler
from typing import Callable, Dict, Any, Awaitable
from aiogram.types import Update

# === MIDDLEWARE ДЛЯ БЛОКИРОВКИ ===
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable

class BlockCheckMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any]
    ) -> Any:
        user = None
        if hasattr(event, 'from_user'):
            user = event.from_user
        elif hasattr(event, 'message') and hasattr(event.message, 'from_user'):
            user = event.message.from_user
            
        if user and db.is_user_blocked(user.id):
            if hasattr(event, 'answer'):
                await event.answer(
                    "❌ Ваш аккаунт заблокирован.\n\n"
                    "Если вы считаете это ошибкой, обратитесь в поддержку."
                )
            elif hasattr(event, 'answer'):
                await event.answer("❌ Аккаунт заблокирован", show_alert=True)
            raise SkipHandler  # Прерываем обработку
            
        return await handler(event, data)

# Регистрируем middleware
dp.message.middleware(BlockCheckMiddleware())
dp.callback_query.middleware(BlockCheckMiddleware())
@dp.message(CommandStart())
async def cmd_start(m: types.Message, state: FSMContext):
    await ensure_user_registered(m)
    user_id = m.from_user.id
    
    # Парсим deep link для определения источника (из какого чата пришёл)
    source_chat_id = None
    if m.text and " " in m.text:
        args = m.text.split(" ", 1)[1]
        if args.startswith("from_chat_"):
            try:
                source_chat_id = int(args.replace("from_chat_", ""))
                logger.info(f"User {user_id} came from chat {source_chat_id}")
            except ValueError:
                pass
        elif args == "partner":
            # Пришёл из группы по кнопке "Заработать"
            await state.clear()
            # Создаём фейковый callback для вызова партнёрского меню
            await m.answer(get_text(user_id, "welcome"), reply_markup=kb_main(user_id))
            # Отправляем сообщение с партнёрским меню
            owner_chats = db.get_owner_chats(user_id)
            lang = get_user_lang(user_id)
            if owner_chats:
                level_info = db.get_owner_level(user_id)
                totals = db.get_owner_total_earnings(user_id)
                level = level_info["level"]
                progress = level_info["progress"]
                total_volume = level_info["total_volume"]
                remaining = level_info["remaining"]
                next_level = level_info["next_level"]
                if lang == "en":
                    text = f"💰 <b>Partner Dashboard</b>\n\n"
                    text += f"🏆 Level: <b>{level['name_en']}</b>\n"
                    text += f"🎰 Casino: <b>{level['spin_commission']}%</b>\n"
                    text += f"⭐ Stars: <b>{level['purchase_commission']}%</b>\n\n"
                    if next_level:
                        text += f"📈 Progress: {progress:.1f}%\n"
                    text += f"💵 Available: {totals['available']:.4f} TON"
                else:
                    text = f"💰 <b>Панель партнёра</b>\n\n"
                    text += f"🏆 Уровень: <b>{level['name']}</b>\n"
                    text += f"🎰 Казино: <b>{level['spin_commission']}%</b>\n"
                    text += f"⭐ Stars: <b>{level['purchase_commission']}%</b>\n\n"
                    if next_level:
                        text += f"📈 Прогресс: {progress:.1f}%\n"
                    text += f"💵 Доступно: {totals['available']:.4f} TON"
                kb = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="💸 Вывести" if lang != "en" else "💸 Withdraw", callback_data="partner_withdraw")],
                    [types.InlineKeyboardButton(text="📋 Мои чаты" if lang != "en" else "📋 My chats", callback_data="partner_chats")],
                    [types.InlineKeyboardButton(text="◀️ Меню" if lang != "en" else "◀️ Menu", callback_data="menu")]
                ])
                await m.answer(text, reply_markup=kb)
            else:
                bot_info = await bot.get_me()
                add_url = f"https://t.me/{bot_info.username}?startgroup=partner_{user_id}"
                text = "💰 <b>Партнёрская программа</b>\n\nДобавьте бота в свой чат чтобы начать зарабатывать!" if lang != "en" else "💰 <b>Partner Program</b>\n\nAdd bot to your chat to start earning!"
                kb = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="➕ Добавить в чат" if lang != "en" else "➕ Add to chat", url=add_url)],
                    [types.InlineKeyboardButton(text="◀️ Меню" if lang != "en" else "◀️ Menu", callback_data="menu")]
                ])
                await m.answer(text, reply_markup=kb)
            return

    
    await state.clear()
    
    # Сохраняем source_chat_id если пришёл из группы
    if source_chat_id:
        await state.update_data(source_chat_id=source_chat_id)
    
    photo_url = "https://example.com/your_logo.png"
    await m.answer_photo(photo_url, caption=get_text(user_id, "welcome"), reply_markup=kb_main(user_id))



# ============================================================
# CHAT PARTNER SYSTEM - Отслеживание добавления бота в чаты
# ============================================================

@dp.my_chat_member()
async def on_chat_member_update(event: ChatMemberUpdated):
    """
    Отслеживание добавления/удаления бота из чатов.
    Когда бота добавляют админом - регистрируем чат.
    """
    logger.info(f"my_chat_member event: chat={event.chat.id}, type={event.chat.type}, from={event.from_user.id}")
    # Проверяем что это групповой чат
    if event.chat.type not in ("group", "supergroup"):
        return
    
    old_status = event.old_chat_member.status if event.old_chat_member else None
    new_status = event.new_chat_member.status if event.new_chat_member else None
    
    # Бота добавили в чат (стал member или administrator)
    if new_status in ("member", "administrator") and old_status in (None, "left", "kicked"):
        owner_id = event.from_user.id  # Кто добавил бота
        chat_id = event.chat.id
        title = event.chat.title or ""
        
        # Регистрируем чат
        db.register_chat(chat_id, owner_id, title)
        
        # Отправляем приветствие в чат
        welcome_text = (
            f"🧪 <b>Wubba lubba dub dub!</b>\n\n"
            f"Йоу! Рик в чате!\n\n"
            f"Теперь тут можно:\n"
            f"⭐ Покупать звёзды дешевле Telegram\n"
            f"🎰 Крутить слоты и умножать TON\n"
            f"🎲 Играть в кости и другие игры\n\n"
            f"Не тупи как Джерри — жми /start"
        )
        photo_url = "https://example.com/your_logo.png"
        try:
            await bot.send_photo(chat_id, photo_url, caption=welcome_text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Could not send welcome to chat {chat_id}: {e}")
        
        logger.info(f"Bot added to chat: {chat_id} ({title}) by user {owner_id}")
        
        # Отправляем личное сообщение владельцу с панелью партнёра
        try:
            level_info = db.get_owner_level(owner_id)
            level = level_info["level"]
            
            partner_text = (
                f"🎉 <b>Чат успешно подключён!</b>\n\n"
                f"📋 Чат: <b>{title}</b>\n"
                f"🏆 Ваш уровень: <b>{level['name']}</b>\n"
                f"🎰 Комиссия казино: <b>{level['spin_commission']}%</b>\n"
                f"⭐ Комиссия Stars: <b>{level['purchase_commission']}%</b>\n\n"
                f"Теперь вы будете получать комиссию от активности участников!"
            )
            
            kb = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="💰 Панель партнёра", callback_data="partner")],
                [types.InlineKeyboardButton(text="📋 Условия", callback_data="partner_terms")]
            ])
            
            await bot.send_message(owner_id, partner_text, reply_markup=kb)
        except Exception as e:
            logger.warning(f"Could not send partner message to {owner_id}: {e}")

    
    # Бота удалили из чата
    elif new_status in ("left", "kicked") and old_status in ("member", "administrator"):
        chat_id = event.chat.id
        db.deactivate_chat(chat_id)
        logger.info(f"Bot removed from chat: {chat_id}")
        
        # Уведомляем владельца
        try:
            chat_info = db.get_chat(chat_id)
            if chat_info:
                owner_id = chat_info.get("owner_id")
                title = chat_info.get("title", "Чат")
                await bot.send_message(
                    owner_id,
                    f"⚠️ Бот был удалён из чата <b>{title}</b>\n\n"
                    f"Вы больше не будете получать комиссию от этого чата."
                )
        except Exception as e:
            logger.warning(f"Could not notify owner about removal: {e}")






@dp.message(Command("star"))
async def cmd_star(message: types.Message, state: FSMContext):
    """Покупка звёзд по команде"""
    await ensure_user_registered(message)
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    # Сохраняем source_chat_id если в группе
    if message.chat.type in ("group", "supergroup"):
        await state.update_data(source_chat_id=message.chat.id)
    
    await state.set_state(Buy.mode)
    
    lang = get_user_lang(user_id)
    if lang == "en":
        text = f"⭐ <b>Buy Stars</b>\n\n👤 @{username}"
    else:
        text = f"⭐ <b>Покупка Stars</b>\n\n👤 @{username}"
    
    await message.answer(text + "\n\n" + get_text(user_id, 'buy_mode_select'), reply_markup=kb_buy_mode(user_id), parse_mode="HTML")


@dp.message(Command("menu"))
async def cmd_menu(m: types.Message, state: FSMContext):
    """/menu - главное меню (работает и в группах)"""
    await ensure_user_registered(m)
    await state.clear()
    user_id = m.from_user.id
    chat_type = m.chat.type
    
    if chat_type in ("group", "supergroup"):
        # В группе - отправляем inline кнопку для перехода в личку
        chat_id = m.chat.id
        chat_info = db.get_chat(chat_id)
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(
                text="🚀 Открыть бота",
                url=f"https://t.me/{(await bot.get_me()).username}?start=from_chat_{chat_id}"
            )],
            [types.InlineKeyboardButton(
                text="🎰 Спины (в чате)",
                callback_data="casino"
            )]
        ])
        
        
        await m.answer(
            f"⭐ <b>RickStar Bot</b>\n\n"
            f"Покупка Stars и казино\n\n"
            f"Нажмите кнопку ниже:",
            reply_markup=kb
        )
    else:
        # В личке - обычное меню
        await m.answer(get_text(user_id, "main_menu"), reply_markup=kb_main(user_id))



# ============================================================
# PARTNER SYSTEM - Партнёрская система
# ============================================================

@dp.callback_query(F.data == "partner")
async def cb_partner(c: types.CallbackQuery):
    """Партнёрское меню"""
    await ensure_user_registered(c)
    await c.answer()
    
    # Если в группе - перенаправляем в личку
    if c.message and c.message.chat.type in ("group", "supergroup"):
        bot_info = await bot.get_me()
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(
                text="💰 Открыть партнёрку",
                url=f"https://t.me/{bot_info.username}?start=partner"
            )]
        ])
        try:
            await c.message.edit_text(
                "💰 Для доступа к партнёрской программе перейдите в бота:",
                reply_markup=kb
            )
        except:
            pass
        return

    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    # Проверяем есть ли у пользователя чаты
    owner_chats = db.get_owner_chats(user_id)
    
    if not owner_chats:
        # Нет чатов - показываем инструкцию
        bot_info = await bot.get_me()
        add_url = f"https://t.me/{bot_info.username}?startgroup=partner_{user_id}"
        
        if lang == "en":
            text = (
                "💰 <b>Partner Program</b>\n\n"
                "Earn up to <b>40%</b> from casino and up to <b>30%</b> from Stars purchases!\n\n"
                "<b>How it works:</b>\n"
                "1. Add the bot to your group/chat\n"
                "2. Your members play and buy Stars\n"
                "3. You earn commission from their activity!\n\n"
                "<b>📊 Levels:</b>\n"
                "🥉 Bronze: 15% casino, 10% Stars\n"
                "🥈 Silver (1000 TON): 25% casino, 20% Stars\n"
                "🥇 Gold (10000 TON): 40% casino, 30% Stars\n\n"
                "Click the button below to add the bot to your chat:"
            )
        else:
            text = (
                "💰 <b>Партнёрская программа</b>\n\n"
                "Зарабатывайте до <b>40%</b> от казино и до <b>30%</b> от покупок Stars!\n\n"
                "<b>Как это работает:</b>\n"
                "1. Добавьте бота в свой чат/группу\n"
                "2. Участники играют и покупают Stars\n"
                "3. Вы получаете комиссию от их активности!\n\n"
                "<b>📊 Уровни:</b>\n"
                "🥉 Бронза: 15% казино, 10% Stars\n"
                "🥈 Серебро (1000 TON): 25% казино, 20% Stars\n"
                "🥇 Золото (10000 TON): 40% казино, 30% Stars\n\n"
                "Нажмите кнопку ниже чтобы добавить бота в чат:"
            )
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(
                text="➕ Add to chat" if lang == "en" else "➕ Добавить в чат",
                url=add_url
            )],
            [types.InlineKeyboardButton(
                text="📋 Terms" if lang == "en" else "📋 Условия",
                callback_data="partner_terms"
            )],
            [types.InlineKeyboardButton(
                text="◀️ Back" if lang == "en" else "◀️ Назад",
                callback_data="menu"
            )]
        ])
        
        await safe_edit(c.message, text, kb)
    else:
        # Есть чаты - показываем панель управления
        level_info = db.get_owner_level(user_id)
        totals = db.get_owner_total_earnings(user_id)
        
        level = level_info["level"]
        progress = level_info["progress"]
        total_volume = level_info["total_volume"]
        remaining = level_info["remaining"]
        next_level = level_info["next_level"]
        
        if lang == "en":
            text = f"💰 <b>Partner Dashboard</b>\n\n"
            text += f"🏆 Level: <b>{level['name_en']}</b>\n"
            text += f"🎰 Casino commission: <b>{level['spin_commission']}%</b>\n"
            text += f"⭐ Stars commission: <b>{level['purchase_commission']}%</b>\n\n"
            if next_level:
                text += f"📈 Progress to {next_level['name_en']}: {progress:.2f}% ({total_volume:.2f}/{next_level['min_volume']} TON)\n"
                text += f"📊 Remaining: {remaining:.2f} TON\n\n"
            text += f"💵 <b>Earnings:</b>\n"
            text += f"Total: {totals['total']:.4f} TON\n"
            text += f"Available: {totals['available']:.4f} TON\n\n"
            text += f"📋 Your chats: {len(owner_chats)}"
        else:
            text = f"💰 <b>Панель партнёра</b>\n\n"
            text += f"🏆 Уровень: <b>{level['name']}</b>\n"
            text += f"🎰 Комиссия казино: <b>{level['spin_commission']}%</b>\n"
            text += f"⭐ Комиссия Stars: <b>{level['purchase_commission']}%</b>\n\n"
            if next_level:
                text += f"📈 Прогресс до {next_level['name']}: {progress:.2f}% ({total_volume:.2f}/{next_level['min_volume']} TON)\n"
                text += f"📊 Осталось: {remaining:.2f} TON\n\n"
            text += f"💵 <b>Заработок:</b>\n"
            text += f"Всего: {totals['total']:.4f} TON\n"
            text += f"Доступно: {totals['available']:.4f} TON\n\n"
            text += f"📋 Ваших чатов: {len(owner_chats)}"
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(
                text="💸 Withdraw" if lang == "en" else "💸 Вывести",
                callback_data="partner_withdraw"
            )],
            [types.InlineKeyboardButton(
                text="📋 My chats" if lang == "en" else "📋 Мои чаты",
                callback_data="partner_chats"
            )],
            [types.InlineKeyboardButton(
                text="📋 Terms" if lang == "en" else "📋 Условия",
                callback_data="partner_terms"
            )],
            [types.InlineKeyboardButton(
                text="◀️ Back" if lang == "en" else "◀️ Назад",
                callback_data="menu"
            )]
        ])
        
        await safe_edit(c.message, text, kb)


@dp.callback_query(F.data == "partner_terms")
async def cb_partner_terms(c: types.CallbackQuery):
    """Условия партнёрской программы"""
    await c.answer()
    lang = get_user_lang(c.from_user.id)
    
    if lang == "en":
        text = (
            "📋 <b>Partner Program Terms</b>\n\n"
            "<b>Levels and commissions:</b>\n\n"
            "🥉 <b>Bronze</b> (start)\n"
            "• Casino: 15% of NGR\n"
            "• Stars: 10% of markup\n\n"
            "🥈 <b>Silver</b> (volume 1,000 TON)\n"
            "• Casino: 25% of NGR\n"
            "• Stars: 20% of markup\n\n"
            "🥇 <b>Gold</b> (volume 10,000 TON)\n"
            "• Casino: 40% of NGR\n"
            "• Stars: 30% of markup\n\n"
            "<b>💡 How NGR works:</b>\n"
            "NGR (Net Gaming Revenue) = player losses - winnings\n\n"
            "You earn commission only from the <b>net loss</b> of each player. "
            "If a player wins, their NGR decreases and you receive nothing until "
            "they lose more than they have won.\n\n"
            "<b>Example:</b>\n"
            "• Player bets 10 TON, loses → NGR = 10, you get 15% = 1.5 TON\n"
            "• Player bets 5 TON, wins 15 TON → NGR = 0, you get 0\n"
            "• Player bets 20 TON, loses → NGR = 15, you get 15% of 15 = 2.25 TON\n\n"
            "<b>Rules:</b>\n"
            "• Volume = sum of all bets and purchases\n"
            "• Volume is summed across all your chats\n"
            "• Minimum withdrawal: 0.5 TON\n"
            "• Withdrawal to your TON wallet"
        )
    else:
        text = (
            "📋 <b>Условия партнёрской программы</b>\n\n"
            "<b>Уровни и комиссии:</b>\n\n"
            "🥉 <b>Бронза</b> (старт)\n"
            "• Казино: 15% от NGR\n"
            "• Stars: 10% от наценки\n\n"
            "🥈 <b>Серебро</b> (объём 1,000 TON)\n"
            "• Казино: 25% от NGR\n"
            "• Stars: 20% от наценки\n\n"
            "🥇 <b>Золото</b> (объём 10,000 TON)\n"
            "• Казино: 40% от NGR\n"
            "• Stars: 30% от наценки\n\n"
            "<b>💡 Как работает NGR:</b>\n"
            "NGR (Net Gaming Revenue) = проигрыши игрока - выигрыши\n\n"
            "Вы получаете комиссию только с <b>чистого проигрыша</b> каждого игрока. "
            "Если игрок выигрывает, его NGR уменьшается и вы ничего не получаете, "
            "пока он не проиграет больше, чем выиграл.\n\n"
            "<b>Пример:</b>\n"
            "• Игрок ставит 10 TON, проигрывает → NGR = 10, вы получаете 15% = 1.5 TON\n"
            "• Игрок ставит 5 TON, выигрывает 15 TON → NGR = 0, вы получаете 0\n"
            "• Игрок ставит 20 TON, проигрывает → NGR = 15, вы получаете 15% от 15 = 2.25 TON\n\n"
            "<b>Правила:</b>\n"
            "• Объём = сумма всех ставок и покупок\n"
            "• Объём суммируется по всем вашим чатам\n"
            "• Минимальный вывод: 0.5 TON\n"
            "• Вывод на ваш TON кошелёк"
        )
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text="◀️ Back" if lang == "en" else "◀️ Назад",
            callback_data="partner"
        )]
    ])
    
    await safe_edit(c.message, text, kb)



@dp.callback_query(F.data == "partner_chats")
async def cb_partner_chats(c: types.CallbackQuery):
    """Список чатов партнёра"""
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    owner_chats = db.get_owner_chats(user_id)
    bot_info = await bot.get_me()
    add_url = f"https://t.me/{bot_info.username}?startgroup=partner_{user_id}"
    
    if lang == "en":
        text = "📋 <b>Your chats</b>\n\nSelect chat to manage:"
    else:
        text = "📋 <b>Ваши чаты</b>\n\nВыберите чат для управления:"
    
    buttons = []
    for chat in owner_chats:
        status = "🟢" if chat.get("is_active") else "🔴"
        title = chat.get("title", "Без имени")[:20]
        chat_id = chat.get("id")
        buttons.append([types.InlineKeyboardButton(
            text=f"{status} {title}",
            callback_data=f"partner_chat_{chat_id}"
        )])
    
    buttons.append([types.InlineKeyboardButton(
        text="➕ Добавить чат" if lang != "en" else "➕ Add chat",
        url=add_url
    )])
    buttons.append([types.InlineKeyboardButton(
        text="◀️ Назад" if lang != "en" else "◀️ Back",
        callback_data="partner"
    )])
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    await safe_edit(c.message, text, kb)


@dp.callback_query(F.data.startswith("partner_chat_") & ~F.data.startswith("partner_chat_del") & ~F.data.startswith("partner_chat_confirm"))
async def cb_partner_chat_detail(c: types.CallbackQuery):
    """Детали чата партнёра"""
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    chat_id = int(c.data.replace("partner_chat_", ""))
    chat = db.get_chat(chat_id)
    
    if not chat or chat.get("owner_id") != user_id:
        await c.message.edit_text("❌ Чат не найден")
        return
    
    status = "🟢 Активен" if chat.get("is_active") else "🔴 Неактивен"
    title = chat.get("title", "Без имени")
    earned = chat.get("total_earnings", 0)
    volume = chat.get("total_volume", 0)
    
    if lang == "en":
        text = f"📋 <b>Chat: {title}</b>\n\n"
        text += f"Status: {status}\n"
        text += f"💰 Earned: {earned:.4f} TON\n"
        text += f"📊 Volume: {volume:.2f} TON"
    else:
        text = f"📋 <b>Чат: {title}</b>\n\n"
        text += f"Статус: {status}\n"
        text += f"💰 Заработано: {earned:.4f} TON\n"
        text += f"📊 Объём: {volume:.2f} TON"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text="🗑 Удалить" if lang != "en" else "🗑 Delete",
            callback_data=f"partner_chat_del_{chat_id}"
        )],
        [types.InlineKeyboardButton(
            text="◀️ Назад" if lang != "en" else "◀️ Back",
            callback_data="partner_chats"
        )]
    ])
    await safe_edit(c.message, text, kb)


@dp.callback_query(F.data.startswith("partner_chat_del_"))
async def cb_partner_chat_delete(c: types.CallbackQuery):
    """Подтверждение удаления чата"""
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    chat_id = int(c.data.replace("partner_chat_del_", ""))
    chat = db.get_chat(chat_id)
    
    if not chat or chat.get("owner_id") != user_id:
        await c.message.edit_text("❌ Чат не найден")
        return
    
    title = chat.get("title", "Без имени")
    
    if lang == "en":
        text = f"⚠️ <b>Confirm deletion</b>\n\nChat: {title}\n\nAre you sure?"
    else:
        text = f"⚠️ <b>Подтверждение удаления</b>\n\nЧат: {title}\n\nВы уверены?"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text="✅ Да, удалить" if lang != "en" else "✅ Yes, delete",
            callback_data=f"partner_chat_confirm_del_{chat_id}"
        )],
        [types.InlineKeyboardButton(
            text="◀️ Отмена" if lang != "en" else "◀️ Cancel",
            callback_data=f"partner_chat_{chat_id}"
        )]
    ])
    await safe_edit(c.message, text, kb)


@dp.callback_query(F.data.startswith("partner_chat_confirm_del_"))
async def cb_partner_chat_confirm_delete(c: types.CallbackQuery):
    """Удаление чата"""
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    chat_id = int(c.data.replace("partner_chat_confirm_del_", ""))
    chat = db.get_chat(chat_id)
    
    if not chat or chat.get("owner_id") != user_id:
        await c.message.edit_text("❌ Чат не найден")
        return
    
    db.remove_chat(chat_id)
    
    if lang == "en":
        text = "✅ Chat removed from partner program"
    else:
        text = "✅ Чат удалён из партнёрской программы"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text="📋 Мои чаты" if lang != "en" else "📋 My chats",
            callback_data="partner_chats"
        )],
        [types.InlineKeyboardButton(
            text="◀️ Партнёрка" if lang != "en" else "◀️ Partner",
            callback_data="partner"
        )]
    ])
    await safe_edit(c.message, text, kb)

@dp.callback_query(F.data == "partner_withdraw")
async def cb_partner_withdraw(c: types.CallbackQuery, state: FSMContext):
    """Вывод партнёрского заработка - выбор способа"""
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    totals = db.get_owner_total_earnings(user_id)
    available = totals["available"]
    
    if available < 0.1:
        if lang == "en":
            text = f"💸 <b>Withdrawal</b>\n\nAvailable: {available:.4f} TON\n\nMinimum: 0.1 TON"
        else:
            text = f"💸 <b>Вывод средств</b>\n\nДоступно: {available:.4f} TON\n\nМинимум: 0.1 TON"
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="◀️ Назад", callback_data="partner")]
        ])
        await safe_edit(c.message, text, kb)
        return
    
    # Показываем выбор способа вывода
    if lang == "en":
        text = f"💸 <b>Withdrawal</b>\n\nAvailable: <b>{available:.4f} TON</b>\n\nSelect withdrawal method:"
        balance_btn = "💰 To bot balance"
        wallet_btn = "👛 To wallet (soon)"
    else:
        text = f"💸 <b>Вывод средств</b>\n\nДоступно: <b>{available:.4f} TON</b>\n\nВыберите способ вывода:"
        balance_btn = "💰 На баланс бота"
        wallet_btn = "👛 На кошелёк (скоро)"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=balance_btn, callback_data="withdraw_to_balance")],
        [types.InlineKeyboardButton(text=wallet_btn, callback_data="withdraw_to_wallet_soon")],
        [types.InlineKeyboardButton(text="◀️ Назад", callback_data="partner")]
    ])
    
    await state.update_data(available=available)
    await safe_edit(c.message, text, kb)


@dp.callback_query(F.data == "withdraw_to_wallet_soon")
async def cb_withdraw_wallet_soon(c: types.CallbackQuery):
    """Вывод на кошелёк - скоро"""
    lang = get_user_lang(c.from_user.id)
    if lang == "en":
        await c.answer("🔜 Coming soon!", show_alert=True)
    else:
        await c.answer("🔜 Скоро!", show_alert=True)


@dp.callback_query(F.data == "withdraw_to_balance")
async def cb_withdraw_to_balance(c: types.CallbackQuery, state: FSMContext):
    """Вывод на баланс бота"""
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    data = await state.get_data()
    available = data.get("available", 0)
    
    if available < 0.1:
        if lang == "en":
            text = "❌ Minimum withdrawal: 0.1 TON"
        else:
            text = "❌ Минимум для вывода: 0.1 TON"
        await c.answer(text, show_alert=True)
        return
    
    if lang == "en":
        text = f"💰 <b>Withdraw to balance</b>\n\nAvailable: <b>{available:.4f} TON</b>\n\nEnter amount (min 0.1 TON):"
    else:
        text = f"💰 <b>Вывод на баланс</b>\n\nДоступно: <b>{available:.4f} TON</b>\n\nВведите сумму (мин. 0.1 TON):"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="◀️ Отмена", callback_data="partner_withdraw")]
    ])
    
    await state.set_state(PartnerWithdraw.amount)
    await state.update_data(withdraw_type="balance")
    await safe_edit(c.message, text, kb)


@dp.message(PartnerWithdraw.amount)
async def partner_withdraw_amount(m: types.Message, state: FSMContext):
    """Получение суммы вывода"""
    user_id = m.from_user.id
    lang = get_user_lang(user_id)
    data = await state.get_data()
    available = data.get("available", 0)
    withdraw_type = data.get("withdraw_type", "wallet")
    
    try:
        amount = float(m.text.replace(",", "."))
    except:
        await m.answer("❌ Введите корректную сумму" if lang != "en" else "❌ Enter valid amount")
        return
    
    min_amount = 0.1 if withdraw_type == "balance" else 0.5
    if amount < min_amount:
        await m.answer(f"❌ Минимум {min_amount} TON" if lang != "en" else f"❌ Minimum {min_amount} TON")
        return
    
    if amount > available:
        await m.answer(f"❌ Недостаточно средств. Доступно: {available:.4f} TON" if lang != "en" else f"❌ Insufficient funds. Available: {available:.4f} TON")
        return
    
    # Вывод на баланс - сразу зачисляем
    if withdraw_type == "balance":
        # Зачисляем на баланс
        db.atomic_balance_change(user_id, amount)
        # Списываем с партнёрского заработка
        db.record_partner_withdrawal_to_balance(user_id, amount)
        
        await state.clear()
        
        new_balance = db.get_user_balance(user_id)
        if lang == "en":
            text = f"✅ <b>Success!</b>\n\n{amount:.4f} TON transferred to your bot balance.\n\n💰 New balance: <b>{new_balance:.4f} TON</b>"
        else:
            text = f"✅ <b>Успешно!</b>\n\n{amount:.4f} TON переведено на ваш баланс бота.\n\n💰 Новый баланс: <b>{new_balance:.4f} TON</b>"
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="◀️ Назад", callback_data="partner")]
        ])
        await m.answer(text, reply_markup=kb, parse_mode="HTML")
        return
    
    # Вывод на кошелёк - запрашиваем адрес
    await state.update_data(amount=amount)
    await state.set_state(PartnerWithdraw.wallet)
    
    if lang == "en":
        text = f"💸 Amount: <b>{amount:.2f} TON</b>\n\nEnter your TON wallet address:"
    else:
        text = f"💸 Сумма: <b>{amount:.2f} TON</b>\n\nВведите адрес вашего TON кошелька:"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="◀️ Отмена", callback_data="partner")]
    ])
    await m.answer(text, reply_markup=kb)


@dp.message(PartnerWithdraw.wallet)
async def partner_withdraw_wallet(m: types.Message, state: FSMContext):
    """Получение адреса кошелька"""
    user_id = m.from_user.id
    lang = get_user_lang(user_id)
    wallet = m.text.strip()
    
    # Простая валидация TON адреса
    # Валидация TON адреса
    if len(wallet) < 48:
        await m.answer("❌ Адрес слишком короткий. TON адрес должен содержать 48 символов" if lang != "en" else "❌ Address too short. TON address should be 48 characters")
        return
    if not (wallet.startswith("EQ") or wallet.startswith("UQ")):
        await m.answer("❌ Некорректный TON адрес" if lang != "en" else "❌ Invalid TON address")
        return
    
    data = await state.get_data()
    amount = data.get("amount")
    
    await state.update_data(wallet=wallet)
    await state.set_state(PartnerWithdraw.confirm)
    
    if lang == "en":
        text = f"💸 <b>Confirm withdrawal</b>\n\nAmount: <b>{amount:.2f} TON</b>\nWallet: <code>{wallet}</code>\n\nConfirm?"
    else:
        text = f"💸 <b>Подтверждение вывода</b>\n\nСумма: <b>{amount:.2f} TON</b>\nКошелёк: <code>{wallet}</code>\n\nПодтвердить?"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✅ Подтвердить", callback_data="partner_withdraw_confirm")],
        [types.InlineKeyboardButton(text="◀️ Отмена", callback_data="partner")]
    ])
    await m.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "partner_withdraw_confirm")
async def cb_partner_withdraw_confirm(c: types.CallbackQuery, state: FSMContext):
    """Подтверждение запроса на вывод"""
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    data = await state.get_data()
    amount = data.get("amount")
    wallet = data.get("wallet")
    
    if not amount or not wallet:
        await c.message.edit_text("❌ Ошибка. Попробуйте снова.")
        await state.clear()
        return
    
    # Создаём запрос на вывод
    request = db.create_withdrawal_request(user_id, amount, wallet)
    
    await state.clear()
    
    if lang == "en":
        text = f"✅ <b>Request created!</b>\n\nAmount: {amount:.2f} TON\nWallet: <code>{wallet}</code>\n\nRequest ID: <code>{request['id']}</code>\n\nYour request will be processed within 24 hours."
    else:
        text = f"✅ <b>Запрос создан!</b>\n\nСумма: {amount:.2f} TON\nКошелёк: <code>{wallet}</code>\n\nID запроса: <code>{request['id']}</code>\n\nВаш запрос будет обработан в течение 24 часов."
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="💰 Партнёрка", callback_data="partner")],
        [types.InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
    ])
    await c.message.edit_text(text, reply_markup=kb)



@dp.callback_query(F.data == "menu")
async def cb_menu(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    try:
        await c.answer()
    except Exception:
        pass
    await state.clear()
    user_id = c.from_user.id
    await safe_edit(c.message, get_text(user_id, 'main_menu'), kb_main(user_id))


@dp.callback_query(F.data == "language")
async def cb_language(c: types.CallbackQuery):
    await ensure_user_registered(c)
    try:
        await c.answer()
    except Exception:
        pass

    user_id = c.from_user.id
    await safe_edit(
        c.message,
        get_text(user_id, 'choose_language'),
        get_language_keyboard()
    )


@dp.callback_query(F.data.startswith("set_lang_"))
async def cb_set_language(c: types.CallbackQuery):
    await ensure_user_registered(c)
    try:
        await c.answer()
    except Exception:
        pass

    lang = c.data.split("_", 2)[2]
    user_id = c.from_user.id

    set_user_lang(user_id, lang)

    await safe_edit(
        c.message,
        get_text(user_id, 'language_changed'),
        kb_main(user_id)
    )


@dp.callback_query(F.data == "bal")
async def cb_bal(c: types.CallbackQuery):
    await ensure_user_registered(c)
    try:
        await c.answer()
    except Exception:
        pass

    usd_rate, rub_rate = ton_rates()
    user_id = c.from_user.id

    bal = db.get_user_balance(user_id)

    currency, symbol = get_display_currency(user_id)
    if currency == 'USD':
        currency_amount = bal * usd_rate
    else:
        currency_amount = bal * rub_rate

    txt = get_text(user_id, 'balance_info',
                   ton=bal,
                   currency_amount=currency_amount,
                   currency_symbol=symbol)

    await safe_edit(c.message, txt, kb_main(user_id))


@dp.callback_query(F.data == "price")
async def cb_price(c: types.CallbackQuery):
    await ensure_user_registered(c)
    await c.answer()
    user_id = c.from_user.id

    price = price_one(with_fee=True)
    if not price:
        txt = get_text(user_id, 'price_error')
    else:
        usd_rate, rub_rate = ton_rates()

        currency, _ = get_display_currency(user_id)
        if currency == 'USD':
            rate = usd_rate
        else:
            rate = rub_rate

        txt = get_text(user_id, 'price_info',
                       price=price,
                       rate=rate,
                       currency=currency)

    await safe_edit(c.message, txt, kb_main(user_id))


# === ЗАДАНИЯ ===
@dp.callback_query(F.data == "tasks")
async def cb_tasks(c: types.CallbackQuery):
    await ensure_user_registered(c)
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    # Получаем заработанные звёзды из БД
    earned_stars = db.get_task_stars(user_id)
    
    if lang == 'en':
        text = (
            "📋 <b>Tasks</b>\n\n"
            "🔗 <b>Profile Link Task</b>\n\n"
            "Add one of these phrases to your bio:\n\n"
            "<code>I buy stars here: t.me/YOUR_BOT</code>\n\n"
            "or\n\n"
            "<code>Stars cheaper than Telegram: t.me/YOUR_BOT</code>\n\n"
            "⚠️ <b>Important:</b> Settings → Privacy → Bio → <b>Everyone</b>\n\n"
            "💰 <b>Reward:</b>\n"
            "• 1 day = 3 ⭐\n"
            "• 100 days = 300 ⭐ passive income!\n\n"
            f"⭐ <b>Earned: {earned_stars} Stars</b>"
        )
        withdraw_text = "💸 Withdraw"
        check_text = "✅ Check profile"
        back_text = "⬅️ Back"
    else:
        text = (
            "📋 <b>Задания</b>\n\n"
            "🔗 <b>Задание: Ссылка в профиле</b>\n\n"
            "Добавь в раздел «О себе» одну из фраз:\n\n"
            "<code>Я покупаю звезды здесь: t.me/YOUR_BOT</code>\n\n"
            "или\n\n"
            "<code>Звезды дешевле чем в Telegram: t.me/YOUR_BOT</code>\n\n"
            "⚠️ <b>Важно:</b> Настройки → Конфиденциальность → О себе → <b>Все</b>\n\n"
            "💰 <b>Награда:</b>\n"
            "• 1 день = 3 ⭐\n"
            "• 100 дней = 300 ⭐ пассивного дохода!\n\n"
            f"⭐ <b>Заработано: {earned_stars} звёзд</b>"
        )
        withdraw_text = "💸 Вывести"
        check_text = "✅ Проверить профиль"
        back_text = "⬅️ Назад"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=check_text, callback_data="check_profile_task")],
        [types.InlineKeyboardButton(text=withdraw_text, callback_data="withdraw_task_stars")],
        [types.InlineKeyboardButton(text=back_text, callback_data="menu")]
    ])
    
    await safe_edit(c.message, text, kb)


@dp.callback_query(F.data == "check_profile_task")
async def cb_check_profile(c: types.CallbackQuery):
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    # Проверяем профиль через getChat (работает если пользователь взаимодействовал с ботом)
    try:
        chat = await bot.get_chat(user_id)
        bio = chat.bio or ""
        logger.info(f"User {user_id} bio check: '{bio}'")
        
        # Проверяем наличие нужной фразы + ссылки
        bio_lower = bio.lower()
        has_phrase = any(phrase in bio_lower for phrase in [
            "покупаю звезды", "buy stars", 
            "дешевле чем в telegram", "cheaper than telegram",
            "звезды дешевле", "stars cheaper"
        ])
        has_link = "your_bot" in bio_lower or "t.me/your_bot" in bio_lower
        has_link = has_phrase and has_link  # Нужна И фраза И ссылка
        
        if has_link:
            # Ссылка есть - начисляем если ещё не начисляли сегодня
            already_claimed = db.check_daily_task_claimed(user_id)
            if already_claimed:
                if lang == 'en':
                    await c.answer("✅ Already claimed today! Come back tomorrow.", show_alert=True)
                else:
                    await c.answer("✅ Уже получено сегодня! Приходи завтра.", show_alert=True)
            else:
                db.add_task_stars(user_id, 3)
                db.set_daily_task_claimed(user_id)
                if lang == 'en':
                    await c.answer("🎉 +3 Stars! Link found in your bio!", show_alert=True)
                else:
                    await c.answer("🎉 +3 звёзды! Ссылка найдена в профиле!", show_alert=True)
                # Обновляем экран
                await cb_tasks(c)
        else:
            if lang == 'en':
                await c.answer("❌ Phrase not found. Add: I buy stars here: t.me/YOUR_BOT", show_alert=True)
            else:
                await c.answer("❌ Фраза не найдена. Добавь: Я покупаю звезды здесь: t.me/YOUR_BOT", show_alert=True)
    except Exception as e:
        logger.error(f"Error checking profile: {e}")
        if lang == 'en':
            await c.answer("❌ Could not check profile. Try again later.", show_alert=True)
        else:
            await c.answer("❌ Не удалось проверить профиль. Попробуй позже.", show_alert=True)


# === FSM для вывода звёзд заданий ===
class WithdrawTasks(StatesGroup):
    mode = State()           # Выбор себе/другу
    entering_username = State()  # Ввод username друга
    amount_selection = State()   # Выбор количества
    confirming = State()     # Подтверждение


@dp.callback_query(F.data == "withdraw_task_stars")
async def cb_withdraw_task_stars(c: types.CallbackQuery, state: FSMContext):
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    earned_stars = db.get_task_stars(user_id)
    
    if earned_stars < 100:
        if lang == 'en':
            await c.answer(f"❌ Minimum 100 stars to withdraw. You have {earned_stars}.", show_alert=True)
        else:
            await c.answer(f"❌ Минимум 100 звёзд для вывода. У тебя {earned_stars}.", show_alert=True)
        return
    
    await c.answer()
    
    await state.set_state(WithdrawTasks.mode)
    
    if lang == 'en':
        text = f"💸 <b>Withdraw Stars</b>\n\nYou have: ⭐ <b>{earned_stars} Stars</b>\n\nWho should receive the stars?"
        self_btn = "👤 Myself"
        friend_btn = "👥 Friend"
    else:
        text = f"💸 <b>Вывод звёзд</b>\n\nУ тебя: ⭐ <b>{earned_stars} звёзд</b>\n\nКому отправить звёзды?"
        self_btn = "👤 Себе"
        friend_btn = "👥 Другу"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=self_btn, callback_data="withdraw_self")],
        [types.InlineKeyboardButton(text=friend_btn, callback_data="withdraw_friend")],
        [types.InlineKeyboardButton(text="⬅️ Back" if lang == 'en' else "⬅️ Назад", callback_data="tasks")]
    ])
    
    await safe_edit(c.message, text, kb)


@dp.callback_query(WithdrawTasks.mode, F.data == "withdraw_self")
async def cb_withdraw_self(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    user_id = c.from_user.id
    username = c.from_user.username
    
    if not username:
        lang = get_user_lang(user_id)
        if lang == 'en':
            await c.answer("❌ You need a username to receive stars", show_alert=True)
        else:
            await c.answer("❌ Для получения звёзд нужен username", show_alert=True)
        return
    
    await state.update_data(recipient_username=username)
    await show_withdraw_amounts(c, state, user_id)


@dp.callback_query(WithdrawTasks.mode, F.data == "withdraw_friend")
async def cb_withdraw_friend(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    await state.set_state(WithdrawTasks.entering_username)
    
    if lang == 'en':
        text = "👥 Enter friend's username (without @):"
    else:
        text = "👥 Введи username друга (без @):"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Back" if lang == 'en' else "⬅️ Назад", callback_data="withdraw_task_stars")]
    ])
    
    await safe_edit(c.message, text, kb)


@dp.message(WithdrawTasks.entering_username)
async def msg_withdraw_username(m: types.Message, state: FSMContext):
    user_id = m.from_user.id
    username = m.text.strip().lstrip("@")
    
    if not username or len(username) < 3:
        lang = get_user_lang(user_id)
        if lang == 'en':
            await m.answer("❌ Invalid username")
        else:
            await m.answer("❌ Неверный username")
        return
    
    await state.update_data(recipient_username=username)
    
    # Создаём фейковый callback для переиспользования логики
    class FakeCallback:
        def __init__(self, message, from_user):
            self.message = message
            self.from_user = from_user
        async def answer(self, *args, **kwargs):
            pass
    
    fake_cb = FakeCallback(m, m.from_user)
    await show_withdraw_amounts(fake_cb, state, user_id, is_message=True)


async def show_withdraw_amounts(c, state: FSMContext, user_id: int, is_message: bool = False):
    lang = get_user_lang(user_id)
    earned_stars = db.get_task_stars(user_id)
    data = await state.get_data()
    recipient = data.get("recipient_username")
    
    await state.set_state(WithdrawTasks.amount_selection)
    
    # Доступные варианты (кратные 50, не больше баланса)
    amounts = [50, 100, 150, 200, 300, 500]
    available = [a for a in amounts if a <= earned_stars]
    
    if lang == 'en':
        text = f"💸 <b>Select amount</b>\n\nAvailable: ⭐ {earned_stars}\nRecipient: @{recipient}"
    else:
        text = f"💸 <b>Выбери количество</b>\n\nДоступно: ⭐ {earned_stars}\nПолучатель: @{recipient}"
    
    buttons = []
    for i in range(0, len(available), 2):
        row = [types.InlineKeyboardButton(text=f"⭐ {available[i]}", callback_data=f"withdraw_amount_{available[i]}")]
        if i + 1 < len(available):
            row.append(types.InlineKeyboardButton(text=f"⭐ {available[i+1]}", callback_data=f"withdraw_amount_{available[i+1]}"))
        buttons.append(row)
    
    buttons.append([types.InlineKeyboardButton(text="⬅️ Back" if lang == 'en' else "⬅️ Назад", callback_data="withdraw_task_stars")])
    kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    
    if is_message:
        await c.message.answer(text, reply_markup=kb, parse_mode="HTML")
    else:
        await safe_edit(c.message, text, kb)


@dp.callback_query(WithdrawTasks.amount_selection, F.data.startswith("withdraw_amount_"))
async def cb_withdraw_amount(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    amount = int(c.data.split("_")[2])
    earned_stars = db.get_task_stars(user_id)
    
    if amount > earned_stars:
        if lang == 'en':
            await c.answer("❌ Not enough stars", show_alert=True)
        else:
            await c.answer("❌ Недостаточно звёзд", show_alert=True)
        return
    
    await state.update_data(withdraw_amount=amount)
    await state.set_state(WithdrawTasks.confirming)
    
    data = await state.get_data()
    recipient = data.get("recipient_username")
    
    if lang == 'en':
        text = f"✅ <b>Confirm withdrawal</b>\n\n⭐ Amount: <b>{amount} Stars</b>\n👤 Recipient: @{recipient}\n\nConfirm?"
        confirm_btn = "✅ Confirm"
    else:
        text = f"✅ <b>Подтверди вывод</b>\n\n⭐ Количество: <b>{amount} звёзд</b>\n👤 Получатель: @{recipient}\n\nПодтвердить?"
        confirm_btn = "✅ Подтвердить"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=confirm_btn, callback_data="withdraw_confirm")],
        [types.InlineKeyboardButton(text="⬅️ Back" if lang == 'en' else "⬅️ Назад", callback_data="withdraw_task_stars")]
    ])
    
    await safe_edit(c.message, text, kb)


@dp.callback_query(WithdrawTasks.confirming, F.data == "withdraw_confirm")
async def cb_withdraw_confirm(c: types.CallbackQuery, state: FSMContext):
    user_id = c.from_user.id
    lang = get_user_lang(user_id)
    
    data = await state.get_data()
    amount = data.get("withdraw_amount")
    recipient = data.get("recipient_username")
    
    # Проверяем баланс ещё раз
    earned_stars = db.get_task_stars(user_id)
    if amount > earned_stars:
        await c.answer("❌ Not enough stars" if lang == 'en' else "❌ Недостаточно звёзд", show_alert=True)
        await state.clear()
        return
    
    await c.answer()
    
    # Показываем прогресс
    if lang == 'en':
        progress_text = f"⏳ <b>Processing withdrawal...</b>\n\n⭐ {amount} Stars → @{recipient}"
    else:
        progress_text = f"⏳ <b>Обработка вывода...</b>\n\n⭐ {amount} звёзд → @{recipient}"
    
    msg = await c.message.edit_text(progress_text, parse_mode="HTML")
    
    try:
        # Получаем recipient_id через DAO
        recipient_data = dao.stars_recipient(recipient)
        recipient_id = recipient_data.get("recipient")
        
        if not recipient_id:
            raise Exception("Recipient not found")
        
        # Покупаем звёзды через DAO (используем баланс бота)
        purchase = dao.stars_buy(recipient_id, amount, TON_WALLET_ADDRESS)
        
        if "messages" not in purchase:
            raise Exception(f"Invalid response: {purchase}")
        
        messages = purchase.get("messages", [])
        if not messages:
            raise Exception("Empty messages")
        
        # Отправляем транзакцию
        ton.send_messages_no_wait(messages)
        tx_result = True
        
        if not tx_result:
            raise Exception("Transaction failed")
        
        # Списываем звёзды с баланса заданий
        db.withdraw_task_stars(user_id, amount)
        
        logger.info(f"Task stars withdrawn: user={user_id}, amount={amount}, recipient=@{recipient}")
        
        # Успех
        if lang == 'en':
            success_text = f"🎉 <b>Success!</b>\n\n⭐ {amount} Stars sent to @{recipient}!\n\nRemaining: ⭐ {earned_stars - amount}"
        else:
            success_text = f"🎉 <b>Успешно!</b>\n\n⭐ {amount} звёзд отправлено на @{recipient}!\n\nОсталось: ⭐ {earned_stars - amount}"
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📋 Tasks" if lang == 'en' else "📋 Задания", callback_data="tasks")],
            [types.InlineKeyboardButton(text="🏠 Menu" if lang == 'en' else "🏠 Меню", callback_data="menu")]
        ])
        
        await msg.edit_text(success_text, reply_markup=kb, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Withdraw task stars error: {e}")
        
        if lang == 'en':
            error_text = f"❌ <b>Error</b>\n\nCould not send stars. Please try again later.\n\nError: {str(e)[:100]}"
        else:
            error_text = f"❌ <b>Ошибка</b>\n\nНе удалось отправить звёзды. Попробуй позже.\n\nОшибка: {str(e)[:100]}"
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📋 Tasks" if lang == 'en' else "📋 Задания", callback_data="tasks")]
        ])
        
        await msg.edit_text(error_text, reply_markup=kb, parse_mode="HTML")
    
    await state.clear()


@dp.callback_query(F.data == "info")
async def cb_info(c: types.CallbackQuery):
    await ensure_user_registered(c)
    await c.answer()
    user_id = c.from_user.id
    info_text = "🧪 And here's the neeeews!" if get_user_lang(user_id) == 'en' else "🧪 И вот такииие у нас новости!"
    await safe_edit(c.message, info_text, kb_info(user_id))

@dp.callback_query(F.data == "topup")
async def cb_topup(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    await c.answer()
    await state.clear()
    user_id = c.from_user.id
    logger.debug(f"User {user_id} opened topup menu, CRYPTOPAY_ENABLED: {CRYPTOPAY_ENABLED}")
    await safe_edit(c.message, get_text(user_id, 'topup_menu'), kb_topup(user_id))

@dp.callback_query(F.data == "topup_ton")
async def cb_topup_ton(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    user_id = c.from_user.id
    await c.answer(get_text(user_id, 'cb_preparing'))

    if user_id in USER_PAYMENTS:
        old_code = USER_PAYMENTS[user_id]
        if old_code in PENDING_PAYMENTS:
            del PENDING_PAYMENTS[old_code]

    payment_code = generate_payment_code(user_id)
    timestamp = time.time()

    PENDING_PAYMENTS[payment_code] = (user_id, timestamp)
    USER_PAYMENTS[user_id] = payment_code

    await state.set_state(TopUpTON.waiting)
    await state.update_data(
        payment_code=payment_code,
        payment_timestamp=timestamp
    )

    text = f"{get_text(user_id, 'ton_payment_title')}\n\n"
    text += f"📋 <b>Инструкция:</b>\n\n"
    text += f"1️⃣ Отправьте от {MIN_DEPOSIT} TON на адрес:\n"
    text += f"<code>{TON_WALLET_ADDRESS}</code>\n\n"
    text += f"2️⃣ В комментарии к переводу укажите:\n"
    text += f"<code>{payment_code}</code>\n\n"
    text += f"3️⃣ Нажмите одну из кнопок ниже для быстрой оплаты через Tonkeeper\n"
    text += f"    или отправьте вручную и нажмите \"Проверить\""

    await c.message.edit_text(
        text,
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            # Кнопки быстрой оплаты через Tonkeeper
            [
                types.InlineKeyboardButton(
                    text="💎 0.5 TON",
                    url=create_tonkeeper_link(TON_WALLET_ADDRESS, 0.5, payment_code)
                ),
                types.InlineKeyboardButton(
                    text="💎 1 TON",
                    url=create_tonkeeper_link(TON_WALLET_ADDRESS, 1.0, payment_code)
                ),
                types.InlineKeyboardButton(
                    text="💎 2 TON",
                    url=create_tonkeeper_link(TON_WALLET_ADDRESS, 2.0, payment_code)
                ),
            ],
            [types.InlineKeyboardButton(text=get_text(user_id, "btn_check"), callback_data="check_ton_deposit")],
            [types.InlineKeyboardButton(text=get_text(user_id, "btn_how_comment"), callback_data="how_comment")],
            [types.InlineKeyboardButton(text=get_text(user_id, "btn_back"), callback_data="menu")]
])
    )


@dp.callback_query(F.data == "check_ton_deposit")
async def cb_check_deposit(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    user_id = c.from_user.id
    try:
        await c.answer(get_text(user_id, 'cb_checking'))
    except Exception:
        pass

    data = await state.get_data()
    payment_code = data.get('payment_code')
    payment_timestamp = data.get('payment_timestamp')

    if not payment_code or not payment_timestamp:
        await safe_edit(
            c.message,
            get_text(user_id, 'no_active_payment'),
            reply_markup=kb_back(user_id)
        )
        return

    if time.time() - payment_timestamp > PAYMENT_TIMEOUT:
        await state.clear()
        if payment_code in PENDING_PAYMENTS:
            del PENDING_PAYMENTS[payment_code]
        if user_id in USER_PAYMENTS:
            del USER_PAYMENTS[user_id]

        await safe_edit(
            c.message,
            get_text(user_id, 'payment_expired'),
            reply_markup=kb_back(user_id)
        )
        return

    await safe_edit(
        c.message,
        get_text(user_id, 'checking_payment'),
        reply_markup=None
    )

    try:
        session = get_session()
        response = session.get(
            "https://toncenter.com/api/v2/getTransactions",
            params={
                "address": TON_WALLET_ADDRESS,
                "limit": 30,
                "archival": True
            },
            headers={"X-API-Key": os.getenv("TONCENTER_API_KEY", "")},
            timeout=15
        )

        if response.status_code == 200:
            data = response.json()
            if data.get("ok"):
                transactions = data.get("result", [])
                found = False

                for tx in transactions:
                    tx_time = tx.get("utime", 0)
                    if tx_time < payment_timestamp:
                        continue

                    in_msg = tx.get("in_msg")
                    if not in_msg or not in_msg.get("value"):
                        continue

                    value = int(in_msg.get("value", 0))
                    amount_ton = value / 1e9

                    if amount_ton < MIN_DEPOSIT:
                        continue

                    msg_data = in_msg.get("msg_data", {})
                    comment = None

                    if isinstance(msg_data, dict):
                        for field in ["text", "comment", "payload", "body"]:
                            value = msg_data.get(field)
                            if value:
                                comment = decode_comment(str(value))
                                if comment:
                                    break

                    if not comment:
                        message = in_msg.get("message")
                        if message:
                            comment = decode_comment(message)

                    logger.debug(f"Transaction comment: {comment}, expected: {payment_code}")

                    if comment and str(comment).strip() == payment_code:
                        tx_id = tx.get("transaction_id", {})
                        tx_hash = tx_id.get("hash", f"tx_{tx_time}_{user_id}")

                        if not db.is_tx_processed(tx_hash):
                            db.record_deposit(user_id, amount_ton, tx_hash, f"Payment {payment_code}")

                            if payment_code in PENDING_PAYMENTS:
                                del PENDING_PAYMENTS[payment_code]
                            if user_id in USER_PAYMENTS:
                                del USER_PAYMENTS[user_id]
                            await state.clear()

                            await safe_edit(
                                c.message,
                                get_text(user_id, 'payment_found',
                                         amount=amount_ton,
                                         balance=db.get_user_balance(user_id)),
                                reply_markup=kb_main(user_id)
                            )

                            found = True
                            break

                if not found:
                    remaining_minutes = max(0, int((payment_timestamp + PAYMENT_TIMEOUT - time.time()) / 60))

                    not_found_text = f"{get_text(user_id, 'payment_not_found')}\n\n"
                    not_found_text += f"💎 Адрес для пополнения:\n<code>{TON_WALLET_ADDRESS}</code>\n\n"
                    not_found_text += f"📝 Комментарий:\n<code>{payment_code}</code>\n\n"
                    not_found_text += f"⏰ Осталось времени: {remaining_minutes} минут"

                    await safe_edit(
                        c.message,
                        not_found_text,
                        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                            [types.InlineKeyboardButton(text=get_text(user_id, 'btn_check_again'),
                                                        callback_data="check_ton_deposit")],
                            [types.InlineKeyboardButton(text=get_text(user_id, 'btn_how_comment'),
                                                        callback_data="how_comment")],
                            [types.InlineKeyboardButton(text=get_text(user_id, 'btn_back'), callback_data="menu")]
                        ])
                    )
        else:
            logger.error(f"TonCenter API error: {response.status_code}")
            await safe_edit(
                c.message,
                get_text(user_id, 'payment_check_error'),
                reply_markup=kb_back(user_id)
            )

    except Exception as e:
        logger.error(f"Error during deposit check: {e}")
        await safe_edit(
            c.message,
            get_text(user_id, 'payment_check_error'),
            reply_markup=kb_back(user_id)
        )


@dp.callback_query(F.data == "how_comment")
async def cb_how_comment(c: types.CallbackQuery):
    await ensure_user_registered(c)
    await c.answer()
    user_id = c.from_user.id

    await safe_edit(
        c.message,
        get_text(user_id, 'how_comment_title') + '\n\n' + get_text(user_id, 'how_comment_text'),
        reply_markup=kb_back(user_id)
    )


@dp.callback_query(F.data == "topup_xrocket")
async def cb_topup_xr(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    await c.answer()
    await state.set_state(TopUpXR.token)
    user_id = c.from_user.id
    await safe_edit(c.message, get_text(user_id, 'choose_token'), kb_tokens(user_id))


@dp.callback_query(TopUpXR.token, F.data.startswith("token_"))
async def cb_pick_token(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    await c.answer()
    token = c.data.split("_", 1)[1]
    await state.update_data(token=token)
    await state.set_state(TopUpXR.usd)
    user_id = c.from_user.id

    display_token = "TON" if token == "TONCOIN" else token
    await safe_edit(
        c.message,
        get_text(user_id, 'enter_usd_amount', token=display_token),
        kb_back(user_id, "btn_cancel"),
    )


@dp.message(TopUpXR.usd)
async def msg_usd(m: types.Message, state: FSMContext):
    # Игнорируем сообщения в группах
    if m.chat.type != "private":
        return
    await ensure_user_registered(m)
    user_id = m.from_user.id
    try:
        usd_amt = float(m.text.replace(",", "."))
        if usd_amt < 1:
            raise ValueError
    except ValueError:
        return await m.answer(get_text(user_id, 'invalid_amount'), reply_markup=kb_back(user_id, "btn_cancel"))

    data = await state.get_data()
    token = data["token"]
    await m.answer(get_text(user_id, 'creating_invoice'))

    try:
        token_amt = await usd_to_token(usd_amt, token)
        inv = await create_invoice(
            amount=token_amt,
            currency=token,
            description="Пополнение Stars-Bot",
            payload=f"{user_id}:{token}:{usd_amt}",
        )
    except Exception as exc:
        logger.exception(f"Invoice creation failed: {exc}")
        await state.clear()
        return await m.answer(get_text(user_id, 'invoice_error'),
                              reply_markup=kb_main(user_id))

    kb_pay = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_pay'), url=inv["pay_url"])],
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_back'), callback_data="menu")],
    ])

    display_token = "TON" if token == "TONCOIN" else token
    await m.answer(
        get_text(user_id, 'invoice_created',
                 id=inv['id'],
                 usd=usd_amt,
                 amount=token_amt,
                 token=display_token),
        reply_markup=kb_pay,
    )
    await state.set_state(TopUpXR.wait)
    asyncio.create_task(_poll_invoice(user_id, inv["id"], usd_amt))


async def _poll_invoice(uid: int, invoice_id: str, usd_amt: float):
    logger.info(f"Start polling invoice {invoice_id} for user {uid}")
    for _ in range(60):
        await asyncio.sleep(10)
        try:
            resp = await check_invoice(invoice_id)
            status = (resp.get("data") or resp).get("status", "").lower()
            if status in {"paid", "success", "completed"}:
                usd_rate, _ = ton_rates()
                ton_amt = round(usd_amt / usd_rate, 6)
                db.update_balance(uid, ton_amt)
                db.add_internal(ton_amt)
                await bot.send_message(
                    uid,
                    get_text(uid, 'payment_confirmed', amount=ton_amt),
                    reply_markup=kb_main(uid),
                )
                logger.info(f"Invoice {invoice_id} paid.")
                return
        except Exception as exc:
            logger.warning(f"Invoice check error {invoice_id}: {exc}")
    await bot.send_message(uid, get_text(uid, 'invoice_expired'), reply_markup=kb_main(uid))


@dp.callback_query(F.data == "topup_crypto")
async def cb_topup_crypto(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    if not CRYPTOPAY_ENABLED:
        await c.answer("CryptoPay не настроен", show_alert=True)
        return

    await c.answer()
    await state.set_state(TopUpCrypto.currency)
    user_id = c.from_user.id
    await safe_edit(c.message, get_text(user_id, 'choose_currency'), kb_crypto_currencies(user_id))


@dp.callback_query(TopUpCrypto.currency, F.data.startswith("crypto_"))
async def cb_crypto_currency(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    await c.answer()

    currency = c.data.split("_", 1)[1]
    await state.update_data(currency=currency)
    await state.set_state(TopUpCrypto.amount)

    user_id = c.from_user.id

    min_amounts = {
        "USDT": 1.0,
        "TON": 0.5,
        "BTC": 0.00001,
        "ETH": 0.0001,
        "USDC": 1.0,
        "BNB": 0.001,
    }

    min_amount = min_amounts.get(currency, 1.0)

    await safe_edit(
        c.message,
        get_text(user_id, 'enter_amount', currency=currency, min=min_amount),
        kb_back(user_id, "btn_cancel")
    )


@dp.message(TopUpCrypto.amount)
async def msg_crypto_amount(m: types.Message, state: FSMContext):
    await ensure_user_registered(m)
    user_id = m.from_user.id
    data = await state.get_data()
    currency = data.get("currency", "USDT")

    min_amounts = {
        "USDT": 1.0,
        "TON": 0.5,
        "BTC": 0.00001,
        "ETH": 0.0001,
        "USDC": 1.0,
        "BNB": 0.001,
    }

    min_amount = min_amounts.get(currency, 1.0)

    try:
        amount = float(m.text.replace(",", "."))
        if amount < min_amount:
            raise ValueError
    except ValueError:
        return await m.answer(
            get_text(user_id, 'invalid_min_amount', min=min_amount, currency=currency),
            reply_markup=kb_back(user_id, "btn_cancel")
        )

    await state.update_data(amount=amount)
    await m.answer(get_text(user_id, 'creating_invoice'))

    try:
        invoice = await crypto_pay.create_invoice(
            amount=amount,
            currency=currency,
            description=f"Top up Stars Bot - {user_id}",
            payload=f"{user_id}:{currency}:{amount}",
            expires_in=1800
        )

        invoice_id = invoice.get("invoice_id")
        pay_url = invoice.get("pay_url")

        if not invoice_id or not pay_url:
            raise Exception("Invalid invoice response")

        await state.update_data(invoice_id=invoice_id)
        await state.set_state(TopUpCrypto.wait)

        kb_pay = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(
                text=get_text(user_id, 'btn_pay'),
                url=pay_url
            )],
            [types.InlineKeyboardButton(
                text=get_text(user_id, 'btn_check'),
                callback_data="check_crypto_payment"
            )],
            [types.InlineKeyboardButton(
                text=get_text(user_id, 'btn_back'),
                callback_data="menu"
            )]
        ])

        bot_username = "CryptoBot" if not getattr(crypto_pay, "IS_TESTNET", False) else "CryptoTestnetBot"

        await m.answer(
            get_text(user_id, 'invoice_created_crypto',
                     amount=amount,
                     currency=currency,
                     bot_username=bot_username),
            reply_markup=kb_pay
        )

        asyncio.create_task(_poll_crypto_invoice(user_id, invoice_id, amount, currency))

    except Exception as e:
        logger.error(f"CryptoPay invoice creation error: {e}")
        await state.clear()
        await m.answer(
            get_text(user_id, 'cryptopay_error', error=str(e)),
            reply_markup=kb_main(user_id)
        )


@dp.callback_query(TopUpCrypto.wait, F.data == "check_crypto_payment")
async def cb_check_crypto_payment(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    user_id = c.from_user.id
    await c.answer(get_text(user_id, 'cb_checking'))

    data = await state.get_data()
    invoice_id = data.get("invoice_id")
    amount = data.get("amount")
    currency = data.get("currency")

    if not invoice_id:
        await state.clear()
        await safe_edit(
            c.message,
            get_text(user_id, 'no_active_payment'),
            kb_main(user_id)
        )
        return

    await safe_edit(
        c.message,
        get_text(user_id, 'checking_payment'),
        None
    )

    try:
        invoice = await crypto_pay.check_invoice(invoice_id)

        if invoice and invoice.get("status") == "paid":
            ton_amount = await crypto_pay.convert_to_ton(amount, currency)

            if not ton_amount:
                usd_rate, _ = ton_rates()
                if currency in ("USDT", "USDC"):
                    ton_amount = amount / usd_rate
                else:
                    ton_amount = 1.0
                    logger.error(f"Failed to convert {amount} {currency} to TON")

            db.update_balance(user_id, ton_amount)
            db.add_internal(ton_amount)

            db.record_deposit(
                user_id,
                ton_amount,
                f"crypto_{invoice_id}",
                f"CryptoPay {currency}"
            )

            await state.clear()

            await safe_edit(
                c.message,
                get_text(user_id, 'crypto_confirmed',
                         received=amount,
                         currency=currency,
                         credited=ton_amount,
                         balance=db.get_user_balance(user_id)),
                kb_main(user_id)
            )

            logger.info(f"CryptoPay payment confirmed: {amount} {currency} -> {ton_amount} TON for user {user_id}")

        else:
            kb_check = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(
                    text=get_text(user_id, 'btn_check_again'),
                    callback_data="check_crypto_payment"
                )],
                [types.InlineKeyboardButton(
                    text=get_text(user_id, 'btn_back'),
                    callback_data="menu"
                )]
            ])

            await safe_edit(
                c.message,
                get_text(user_id, 'crypto_not_found',
                         amount=amount,
                         currency=currency),
                kb_check
            )

    except Exception as e:
        logger.error(f"Error checking CryptoPay payment: {e}")
        await safe_edit(
            c.message,
            get_text(user_id, 'payment_check_error'),
            kb_back(user_id)
        )


async def _poll_crypto_invoice(user_id: int, invoice_id: int, amount: float, currency: str):
    logger.info(f"Start polling CryptoPay invoice {invoice_id} for user {user_id}")

    for _ in range(60):
        await asyncio.sleep(10)

        try:
            invoice = await crypto_pay.check_invoice(invoice_id)

            if invoice and invoice.get("status") == "paid":
                ton_amount = await crypto_pay.convert_to_ton(amount, currency)

                if not ton_amount:
                    usd_rate, _ = ton_rates()
                    if currency in ("USDT", "USDC"):
                        ton_amount = amount / usd_rate
                    else:
                        ton_amount = 1.0
                        logger.error(f"Failed to convert {amount} {currency} to TON")

                db.update_balance(user_id, ton_amount)
                db.add_internal(ton_amount)

                db.record_deposit(
                    user_id,
                    ton_amount,
                    f"crypto_{invoice_id}",
                    f"CryptoPay {currency}"
                )

                await bot.send_message(
                    user_id,
                    get_text(user_id, 'crypto_auto_confirmed',
                             received=amount,
                             currency=currency,
                             credited=ton_amount),
                    reply_markup=kb_main(user_id)
                )

                logger.info(f"CryptoPay invoice {invoice_id} paid automatically")
                return

        except Exception as e:
            logger.warning(f"CryptoPay poll error for invoice {invoice_id}: {e}")

    logger.info(f"CryptoPay invoice {invoice_id} expired without payment")


@dp.callback_query(F.data == "buy")
async def cb_buy(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    await c.answer()
    # Сохраняем source_chat_id если покупка из группы
    if c.message and c.message.chat.type in ("group", "supergroup"):
        await state.update_data(source_chat_id=c.message.chat.id)
    await state.set_state(Buy.mode)
    user_id = c.from_user.id
    await safe_edit(c.message, get_text(user_id, 'buy_mode_select'), kb_buy_mode(user_id))


@dp.callback_query(Buy.mode, F.data == "buy_self")
async def cb_buy_self(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    user_id = c.from_user.id
    username = c.from_user.username
    if not username:
        await c.answer(get_text(user_id, 'no_username_short'), show_alert=True)
        return await safe_edit(
            c.message,
            get_text(user_id, 'no_username'),
            kb_back(user_id),
        )
    await c.answer()
    await state.update_data(user=username)
    await state.set_state(Buy.amount_selection)

    text = get_text(user_id, 'select_stars_amount')
    await safe_edit(c.message, text, kb_stars_amount(user_id))


@dp.callback_query(Buy.mode, F.data == "buy_friend")
async def cb_buy_friend(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    await c.answer()
    await state.set_state(Buy.user)
    user_id = c.from_user.id
    
    # В группах нужно reply, в ЛС просто ввод
    if c.message.chat.type in ("group", "supergroup"):
        text = "👤 Ответьте на это сообщение, указав @username друга\n\n💡 Нажмите Reply (Ответить) на это сообщение"
        if get_user_lang(user_id) == "en":
            text = "👤 Reply to this message with your friend's @username\n\n💡 Press Reply on this message"
        msg = await c.message.answer(text, reply_markup=kb_back(user_id))
        await state.update_data(reply_msg_id=msg.message_id)
    else:
        await safe_edit(c.message, get_text(user_id, 'enter_friend_link'), kb_back(user_id))
    return


@dp.message(Buy.user, F.reply_to_message)
async def msg_user_reply(m: types.Message, state: FSMContext):
    """Обработчик reply в группах"""
    await ensure_user_registered(m)
    import re
    
    # Проверяем что это reply на наше сообщение
    state_data = await state.get_data()
    reply_msg_id = state_data.get("reply_msg_id")
    
    if m.chat.type in ("group", "supergroup"):
        if not m.reply_to_message or m.reply_to_message.message_id != reply_msg_id:
            return  # Игнорируем не-reply в группах
    
    u = m.text.lstrip("@") if m.text else ""
    user_id = m.from_user.id
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", u):
        return await m.answer(get_text(user_id, 'invalid_link'), reply_markup=kb_back(user_id))
    await state.update_data(user=u)
    await state.set_state(Buy.amount_selection)
    
    text = get_text(user_id, 'select_stars_amount')
    await m.answer(text, reply_markup=kb_stars_amount(user_id))


@dp.message(Buy.user)
async def msg_user(m: types.Message, state: FSMContext):
    await ensure_user_registered(m)
    import re


    u = m.text.lstrip("@")
    user_id = m.from_user.id
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", u):
        return await m.answer(get_text(user_id, 'invalid_link'), reply_markup=kb_back(user_id))
    await state.update_data(user=u)
    await state.set_state(Buy.amount_selection)

    text = get_text(user_id, 'select_stars_amount')
    await m.answer(text, reply_markup=kb_stars_amount(user_id))


@dp.callback_query(Buy.amount_selection, F.data.startswith("stars_amount_"))
async def cb_stars_amount(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    user_id = c.from_user.id
    await c.answer()

    amount = int(c.data.split("_", 2)[2])

    await state.update_data(qty=amount)
    await process_purchase(c.message, state, user_id, amount)


@dp.callback_query(Buy.amount_selection, F.data == "stars_custom")
async def cb_stars_custom(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    await c.answer()
    await state.set_state(Buy.qty)
    user_id = c.from_user.id
    await safe_edit(c.message, get_text(user_id, 'enter_stars_amount', min=50), kb_back(user_id))


@dp.message(Buy.qty)
async def msg_qty(m: types.Message, state: FSMContext):
    await ensure_user_registered(m)
    user_id = m.from_user.id

    if not check_rate_limit(user_id):
        return await m.answer("⚠️ Too many requests. Please wait.", reply_markup=kb_main(user_id))

    try:
        qty = int(m.text)
        if qty < 50 or qty > 100000:
            raise ValueError
    except ValueError:
        return await m.answer(get_text(user_id, 'invalid_stars_amount'), reply_markup=kb_back(user_id))

    await state.update_data(qty=qty)
    await process_purchase(m, state, user_id, qty)


async def process_purchase(message: types.Message, state: FSMContext, user_id: int, qty: int):
    data = await state.get_data()
    price = price_one(with_fee=True)

    if not price:
        await state.clear()
        return await message.answer(get_text(user_id, 'price_error'),
                                    reply_markup=kb_main(user_id))

    cost = qty * price
    user_balance = db.get_user_balance(user_id)

    if user_balance < cost:
        await state.clear()
        deficit = cost - user_balance
        return await message.answer(
            get_text(user_id, 'insufficient_balance',
                     balance=user_balance,
                     required=cost,
                     deficit=deficit),
            reply_markup=kb_main(user_id),
        )

    purchase_id = f"{user_id}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

    await state.update_data(
        cost=cost,
        purchase_id=purchase_id
    )
    await state.set_state(Buy.confirming)

    kb_conf = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text=get_text(user_id, 'btn_confirm'),
            callback_data=f"buygo:{purchase_id}"
        )],
        [types.InlineKeyboardButton(text=get_text(user_id, 'btn_cancel'), callback_data="menu")],
    ])

    await message.answer(
        get_text(user_id, 'stars_purchase_confirm',
                 stars=qty,
                 price=cost,
                 recipient=f"@{data['user']}"),
        reply_markup=kb_conf,
    )


@dp.callback_query(Buy.confirming, F.data.startswith("buygo:"))
async def cb_buy_go(c: types.CallbackQuery, state: FSMContext):
    await ensure_user_registered(c)
    user_id = c.from_user.id
    lang = get_user_lang(user_id)

    try:
        purchase_id = c.data.split(":", 1)[1]
    except Exception:
        await c.answer("❌ Invalid request", show_alert=True)
        await state.clear()
        return

    now = time.time()
    if user_id in USER_LAST_ACTION:
        if now - USER_LAST_ACTION[user_id] < PURCHASE_COOLDOWN:
            await c.answer("⏳ Please wait a few seconds between purchases", show_alert=True)
            return

    if purchase_id in PROCESSING_PURCHASES:
        await c.answer("⏳ Already processing, please wait...", show_alert=True)
        return

    if purchase_id in COMPLETED_PURCHASES:
        await c.answer("✅ This purchase was already completed", show_alert=True)
        await state.clear()
        return await safe_edit(c.message, get_text(user_id, 'main_menu'), kb_main(user_id))

    PROCESSING_PURCHASES.add(purchase_id)
    USER_LAST_ACTION[user_id] = now

    def create_progress_bar(percent: int, width: int = 15) -> str:
        filled = int(width * percent / 100)
        empty = width - filled
        bar = "🟩" * filled + "⬜" * empty
        return f"{bar} {percent}%"

    def create_progress_message(stage: str, percent: int, stars: int, recipient: str) -> str:
        if lang == 'en':
            title = "🌟 <b>Processing Stars Purchase</b>\n\n"
            info = f"⭐ Stars: <b>{stars}</b>\n"
            info += f"👤 Recipient: <b>{recipient}</b>\n"
            info += f"💰 Cost: <b>{cost:.4f} TON</b>\n\n"
            stage_text = f"📍 <b>{stage}</b>\n\n"
        else:
            title = "🌟 <b>Обработка покупки Stars</b>\n\n"
            info = f"⭐ Количество: <b>{stars}</b>\n"
            info += f"👤 Получатель: <b>{recipient}</b>\n"
            info += f"💰 Стоимость: <b>{cost:.4f} TON</b>\n\n"
            stage_text = f"📍 <b>{stage}</b>\n\n"

        progress = create_progress_bar(percent)
        return f"{title}{info}{stage_text}{progress}"

    try:
        await c.answer()

        data = await state.get_data()
        qty = data.get("qty")
        cost = data.get("cost")
        username = data.get("user")
        saved_purchase_id = data.get("purchase_id")

        if saved_purchase_id != purchase_id:
            raise ValueError("Purchase ID mismatch")

        current_balance = db.get_user_balance(user_id)
        if current_balance < cost:
            raise ValueError("Insufficient balance")

        if get_wallet_balance() < cost * 1.1:
            raise ValueError("Insufficient bot wallet balance")

        msg = await c.message.edit_text(
            create_progress_message(
                stage="Инициализация..." if lang == 'ru' else "Initializing...",
                percent=5,
                stars=qty,
                recipient=f"@{username}"
            ),
            reply_markup=None
        )

        await asyncio.sleep(0.3)

        await msg.edit_text(
            create_progress_message(
                stage="Проверка получателя..." if lang == 'ru' else "Checking recipient...",
                percent=15,
                stars=qty,
                recipient=f"@{username}"
            )
        )

        try:
            recipient_data = dao.stars_recipient(username)
            recipient_id = recipient_data.get("recipient")
            if not recipient_id:
                raise dao.DAOLamaError("No recipient ID returned")

            logger.info(f"Got recipient ID for @{username}: {recipient_id}")

        except dao.DAOLamaError as e:
            error_msg = str(e).lower()
            if "not found" in error_msg or "does not exist" in error_msg:
                await msg.edit_text(
                    get_text(user_id, 'user_not_found', username=username),
                    reply_markup=kb_main(user_id)
                )
                await state.clear()
                return
            raise

        await msg.edit_text(
            create_progress_message(
                stage="Получатель подтвержден ✅" if lang == 'ru' else "Recipient verified ✅",
                percent=30,
                stars=qty,
                recipient=f"@{username}"
            )
        )

        await asyncio.sleep(0.5)

        await msg.edit_text(
            create_progress_message(
                stage="Создание транзакции..." if lang == 'ru' else "Creating transaction...",
                percent=40,
                stars=qty,
                recipient=f"@{username}"
            )
        )

        purchase = dao.stars_buy(recipient_id, qty, TON_WALLET_ADDRESS)

        if "messages" not in purchase:
            raise dao.DAOLamaError(f"Invalid response structure: {purchase}")

        messages = purchase.get("messages", [])
        if not messages:
            raise dao.DAOLamaError("Empty messages array")

        valid_until = purchase.get("validUntil")
        if valid_until:
            current_time = int(time.time())
            if current_time > valid_until:
                raise dao.DAOLamaError("Transaction expired")

        await msg.edit_text(
            create_progress_message(
                stage="Транзакция подготовлена ✅" if lang == 'ru' else "Transaction prepared ✅",
                percent=50,
                stars=qty,
                recipient=f"@{username}"
            )
        )

        await asyncio.sleep(0.5)

        await msg.edit_text(
            create_progress_message(
                stage="Обработка платежа..." if lang == 'ru' else "Processing payment...",
                percent=60,
                stars=qty,
                recipient=f"@{username}"
            )
        )

        success = db.atomic_purchase(
            user_id=user_id,
            cost=cost,
            stars=qty,
            purchase_id=purchase_id
        )

        if not success:
            raise ValueError("Failed to deduct balance (possibly insufficient funds)")

        await msg.edit_text(
            create_progress_message(
                stage="Платеж обработан ✅" if lang == 'ru' else "Payment processed ✅",
                percent=70,
                stars=qty,
                recipient=f"@{username}"
            )
        )

        await asyncio.sleep(0.5)

        await msg.edit_text(
            create_progress_message(
                stage="Отправка в блокчейн TON..." if lang == 'ru' else "Sending to TON blockchain...",
                percent=80,
                stars=qty,
                recipient=f"@{username}"
            )
        )

        try:
            ton.send_messages_no_wait(messages)

            await asyncio.sleep(0.3)

            await msg.edit_text(
                create_progress_message(
                    stage="Подтверждение транзакции..." if lang == 'ru' else "Confirming transaction...",
                    percent=90,
                    stars=qty,
                    recipient=f"@{username}"
                )
            )

            await asyncio.sleep(0.5)

            COMPLETED_PURCHASES[purchase_id] = now

            await msg.edit_text(
                create_progress_message(
                    stage="Завершение..." if lang == 'ru' else "Finalizing...",
                    percent=100,
                    stars=qty,
                    recipient=f"@{username}"
                )
            )

            await asyncio.sleep(0.5)

            if lang == 'en':
                success_text = (
                    "🧪 <b>Wubba lubba dub dub!</b>\n\n"
                    f"⭐ <b>{qty} Stars</b> sent to <b>@{username}</b>!\n\n"
                    "Science wins again! Stars will arrive in <b>1-3 minutes</b>.\n\n"
                    "Check balance: Telegram Settings → Stars"
                )
            else:
                success_text = (
                    "🧪 <b>Wubba lubba dub dub!</b>\n\n"
                    f"⭐ <b>{qty} Stars</b> улетели к <b>@{username}</b>!\n\n"
                    "Наука победила! Звёзды прилетят через <b>1-3 минуты</b>.\n\n"
                    "Проверить баланс: Настройки Telegram → Stars"
                )

            await msg.edit_text(success_text, reply_markup=kb_main(user_id))
            logger.info(f"★ Purchase completed: {qty} → @{username} (cost {cost:.6f} TON)")

            # === КОМИССИЯ ВЛАДЕЛЬЦУ ЧАТА ОТ ПОКУПКИ ===
            state_data = await state.get_data()
            logger.info(f"Purchase commission check: state_data={state_data}")
            source_chat_id = state_data.get("source_chat_id")
            logger.info(f"source_chat_id={source_chat_id}")
            if source_chat_id:
                chat_info = db.get_chat(source_chat_id)
                if chat_info:
                    owner_id_chat = chat_info.get("owner_id")
                    # Объём и комиссия только если покупатель НЕ владелец чата
                    if user_id != owner_id_chat:
                        db.add_chat_volume(source_chat_id, cost)
                        fee_percent = db.get_fee_percent()
                        base_price = price_one(with_fee=False)
                        logger.info(f"fee_percent={fee_percent}, base_price={base_price}")
                        if base_price and fee_percent > 0:
                            commission = db.calculate_purchase_commission_by_level(qty, fee_percent, base_price, owner_id_chat)
                            if commission > 0:
                                db.add_chat_earning(
                                    chat_id=source_chat_id,
                                    amount=commission,
                                    earning_type="purchase",
                                    user_id=user_id,
                                    details=f"Stars: {qty}, cost={cost:.4f} TON"
                                )
                                logger.info(f"Chat purchase commission: chat={source_chat_id}, owner={owner_id_chat}, amount={commission:.6f}")


        except RuntimeError as e:
            if "TX not confirmed" in str(e):
                COMPLETED_PURCHASES[purchase_id] = now

                await msg.edit_text(
                    create_progress_message(
                        stage="Ожидание подтверждения сети..." if lang == 'ru' else "Waiting for network confirmation...",
                        percent=95,
                        stars=qty,
                        recipient=f"@{username}"
                    )
                )

                await asyncio.sleep(1)

                if lang == 'en':
                    delayed_text = (
                        "✅ <b>Transaction sent successfully!</b>\n\n"
                        f"🌟 <b>{qty} Stars</b> are being delivered to <b>@{username}</b>\n\n"
                        "⏳ <b>Network confirmation in progress...</b>\n\n"
                        "The transaction has been sent to the TON blockchain and will be confirmed shortly.\n"
                        "Stars will be credited automatically once the network confirms the transaction.\n\n"
                        "⚠️ <b>Note:</b> Due to current network conditions, confirmation may take longer than usual."
                    )
                else:
                    delayed_text = (
                        "✅ <b>Транзакция успешно отправлена!</b>\n\n"
                        f"🌟 <b>{qty} Stars</b> доставляются пользователю <b>@{username}</b>\n\n"
                        "⏳ <b>Подтверждение сети в процессе...</b>\n\n"
                        "Транзакция отправлена в блокчейн TON и будет подтверждена в ближайшее время.\n"
                        "Stars будут начислены автоматически после подтверждения транзакции сетью.\n\n"
                        "⚠️ <b>Внимание:</b> Из-за текущей загрузки сети подтверждение может занять больше времени."
                    )

                await msg.edit_text(delayed_text, reply_markup=kb_main(user_id))
                logger.info(f"★ Purchase sent (slow confirm): {qty} → @{username}")
            else:
                db.rollback_purchase(user_id, cost, qty, purchase_id)
                raise

    except dao.DAOLamaError as exc:
        # Возврат баланса при ошибке DAO
        if 'cost' in locals() and 'qty' in locals() and 'purchase_id' in locals():
            try:
                db.rollback_purchase(user_id, cost, qty, purchase_id)
                logger.info(f"Balance rollback for {user_id} after DAO error")
            except Exception as e:
                logger.error(f"Rollback failed: {e}")
        logger.error(f"DAO Lama error: {exc}")
        error_msg = str(exc).lower()

        if "timeout" in error_msg or "connection" in error_msg:
            await c.message.edit_text(
                get_text(user_id, 'service_unavailable'),
                reply_markup=kb_main(user_id)
            )
        else:
            await c.message.edit_text(
                get_text(user_id, 'purchase_error', error=html.escape(str(exc))),
                reply_markup=kb_main(user_id)
            )

    except ValueError as e:
        # Возврат баланса при ошибке валидации
        if "cost" in locals() and "qty" in locals() and "purchase_id" in locals():
            try:
                db.rollback_purchase(user_id, cost, qty, purchase_id)
                logger.info(f"Balance returned to user {user_id} after ValueError")
            except Exception:
                pass
        logger.error(f"Validation error: {e}")
        # Возврат баланса при ошибке
        if "cost" in locals() and "qty" in locals() and "purchase_id" in locals():
            try:
                db.rollback_purchase(user_id, cost, qty, purchase_id)
                logger.info(f"Balance returned to user {user_id} after ValueError")
            except Exception:
                pass
        error_text = str(e)

        if "balance" in error_text.lower():
            await c.message.edit_text(
                get_text(user_id, 'balance_changed'),
                reply_markup=kb_main(user_id)
            )
        else:
            await c.message.edit_text(
                get_text(user_id, 'processing_error'),
                reply_markup=kb_main(user_id)
            )

    except Exception as exc:
        logger.exception(f"Unexpected error during purchase: {exc}")

        try:
            db.rollback_purchase(user_id, data.get("cost"), data.get("qty"), purchase_id)
        except Exception:
            pass

        await c.message.edit_text(
            get_text(user_id, 'processing_error'),
            reply_markup=kb_main(user_id)
        )

    finally:
        PROCESSING_PURCHASES.discard(purchase_id)
        await state.clear()


@dp.message(F.text & ~F.text.startswith("/"))
async def unknown(m: types.Message, state: FSMContext):
    # Не реагируем на обычные сообщения в группах
    if m.chat.type in ("group", "supergroup"):
        return
    
    await ensure_user_registered(m)
    current_state = await state.get_state()
    if current_state:
        return

    user_id = m.from_user.id
    await m.answer(get_text(user_id, 'use_buttons'), reply_markup=kb_main(user_id))


async def self_diagnostics():
    problems: list[str] = []

    try:
        price = price_one()
        if price is None:
            problems.append("Cannot get Stars price (using default)")
    except Exception as exc:
        logger.warning(f"Price check failed: {exc}")
        problems.append("Price API temporarily unavailable")

    try:
        bal = get_wallet_balance()
        if bal < 0.1:
            problems.append(f"Low TON wallet balance ({bal:.4f} TON)")
    except Exception as exc:
        problems.append(f"Cannot check wallet balance: {exc}")

    try:
        test_req = get_session().get("https://fragment.daolama.co/api", timeout=5)
        test_req.raise_for_status()
    except Exception as exc:
        problems.append("SSL/Connection issues with DAO Lama API")
        logger.warning(f"DAO Lama connection test failed: {exc}")

    if problems:
        text = ("⚠️ <b>Предупреждения при запуске:</b>\n" +
                "\n".join(f"• {html.escape(p)}" for p in problems))
        logger.warning(text)
        if ADMIN_ID:
            try:
                await bot.send_message(ADMIN_ID, text)
            except Exception as exc:
                logger.error(f"Cannot notify admin: {exc}")
    else:
        logger.info("Self-diagnostics: OK")


async def cleanup_task():
    while True:
        try:
            cleanup_expired_payments()
            cleanup_old_purchases()

            now = time.time()
            for user_id in list(USER_RATE_LIMITS.keys()):
                USER_RATE_LIMITS[user_id] = [
                    ts for ts in USER_RATE_LIMITS[user_id]
                    if now - ts < RATE_LIMIT_WINDOW
                ]
                if not USER_RATE_LIMITS[user_id]:
                    del USER_RATE_LIMITS[user_id]

        except Exception as e:
            logger.error(f"Error in cleanup task: {e}")

        await asyncio.sleep(300)


async def main():
    logger.info("Starting bot...")
    db.init_schema()

    asyncio.create_task(cleanup_task())

    await self_diagnostics()

    try:


        # Регистрируем команды для групп
        group_commands = [
            BotCommand(command="dice", description="🎲 Кубик"),
            BotCommand(command="football", description="⚽ Футбол"),
            BotCommand(command="basketball", description="🏀 Баскетбол"),
            BotCommand(command="darts", description="🎯 Дартс"),
            BotCommand(command="bowling", description="🎳 Боулинг"),
            BotCommand(command="slot", description="🎰 Слоты"),
            BotCommand(command="balance", description="💰 Баланс"),
            BotCommand(command="star", description="⭐ Купить Stars"),
            BotCommand(command="top", description="🏆 Лидерборд"),
        ]
        await bot.set_my_commands(group_commands, scope=BotCommandScopeAllGroupChats())
        
        # Команды для личных чатов
        private_commands = [
            BotCommand(command="start", description="🚀 Начать"),
            BotCommand(command="menu", description="📱 Меню"),
        ]
        await bot.set_my_commands(private_commands, scope=BotCommandScopeDefault())
        
        logger.info("Bot commands registered")

        # Запускаем polling
        await dp.start_polling(bot, allowed_updates=['message', 'callback_query', 'my_chat_member'])
    except TelegramNetworkError as e:
        logger.error(f"Telegram network error: {e}")
        raise
    except Exception as e:
        logger.exception(f"Bot crashed: {e}")
        raise
    finally:
        try:
            if _session is not None:
                _session.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")





















