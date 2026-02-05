# locales.py - Полный исправленный файл локализации
import json
import os
from pathlib import Path
from typing import Dict, Optional
from loguru import logger

# Хранилище языковых настроек пользователей
USER_LANGUAGES: Dict[int, str] = {}


# Загрузка настроек языка из файла
def _load_user_languages():
    """Загрузка настроек языка пользователей из файла"""
    try:
        lang_file = Path("data/user_languages.json")
        if lang_file.exists():
            with open(lang_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                USER_LANGUAGES.update({int(k): v for k, v in data.items()})
                logger.info(f"Loaded {len(USER_LANGUAGES)} user language preferences")
    except Exception as e:
        logger.error(f"Failed to load user languages: {e}")


# Сохранение настроек языка в файл
def _save_user_languages():
    """Сохранение настроек языка пользователей в файл"""
    try:
        os.makedirs("data", exist_ok=True)
        with open("data/user_languages.json", "w", encoding="utf-8") as f:
            json.dump(USER_LANGUAGES, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save user languages: {e}")


# Полный словарь переводов
TRANSLATIONS = {
    # === Основные кнопки ===
    'btn_topup': {
        'ru': '💰 Пополнить баланс',
        'en': '💰 Top Up Balance'
    },
    'btn_buy_stars': {
        'ru': '⭐ Купить Stars',
        'en': '⭐ Buy Stars'
    },
    'btn_balance': {
        'ru': '💳 Баланс',
        'en': '💳 Balance'
    },
    'btn_price': {
        'ru': '💵 Цена',
        'en': '💵 Price'
    },
    'btn_language': {
        'ru': '🌐 Язык',
        'en': '🌐 Language'
    },
    'btn_back': {
        'ru': '⬅️ Назад',
        'en': '⬅️ Back'
    },
    'btn_cancel': {
        'ru': '❌ Отмена',
        'en': '❌ Cancel'
    },
    'btn_yes': {
        'ru': '✅ Да',
        'en': '✅ Yes'
    },
    'btn_no': {
        'ru': '❌ Нет',
        'en': '❌ No'
    },
    'btn_confirm': {
        'ru': '✅ Подтвердить',
        'en': '✅ Confirm'
    },
    'btn_pay': {
        'ru': '💳 Оплатить',
        'en': '💳 Pay'
    },
    'btn_check': {
        'ru': '🔍 Проверить',
        'en': '🔍 Check'
    },
    'btn_check_again': {
        'ru': '🔍 Проверить еще раз',
        'en': '🔍 Check Again'
    },

    # === Главное меню ===
    'welcome': {
        'ru': '🧪 Wubba lubba dub dub!\n\nЗдесь звёзды дешевле чем в Telegram!\nЭкономия — это тоже наука!\n\nПроверь сам, если не веришь 👇',
        'en': '🧪 Wubba lubba dub dub!\n\nStars cheaper than Telegram!\nSaving money is science too!\n\nCheck it yourself 👇'
    },
    'main_menu': {
        'ru': '📱 Главное меню',
        'en': '📱 Main Menu'
    },
    'choose_action': {
        'ru': 'Выберите действие:',
        'en': 'Choose an action:'
    },
    'use_buttons': {
        'ru': '👆 Используйте кнопки для навигации',
        'en': '👆 Use buttons for navigation'
    },

    # === Язык ===
    'choose_language': {
        'ru': '🌐 Выберите язык / Choose language:',
        'en': '🌐 Choose language / Выберите язык:'
    },
    'language_changed': {
        'ru': '✅ Язык успешно изменен!',
        'en': '✅ Language changed successfully!'
    },

    # === Пополнение баланса ===
    'topup_menu': {
        'ru': '💰 Выберите способ пополнения:',
        'en': '💰 Choose deposit method:'
    },
    'btn_ton': {
        'ru': '💎 TON',
        'en': '💎 TON'
    },
    'btn_xrocket': {
        'ru': '🚀 xRocket',
        'en': '🚀 xRocket'
    },
    'btn_sbp': {
        'ru': '💳 СБП (Быстрые платежи)',
        'en': '💳 Fast Payment System'
    },
    'btn_cryptopay': {
        'ru': '🔐 CryptoPay',
        'en': '🔐 CryptoPay'
    },
    'enter_amount': {
        'ru': '💵 Введите сумму пополнения в {currency}:\n\n⚠️ Минимум: {min} {currency}',
        'en': '💵 Enter deposit amount in {currency}:\n\n⚠️ Minimum: {min} {currency}'
    },
    'min_amount': {
        'ru': '⚠️ Минимальная сумма: {amount} {currency}',
        'en': '⚠️ Minimum amount: {amount} {currency}'
    },
    'max_amount': {
        'ru': '⚠️ Максимальная сумма: {amount} {currency}',
        'en': '⚠️ Maximum amount: {amount} {currency}'
    },
    'invalid_amount': {
        'ru': '❌ Неверная сумма! Введите число.',
        'en': '❌ Invalid amount! Enter a number.'
    },
    'invalid_min_amount': {
        'ru': '❌ Сумма меньше минимальной ({min} {currency})',
        'en': '❌ Amount is less than minimum ({min} {currency})'
    },
    'payment_created': {
        'ru': '✅ Счет создан! Оплатите в течение {minutes} минут.',
        'en': '✅ Invoice created! Pay within {minutes} minutes.'
    },
    'payment_cancelled': {
        'ru': '❌ Платеж отменен.',
        'en': '❌ Payment cancelled.'
    },
    'payment_success': {
        'ru': '✅ Платеж успешно получен!\n💰 Зачислено: {amount} TON',
        'en': '✅ Payment received successfully!\n💰 Credited: {amount} TON'
    },
    'payment_expired': {
        'ru': '⏰ Время оплаты истекло.',
        'en': '⏰ Payment time expired.'
    },

    # === CryptoPay тексты ===
    'choose_currency': {
        'ru': '💰 Выберите валюту для пополнения:',
        'en': '💰 Choose currency for deposit:'
    },
    'creating_invoice': {
        'ru': '⏳ Создаю счет для оплаты...',
        'en': '⏳ Creating payment invoice...'
    },
    'invoice_created_crypto': {
        'ru': '✅ Счет создан!\n\n'
               '💰 Сумма: {amount} {currency}\n'
               '⏱ Время на оплату: 30 минут\n\n'
               '1️⃣ Нажмите кнопку "💳 Оплатить"\n'
               '2️⃣ Перейдите в @{bot_username}\n'
               '3️⃣ Оплатите счет\n'
               '4️⃣ Вернитесь и нажмите "🔍 Проверить платеж"',
        'en': '✅ Invoice created!\n\n'
               '💰 Amount: {amount} {currency}\n'
               '⏱ Payment time: 30 minutes\n\n'
               '1️⃣ Click "💳 Pay" button\n'
               '2️⃣ Go to @{bot_username}\n'
               '3️⃣ Complete the payment\n'
               '4️⃣ Return and click "🔍 Check Payment"'
    },
    'crypto_confirmed': {
        'ru': '✅ Платеж успешно подтвержден!\n\n'
               '💵 Получено: {received} {currency}\n'
               '💰 Зачислено: {credited:.6f} TON\n'
               '💳 Текущий баланс: {balance:.6f} TON',
        'en': '✅ Payment successfully confirmed!\n\n'
               '💵 Received: {received} {currency}\n'
               '💰 Credited: {credited:.6f} TON\n'
               '💳 Current balance: {balance:.6f} TON'
    },
    'crypto_auto_confirmed': {
        'ru': '✅ Платеж автоматически подтвержден!\n\n'
               '💵 Получено: {received} {currency}\n'
               '💰 Зачислено: {credited:.6f} TON',
        'en': '✅ Payment automatically confirmed!\n\n'
               '💵 Received: {received} {currency}\n'
               '💰 Credited: {credited:.6f} TON'
    },
    'crypto_not_found': {
        'ru': '⏳ Платеж еще не получен\n\n'
               'Ожидаем поступление {amount} {currency}\n\n'
               'Если вы уже оплатили, подождите немного и проверьте снова.',
        'en': '⏳ Payment not received yet\n\n'
               'Waiting for {amount} {currency}\n\n'
               'If you already paid, please wait a moment and check again.'
    },
    'cryptopay_error': {
        'ru': '❌ Ошибка при создании счета CryptoPay: {error}',
        'en': '❌ Error creating CryptoPay invoice: {error}'
    },
    'checking_payment': {
        'ru': '🔍 Проверяю платеж...',
        'en': '🔍 Checking payment...'
    },
    'no_active_payment': {
        'ru': '❌ Нет активного платежа для проверки',
        'en': '❌ No active payment to check'
    },
    'payment_check_error': {
        'ru': '❌ Ошибка при проверке платежа. Попробуйте позже.',
        'en': '❌ Error checking payment. Please try later.'
    },

    # === Покупка Stars ===
    'btn_self': {
        'ru': '👤 Себе',
        'en': '👤 For Myself'
    },
    'btn_friend': {
        'ru': '🎁 Другу',
        'en': '🎁 For Friend'
    },
    'buy_mode_select': {
        'ru': '🛍 Кому покупаем Stars?',
        'en': '🛍 Who are you buying Stars for?'
    },
    'select_stars_amount': {
        'ru': '⭐ Выберите количество Stars:',
        'en': '⭐ Choose Stars amount:'
    },
    'enter_stars_amount': {
        'ru': '⭐ Введите количество Stars (минимум {min}):',
        'en': '⭐ Enter Stars amount (minimum {min}):'
    },
    'invalid_stars_amount': {
        'ru': '❌ Неверное количество! Введите число от 50 до 100000.',
        'en': '❌ Invalid amount! Enter a number from 50 to 100000.'
    },
    'enter_friend_link': {
        'ru': '🔗 Отправьте ссылку на друга (или его @username):',
        'en': '🔗 Send friend\'s link (or @username):'
    },
    'invalid_link': {
        'ru': '❌ Неверная ссылка или username!',
        'en': '❌ Invalid link or username!'
    },
    'stars_purchase_confirm': {
        'ru': '📋 Подтверждение покупки:\n\n⭐ Stars: {stars}\n💰 Стоимость: {price:.4f} TON\n👤 Получатель: {recipient}\n\nПодтвердить?',
        'en': '📋 Purchase confirmation:\n\n⭐ Stars: {stars}\n💰 Cost: {price:.4f} TON\n👤 Recipient: {recipient}\n\nConfirm?'
    },
    'purchase_success': {
        'ru': '✅ Покупка успешно завершена!\n\n⭐ {stars} Stars отправлены {recipient}\n💰 Списано: {price:.4f} TON',
        'en': '✅ Purchase successful!\n\n⭐ {stars} Stars sent to {recipient}\n💰 Charged: {price:.4f} TON'
    },
    'insufficient_balance': {
        'ru': '❌ Недостаточно средств!\n\n💰 Ваш баланс: {balance:.4f} TON\n💵 Требуется: {required:.4f} TON\n➕ Пополните на: {deficit:.4f} TON',
        'en': '❌ Insufficient funds!\n\n💰 Your balance: {balance:.4f} TON\n💵 Required: {required:.4f} TON\n➕ Top up: {deficit:.4f} TON'
    },
    'no_username': {
        'ru': '❌ Для покупки Stars необходимо установить username в настройках Telegram!',
        'en': '❌ You need to set a username in Telegram settings to buy Stars!'
    },
    'no_username_short': {
        'ru': 'Требуется username',
        'en': 'Username required'
    },
    'stars_sent_success': {
        'ru': '✅ Stars успешно отправлены!\n⭐ Количество: {stars}\n👤 Получатель: {recipient}',
        'en': '✅ Stars sent successfully!\n⭐ Amount: {stars}\n👤 Recipient: {recipient}'
    },

    # === Процесс покупки Stars ===
    'checking_recipient': {
        'ru': '🔍 Проверяю получателя...',
        'en': '🔍 Checking recipient...'
    },
    'user_not_found': {
        'ru': '❌ Пользователь @{username} не найден!\n\nУбедитесь, что username написан правильно.',
        'en': '❌ User @{username} not found!\n\nMake sure the username is correct.'
    },
    'service_unavailable': {
        'ru': '⚠️ Сервис временно недоступен. Попробуйте позже.',
        'en': '⚠️ Service temporarily unavailable. Please try later.'
    },
    'creating_transaction': {
        'ru': '📝 Создаю транзакцию...',
        'en': '📝 Creating transaction...'
    },
    'sending_transaction': {
        'ru': '📤 Отправляю транзакцию...',
        'en': '📤 Sending transaction...'
    },
    'transaction_processing': {
        'ru': '⏳ Транзакция обрабатывается...\n\n⭐ {stars} Stars будут отправлены {recipient}\n\nОбычно это занимает несколько секунд.',
        'en': '⏳ Transaction processing...\n\n⭐ {stars} Stars will be sent to {recipient}\n\nThis usually takes a few seconds.'
    },
    'processing_purchase': {
        'ru': '⏳ Обрабатываю покупку...',
        'en': '⏳ Processing purchase...'
    },
    'insufficient_for_purchase': {
        'ru': '❌ Недостаточно средств для этой покупки.',
        'en': '❌ Insufficient funds for this purchase.'
    },
    'purchase_error': {
        'ru': '❌ Ошибка покупки: {error}',
        'en': '❌ Purchase error: {error}'
    },
    'balance_changed': {
        'ru': '⚠️ Ваш баланс изменился. Проверьте и попробуйте снова.',
        'en': '⚠️ Your balance has changed. Check and try again.'
    },
    'processing_error': {
        'ru': '❌ Ошибка обработки. Попробуйте позже.',
        'en': '❌ Processing error. Please try later.'
    },
    'price_error': {
        'ru': '❌ Не удалось получить цену. Попробуйте позже.',
        'en': '❌ Could not get price. Please try later.'
    },

    # === Варианты Stars с ценами ===
    'btn_stars_100': {
        'ru': '⭐ 100 Stars',
        'en': '⭐ 100 Stars'
    },
    'btn_stars_100_price': {
        'ru': '⭐ 100 Stars - {price} ₽',
        'en': '⭐ 100 Stars - ${price}'
    },
    'btn_stars_500': {
        'ru': '⭐ 500 Stars',
        'en': '⭐ 500 Stars'
    },
    'btn_stars_500_price': {
        'ru': '⭐ 500 Stars - {price} ₽',
        'en': '⭐ 500 Stars - ${price}'
    },
    'btn_stars_1000': {
        'ru': '⭐ 1000 Stars',
        'en': '⭐ 1000 Stars'
    },
    'btn_stars_1000_price': {
        'ru': '⭐ 1000 Stars - {price} ₽',
        'en': '⭐ 1000 Stars - ${price}'
    },
    'btn_stars_5000': {
        'ru': '⭐ 5000 Stars',
        'en': '⭐ 5000 Stars'
    },
    'btn_stars_5000_price': {
        'ru': '⭐ 5000 Stars - {price} ₽',
        'en': '⭐ 5000 Stars - ${price}'
    },
    'btn_stars_10000': {
        'ru': '⭐ 10000 Stars',
        'en': '⭐ 10000 Stars'
    },
    'btn_stars_10000_price': {
        'ru': '⭐ 10000 Stars - {price} ₽',
        'en': '⭐ 10000 Stars - ${price}'
    },
    'btn_stars_custom': {
        'ru': '✏️ Свое количество',
        'en': '✏️ Custom Amount'
    },

    # === Баланс и цены ===
    'balance_info': {
        'ru': '💳 Ваш баланс:\n\n💎 {ton:.6f} TON\n💵 ≈ {currency_amount:.2f} {currency_symbol}',
        'en': '💳 Your balance:\n\n💎 {ton:.6f} TON\n💵 ≈ {currency_amount:.2f} {currency_symbol}'
    },
    'price_info': {
        'ru': '💵 Текущая цена 1 Star:\n\n💎 {price:.6f} TON\n💵 ≈ {rate:.2f} {currency}/TON',
        'en': '💵 Current price for 1 Star:\n\n💎 {price:.6f} TON\n💵 ≈ {rate:.2f} {currency}/TON'
    },

    # === TON платежи ===
    'ton_payment_title': {
        'ru': '💎 Пополнение через TON',
        'en': '💎 Top up via TON'
    },
    'ton_payment_instruction': {
        'ru': '📋 Инструкция:\n\n'
               '1️⃣ Отправьте от {min} TON на адрес:\n'
               '`{address}`\n\n'
               '2️⃣ В комментарии к переводу укажите:\n'
               '`{code}`\n\n'
               '3️⃣ После отправки нажмите кнопку "Проверить"',
        'en': '📋 Instructions:\n\n'
               '1️⃣ Send from {min} TON to address:\n'
               '`{address}`\n\n'
               '2️⃣ In the transfer comment, specify:\n'
               '`{code}`\n\n'
               '3️⃣ After sending, click "Check" button'
    },
    'btn_how_comment': {
        'ru': '❓ Как добавить комментарий?',
        'en': '❓ How to add comment?'
    },
    'how_comment_title': {
        'ru': '❓ Как добавить комментарий к переводу',
        'en': '❓ How to add comment to transfer'
    },
    'how_comment_text': {
        'ru': '📱 В Tonkeeper/Tonhub:\n'
               '• Нажмите на поле "Комментарий" под суммой\n'
               '• Введите код из инструкции\n'
               '• Отправьте перевод\n\n'
               '⚠️ Без комментария платеж не будет зачислен автоматически!',
        'en': '📱 In Tonkeeper/Tonhub:\n'
               '• Click on "Comment" field under amount\n'
               '• Enter the code from instructions\n'
               '• Send the transfer\n\n'
               '⚠️ Without comment, payment won\'t be credited automatically!'
    },
    'payment_found': {
        'ru': '✅ Платеж найден и зачислен!\n\n💰 Сумма: {amount:.4f} TON\n💳 Новый баланс: {balance:.4f} TON',
        'en': '✅ Payment found and credited!\n\n💰 Amount: {amount:.4f} TON\n💳 New balance: {balance:.4f} TON'
    },
    'payment_not_found': {
        'ru': '⏳ Платеж пока не найден\n\nЕсли вы уже отправили TON, подождите 1-2 минуты и проверьте снова.',
        'en': '⏳ Payment not found yet\n\nIf you already sent TON, wait 1-2 minutes and check again.'
    },

    # === xRocket ===
    'choose_token': {
        'ru': '💰 Выберите токен для оплаты:',
        'en': '💰 Choose payment token:'
    },
    'enter_usd_amount': {
        'ru': '💵 Введите сумму в USD для токена {token}:',
        'en': '💵 Enter USD amount for {token} token:'
    },
    'invoice_created': {
        'ru': '✅ Счет #{id} создан!\n\n'
               '💵 Сумма: {usd} USD\n'
               '💰 К оплате: {amount} {token}\n\n'
               'Нажмите кнопку ниже для оплаты:',
        'en': '✅ Invoice #{id} created!\n\n'
               '💵 Amount: {usd} USD\n'
               '💰 To pay: {amount} {token}\n\n'
               'Click button below to pay:'
    },
    'invoice_error': {
        'ru': '❌ Ошибка создания счета',
        'en': '❌ Error creating invoice'
    },
    'payment_confirmed': {
        'ru': '✅ Платеж подтвержден!\n💰 Зачислено: {amount} TON',
        'en': '✅ Payment confirmed!\n💰 Credited: {amount} TON'
    },
    'invoice_expired': {
        'ru': '⏰ Время оплаты счета истекло',
        'en': '⏰ Invoice payment time expired'
    },

    # === Казино (Casino) ===
    'casino': {
        'ru': '🎰 Казино',
        'en': '🎰 Casino'
    },
    'balance_spin': {
        'ru': '🎰 Казино',
        'en': '🎰 Casino'
    },
    'casino_button': {
        'ru': '🎰 Казино',
        'en': '🎰 Casino'
    },
    'spin_welcome': {
        'ru': '🎰 <b>Добро пожаловать в Balance Spin!</b>\n\n💰 Ваш баланс: {balance:.6f} TON\n\nИспытайте удачу в нашей игре!',
        'en': '🎰 <b>Welcome to Balance Spin!</b>\n\n💰 Your balance: {balance:.6f} TON\n\nTest your luck in our game!'
    },
    'spin_select_bet': {
        'ru': '💰 Выберите ставку',
        'en': '💰 Select Bet'
    },
    'spin_change_bet': {
        'ru': '🔄 Изменить ставку',
        'en': '🔄 Change Bet'
    },
    'spin_button': {
        'ru': '🎲 Крутить',
        'en': '🎲 Spin'
    },
    'spin_button_hot': {
        'ru': '🔥 Крутить (Горячий спин!)',
        'en': '🔥 Spin (Hot Spin!)'
    },
    'spin_again': {
        'ru': '🔄 Крутить еще',
        'en': '🔄 Spin Again'
    },
    'casino_spin_again': {
        'ru': '🔄 Крутить еще',
        'en': '🔄 Spin Again'
    },
    'spin_rules_btn': {
        'ru': '📋 Правила',
        'en': '📋 Rules'
    },
    'spin_rules': {
        'ru': '''📋 <b>Правила Balance Spin:</b>

🎰 <b>Комбинации и множители:</b>
• 777 - Джекпот! x15
• Три одинаковых BAR - x2
• Три винограда 🍇 - x3
• Три лимона 🍋 - x5
• Две семерки - x1.3
• Два лимона 🍋🍋 - x0.28
• Разные символы - x0

💰 <b>Ставки:</b>
• Минимум: 0.01 TON
• Максимум: ваш баланс

🔥 <b>Горячие спины:</b>
Случайные события с повышенными выигрышами!

Удачи! 🍀''',
        'en': '''📋 <b>Balance Spin Rules:</b>

🎰 <b>Combinations and multipliers:</b>
• 777 - Jackpot! x15
• Three BARs - x2
• Three grapes 🍇 - x3
• Three lemons 🍋 - x5
• Two sevens - x1.3
• Two lemons 🍋🍋 - x0.28
• Different symbols - x0

💰 <b>Bets:</b>
• Minimum: 0.01 TON
• Maximum: your balance

🔥 <b>Hot Spins:</b>
Random events with increased winnings!

Good luck! 🍀'''
    },
    'spin_custom_bet_btn': {
        'ru': '✏️ Своя ставка',
        'en': '✏️ Custom Bet'
    },
    'spin_enter_bet': {
        'ru': '💰 Введите вашу ставку в TON (минимум 0.01):',
        'en': '💰 Enter your bet in TON (minimum 0.01):'
    },
    'spin_invalid_bet': {
        'ru': '❌ Неверный формат! Введите число, например: 0.5',
        'en': '❌ Invalid format! Enter a number, for example: 0.5'
    },
    'spin_min_bet': {
        'ru': '❌ Минимальная ставка 0.01 TON!',
        'en': '❌ Minimum bet 0.01 TON!'
    },
    'spin_insufficient_balance': {
        'ru': '❌ Недостаточно средств! Ваш баланс: {balance:.6f} TON',
        'en': '❌ Insufficient funds! Your balance: {balance:.6f} TON'
    },
    'spin_spinning': {
        'ru': '🎰 Ставка {bet:.4f} TON принята!\n\nКручу барабаны...',
        'en': '🎰 Bet {bet:.4f} TON accepted!\n\nSpinning the reels...'
    },
    'casino_spinning': {
        'ru': '🎰 Ставка {bet:.4f} TON принята!\n\nКручу барабаны...',
        'en': '🎰 Bet {bet:.4f} TON accepted!\n\nSpinning the reels...'
    },
    'spin_back_to': {
        'ru': '🎰 Вернуться в Balance Spin',
        'en': '🎰 Back to Balance Spin'
    },
    'spin_main_menu': {
        'ru': '🏠 Главное меню',
        'en': '🏠 Main Menu'
    },
    'spin_jackpot': {
        'ru': '🎊 ДЖЕКПОТ! Три семерки!',
        'en': '🎊 JACKPOT! Three sevens!'
    },
    'spin_three_grapes': {
        'ru': '🍇 Три винограда!',
        'en': '🍇 Three grapes!'
    },
    'spin_three_lemons': {
        'ru': '🍋 Три лимона!',
        'en': '🍋 Three lemons!'
    },
    'spin_three_bars': {
        'ru': '🎰 Три BAR!',
        'en': '🎰 Three BARs!'
    },
    'spin_two_sevens': {
        'ru': '🎲 Две семерки',
        'en': '🎲 Two sevens'
    },
    'spin_pair_non7': {
        'ru': '🎯 Пара символов',
        'en': '🎯 Pair of symbols'
    },
    'spin_no_luck': {
        'ru': '😔 Не повезло',
        'en': '😔 No luck'
    },
    'spin_congrats': {
        'ru': '🎊 <b>ПОЗДРАВЛЯЕМ!</b> 🎊',
        'en': '🎊 <b>CONGRATULATIONS!</b> 🎊'
    },
    'spin_your_bet': {
        'ru': '💰 Ваша ставка:',
        'en': '💰 Your bet:'
    },
    'spin_win': {
        'ru': '🏆 Выигрыш:',
        'en': '🏆 Win:'
    },
    'spin_new_balance': {
        'ru': '💎 Новый баланс:',
        'en': '💎 New balance:'
    },
    'spin_balance': {
        'ru': '💳 Баланс:',
        'en': '💳 Balance:'
    },
    'spin_lost': {
        'ru': '💔 Ставка {bet:.4f} TON проиграна',
        'en': '💔 Bet {bet:.4f} TON lost'
    },
    'spin_try_again': {
        'ru': 'Попробуйте еще раз!',
        'en': 'Try again!'
    },
    'spin_cancel': {
        'ru': '❌ Отмена',
        'en': '❌ Cancel'
    },

    # === Дополнительные ключи для казино ===
    'casino_hot_event_started': {
        'ru': '🔥 Горячие спины активированы!',
        'en': '🔥 Hot Spins Event Started!'
    },
    'casino_menu': {
        'ru': '🎰 <b>Добро пожаловать в казино!</b>\n\n💰 Ваш баланс: {balance:.6f} TON',
        'en': '🎰 <b>Welcome to the casino!</b>\n\n💰 Your balance: {balance:.6f} TON'
    },
    'casino_choose_bet': {
        'ru': '💰 Выберите размер ставки:\n\n💳 Баланс: {balance:.6f} TON',
        'en': '💰 Choose bet amount:\n\n💳 Balance: {balance:.6f} TON'
    },
    'casino_bet_too_small': {
        'ru': '❌ Ставка слишком мала!',
        'en': '❌ Bet is too small!'
    },
    'casino_bet_saved': {
        'ru': '✅ Ставка сохранена: {bet:.4f} TON',
        'en': '✅ Bet saved: {bet:.4f} TON'
    },
    'casino_no_bet_selected': {
        'ru': '❌ Сначала выберите ставку!',
        'en': '❌ Please select a bet first!'
    },
    'casino_insufficient': {
        'ru': '❌ Недостаточно средств для игры!',
        'en': '❌ Insufficient funds to play!'
    },
    'casino_enter_bet': {
        'ru': '💰 Введите размер ставки в TON (минимум 0.01):\n\n💳 Баланс: {balance:.6f} TON',
        'en': '💰 Enter bet amount in TON (minimum 0.01):\n\n💳 Balance: {balance:.6f} TON'
    },
    'casino_invalid_format': {
        'ru': '❌ Неверный формат! Введите число.',
        'en': '❌ Invalid format! Enter a number.'
    },
    'casino_no_hot_event': {
        'ru': 'Событие завершено',
        'en': 'Event has ended'
    },
    'casino_back': {
        'ru': '⬅️ Назад',
        'en': '⬅️ Back'
    },
    'casino_back_to': {
        'ru': '🎰 Вернуться в казино',
        'en': '🎰 Back to Casino'
    },
    'casino_lost': {
        'ru': '💔 Ставка {bet:.4f} TON проиграна',
        'en': '💔 Bet {bet:.4f} TON lost'
    },
    'casino_try_again': {
        'ru': 'Попробуйте еще раз!',
        'en': 'Try again!'
    },
    'casino_rules_updated': {
        'ru': '''📋 <b>Правила казино:</b>

🎰 <b>Комбинации и выплаты:</b>
• 777 - Джекпот! x25
• Три лимона 🍋 - x5
• Три винограда 🍇 - x3
• Три BAR - x2
• Две семерки - x1.3
• Пара (не семерки) - x0.28
• Разные символы - x0

💰 <b>Ставки:</b>
• Минимум: 0.01 TON
• Максимум: ваш баланс

🔥 <b>Горячие спины:</b>
Случайные события с увеличенными выплатами!
Буст применяется на первый юнит ставки.

Удачи! 🍀''',
        'en': '''📋 <b>Casino Rules:</b>

🎰 <b>Combinations and payouts:</b>
• 777 - Jackpot! x25
• Three lemons 🍋 - x5
• Three grapes 🍇 - x3
• Three BARs - x2
• Two sevens - x1.3
• Pair (not sevens) - x0.28
• Different symbols - x0

💰 <b>Bets:</b>
• Minimum: 0.01 TON
• Maximum: your balance

🔥 <b>Hot Spins:</b>
Random events with increased payouts!
Boost applies to the first unit of the bet.

Good luck! 🍀'''
    },

    # === Callback-ответы ===
    'cb_checking': {
        'ru': '🔍 Проверяю...',
        'en': '🔍 Checking...'
    },
    'cb_preparing': {
        'ru': '⏳ Подготовка...',
        'en': '⏳ Preparing...'
    },

    # === Ошибки ===
    'error_occurred': {
        'ru': '❌ Произошла ошибка. Попробуйте позже.',
        'en': '❌ An error occurred. Please try later.'
    },
    'error_payment': {
        'ru': '❌ Ошибка при создании платежа.',
        'en': '❌ Error creating payment.'
    },
    'error_network': {
        'ru': '❌ Ошибка сети. Проверьте подключение.',
        'en': '❌ Network error. Check your connection.'
    },
    'error_timeout': {
        'ru': '⏰ Время ожидания истекло.',
        'en': '⏰ Timeout expired.'
    },
    'try_again': {
        'ru': 'Попробуйте еще раз',
        'en': 'Try again'
    },

    # === Дополнительные тексты ===
    'processing': {
        'ru': '⏳ Обработка...',
        'en': '⏳ Processing...'
    },
    'please_wait': {
        'ru': '⏳ Пожалуйста, подождите...',
        'en': '⏳ Please wait...'
    },
    'success': {
        'ru': '✅ Успешно!',
        'en': '✅ Success!'
    },
    'cancelled': {
        'ru': '❌ Отменено',
        'en': '❌ Cancelled'
    },
    'completed': {
        'ru': '✅ Завершено',
        'en': '✅ Completed'
    },
    'pending': {
        'ru': '⏳ Ожидание',
        'en': '⏳ Pending'
    },
    'failed': {
        'ru': '❌ Ошибка',
        'en': '❌ Failed'
    }
}


def get_user_lang(user_id: int) -> str:
    """Получить язык пользователя"""
    return USER_LANGUAGES.get(user_id, 'ru')


def set_user_lang(user_id: int, lang: str):
    """Установить язык пользователя"""
    if lang not in ['ru', 'en']:
        lang = 'ru'
    USER_LANGUAGES[user_id] = lang
    _save_user_languages()
    logger.info(f"Set language {lang} for user {user_id}")


def get_text(user_id: int, key: str, **kwargs) -> str:
    """Получить локализованный текст"""
    lang = get_user_lang(user_id)

    # Получаем перевод
    translation = TRANSLATIONS.get(key, {})

    if isinstance(translation, dict):
        text = translation.get(lang, translation.get('ru', key))
    else:
        text = translation

    # Форматируем текст с переданными параметрами
    try:
        return text.format(**kwargs)
    except:
        return text


def get_language_keyboard():
    """Получить клавиатуру выбора языка"""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🇷🇺 Русский", callback_data="set_lang_ru"),
            InlineKeyboardButton(text="🇬🇧 English", callback_data="set_lang_en")
        ]
    ])


# Загружаем настройки при импорте модуля
_load_user_languages()
