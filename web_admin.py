"""
web_admin.py - Защищенная веб-панель администратора для Stars Bot
Исправленная версия с корректной работой с БД
"""

import os
import json
import time
import hashlib
import secrets
import re
from datetime import datetime, timedelta
from functools import wraps

def check_admin(f):
    """Декоратор для проверки админских прав"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('X-Admin-Token')
        # Проверяем токен админа из переменной окружения
        admin_token = os.getenv('ADMIN_TOKEN', 'default_admin_token_change_me')
        if auth_header != admin_token:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated_function
from typing import Dict, Any, Optional, List

from flask import (
    Flask,
    render_template,
    render_template_string,
    request,
    jsonify,
    redirect,
    url_for,
    session,
    make_response,
    abort
)
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import requests
import certifi
from loguru import logger
from dotenv import load_dotenv
from markupsafe import escape
import bleach

import db_selector as db  # локальный модуль работы с БД
import dao_wallet as ton_wallet

load_dotenv()

# -------------------------
# Инициализация Flask с безопасностью
# -------------------------
app = Flask(__name__, template_folder="templates")

# Генерируем криптографически стойкий секретный ключ
app.secret_key = os.getenv(
    "FLASK_SECRET_KEY", 
    secrets.token_hex(32)  # 64 символа hex
)

# Настройки безопасности сессий
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.getenv("FLASK_ENV") == "production"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=24)
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024  # Максимум 1MB для запросов

# CORS с ограничениями
CORS(app, 
     supports_credentials=True,
     origins=os.getenv("ALLOWED_ORIGINS", "http://localhost:5001").split(","))

# Rate limiting для защиты от brute force
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

# -------------------------
# Конфигурация
# -------------------------
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "password")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WALLET_ADDRESS = os.getenv("TON_WALLET_ADDRESS")

# Защита от brute force - хранение попыток входа
login_attempts: Dict[str, list] = {}
MAX_LOGIN_ATTEMPTS = 5
LOGIN_COOLDOWN = 300  # 5 минут

# CSRF токены
csrf_tokens: Dict[str, float] = {}

# -------------------------
# Безопасные регулярные выражения для валидации
# -------------------------
PATTERNS = {
    'user_id': re.compile(r'^[0-9]{1,15}$'),
    'username': re.compile(r'^[\w\-\.@]{1,64}$'),
    'amount': re.compile(r'^-?\d{1,10}(\.\d{1,6})?$'),
    'fee': re.compile(r'^\d{1,3}(\.\d{1,2})?$'),
    'hash': re.compile(r'^[a-fA-F0-9]{1,64}$'),
    'spin_id': re.compile(r'^[a-fA-F0-9]{1,20}$'),
}

# -------------------------
# Хелперы безопасности
# -------------------------
def sanitize_input(value: Any, input_type: str = 'text') -> Optional[str]:
    """Очищает и валидирует входные данные"""
    if value is None:
        return None
    
    # Преобразуем в строку
    value = str(value).strip()
    
    # Ограничиваем длину
    if len(value) > 1000:
        value = value[:1000]
    
    # Экранируем HTML
    value = escape(value)
    
    # Дополнительная очистка для HTML
    if input_type == 'html':
        allowed_tags = []  # Никаких тегов не разрешаем
        value = bleach.clean(value, tags=allowed_tags, strip=True)
    
    return value

def validate_input(value: str, pattern_name: str) -> bool:
    """Валидирует входные данные по паттерну"""
    pattern = PATTERNS.get(pattern_name)
    if not pattern:
        return False
    return bool(pattern.match(value))

def generate_csrf_token() -> str:
    """Генерирует CSRF токен"""
    token = secrets.token_urlsafe(32)
    csrf_tokens[token] = time.time()
    
    # Очищаем старые токены (старше 1 часа)
    current_time = time.time()
    csrf_tokens_copy = csrf_tokens.copy()
    for t, timestamp in csrf_tokens_copy.items():
        if current_time - timestamp > 3600:
            del csrf_tokens[t]
    
    return token

def check_login_attempts(ip: str) -> bool:
    """Проверка количества попыток входа"""
    current_time = time.time()
    
    if ip not in login_attempts:
        login_attempts[ip] = []
    
    # Удаляем старые попытки
    login_attempts[ip] = [
        timestamp for timestamp in login_attempts[ip]
        if current_time - timestamp < LOGIN_COOLDOWN
    ]
    
    # Проверяем лимит
    if len(login_attempts[ip]) >= MAX_LOGIN_ATTEMPTS:
        return False
    
    return True

def record_login_attempt(ip: str):
    """Записывает попытку входа"""
    if ip not in login_attempts:
        login_attempts[ip] = []
    login_attempts[ip].append(time.time())

def login_required(f):
    """Декоратор для проверки авторизации с защитой"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            if request.path.startswith("/api"):
                logger.warning(f"Unauthorized API access attempt from {request.remote_addr}")
                abort(401)
            return redirect(url_for("login"))
        
        # Проверяем время сессии
        if session.get("login_time"):
            login_time = session.get("login_time")
            if time.time() - login_time > 86400:  # 24 часа
                session.clear()
                return redirect(url_for("login"))
        
        return f(*args, **kwargs)
    
    return decorated_function

def get_ton_rates():
    """Получает курсы TON безопасно"""
    try:
        r = requests.get(
            "https://min-api.cryptocompare.com/data/price",
            params={"fsym": "TON", "tsyms": "USD,RUB"},
            timeout=10,
            verify=certifi.where(),
        )
        r.raise_for_status()
        data = r.json()
        usd = float(data.get("USD", 0) or 0)
        rub = float(data.get("RUB", 0) or 0)
        return usd, rub
    except Exception as e:
        logger.error(f"Failed to get TON rates: {e}")
        return 0.0, 0.0

def get_wallet_balance():
    """Получает баланс TON-кошелька безопасно"""
    if not WALLET_ADDRESS:
        return 0.0
    
    # Валидация адреса кошелька
    if not re.match(r'^[A-Za-z0-9\-_]{48}$', WALLET_ADDRESS):
        logger.error(f"Invalid wallet address format")
        return 0.0
    
    try:
        r = requests.get(
            f"https://tonapi.io/v2/accounts/{WALLET_ADDRESS}",
            headers={"accept": "application/json"},
            timeout=15,
            verify=certifi.where(),
        )
        r.raise_for_status()
        bal = r.json().get("balance", 0)
        return float(bal) / 1e9
    except Exception as e:
        logger.error(f"Failed to get wallet balance: {e}")
        return 0.0

# -------------------------
# Роуты: аутентификация и UI
# -------------------------
@app.route("/")
@check_admin
def index():
    """Главная страница"""
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    
    # Генерируем CSRF токен для сессии
    if "csrf_token" not in session:
        session["csrf_token"] = generate_csrf_token()
    
    return render_template("admin_dashboard.html", csrf_token=session["csrf_token"])

@app.route("/login", methods=["GET", "POST"])
@check_admin
@limiter.limit("10 per hour")  # Максимум 10 попыток в час
def login():
    """Страница входа с защитой от brute force"""
    ip = request.remote_addr
    
    if request.method == "POST":
        # Проверяем количество попыток
        if not check_login_attempts(ip):
            logger.warning(f"Too many login attempts from {ip}")
            return render_template(
                "login.html", 
                error="Слишком много попыток. Попробуйте через 5 минут"
            ), 429
        
        # Валидация и санитизация входных данных
        username = sanitize_input(request.form.get("username", ""))
        password = sanitize_input(request.form.get("password", ""))
        
        # Записываем попытку
        record_login_attempt(ip)
        
        # Проверка учетных данных (constant-time comparison)
        username_match = secrets.compare_digest(username, ADMIN_USERNAME)
        password_match = secrets.compare_digest(password, ADMIN_PASSWORD)
        
        if username_match and password_match:
            session["logged_in"] = True
            session["login_time"] = time.time()
            session["user_ip"] = ip
            session.permanent = True
            session["csrf_token"] = generate_csrf_token()
            
            # Очищаем попытки входа при успехе
            if ip in login_attempts:
                del login_attempts[ip]
            
            logger.info(f"Admin logged in from {ip}")
            return redirect(url_for("index"))
        
        logger.warning(f"Failed login attempt from {ip} with username: {username[:10]}...")
        return render_template("login.html", error="Неверный логин или пароль")
    
    return render_template("login.html")

@app.route("/logout")
@check_admin
@login_required
def logout():
    """Выход из системы"""
    logger.info(f"Admin logged out from {session.get('user_ip', 'unknown')}")
    session.clear()
    return redirect(url_for("login"))

# -------------------------
# API роуты с защитой
# -------------------------
@app.route("/api/stats")
@check_admin
@login_required
@limiter.limit("60 per minute")
def api_stats():
    """API для получения статистики с валидацией"""
    try:
        wallet_balance = get_wallet_balance()
        internal_balance = db.get_internal()
        usd_rate, rub_rate = get_ton_rates()
        fee_percent = db.get_fee_percent()

        stats = db.get_statistics()
        users = db.get_all_users()

        # Санитизация данных пользователей
        enriched_users = []
        now = time.time()
        activity = {}
        
        for user in users:
            user_data = dict(user)
            uid = user_data.get("id") or user_data.get("user_id") or 0
            
            # Валидация user_id
            if not str(uid).isdigit():
                continue
            
            uid = int(uid)
            
            # Определение активности
            last_msg = user_data.get("last_message_time", 0)
            if now - last_msg < 86400:  # 24 часа
                activity[uid] = "active"
            elif now - last_msg < 604800:  # 7 дней
                activity[uid] = "recent"
            else:
                activity[uid] = "inactive"
            
            # Санитизация данных
            user_data["id"] = uid
            user_data["username"] = sanitize_input(user_data.get("username", f"User_{uid}"))
            user_data["balance"] = float(user_data.get("balance", 0))
            user_data["total_deposited"] = float(user_data.get("total_deposited", 0))
            user_data["total_bought"] = int(user_data.get("total_bought", 0))
            
            enriched_users.append(user_data)
        
        # Получаем топ пользователей по балансу
        top_users = sorted(
            enriched_users,
            key=lambda x: float(x.get("balance", 0)),
            reverse=True
        )[:10]
        
        # Форматируем статистику
        formatted_stats = {
            "wallet_balance": wallet_balance,
            "internal_balance": internal_balance,
            "fee_percent": fee_percent,
            "rates": {"usd": usd_rate, "rub": rub_rate},
            "stats": {
                "total_deals": stats.get("deposits", {}).get("count", 0),
                "total_stars": stats.get("purchases", {}).get("total_stars", 0),
                "total_balance": stats.get("users", {}).get("total_balance", 0)
            },
            "users": enriched_users,
            "top_users": top_users,
            "activity": activity,
            "users_count": len(enriched_users),
        }
        
        return jsonify(formatted_stats)
    except Exception as e:
        logger.error(f"Error in api_stats: {e}")
        abort(500)

@app.route("/api/user/search", methods=["POST"])
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_user_search():
    """API для поиска пользователей"""
    try:
        payload = request.get_json(silent=True) or {}
        query = sanitize_input(payload.get("query", ""))
        
        if not query:
            return jsonify([])
        
        users = db.get_all_users()
        results = []
        
        for user in users:
            user_data = dict(user)
            uid = str(user_data.get("id") or user_data.get("user_id", ""))
            username = str(user_data.get("username", ""))
            
            # Поиск по ID или username
            if query.lower() in uid or query.lower() in username.lower():
                results.append({
                    "id": int(uid) if uid.isdigit() else 0,
                    "username": sanitize_input(username),
                    "balance": float(user_data.get("balance", 0))
                })
                
                if len(results) >= 10:  # Максимум 10 результатов
                    break
        
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in user search: {e}")
        return jsonify([])

@app.route("/api/user/<user_id>")
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_user(user_id: str):
    """API для получения детальных данных пользователя"""
    # Валидация user_id
    if not validate_input(user_id, 'user_id'):
        logger.warning(f"Invalid user_id attempt: {user_id}")
        abort(400)
    
    try:
        user_id = int(user_id)
        user = db.get_user(user_id)
        
        if not user:
            abort(404)
        
        # Получаем дополнительную статистику
        user_deposits = db.get_user_deposits(user_id)
        user_purchases = db.get_user_purchases(user_id)
        user_spins = db.get_user_spins(user_id)
        
        # Подсчет статистики по спинам
        total_bet = sum(float(s.get("bet", 0)) for s in user_spins)
        total_win = sum(float(s.get("win", 0)) for s in user_spins)
        casino_profit = total_win - total_bet
        
        # Санитизация данных перед отправкой
        safe_user = {
            "id": user_id,
            "username": sanitize_input(user.get("username", "")),
            "balance": float(user.get("balance", 0)),
            "total_bought": int(user.get("total_bought", 0)),
            "total_deposited": float(user.get("total_deposited", 0)),
            "deposits_count": len(user_deposits),
            "purchases_count": len(user_purchases),
            "spins_count": len(user_spins),
            "total_bet": total_bet,
            "total_win": total_win,
            "casino_profit": casino_profit,
            "created_at": user.get("created_at", 0),
            "last_active": user.get("last_active", 0)
        }
        
        return jsonify(safe_user)
    except ValueError:
        abort(400)
    except Exception as e:
        logger.error(f"Error in api_user: {e}")
        abort(500)

@app.route("/api/user/<user_id>/transactions")
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_user_transactions(user_id: str):
    """API для получения транзакций пользователя"""
    # Валидация user_id
    if not validate_input(user_id, 'user_id'):
        abort(400)
    
    try:
        user_id = int(user_id)
        user_transactions = db.get_user_transactions(user_id)
        return jsonify(user_transactions)
    except Exception as e:
        logger.error(f"Error in user transactions: {e}")
        return jsonify([])

@app.route("/api/user/<user_id>/spins")
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_user_spins(user_id: str):
    """API для получения спинов пользователя"""
    # Валидация user_id
    if not validate_input(user_id, 'user_id'):
        abort(400)
    
    try:
        user_id = int(user_id)
        user_spins = db.get_user_spins(user_id)
        
        # Форматируем спины
        formatted_spins = []
        for spin in user_spins:
            formatted_spins.append({
                "spin_id": spin.get("spin_id", ""),
                "bet": float(spin.get("bet", 0)),
                "win": float(spin.get("win", 0)),
                "combo": spin.get("combo", ""),
                "mult": float(spin.get("mult", 0)),
                "timestamp": spin.get("timestamp", 0),
                "profit": float(spin.get("win", 0)) - float(spin.get("bet", 0))
            })
        
        # Сортируем по времени (новые сверху)
        formatted_spins.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return jsonify(formatted_spins[:100])  # Максимум 100 последних
    except Exception as e:
        logger.error(f"Error in user spins: {e}")
        return jsonify([])

@app.route("/api/spin/search", methods=["POST"])
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_spin_search():
    """API для поиска спина по хэшу"""
    try:
        payload = request.get_json(silent=True) or {}
        spin_hash = sanitize_input(payload.get("hash", ""))
        
        if not spin_hash:
            return jsonify({"error": "Hash required"}), 400
        
        # Ищем спин
        spin = db.find_spin_by_hash(spin_hash)
        
        if spin:
            # Получаем информацию о пользователе
            user_id = spin.get("user_id")
            user = db.get_user(user_id) if user_id else None
            
            return jsonify({
                "found": True,
                "spin": {
                    "spin_id": spin.get("spin_id", ""),
                    "user_id": user_id,
                    "username": user.get("username", f"User_{user_id}") if user else "Unknown",
                    "bet": float(spin.get("bet", 0)),
                    "win": float(spin.get("win", 0)),
                    "combo": spin.get("combo", ""),
                    "mult": float(spin.get("mult", 0)),
                    "timestamp": spin.get("timestamp", 0),
                    "profit": float(spin.get("win", 0)) - float(spin.get("bet", 0))
                }
            })
        
        return jsonify({"found": False, "error": "Spin not found"})
    except Exception as e:
        logger.error(f"Error in spin search: {e}")
        return jsonify({"error": "Search error"}), 500

@app.route("/api/user/<user_id>/balance", methods=["POST"])
@check_admin
@login_required
@limiter.limit("10 per minute")
def api_update_balance(user_id: str):
    """API для изменения баланса с защитой от инъекций"""
    # Валидация user_id
    if not validate_input(user_id, 'user_id'):
        logger.warning(f"Invalid user_id in balance update: {user_id}")
        abort(400)
    
    try:
        user_id = int(user_id)
        payload = request.get_json(silent=True) or {}
        
        # Валидация delta
        delta_str = str(payload.get("delta", "0"))
        if not validate_input(delta_str, 'amount'):
            logger.warning(f"Invalid amount format: {delta_str}")
            abort(400)
        
        delta = float(delta_str)
        comment = sanitize_input(payload.get("comment", ""))
        
        # Получаем текущий баланс
        current_balance = db.get_user_balance(user_id)
        if current_balance is None:
            # Если пользователь не существует, создаем его
            db.get_user(user_id)
            current_balance = 0.0
        
        # Вычисляем новый баланс
        new_balance = max(0, current_balance + delta)
        
        # Обновляем баланс
        if delta != 0:
            # Используем update_user_stat для обновления баланса
            db.update_user_stat(user_id, "balance", new_balance)
            
            # Логируем изменение
            db.log_transaction(
                user_id=user_id,
                type="admin_balance_change",
                amount=delta,
                description=f"Admin balance change. Comment: {comment}"
            )
            
            logger.info(f"Balance updated for user {user_id}: {current_balance} -> {new_balance} (comment: {comment})")
            
            return jsonify({
                "success": True,
                "old_balance": current_balance,
                "new_balance": new_balance,
                "delta": delta
            })
        else:
            return jsonify({"success": False, "error": "Delta is zero"}), 400
            
    except ValueError:
        abort(400)
    except Exception as e:
        logger.error(f"Error updating balance: {e}")
        abort(500)

# Остальные endpoints
@app.route("/api/wallets")
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_wallets():
    """API для получения списка кошельков"""
    try:
        wallets = db.get_all_wallets()
        safe_wallets = []
        
        for user_id, wallet in wallets.items():
            safe_wallets.append({
                "user_id": user_id,
                "address": wallet.get("address", "")[:10] + "..." if wallet.get("address") else "",
                "total_received": float(wallet.get("total_received", 0)),
                "created_at": wallet.get("created_at", 0)
            })
        
        return jsonify(safe_wallets)
    except Exception as e:
        logger.error(f"Error in api_wallets: {e}")
        return jsonify([])

@app.route("/api/deposits")
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_deposits():
    """API для получения списка депозитов"""
    try:
        deposits = db.get_deposits_list()
        
        safe_deposits = []
        for dep in deposits[-100:]:  # Последние 100
            safe_deposits.append({
                "user_id": dep.get("user_id"),
                "amount": float(dep.get("amount", 0)),
                "timestamp": dep.get("timestamp", 0),
                "hash": (dep.get("hash", "")[:10] + "...") if dep.get("hash") else ""
            })
        
        return jsonify(safe_deposits)
    except Exception as e:
        logger.error(f"Error in api_deposits: {e}")
        return jsonify([])

@app.route("/api/purchases")
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_purchases():
    """API для получения списка покупок Stars"""
    try:
        purchases = db.get_purchases_list()
        
        safe_purchases = []
        for pur in purchases[-100:]:  # Последние 100
            safe_purchases.append({
                "user_id": pur.get("user_id"),
                "stars": int(pur.get("stars", 0)),
                "amount": float(pur.get("amount", 0)),
                "timestamp": pur.get("timestamp", 0)
            })
        
        return jsonify(safe_purchases)
    except Exception as e:
        logger.error(f"Error in api_purchases: {e}")
        return jsonify([])

@app.route("/api/spins")
@check_admin
@login_required
@limiter.limit("30 per minute")
def api_spins():
    """API для получения списка спинов казино"""
    try:
        spins = db.get_spins_list()
        
        safe_spins = []
        for spin in spins[-100:]:  # Последние 100
            safe_spins.append({
                "user_id": spin.get("user_id"),
                "spin_id": spin.get("spin_id", ""),
                "bet": float(spin.get("bet", 0)),
                "win": float(spin.get("win", 0)),
                "combo": spin.get("combo", ""),
                "mult": float(spin.get("mult", 0)),
                "timestamp": spin.get("timestamp", 0)
            })
        
        return jsonify(safe_spins)
    except Exception as e:
        logger.error(f"Error in api_spins: {e}")
        return jsonify([])

@app.route("/api/fee", methods=["POST"])
@check_admin
@login_required
@limiter.limit("5 per minute")
def api_update_fee():
    """API для изменения комиссии"""
    try:
        payload = request.get_json(silent=True) or {}
        
        # Валидация fee
        fee_str = str(payload.get("fee", "0"))
        if not validate_input(fee_str, 'fee'):
            logger.warning(f"Invalid fee format: {fee_str}")
            abort(400)
        
        fee = float(fee_str)
        
        # Ограничиваем диапазон комиссии
        if fee < 0 or fee > 50:
            abort(400)
        
        # Обновляем комиссию
        db.set_fee_percent(fee)
        
        logger.info(f"Fee updated to {fee}% by admin")
        
        return jsonify({"success": True, "fee": fee})
    except ValueError:
        abort(400)
    except Exception as e:
        logger.error(f"Error updating fee: {e}")
        abort(500)

@app.route("/api/test-connection")
@check_admin
@login_required
@limiter.limit("10 per minute")
def api_test_connection():
    """API для тестирования подключений"""
    try:
        results = {}
        
        # Тест TON API
        try:
            balance = get_wallet_balance()
            results["TON API"] = {
                "status": balance >= 0,
                "message": f"Balance: {balance:.2f} TON" if balance >= 0 else "Failed"
            }
        except:
            results["TON API"] = {"status": False, "message": "Connection failed"}
        
        # Тест базы данных
        try:
            users_count = len(db.get_all_users())
            results["Database"] = {
                "status": True,
                "message": f"Users: {users_count}"
            }
        except:
            results["Database"] = {"status": False, "message": "Database error"}
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Error in test connection: {e}")
        abort(500)

# -------------------------
# Обработчики ошибок
# -------------------------
@app.errorhandler(400)
def bad_request(e):
    logger.warning(f"Bad request from {request.remote_addr}: {request.url}")
    return jsonify({"error": "Bad Request"}), 400

@app.errorhandler(401)
def unauthorized(e):
    logger.warning(f"Unauthorized access from {request.remote_addr}: {request.url}")
    return jsonify({"error": "Unauthorized"}), 401

@app.errorhandler(403)
def forbidden(e):
    logger.warning(f"Forbidden access from {request.remote_addr}: {request.url}")
    return jsonify({"error": "Forbidden"}), 403

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not Found"}), 404

@app.errorhandler(429)
def too_many_requests(e):
    logger.warning(f"Rate limit exceeded from {request.remote_addr}")
    return jsonify({"error": "Too Many Requests"}), 429

@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {e}")
    return jsonify({"error": "Internal Server Error"}), 500

# -------------------------
# Запуск приложения
# -------------------------

# === API ENDPOINTS ДЛЯ БЛОКИРОВКИ ===
@app.route("/api/user/<int:user_id>/block", methods=["POST"])
@check_admin
@login_required
def block_user_api(user_id):
    """Заблокировать пользователя"""
    try:
        data = request.json
        reason = data.get("reason", "")
        
        if db.block_user(user_id, reason):
            return jsonify({"success": True, "message": "Пользователь заблокирован"})
        else:
            return jsonify({"success": False, "error": "Пользователь не найден"})
    except Exception as e:
        logger.error(f"Error blocking user {user_id}: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/user/<int:user_id>/unblock", methods=["POST"])
@check_admin
@login_required
def unblock_user_api(user_id):
    """Разблокировать пользователя"""
    try:
        if db.unblock_user(user_id):
            return jsonify({"success": True, "message": "Пользователь разблокирован"})
        else:
            return jsonify({"success": False, "error": "Пользователь не найден"})
    except Exception as e:
        logger.error(f"Error unblocking user {user_id}: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/blocked-users", methods=["GET"])
@check_admin
@login_required
def get_blocked_users_api():
    """Получить список заблокированных пользователей"""
    try:
        users = db.get_blocked_users()
        return jsonify(users)
    except Exception as e:
        logger.error(f"Error getting blocked users: {e}")
        return jsonify([])

if __name__ == "__main__":
    # В продакшене используйте gunicorn или другой WSGI сервер
    app.run(
        host="127.0.0.1",  # Только локальный доступ
        port=5001,
        debug=False,  # НИКОГДА не включайте debug в продакшене
        use_reloader=False
    )





@app.route('/broadcast')
@login_required
def broadcast_page():
    """Страница рассылки"""
    users = db.get_all_users()
    total_users = len(users)
    
    return f'''<!DOCTYPE html>
<html>
<head>
    <title>📢 Рассылка</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{ max-width: 900px; margin: 0 auto; }}
        .header {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .header h1 {{ font-size: 24px; color: #333; }}
        .back-btn {{
            text-decoration: none;
            color: #667eea;
            font-weight: 600;
            padding: 8px 16px;
            border: 2px solid #667eea;
            border-radius: 6px;
            transition: all 0.3s;
        }}
        .back-btn:hover {{ background: #667eea; color: white; }}
        .form-card {{
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }}
        .form-group {{ margin-bottom: 24px; }}
        label {{
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
            color: #333;
            font-size: 14px;
        }}
        input[type="text"], textarea, select {{
            width: 100%;
            padding: 12px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 14px;
            transition: border 0.3s;
        }}
        input:focus, textarea:focus, select:focus {{
            outline: none;
            border-color: #667eea;
        }}
        textarea {{ min-height: 120px; resize: vertical; font-family: inherit; }}
        .radio-group {{
            display: flex;
            gap: 20px;
            margin-top: 8px;
        }}
        .radio-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            cursor: pointer;
        }}
        .radio-item input[type="radio"] {{
            width: 18px;
            height: 18px;
            cursor: pointer;
        }}
        .hidden {{ display: none; }}
        .buttons-section {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .button-row {{
            display: flex;
            gap: 10px;
            margin-bottom: 12px;
            padding: 12px;
            background: white;
            border-radius: 6px;
            border: 1px solid #e0e0e0;
        }}
        .button-row input {{
            flex: 1;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }}
        .button-row select {{
            width: 120px;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }}
        .remove-btn {{
            background: #dc3545;
            color: white;
            border: none;
            padding: 8px 12px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }}
        .add-btn {{
            background: #28a745;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 600;
        }}
        .submit-btn {{
            width: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 16px;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s;
        }}
        .submit-btn:hover {{ transform: translateY(-2px); }}
        .info-box {{
            background: #e7f3ff;
            border-left: 4px solid #2196F3;
            padding: 12px;
            border-radius: 4px;
            margin-bottom: 20px;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📢 Рассылка сообщений</h1>
            <a href="/" class="back-btn">← Назад</a>
        </div>
    </div>
        
        <div class="info-box">
            Всего пользователей в базе: <strong>{total_users}</strong>
        </div>
    </div>

        <form method="POST" action="/broadcast/send" class="form-card">
            <div class="form-group">
                <label>📝 Текст сообщения *</label>
                <textarea name="message" required placeholder="Введите текст рассылки (HTML разрешён)"></textarea>
            </div>
    </div>

            <div class="form-group">
                <label>🖼 URL картинки/гифки (необязательно)</label>
                <input type="text" name="photo_url" placeholder="https://example.com/image.jpg">
            </div>
    </div>

            <div class="form-group">
                <label>👥 Аудитория</label>
                <div class="radio-group">
                    <label class="radio-item">
                        <input type="radio" name="audience" value="all" checked onchange="toggleUsernames()">
                        <span>Всем пользователям</span>
                    </label>
                    <label class="radio-item">
                        <input type="radio" name="audience" value="selected" onchange="toggleUsernames()">
                        <span>Выбранным пользователям</span>
                    </label>
                </div>
    </div>
            </div>
    </div>

            <div class="form-group hidden" id="usernames-block">
                <label>📋 Юзернеймы пользователей</label>
                <input type="text" name="usernames" placeholder="username1, username2, username3">
                <small style="color: #666; display: block; margin-top: 4px;">
                    Введите через запятую или пробел (без @)
                </small>
            </div>
    </div>

            <div class="buttons-section">
                <label style="margin-bottom: 12px; display: block;">🔘 Инлайн-кнопки</label>
                <div id="buttons-container"></div>
                <button type="button" class="add-btn" onclick="addButton()">+ Добавить кнопку</button>
            </div>
    </div>

            <button type="submit" class="submit-btn">📤 Отправить рассылку</button>
        </form>
    </div>

    <script>
        let buttonCount = 0;

        function toggleUsernames() {{
            const selected = document.querySelector('input[name="audience"]:checked').value;
            const block = document.getElementById('usernames-block');
            if (selected === 'selected') {{
                block.classList.remove('hidden');
            }} else {{
                block.classList.add('hidden');
            }}
        }}

        function addButton() {{
            buttonCount++;
            const container = document.getElementById('buttons-container');
            const row = document.createElement('div');
            row.className = 'button-row';
            row.id = 'btn-' + buttonCount;
            row.innerHTML = `
                <input type="text" name="btn_text_${{buttonCount}}" placeholder="Текст кнопки" required>
                <input type="text" name="btn_url_${{buttonCount}}" placeholder="URL или callback" required>
                <select name="btn_type_${{buttonCount}}">
                    <option value="url">Ссылка</option>
                    <option value="callback">Callback</option>
                </select>
                <button type="button" class="remove-btn" onclick="removeButton(${{buttonCount}})">✕</button>
            `;
            container.appendChild(row);
        }}

        function removeButton(id) {{
            document.getElementById('btn-' + id).remove();
        }}
    </script>
</body>
</html>
'''

@app.route('/broadcast/send', methods=['POST'])
@login_required
def broadcast_send():
    """Отправка рассылки"""
    message_text = request.form.get('message', '').strip()
    photo_url = request.form.get('photo_url', '').strip() or None
    audience = request.form.get('audience', 'all')
    
    if not message_text:
        return '<h1>Ошибка: текст сообщения пустой</h1><a href="/broadcast">Назад</a>'
    
    # Получаем список user_id
    users = db.get_all_users()
    if audience == 'all':
        user_ids = [int(user.get('id') or user.get('user_id', 0)) for user in users if user.get('id') or user.get('user_id')]
    else:
        usernames_input = request.form.get('usernames', '')
        usernames = [u.strip().replace('@', '') for u in usernames_input.replace(',', ' ').split() if u.strip()]
        user_ids = []
        for user in users:
            if user.get('username') in usernames:
                uid = user.get('id') or user.get('user_id')
                if uid:
                    user_ids.append(int(uid))
    
    # Собираем кнопки
    buttons = []
    for key in request.form:
        if key.startswith('btn_text_'):
            num = key.split('_')[-1]
            btn_text = request.form.get(f'btn_text_{num}', '').strip()
            btn_url = request.form.get(f'btn_url_{num}', '').strip()
            btn_type = request.form.get(f'btn_type_{num}', 'url')
            if btn_text and btn_url:
                buttons.append({
                    'text': btn_text,
                    'url': btn_url if btn_type == 'url' else None,
                    'callback': btn_url if btn_type == 'callback' else None
                })
    
    # Отправляем рассылку
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(
        send_broadcast_to_users(user_ids, message_text, photo_url, buttons)
    )
    loop.close()
    
    return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Результат рассылки</title>
    <style>
        body {{{{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0;
        }}}}
        .result-card {{{{
            background: white;
            padding: 40px;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.2);
            text-align: center;
            max-width: 500px;
        }}}}
        .success-icon {{{{ font-size: 60px; margin-bottom: 20px; }}}}
        h1 {{{{ color: #28a745; margin-bottom: 20px; }}}}
        .stats {{{{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }}}}
        .stat-row {{{{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #e0e0e0;
        }}}}
        .stat-row:last-child {{{{ border-bottom: none; }}}}
        .buttons {{{{
            display: flex;
            gap: 12px;
            margin-top: 24px;
        }}}}
        .btn {{{{
            flex: 1;
            padding: 12px;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 600;
            transition: transform 0.2s;
        }}}}
        .btn:hover {{{{ transform: translateY(-2px); }}}}
        .btn-primary {{{{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }}}}
        .btn-secondary {{{{
            background: #6c757d;
            color: white;
        }}}}
    </style>
</head>
<body>
    <div class="result-card">
        <div class="success-icon">✅</div>
        <h1>Рассылка завершена</h1>
        <div class="stats">
            <div class="stat-row">
                <span>Успешно отправлено:</span>
                <strong style="color: #28a745;">{result['success']}</strong>
            </div>
    </div>
            <div class="stat-row">
                <span>Не удалось отправить:</span>
                <strong style="color: #dc3545;">{result['failed']}</strong>
            </div>
    </div>
            <div class="stat-row">
                <span>Всего пользователей:</span>
                <strong>{result['success'] + result['failed']}</strong>
            </div>
    </div>
        </div>
    </div>
        <div class="buttons">
            <a href="/broadcast" class="btn btn-secondary">Новая рассылка</a>
            <a href="/" class="btn btn-primary">На главную</a>
        </div>
    </div>
    </div>
</body>
</html>
'''

async def send_broadcast_to_users(user_ids, text, photo_url=None, buttons=None):
    """Отправка рассылки пользователям"""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    from aiogram import Bot
    import logging
    import asyncio
    
    logger = logging.getLogger(__name__)
    success = 0
    failed = 0
    
    # Используем BOT_TOKEN из глобальной области
    bot_instance = Bot(token=BOT_TOKEN)
    
    # Формируем клавиатуру
    keyboard = None
    if buttons:
        kb_buttons = []
        for btn in buttons:
            if btn.get('url'):
                kb_buttons.append([InlineKeyboardButton(text=btn['text'], url=btn['url'])])
            elif btn.get('callback'):
                kb_buttons.append([InlineKeyboardButton(text=btn['text'], callback_data=btn['callback'])])
        if kb_buttons:
            keyboard = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    
    try:
        for user_id in user_ids:
            try:
                if photo_url:
                    await bot_instance.send_photo(
                        chat_id=user_id,
                        photo=photo_url,
                        caption=text,
                        reply_markup=keyboard,
                        parse_mode='HTML'
                    )
                else:
                    await bot_instance.send_message(
                        chat_id=user_id,
                        text=text,
                        reply_markup=keyboard,
                        parse_mode='HTML'
                    )
                success += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Broadcast failed for {user_id}: {e}")
                failed += 1
    finally:
        await bot_instance.session.close()
    
    return {'success': success, 'failed': failed}

@app.route('/users')
@check_admin
@login_required
def users_page():
    """Страница списка всех пользователей"""
    return render_template_string('''
<!DOCTYPE html>
<html>
<head>
    <title>👥 Все пользователи</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .user-row:hover { background: #f3f4f6; }
        .filter-input { padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
    </style>
</head>
<body class="bg-gray-100">
    <nav class="bg-white shadow-lg">
        <div class="max-w-7xl mx-auto px-4">
            <div class="flex justify-between h-16">
                <div class="flex items-center">
                    <h1 class="text-xl font-semibold">👥 Все пользователи</h1>
                </div>
                <div class="flex items-center space-x-4">
                    <a href="/" class="text-blue-600 hover:text-blue-800">← Назад в админку</a>
                    <a href="/logout" class="text-gray-500 hover:text-gray-700">Выйти</a>
                </div>
            </div>
        </div>
    </nav>

    <div class="max-w-7xl mx-auto px-4 py-8">
        <!-- Фильтры -->
        <div class="bg-white rounded-lg shadow p-6 mb-6">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                <input type="text" id="search-username" class="filter-input" placeholder="🔍 Поиск по username...">
                <input type="number" id="search-id" class="filter-input" placeholder="🔍 Поиск по ID...">
                <select id="filter-activity" class="filter-input">
                    <option value="">Все</option>
                    <option value="active">Активные (24ч)</option>
                    <option value="recent">Недавние (7д)</option>
                    <option value="inactive">Неактивные</option>
                </select>
            </div>
        <!-- Статистика -->
        </div>
        <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-6">
            <div class="bg-white rounded-lg shadow p-6">
                <h3 class="text-gray-500 text-sm">Всего пользователей</h3>
                <p class="text-2xl font-bold" id="total-count">-</p>
            </div>
            <div class="bg-white rounded-lg shadow p-6">
                <h3 class="text-gray-500 text-sm">Активных (24ч)</h3>
                <p class="text-2xl font-bold text-green-600" id="active-count">-</p>
            </div>
            <div class="bg-white rounded-lg shadow p-6">
                <h3 class="text-gray-500 text-sm">Общий баланс</h3>
                <p class="text-2xl font-bold" id="total-balance">-</p>
            </div>
            <div class="bg-white rounded-lg shadow p-6">
                <h3 class="text-gray-500 text-sm">Всего депозитов</h3>
                <p class="text-2xl font-bold" id="total-deposited">-</p>
            </div>
        </div>
        <!-- Таблица пользователей -->
        <div class="bg-white rounded-lg shadow overflow-hidden">
            <table class="min-w-full">
                <thead class="bg-gray-50">
                    <tr>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Username</th>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Баланс</th>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Депозитов</th>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Куплено Stars</th>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Статус</th>
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Регистрация</th>
                    </tr>
                </thead>
                <tbody id="users-table" class="bg-white divide-y divide-gray-200">
                    <tr><td colspan="7" class="text-center py-8 text-gray-500">Загрузка...</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    </div>

    <script>
        let allUsers = [];
        let filteredUsers = [];

        async function loadUsers() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                allUsers = data.users || [];
                filteredUsers = allUsers;
                
                updateStats();
                renderTable();
                
                // Применяем фильтры
                document.getElementById('search-username').addEventListener('input', applyFilters);
                document.getElementById('search-id').addEventListener('input', applyFilters);
                document.getElementById('filter-activity').addEventListener('change', applyFilters);
            } catch (error) {
                console.error('Error loading users:', error);
            }
        }

        function updateStats() {
            const now = Date.now() / 1000;
            const active24h = allUsers.filter(u => (now - (u.last_active || 0)) < 86400).length;
            const totalBalance = allUsers.reduce((sum, u) => sum + (u.balance || 0), 0);
            const totalDeposited = allUsers.reduce((sum, u) => sum + (u.total_deposited || 0), 0);

            document.getElementById('total-count').textContent = allUsers.length;
            document.getElementById('active-count').textContent = active24h;
            document.getElementById('total-balance').textContent = totalBalance.toFixed(2) + ' TON';
            document.getElementById('total-deposited').textContent = totalDeposited.toFixed(2) + ' TON';
        }

        function applyFilters() {
            const usernameQuery = document.getElementById('search-username').value.toLowerCase();
            const idQuery = document.getElementById('search-id').value;
            const activityFilter = document.getElementById('filter-activity').value;
            const now = Date.now() / 1000;

            filteredUsers = allUsers.filter(user => {
                // Фильтр по username
                if (usernameQuery && !user.username.toLowerCase().includes(usernameQuery)) {
                    return false;
                }
                
                // Фильтр по ID
                if (idQuery && !String(user.id).includes(idQuery)) {
                    return false;
                }
                
                // Фильтр по активности
                if (activityFilter) {
                    const lastActive = user.last_active || 0;
                    const diff = now - lastActive;
                    
                    if (activityFilter === 'active' && diff >= 86400) return false;
                    if (activityFilter === 'recent' && (diff < 86400 || diff >= 604800)) return false;
                    if (activityFilter === 'inactive' && diff < 604800) return false;
                }
                
                return true;
            });

            renderTable();
        }

        function renderTable() {
            const tbody = document.getElementById('users-table');
            
            if (filteredUsers.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" class="text-center py-8 text-gray-500">Пользователи не найдены</td></tr>';
                return;
            }

            const now = Date.now() / 1000;
            tbody.innerHTML = filteredUsers.map(user => {
                const lastActive = user.last_active || 0;
                const diff = now - lastActive;
                let statusBadge, statusText;
                
                if (diff < 86400) {
                    statusBadge = 'bg-green-100 text-green-800';
                    statusText = '🟢 Активен';
                } else if (diff < 604800) {
                    statusBadge = 'bg-yellow-100 text-yellow-800';
                    statusText = '🟡 Недавно';
                } else {
                    statusBadge = 'bg-gray-100 text-gray-800';
                    statusText = '⚪ Неактивен';
                }

                const createdDate = new Date((user.created_at || 0) * 1000).toLocaleDateString('ru-RU');

                return `
                    <tr class="user-row cursor-pointer" onclick="showUserModal(${user.id}, '${user.username}')">
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${user.id}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">@${user.username || 'user_' + user.id}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${(user.balance || 0).toFixed(4)} TON</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${(user.total_deposited || 0).toFixed(4)} TON</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${user.total_bought || 0}</td>
                        <td class="px-6 py-4 whitespace-nowrap">
                            <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${statusBadge}">
                                ${statusText}
                            </span>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${createdDate}</td>
                    </tr>
                `;
            }).join('');
        }

        // Модальное окно пользователя
        function showUserModal(userId, username) {
            const modal = document.createElement('div');
            modal.id = 'user-modal';
            modal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
            modal.innerHTML = `
                <div class="bg-white rounded-lg shadow-xl max-w-2xl w-full mx-4 max-h-[90vh] overflow-y-auto">
                    <div class="p-6">
                        <div class="flex justify-between items-center mb-4">
                            <h2 class="text-xl font-bold">👤 Пользователь #${userId}</h2>
                            <button onclick="closeUserModal()" class="text-gray-500 hover:text-gray-700 text-2xl">&times;</button>
                        </div>
                        <div id="user-modal-content">
                            <p class="text-center text-gray-500">Загрузка...</p>
                        </div>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            modal.onclick = (e) => { if (e.target === modal) closeUserModal(); };
            loadUserDetails(userId);
        }
        
        function closeUserModal() {
            const modal = document.getElementById('user-modal');
            if (modal) modal.remove();
        }
        
        async function loadUserDetails(userId) {
            try {
                const res = await fetch(`/api/user/${userId}`);
                if (!res.ok) throw new Error('User not found');
                const user = await res.json();
                
                const content = document.getElementById('user-modal-content');
                content.innerHTML = `
                    <div class="space-y-4">
                        <div class="grid grid-cols-2 gap-4">
                            <div class="bg-gray-50 p-3 rounded">
                                <p class="text-sm text-gray-500">Username</p>
                                <p class="font-medium">@${user.username || 'N/A'}</p>
                            </div>
                            <div class="bg-gray-50 p-3 rounded">
                                <p class="text-sm text-gray-500">ID</p>
                                <p class="font-medium">${user.id}</p>
                            </div>
                            <div class="bg-blue-50 p-3 rounded">
                                <p class="text-sm text-gray-500">Баланс</p>
                                <p class="font-medium text-blue-600">${user.balance.toFixed(4)} TON</p>
                            </div>
                            <div class="bg-green-50 p-3 rounded">
                                <p class="text-sm text-gray-500">Депозиты</p>
                                <p class="font-medium text-green-600">${user.total_deposited.toFixed(4)} TON</p>
                            </div>
                            <div class="bg-purple-50 p-3 rounded">
                                <p class="text-sm text-gray-500">Спинов</p>
                                <p class="font-medium">${user.spins_count}</p>
                            </div>
                            <div class="bg-yellow-50 p-3 rounded">
                                <p class="text-sm text-gray-500">Поставлено</p>
                                <p class="font-medium">${user.total_bet.toFixed(4)} TON</p>
                            </div>
                            <div class="bg-orange-50 p-3 rounded">
                                <p class="text-sm text-gray-500">Выиграно</p>
                                <p class="font-medium">${user.total_win.toFixed(4)} TON</p>
                            </div>
                            <div class="${user.casino_profit >= 0 ? 'bg-red-50' : 'bg-green-50'} p-3 rounded">
                                <p class="text-sm text-gray-500">Профит казино</p>
                                <p class="font-medium ${user.casino_profit >= 0 ? 'text-green-600' : 'text-red-600'}">${(-user.casino_profit).toFixed(4)} TON</p>
                            </div>
                        </div>
                        
                        <div class="border-t pt-4 mt-4">
                            <h3 class="font-medium mb-2">Управление балансом</h3>
                            <div class="flex gap-2">
                                <input type="number" id="balance-change" step="0.01" placeholder="Сумма" class="border rounded px-3 py-2 w-32">
                                <button onclick="changeBalance(${user.id}, 'add')" class="bg-green-500 text-white px-4 py-2 rounded hover:bg-green-600">+ Добавить</button>
                                <button onclick="changeBalance(${user.id}, 'subtract')" class="bg-red-500 text-white px-4 py-2 rounded hover:bg-red-600">- Списать</button>
                            </div>
                        </div>
                    </div>
                `;
            } catch (e) {
                document.getElementById('user-modal-content').innerHTML = 
                    '<p class="text-center text-red-500">Ошибка загрузки данных пользователя</p>';
            }
        }
        
        async function changeBalance(userId, action) {
            const amount = parseFloat(document.getElementById('balance-change').value);
            if (!amount || amount <= 0) {
                alert('Введите корректную сумму');
                return;
            }
            
            const finalAmount = action === 'subtract' ? -amount : amount;
            
            try {
                const res = await fetch(`/api/user/${userId}/balance`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({amount: finalAmount})
                });
                
                if (res.ok) {
                    alert('Баланс изменён!');
                    loadUserDetails(userId);
                    loadUsers();
                } else {
                    alert('Ошибка изменения баланса');
                }
            } catch (e) {
                alert('Ошибка: ' + e.message);
            }
        }
        
        // Загрузка при старте
        loadUsers();
        
        // Автообновление каждые 30 секунд
        setInterval(loadUsers, 30000);
    </script>
</body>
</html>
''')


# ============================================================
# PARTNERS MANAGEMENT - Управление партнёрами
# ============================================================

@app.route('/partners')
@app.route("/partners")
@login_required
def partners_page():
    return render_template_string('''
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Партнёры - Admin Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 min-h-screen">
    <nav class="bg-white shadow-sm border-b">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="flex justify-between h-16">
                <div class="flex items-center space-x-8">
                    <span class="text-xl font-bold text-yellow-500">⭐ RickStar Admin</span>
                    <a href="/dashboard" class="text-gray-600 hover:text-gray-900">Dashboard</a>
                    <a href="/users" class="text-gray-600 hover:text-gray-900">Users</a>
                    <a href="/partners" class="text-indigo-600 font-medium">Partners</a>
                    <a href="/broadcast" class="text-gray-600 hover:text-gray-900">Broadcast</a>
                </div>
                <div class="flex items-center">
                    <a href="/logout" class="text-gray-600 hover:text-gray-900">Выйти</a>
                </div>
            </div>
        </div>
    </nav>

    <main class="max-w-7xl mx-auto py-6 px-4">
        <!-- Partners List -->
        <div class="bg-white rounded-lg shadow mb-6">
            <div class="px-6 py-4 border-b">
                <h2 class="text-lg font-semibold">👥 Партнёры</h2>
            </div>
            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50">
                        <tr>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">User ID</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Уровень</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Чатов</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Объём</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Заработано</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Выведено</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Доступно</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Действия</th>
                        </tr>
                    </thead>
                    <tbody id="partners-table" class="bg-white divide-y divide-gray-200"></tbody>
                </table>
            </div>
        </div>

        <!-- Pending Withdrawals -->
        <div class="bg-white rounded-lg shadow">
            <div class="px-6 py-4 border-b flex justify-between items-center">
                <h2 class="text-lg font-semibold">💸 Запросы на вывод</h2>
                <span id="pending-count" class="bg-red-100 text-red-800 px-3 py-1 rounded-full text-sm font-medium">0</span>
            </div>
            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50">
                        <tr>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">User ID</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Сумма</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Кошелёк</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Дата</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Статус</th>
                            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Действия</th>
                        </tr>
                    </thead>
                    <tbody id="withdrawals-table" class="bg-white divide-y divide-gray-200"></tbody>
                </table>
            </div>
        </div>
    </main>

    <!-- Withdraw Modal -->
    <div id="withdraw-modal" class="fixed inset-0 bg-black bg-opacity-50 hidden flex items-center justify-center z-50">
        <div class="bg-white rounded-lg p-6 max-w-md w-full mx-4">
            <h3 class="text-lg font-semibold mb-4">Обработка вывода</h3>
            <p id="modal-content" class="mb-4"></p>
            <input type="text" id="tx-hash" placeholder="TX Hash (опционально)" class="w-full border rounded px-3 py-2 mb-4">
            <div class="flex space-x-3">
                <button onclick="processWithdrawal('completed')" class="flex-1 bg-green-600 text-white py-2 rounded hover:bg-green-700">✅ Подтвердить</button>
                <button onclick="processWithdrawal('rejected')" class="flex-1 bg-red-600 text-white py-2 rounded hover:bg-red-700">❌ Отклонить</button>
                <button onclick="closeModal()" class="flex-1 bg-gray-300 text-gray-700 py-2 rounded hover:bg-gray-400">Отмена</button>
            </div>
        </div>
    </div>

    <!-- Edit Partner Modal -->
    <div id="edit-partner-modal" class="fixed inset-0 bg-black bg-opacity-50 hidden flex items-center justify-center z-50">
        <div class="bg-white rounded-lg p-6 max-w-md w-full mx-4">
            <h3 class="text-lg font-semibold mb-4">✏️ Редактирование партнёра</h3>
            <p class="mb-4">ID: <b id="edit-partner-id"></b></p>
            <div class="mb-4">
                <label class="block text-sm font-medium mb-2">Уровень:</label>
                <select id="edit-level" class="w-full border rounded px-3 py-2">
                    <option value="bronze">🥉 Бронза</option>
                    <option value="silver">🥈 Серебро</option>
                    <option value="gold">🥇 Золото</option>
                </select>
                <button onclick="savePartnerLevel()" class="mt-2 w-full bg-indigo-600 text-white py-2 rounded hover:bg-indigo-700">Сохранить уровень</button>
            </div>
            <hr class="my-4">
            <div class="mb-4">
                <label class="block text-sm font-medium mb-2">Корректировка баланса (TON):</label>
                <input type="number" step="0.01" id="edit-balance-adj" placeholder="+1.5 или -0.5" class="w-full border rounded px-3 py-2">
                <input type="text" id="edit-reason" placeholder="Причина" class="w-full border rounded px-3 py-2 mt-2">
                <button onclick="adjustBalance()" class="mt-2 w-full bg-green-600 text-white py-2 rounded hover:bg-green-700">Применить</button>
            </div>
            <button onclick="closeEditModal()" class="w-full bg-gray-300 text-gray-700 py-2 rounded hover:bg-gray-400">Закрыть</button>
        </div>
    </div>

    <script>
        let currentWithdrawalId = null;
        let currentPartnerId = null;

        async function loadData() {
            const [partnersRes, withdrawalsRes] = await Promise.all([
                fetch("/api/partners/list"),
                fetch("/api/partners/withdrawals")
            ]);
            const partners = await partnersRes.json();
            const withdrawals = await withdrawalsRes.json();

            const levels = {bronze: "🥉 Бронза", silver: "🥈 Серебро", gold: "🥇 Золото"};
            document.getElementById("partners-table").innerHTML = partners.map(p => `
                <tr>
                    <td class="px-4 py-3">${p.owner_id}</td>
                    <td class="px-4 py-3">${levels[p.level] || p.level}</td>
                    <td class="px-4 py-3">${p.chats_count}</td>
                    <td class="px-4 py-3">${p.total_volume.toFixed(2)} TON</td>
                    <td class="px-4 py-3">${p.total_earned.toFixed(4)} TON</td>
                    <td class="px-4 py-3">${p.withdrawn.toFixed(4)} TON</td>
                    <td class="px-4 py-3 text-green-600 font-medium">${p.available.toFixed(4)} TON</td>
                    <td class="px-4 py-3"><a href="#" onclick="editPartner(${p.owner_id}, '${p.level}', ${p.available})" class="text-indigo-600 hover:underline">✏️ Изменить</a></td>
                </tr>
            `).join("") || '<tr><td colspan=8 class="px-4 py-3 text-center text-gray-500">Нет партнёров</td></tr>';

            const pending = withdrawals.filter(w => w.status === "pending");
            document.getElementById("pending-count").textContent = pending.length;
            document.getElementById("withdrawals-table").innerHTML = withdrawals.map(w => {
                const statusClass = w.status === "completed" ? "bg-green-100 text-green-800" : w.status === "rejected" ? "bg-red-100 text-red-800" : "bg-yellow-100 text-yellow-800";
                const actionBtn = w.status === "pending" ? `<a href="#" onclick="openModal('${w.id}')" class="text-indigo-600 hover:underline">Обработать</a>` : "-";
                return `<tr>
                    <td class="px-4 py-3 text-sm">${w.id}</td>
                    <td class="px-4 py-3">${w.owner_id}</td>
                    <td class="px-4 py-3 font-medium">${w.amount.toFixed(2)} TON</td>
                    <td class="px-4 py-3 text-sm">${w.wallet_address.slice(0,15)}...</td>
                    <td class="px-4 py-3 text-sm">${new Date(w.created_at * 1000).toLocaleString("ru")}</td>
                    <td class="px-4 py-3"><span class="px-2 py-1 rounded-full text-xs ${statusClass}">${w.status}</span></td>
                    <td class="px-4 py-3">${actionBtn}</td>
                </tr>`;
            }).join("") || '<tr><td colspan=7 class="px-4 py-3 text-center text-gray-500">Нет запросов</td></tr>';
        }

        function openModal(id) {
            currentWithdrawalId = id;
            document.getElementById("modal-content").innerHTML = "ID: <b>" + id + "</b>";
            document.getElementById("withdraw-modal").classList.remove("hidden");
        }

        function closeModal() {
            document.getElementById("withdraw-modal").classList.add("hidden");
            currentWithdrawalId = null;
        }

        async function processWithdrawal(status) {
            const txHash = document.getElementById("tx-hash").value;
            const res = await fetch("/api/partners/withdrawal/process", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({withdrawal_id: currentWithdrawalId, status, tx_hash: txHash})
            });
            if (res.ok) { closeModal(); loadData(); }
            else { alert("Ошибка обработки"); }
        }

        function editPartner(ownerId, level, available) {
            currentPartnerId = ownerId;
            document.getElementById("edit-partner-id").textContent = ownerId;
            document.getElementById("edit-level").value = level;
            document.getElementById("edit-partner-modal").classList.remove("hidden");
        }

        function closeEditModal() {
            document.getElementById("edit-partner-modal").classList.add("hidden");
            currentPartnerId = null;
        }

        async function savePartnerLevel() {
            const level = document.getElementById("edit-level").value;
            const res = await fetch("/api/partners/set_level", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({owner_id: currentPartnerId, level})
            });
            if (res.ok) { closeEditModal(); loadData(); alert("Уровень сохранён"); }
            else { alert("Ошибка"); }
        }

        async function adjustBalance() {
            const adj = parseFloat(document.getElementById("edit-balance-adj").value);
            const reason = document.getElementById("edit-reason").value;
            if (isNaN(adj)) { alert("Введите сумму"); return; }
            const res = await fetch("/api/partners/adjust_balance", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({owner_id: currentPartnerId, adjustment: adj, reason})
            });
            if (res.ok) { closeEditModal(); loadData(); alert("Баланс изменён"); }
            else { alert("Ошибка"); }
        }

        loadData();
        setInterval(loadData, 30000);
    </script>
</body>
</html>
''')

@app.route('/api/partners/list')
@login_required
def api_partners_list():
    """Список всех партнёров"""
    partners = db.get_all_partners()
    print(f"API partners: {partners}")
    return jsonify(partners)


@app.route('/api/partners/withdrawals')
@login_required
def api_partners_withdrawals():
    """Все запросы на вывод"""
    withdrawals = db.get_withdrawal_requests()
    return jsonify(withdrawals)


@app.route('/api/partners/withdrawal/process', methods=['POST'])
@login_required
def api_process_withdrawal():
    """Обработка запроса на вывод с автоматической отправкой TON"""
    data = request.get_json()
    withdrawal_id = data.get('withdrawal_id')
    status = data.get('status')
    tx_hash = data.get('tx_hash', '')
    
    if not withdrawal_id or status not in ('completed', 'rejected'):
        return jsonify({'error': 'Invalid params'}), 400
    
    # Получаем данные о выводе
    withdrawal = db.get_withdrawal_by_id(withdrawal_id)
    if not withdrawal:
        return jsonify({'error': 'Not found'}), 404
    
    owner_id = withdrawal.get('owner_id')
    amount = withdrawal.get('amount', 0)
    wallet_address = withdrawal.get('wallet_address', '')
    
    # Если подтверждаем - отправляем TON
    if status == 'completed' and not tx_hash:
        try:
            tx_hash = ton_wallet.send_ton_simple(
                to_address=wallet_address,
                amount_ton=amount,
                comment=f'RickStar Partner Withdrawal {withdrawal_id}'
            )
            logger.info(f'Partner withdrawal sent: {amount} TON to {wallet_address}, tx={tx_hash}')
        except Exception as e:
            logger.error(f'Failed to send TON: {e}')
            return jsonify({'error': f'Failed to send TON: {str(e)}'}), 500
    
    # Обновляем статус
    success = db.update_withdrawal_status(withdrawal_id, status, tx_hash)
    
    if success:
        # Отправляем уведомление партнёру
        try:
            if status == 'completed':
                msg = f'✅ Ваш запрос на вывод <b>{amount:.2f} TON</b> одобрен!'
                if tx_hash:
                    msg += f'\n\n🔗 <a href="https://tonviewer.com/transaction/{tx_hash}">Проверить транзакцию</a>'
                else:
                    msg += '\n\nТранзакция успешно отправлена!'
            else:
                msg = f'❌ Ваш запрос на вывод <b>{amount:.2f} TON</b> отклонён.'
            
            import requests as req
            req.post(
                f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
                json={'chat_id': owner_id, 'text': msg, 'parse_mode': 'HTML'}
            )
        except Exception as e:
            logger.warning(f'Failed to notify partner: {e}')
        
        return jsonify({'ok': True, 'tx_hash': tx_hash})
    else:
        return jsonify({'error': 'Failed to update status'}), 500


@app.route('/api/partners/set_level', methods=['POST'])
@login_required
def api_set_partner_level():
    """Установить уровень партнёра"""
    data = request.get_json()
    owner_id = data.get('owner_id')
    level = data.get('level')
    
    if not owner_id or not level:
        return jsonify({'error': 'Missing params'}), 400
    
    success = db.set_partner_level(int(owner_id), level)
    return jsonify({'ok': success})


@app.route('/api/partners/adjust_balance', methods=['POST'])
@login_required
def api_adjust_partner_balance():
    """Изменить баланс партнёра"""
    data = request.get_json()
    owner_id = data.get('owner_id')
    amount = data.get('amount')
    reason = data.get('reason', 'Admin adjustment')
    
    if not owner_id or amount is None:
        return jsonify({'error': 'Missing params'}), 400
    
    success = db.adjust_partner_balance(int(owner_id), float(amount), reason)
    return jsonify({'ok': success})
