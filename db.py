#    
DB_PATH = 'data/bot_data.db'

"""
db.py -       (JSON )
     -
"""

import json
import os
import time
import threading
from typing import Dict, Any, Optional, List
from loguru import logger
from decimal import Decimal, ROUND_DOWN

# ========================
#   
# ========================

DATA_DIR = "data"
USERS_FILE = os.path.join(DATA_DIR, "users.json")
WALLETS_FILE = os.path.join(DATA_DIR, "wallets.json")
DEPOSITS_FILE = os.path.join(DATA_DIR, "deposits.json")
PURCHASES_FILE = os.path.join(DATA_DIR, "purchases.json")
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
SPINS_FILE = os.path.join(DATA_DIR, "spins.json")
TRANSACTIONS_FILE = os.path.join(DATA_DIR, "transactions.json")

#    
LOCK = threading.RLock()

# Быстрый кэш для балансов (обновляется каждые 5 сек)
_balance_cache: Dict[int, tuple] = {}  # user_id -> (balance, timestamp)
_BALANCE_TTL = 5.0  # секунд

def _get_cached_balance(user_id: int) -> Optional[float]:
    """Получить баланс из быстрого кэша"""
    if user_id in _balance_cache:
        bal, ts = _balance_cache[user_id]
        if time.time() - ts < _BALANCE_TTL:
            return bal
    return None

def _set_cached_balance(user_id: int, balance: float):
    """Сохранить баланс в быстрый кэш"""
    _balance_cache[user_id] = (balance, time.time())

def _invalidate_balance(user_id: int):
    """Сбросить кэш баланса"""
    _balance_cache.pop(user_id, None)


# ========================
#    I/O
# ========================

class JSONCache:
    """  JSON    """
    
    def __init__(self, write_delay: float = 1.0):
        self._cache: Dict[str, Any] = {}
        self._dirty: Dict[str, bool] = {}
        self._write_delay = write_delay
        self._timers: Dict[str, threading.Timer] = {}
        self._lock = threading.RLock()
    
    def load(self, filepath: str, default: Any = None) -> Any:
        """     """
        with self._lock:
            #  
            if filepath in self._cache:
                return self._cache[filepath]
            
            #   
            try:
                if os.path.exists(filepath):
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        self._cache[filepath] = data
                        return data
            except Exception as e:
                logger.error(f"Error loading {filepath}: {e}")
            
            #    
            self._cache[filepath] = default
            return default
    
    def save(self, filepath: str, data: Any, immediate: bool = False):
        """    """
        with self._lock:
            self._cache[filepath] = data
            self._dirty[filepath] = True
            
            if immediate:
                self._write_now(filepath)
            else:
                self._schedule_write(filepath)
    
    def _schedule_write(self, filepath: str):
        """  """
        #     
        if filepath in self._timers:
            self._timers[filepath].cancel()
        
        #   
        timer = threading.Timer(self._write_delay, self._write_now, args=[filepath])
        self._timers[filepath] = timer
        timer.start()
    
    def _write_now(self, filepath: str):
        """   """
        with self._lock:
            if not self._dirty.get(filepath, False):
                return
            
            try:
                #    
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                
                #    
                temp_file = filepath + '.tmp'
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self._cache[filepath], f, ensure_ascii=False, indent=2)
                
                #  
                os.replace(temp_file, filepath)
                
                self._dirty[filepath] = False
                
                #  
                if filepath in self._timers:
                    del self._timers[filepath]
                    
            except Exception as e:
                logger.error(f"Error saving {filepath}: {e}")
    
    def flush_all(self):
        """   """
        with self._lock:
            for filepath in list(self._dirty.keys()):
                if self._dirty[filepath]:
                    self._write_now(filepath)

#   
_cache = JSONCache(write_delay=3.0)

# ========================
# 
# ========================

def init():
    """  """
    os.makedirs(DATA_DIR, exist_ok=True)
    
    #     
    if not os.path.exists(USERS_FILE):
        _cache.save(USERS_FILE, {})
    
    if not os.path.exists(WALLETS_FILE):
        _cache.save(WALLETS_FILE, {})
    
    if not os.path.exists(DEPOSITS_FILE):
        _cache.save(DEPOSITS_FILE, [])
    
    if not os.path.exists(PURCHASES_FILE):
        _cache.save(PURCHASES_FILE, [])
    
    if not os.path.exists(SETTINGS_FILE):
        _cache.save(SETTINGS_FILE, {
            "fee_percent": 5.0,
            "internal_balance": 0.0,
            "min_purchase": 10,
            "max_purchase": 10000,
            "min_deposit": 0.1,
            "ton_rate": 5.5
        })
    
    if not os.path.exists(SPINS_FILE):
        _cache.save(SPINS_FILE, [])
    
    if not os.path.exists(TRANSACTIONS_FILE):
        _cache.save(TRANSACTIONS_FILE, [])
    
    logger.info("Database initialized")

# ========================
# 
# ========================

def get_user(user_id: int) -> Dict[str, Any]:
    """   """
    with LOCK:
        users = _cache.load(USERS_FILE, {})
        
        user_id_str = str(user_id)
        if user_id_str not in users:
            users[user_id_str] = {
                "id": user_id,
                "username": None,
                "balance": 0.0,
                "total_deposited": 0.0,
                "total_bought": 0,
                "created_at": time.time(),
                "last_active": time.time(),
                "last_message_time": time.time(),
                "language": "ru",
                "referrer": None,
                "referred_users": [],
                "spin_count": 0,
                "total_spin_win": 0.0,
                "total_spin_bet": 0.0
            }
            _cache.save(USERS_FILE, users)
        else:
            #  last_active
            users[user_id_str]["last_active"] = time.time()
            _cache.save(USERS_FILE, users)
        
        return users[user_id_str]

def update_user(user_id: int, data: Dict[str, Any]):
    """  """
    with LOCK:
        users = _cache.load(USERS_FILE, {})
        user_id_str = str(user_id)
        
        if user_id_str not in users:
            get_user(user_id)  #   
            users = _cache.load(USERS_FILE, {})
        
        users[user_id_str].update(data)
        users[user_id_str]["last_active"] = time.time()
        _cache.save(USERS_FILE, users)

def get_user_balance(user_id: int) -> float:
    """  """
    # Проверяем быстрый кэш
    cached = _get_cached_balance(user_id)
    if cached is not None:
        return cached
    
    user = get_user(user_id)
    balance = float(user.get("balance", 0))
    _set_cached_balance(user_id, balance)
    return balance

def update_user_balance(user_id: int, amount: float, operation: str = "set") -> bool:
    """  """
    _invalidate_balance(user_id)  # Сбрасываем кэш
    with LOCK:
        users = _cache.load(USERS_FILE, {})
        user_id_str = str(user_id)
        
        if user_id_str not in users:
            get_user(user_id)
            users = _cache.load(USERS_FILE, {})
        
        current = float(users[user_id_str].get("balance", 0))
        
        if operation == "add":
            new_balance = current + amount
        elif operation == "subtract":
            new_balance = current - amount
            if new_balance < 0:
                return False
        else:  # set
            new_balance = amount
        
        users[user_id_str]["balance"] = new_balance
        _cache.save(USERS_FILE, users)
        return True

def atomic_balance_change(user_id: int, delta: float) -> bool:
    """  """
    with LOCK:
        current = get_user_balance(user_id)
        new_balance = current + delta
        
        if new_balance < 0:
            return False
        
        return update_user_balance(user_id, new_balance, "set")

def update_user_stat(user_id: int, stat_name: str, value: Any):
    """  """
    with LOCK:
        users = _cache.load(USERS_FILE, {})
        user_id_str = str(user_id)
        
        if user_id_str not in users:
            get_user(user_id)
            users = _cache.load(USERS_FILE, {})
        
        users[user_id_str][stat_name] = value
        _cache.save(USERS_FILE, users)

def get_all_users() -> List[Dict[str, Any]]:
    """  """
    with LOCK:
        users = _cache.load(USERS_FILE, {})
        return list(users.values())

def get_user_count() -> int:
    """  """
    with LOCK:
        users = _cache.load(USERS_FILE, {})
        return len(users)

# ========================
# 
# ========================

def create_wallet(user_id: int, address: str) -> Dict[str, Any]:
    """   """
    with LOCK:
        wallets = _cache.load(WALLETS_FILE, {})
        
        wallet = {
            "user_id": user_id,
            "address": address,
            "created_at": time.time(),
            "last_checked": 0,
            "total_received": 0.0,
            "last_tx_lt": None,
            "last_tx_hash": None
        }
        
        wallets[str(user_id)] = wallet
        _cache.save(WALLETS_FILE, wallets)
        return wallet

def get_wallet(user_id: int) -> Optional[Dict[str, Any]]:
    """  """
    with LOCK:
        wallets = _cache.load(WALLETS_FILE, {})
        return wallets.get(str(user_id))

def update_wallet(user_id: int, data: Dict[str, Any]):
    """  """
    with LOCK:
        wallets = _cache.load(WALLETS_FILE, {})
        user_id_str = str(user_id)
        
        if user_id_str in wallets:
            wallets[user_id_str].update(data)
            _cache.save(WALLETS_FILE, wallets)

def get_all_wallets() -> Dict[str, Dict[str, Any]]:
    """  """
    with LOCK:
        return _cache.load(WALLETS_FILE, {})

def get_wallet_by_address(address: str) -> Optional[Dict[str, Any]]:
    """   """
    with LOCK:
        wallets = _cache.load(WALLETS_FILE, {})
        for wallet in wallets.values():
            if wallet.get("address") == address:
                return wallet
        return None

# ========================
# 
# ========================

def log_deposit(user_id: int, amount: float, hash: str, from_address: str = None):
    """ """
    with LOCK:
        deposits = _cache.load(DEPOSITS_FILE, [])
        
        deposit = {
            "user_id": user_id,
            "amount": amount,
            "hash": hash,
            "from_address": from_address,
            "timestamp": time.time()
        }
        
        deposits.append(deposit)
        _cache.save(DEPOSITS_FILE, deposits)
        
        #   
        user = get_user(user_id)
        total_deposited = user.get("total_deposited", 0) + amount
        update_user_stat(user_id, "total_deposited", total_deposited)

def get_deposits(user_id: int = None, limit: int = 100) -> List[Dict[str, Any]]:
    """ """
    with LOCK:
        deposits = _cache.load(DEPOSITS_FILE, [])
        
        if user_id:
            deposits = [d for d in deposits if d.get("user_id") == user_id]
        
        return deposits[-limit:]

def is_deposit_processed(hash: str) -> bool:
    """,   """
    with LOCK:
        deposits = _cache.load(DEPOSITS_FILE, [])
        return any(d.get("hash") == hash for d in deposits)

# ========================
#  Stars
# ========================

def log_purchase(user_id: int, stars: int, amount: float, tx_hash: str = None):
    """  Stars"""
    with LOCK:
        purchases = _cache.load(PURCHASES_FILE, [])
        
        purchase = {
            "user_id": user_id,
            "stars": stars,
            "amount": amount,
            "tx_hash": tx_hash,
            "timestamp": time.time()
        }
        
        purchases.append(purchase)
        _cache.save(PURCHASES_FILE, purchases)
        
        #   
        user = get_user(user_id)
        total_bought = user.get("total_bought", 0) + stars
        update_user_stat(user_id, "total_bought", total_bought)

def get_purchases(user_id: int = None, limit: int = 100) -> List[Dict[str, Any]]:
    """ """
    with LOCK:
        purchases = _cache.load(PURCHASES_FILE, [])
        
        if user_id:
            purchases = [p for p in purchases if p.get("user_id") == user_id]
        
        return purchases[-limit:]

# ========================
#  
# ========================

def log_spin(user_id: int, spin_id: str = None, bet: float = 0, win: float = 0, 
             combo: str = None, mult: float = None, result: str = None, 
             multiplier: float = None, chat_id: int = None, **kwargs):
    """  """
    with LOCK:
        #      kwargs
        if spin_id is None:
            spin_id = kwargs.get('spin_hash', kwargs.get('hash', ''))
        if bet == 0:
            bet = kwargs.get('bet_amount', 0)
        if win == 0:
            win = kwargs.get('win_amount', 0)
        if combo is None:
            combo = kwargs.get('combination', '')
        if mult is None and multiplier is None:
            mult = kwargs.get('mult', kwargs.get('multiplier', 0))
        elif mult is None and multiplier is not None:
            mult = multiplier
        if result is None:
            result = kwargs.get('result', combo or '')
            
        #  spin_id   
        if not spin_id:
            import hashlib
            spin_id = hashlib.md5(f"{user_id}{time.time()}".encode()).hexdigest()[:8]
        
        spins = _cache.load(SPINS_FILE, [])
        
        # Получаем chat_id из kwargs если не передан напрямую
        if chat_id is None:
            chat_id = kwargs.get('chat_id')
        
        spin = {
            "user_id": user_id,
            "spin_id": spin_id,
            "bet": float(bet),
            "win": float(win),
            "combo": result or combo or "",
            "mult": float(mult) if mult else 0.0,
            "result": result or combo or "",
            "timestamp": time.time(),
            "chat_id": chat_id
        }
        
        spins.append(spin)
        
        # Ротация - храним только последние 10000 спинов
        MAX_SPINS = 10000
        if len(spins) > MAX_SPINS:
            spins = spins[-MAX_SPINS:]
        
        _cache.save(SPINS_FILE, spins)
        
        #   
        user = get_user(user_id)
        spin_count = user.get("spin_count", 0) + 1
        total_spin_bet = user.get("total_spin_bet", 0) + float(bet)
        total_spin_win = user.get("total_spin_win", 0) + float(win)
        
        update_user_stat(user_id, "spin_count", spin_count)
        update_user_stat(user_id, "total_spin_bet", total_spin_bet)
        update_user_stat(user_id, "total_spin_win", total_spin_win)
        
        return spin_id  #  ID 

def get_spin_by_id(spin_id: str) -> Optional[Dict[str, Any]]:
    """   ID"""
    with LOCK:
        spins = _cache.load(SPINS_FILE, [])
        for spin in spins:
            if spin.get("spin_id") == spin_id:
                return spin
        return None

# ========================
# 
# ========================

def log_transaction(user_id: int, type: str, amount: float, description: str = None):
    """ """
    with LOCK:
        transactions = _cache.load(TRANSACTIONS_FILE, [])
        
        transaction = {
            "user_id": user_id,
            "type": type,
            "amount": amount,
            "description": description,
            "timestamp": time.time()
        }
        
        transactions.append(transaction)
        _cache.save(TRANSACTIONS_FILE, transactions)

def get_transactions(user_id: int = None, limit: int = 100) -> List[Dict[str, Any]]:
    """ """
    with LOCK:
        transactions = _cache.load(TRANSACTIONS_FILE, [])
        
        if user_id:
            transactions = [t for t in transactions if t.get("user_id") == user_id]
        
        return transactions[-limit:]

# ========================
# 
# ========================

def get_settings() -> Dict[str, Any]:
    """ """
    with LOCK:
        return _cache.load(SETTINGS_FILE, {
            "fee_percent": 5.0,
            "internal_balance": 0.0,
            "min_purchase": 10,
            "max_purchase": 10000,
            "min_deposit": 0.1,
            "ton_rate": 5.5
        })

def update_settings(data: Dict[str, Any]):
    """ """
    with LOCK:
        settings = get_settings()
        settings.update(data)
        _cache.save(SETTINGS_FILE, settings)

def get_fee_percent() -> float:
    """  """
    settings = get_settings()
    return float(settings.get("fee_percent", 5.0))

def set_fee_percent(fee: float):
    """  """
    update_settings({"fee_percent": fee})

def get_internal() -> float:
    """  """
    settings = get_settings()
    return float(settings.get("internal_balance", 0))

def add_internal(amount: float):
    """   """
    with LOCK:
        settings = get_settings()
        current = float(settings.get("internal_balance", 0))
        settings["internal_balance"] = current + amount
        _cache.save(SETTINGS_FILE, settings)

def get_ton_rate() -> float:
    """  TON  USD"""
    settings = get_settings()
    return float(settings.get("ton_rate", 5.5))

def set_ton_rate(rate: float):
    """  TON  USD"""
    update_settings({"ton_rate": rate})

# ========================
# 
# ========================

def get_statistics() -> Dict[str, Any]:
    """  """
    with LOCK:
        users = _cache.load(USERS_FILE, {})
        deposits = _cache.load(DEPOSITS_FILE, [])
        purchases = _cache.load(PURCHASES_FILE, [])
        spins = _cache.load(SPINS_FILE, [])
        
        #   
        total_balance = sum(float(u.get("balance", 0)) for u in users.values())
        active_24h = sum(1 for u in users.values() 
                        if time.time() - u.get("last_active", 0) < 86400)
        active_7d = sum(1 for u in users.values() 
                       if time.time() - u.get("last_active", 0) < 604800)
        
        #   
        total_deposited = sum(float(d.get("amount", 0)) for d in deposits)
        deposits_24h = sum(float(d.get("amount", 0)) for d in deposits 
                          if time.time() - d.get("timestamp", 0) < 86400)
        
        #   
        total_stars_bought = sum(int(p.get("stars", 0)) for p in purchases)
        total_spent = sum(float(p.get("amount", 0)) for p in purchases)
        purchases_24h = len([p for p in purchases 
                            if time.time() - p.get("timestamp", 0) < 86400])
        
        #   
        total_bets = sum(float(s.get("bet", 0)) for s in spins)
        total_wins = sum(float(s.get("win", 0)) for s in spins)
        casino_profit = total_bets - total_wins
        spins_24h = len([s for s in spins 
                         if time.time() - s.get("timestamp", 0) < 86400])
        
        return {
            "users": {
                "total": len(users),
                "active_24h": active_24h,
                "active_7d": active_7d,
                "total_balance": total_balance
            },
            "deposits": {
                "count": len(deposits),
                "total": total_deposited,
                "last_24h": deposits_24h
            },
            "purchases": {
                "count": len(purchases),
                "total_stars": total_stars_bought,
                "total_spent": total_spent,
                "last_24h": purchases_24h
            },
            "spins": {
                "count": len(spins),
                "total_bets": total_bets,
                "total_wins": total_wins,
                "casino_profit": casino_profit,
                "last_24h": spins_24h
            }
        }

# ========================
# 
# ========================

def backup_database(backup_dir: str = "backups"):
    """    """
    import shutil
    from datetime import datetime
    
    with LOCK:
        #    
        _cache.flush_all()
        
        #    
        os.makedirs(backup_dir, exist_ok=True)
        
        #     
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f"backup_{timestamp}")
        
        #    
        shutil.copytree(DATA_DIR, backup_path)
        
        logger.info(f"Database backup created: {backup_path}")
        return backup_path

def restore_database(backup_path: str):
    """     """
    import shutil
    
    with LOCK:
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup not found: {backup_path}")
        
        #   
        if os.path.exists(DATA_DIR):
            shutil.rmtree(DATA_DIR)
        
        #   
        shutil.copytree(backup_path, DATA_DIR)
        
        #  
        _cache._cache.clear()
        _cache._dirty.clear()
        
        logger.info(f"Database restored from: {backup_path}")

def cleanup_old_data(days: int = 30):
    """  """
    with LOCK:
        cutoff_time = time.time() - (days * 86400)
        
        #   
        deposits = _cache.load(DEPOSITS_FILE, [])
        deposits = [d for d in deposits if d.get("timestamp", 0) > cutoff_time]
        _cache.save(DEPOSITS_FILE, deposits)
        
        #   
        purchases = _cache.load(PURCHASES_FILE, [])
        purchases = [p for p in purchases if p.get("timestamp", 0) > cutoff_time]
        _cache.save(PURCHASES_FILE, purchases)
        
        #   
        spins = _cache.load(SPINS_FILE, [])
        spins = [s for s in spins if s.get("timestamp", 0) > cutoff_time]
        _cache.save(SPINS_FILE, spins)
        
        #   
        transactions = _cache.load(TRANSACTIONS_FILE, [])
        transactions = [t for t in transactions if t.get("timestamp", 0) > cutoff_time]
        _cache.save(TRANSACTIONS_FILE, transactions)
        
        logger.info(f"Cleaned up data older than {days} days")

# ========================
# 
# ========================

def test_all_services() -> Dict[str, bool]:
    """   """
    results = {}
    
    try:
        #  
        test_user = get_user(999999999)
        results["users"] = test_user is not None
    except:
        results["users"] = False
    
    try:
        #  
        settings = get_settings()
        results["settings"] = settings is not None
    except:
        results["settings"] = False
    
    try:
        #  
        stats = get_statistics()
        results["statistics"] = stats is not None
    except:
        results["statistics"] = False
    
    return results

# ===   - ===
def get_deposits_list() -> List[Dict]:
    """    -"""
    with LOCK:
        try:
            deposits = _cache.load(DEPOSITS_FILE, [])
            if isinstance(deposits, list):
                return deposits
            return []
        except Exception as e:
            logger.error(f"Error loading deposits: {e}")
            return []

def get_purchases_list() -> List[Dict]:
    """    -"""
    with LOCK:
        try:
            purchases = _cache.load(PURCHASES_FILE, [])
            if isinstance(purchases, list):
                return purchases
            return []
        except Exception as e:
            logger.error(f"Error loading purchases: {e}")
            return []

def get_spins_list() -> List[Dict]:
    """    -"""
    with LOCK:
        try:
            spins = _cache.load(SPINS_FILE, [])
            if isinstance(spins, list):
                return spins
            return []
        except Exception as e:
            logger.error(f"Error loading spins: {e}")
            return []

def get_transactions_list() -> List[Dict]:
    """    -"""
    with LOCK:
        try:
            transactions = _cache.load(TRANSACTIONS_FILE, [])
            if isinstance(transactions, list):
                return transactions
            return []
        except Exception as e:
            logger.error(f"Error loading transactions: {e}")
            return []

def find_spin_by_hash(spin_hash: str) -> Optional[Dict]:
    """   """
    try:
        spins = get_spins_list()
        spin_hash = spin_hash.strip().replace("#", "").lower()
        
        for spin in spins:
            spin_id = str(spin.get("spin_id", "")).lower()
            if spin_id == spin_hash or spin_id.startswith(spin_hash) or spin_hash in spin_id:
                return spin
        return None
    except Exception as e:
        logger.error(f"Error finding spin: {e}")
        return None

def get_user_deposits(user_id: int) -> List[Dict]:
    """  """
    deposits = get_deposits_list()
    return [d for d in deposits if d.get("user_id") == user_id]

def get_user_purchases(user_id: int) -> List[Dict]:
    """  """
    purchases = get_purchases_list()
    return [p for p in purchases if p.get("user_id") == user_id]

def get_user_spins(user_id: int) -> List[Dict]:
    """  """
    spins = get_spins_list()
    return [s for s in spins if s.get("user_id") == user_id]

def get_user_transactions(user_id: int) -> List[Dict]:
    """   """
    transactions = []
    
    # 
    for dep in get_user_deposits(user_id):
        transactions.append({
            "type": "deposit",
            "amount": float(dep.get("amount", 0)),
            "timestamp": dep.get("timestamp", 0),
            "description": " TON",
            "hash": (dep.get("hash", "")[:10] + "...") if dep.get("hash") else ""
        })
    
    # 
    for pur in get_user_purchases(user_id):
        transactions.append({
            "type": "purchase",
            "amount": -float(pur.get("amount", 0)),
            "timestamp": pur.get("timestamp", 0),
            "description": f" {pur.get('stars', 0)} Stars",
            "hash": ""
        })
    
    #  
    for tx in get_transactions_list():
        if tx.get("user_id") == user_id:
            transactions.append({
                "type": tx.get("type", "unknown"),
                "amount": float(tx.get("amount", 0)),
                "timestamp": tx.get("timestamp", 0),
                "description": tx.get("description", ""),
                "hash": ""
            })
    
    #   
    transactions.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    return transactions[:100]  #  100

#   
init()
def init_schema():
    """    ( )"""
    #       bot.py
    #      init()
    pass

#    bot.py
def ensure_user(user_id: int, username: str = None):
    """   (   bot.py)"""
    user = get_user(user_id)
    if username and username != user.get("username"):
        update_user(user_id, {"username": username})
    return user

def init_schema():
    """    ( )"""
    pass

def add_deposit(user_id: int, amount: float, tx_hash: str, from_address: str = None):
    """  ( )"""
    log_deposit(user_id, amount, tx_hash, from_address)
    return True

def add_purchase(user_id: int, stars: int, amount: float):
    """  ( )"""
    log_purchase(user_id, stars, amount)
    return True

def add_spin(user_id: int, spin_id: str = None, bet: float = 0, win: float = 0, 
             combo: str = None, mult: float = None, result: str = None, 
             multiplier: float = None, **kwargs):
    """  ( )"""
    return log_spin(user_id, spin_id, bet, win, combo, mult, result, multiplier, **kwargs)

def get_user_stats(user_id: int) -> Dict[str, Any]:
    """   ( )"""
    user = get_user(user_id)
    return {
        "total_deposited": user.get("total_deposited", 0),
        "total_bought": user.get("total_bought", 0),
        "spin_count": user.get("spin_count", 0),
        "total_spin_win": user.get("total_spin_win", 0),
        "total_spin_bet": user.get("total_spin_bet", 0)
    }

def set_user_language(user_id: int, language: str):
    """   ( )"""
    update_user_stat(user_id, "language", language)

def get_user_language(user_id: int) -> str:
    """   ( )"""
    user = get_user(user_id)
    return user.get("language", "ru")

#   
def get_user_saved_bet(user_id: int) -> Optional[float]:
    """     """
    user = get_user(user_id)
    return user.get("saved_bet", None)

def set_user_saved_bet(user_id: int, bet: float):
    """    """
    update_user_stat(user_id, "saved_bet", bet)

def get_user_casino_stats(user_id: int) -> Dict[str, Any]:
    """   """
    user = get_user(user_id)
    return {
        "spin_count": user.get("spin_count", 0),
        "total_spin_win": user.get("total_spin_win", 0),
        "total_spin_bet": user.get("total_spin_bet", 0),
        "saved_bet": user.get("saved_bet", None)
    }

def save_user_bet(user_id: int, bet: float):
    """   (  set_user_saved_bet)"""
    set_user_saved_bet(user_id, bet)








def test_block_user(user_id):
    """   """
    import sqlite3
    import os
    
    #  
    db_path = 'data/bot_data.db'
    print(f"DB path: {db_path}")
    print(f"DB exists: {os.path.exists(db_path)}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # ,   
    cursor.execute("SELECT id, username, balance, is_blocked FROM users WHERE id = ?", (user_id,))
    user = cursor.fetchone()
    if user:
        print(f"User found: ID={user[0]}, username={user[1]}, balance={user[2]}, is_blocked={user[3]}")
    else:
        print(f"User {user_id} NOT FOUND in database")
    
    #  
    cursor.execute("UPDATE users SET is_blocked = 1 WHERE id = ?", (user_id,))
    print(f"Rows affected by UPDATE: {cursor.rowcount}")
    
    conn.commit()
    conn.close()
    return cursor.rowcount > 0







# ===    ===
def block_user(user_id, reason=''):
    """ """
    import time
    import json
    import os
    
    user_id_str = str(user_id)
    users_file = 'data/users.json'
    
    try:
        #  
        with open(users_file, 'r', encoding='utf-8') as f:
            users = json.load(f)
        
        if user_id_str not in users:
            print(f"User {user_id_str} not found in users.json")
            return False
        
        #  
        users[user_id_str]['is_blocked'] = True
        users[user_id_str]['blocked_at'] = int(time.time())
        users[user_id_str]['blocked_reason'] = reason
        
        #    
        with open(users_file, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        
        print(f"User {user_id_str} successfully blocked")
        
        #    
        import sqlite3
        try:
            conn = sqlite3.connect('data/bot_data.db')
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE users 
                SET is_blocked = 1, blocked_at = ?, blocked_reason = ?
                WHERE id = ?
            """, (int(time.time()), reason, user_id_str))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"DB update error: {e}")
        
        return True
        
    except Exception as e:
        print(f"Error blocking user: {e}")
        return False

def unblock_user(user_id):
    """ """
    import json
    import os
    
    user_id_str = str(user_id)
    users_file = 'data/users.json'
    
    try:
        with open(users_file, 'r', encoding='utf-8') as f:
            users = json.load(f)
        
        if user_id_str not in users:
            return False
        
        # 
        users[user_id_str]['is_blocked'] = False
        users[user_id_str]['blocked_at'] = None
        users[user_id_str]['blocked_reason'] = None
        
        with open(users_file, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        
        print(f"User {user_id_str} unblocked")
        
        #  
        import sqlite3
        try:
            conn = sqlite3.connect('data/bot_data.db')
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE users 
                SET is_blocked = 0, blocked_at = NULL, blocked_reason = NULL
                WHERE id = ?
            """, (user_id_str,))
            conn.commit()
            conn.close()
        except:
            pass
        
        return True
        
    except Exception as e:
        print(f"Error unblocking user: {e}")
        return False

def is_user_blocked(user_id):
    """,   """
    import json
    import os
    
    user_id_str = str(user_id)
    users_file = 'data/users.json'
    
    try:
        with open(users_file, 'r', encoding='utf-8') as f:
            users = json.load(f)
        
        if user_id_str in users:
            return users[user_id_str].get('is_blocked', False) == True
    except:
        pass
    
    return False

def get_blocked_users():
    """   """
    import json
    import os
    
    blocked = []
    users_file = 'data/users.json'
    
    try:
        with open(users_file, 'r', encoding='utf-8') as f:
            users = json.load(f)
        
        for user_id, user_data in users.items():
            if user_data.get('is_blocked', False) == True:
                blocked.append({
                    "id": user_id,
                    "username": user_data.get('username'),
                    "blocked_at": user_data.get('blocked_at'),
                    "blocked_reason": user_data.get('blocked_reason')
                })
    except Exception as e:
        print(f"Error getting blocked users: {e}")
    
    return blocked

#    bot.py ( )
def is_tx_processed(tx_hash: str) -> bool:
    """,      (   bot.py)"""
    return is_deposit_processed(tx_hash)

def record_deposit(user_id: int, amount: float, tx_hash: str, description: str = None):
    """  (   bot.py)"""
    #   
    update_user_stat(user_id, balance_delta=amount)
    
    #      
    add_internal(amount)
    
    #  
    log_deposit(user_id, amount, tx_hash, description)
    
    return True

#    bot.py ( )
def is_tx_processed(tx_hash: str) -> bool:
    """,      (   bot.py)"""
    return is_deposit_processed(tx_hash)

def record_deposit(user_id: int, amount: float, tx_hash: str, description: str = None):
    """  (   bot.py)"""
    #      
    update_user_balance(user_id, amount, "add")
    
    #      
    add_internal(amount)
    
    #  
    log_deposit(user_id, amount, tx_hash, description)
    
    #   total_deposited
    user = get_user(user_id)
    current_deposited = user.get("total_deposited", 0)
    update_user_stat(user_id, "total_deposited", current_deposited + amount)
    
    return True


# Функции совместимости для bot.py
def is_tx_processed(tx_hash: str) -> bool:
    """Проверяет, была ли транзакция уже обработана"""
    return is_deposit_processed(tx_hash)

def record_deposit(user_id: int, amount: float, tx_hash: str, description: str = None):
    """Записывает депозит и обновляет баланс"""
    # Обновляем баланс пользователя
    update_user_balance(user_id, amount, "add")
    
    # Добавляем к внутреннему балансу  
    add_internal(amount)
    
    # Логируем депозит
    log_deposit(user_id, amount, tx_hash, description)
    
    logger.info(f"Deposit recorded: user_id={user_id}, amount={amount}, hash={tx_hash[:10]}...")
    
    return True


def atomic_purchase(user_id: int, cost: float, stars: int, purchase_id: str) -> bool:
    """Атомарно обрабатывает покупку Stars."""
    with LOCK:
        current_balance = get_user_balance(user_id)
        if current_balance < cost:
            logger.warning(f"Insufficient balance for user {user_id}: {current_balance} < {cost}")
            return False
        
        success = update_user_balance(user_id, cost, "subtract")
        if not success:
            logger.error(f"Failed to deduct balance for user {user_id}")
            return False
        
        log_purchase(user_id, stars, cost, purchase_id)
        
        settings = get_settings()
        fee_percent = settings.get("fee_percent", 5.0)
        fee = cost * (fee_percent / 100)
        add_internal(fee)
        
        user = get_user(user_id)
        total_bought = user.get("total_bought", 0) + stars
        update_user_stat(user_id, "total_bought", total_bought)
        
        logger.info(f"Purchase completed: user={user_id}, stars={stars}, cost={cost:.4f}, id={purchase_id}")
        return True

def update_balance(user_id: int, amount: float):
    """Обновляет баланс пользователя"""
    with LOCK:
        users = _cache.load(USERS_FILE, {})
        user_id_str = str(user_id)
        
        if user_id_str not in users:
            get_user(user_id)
            users = _cache.load(USERS_FILE, {})
        
        current_balance = users[user_id_str].get('balance', 0.0)
        new_balance = current_balance + amount
        users[user_id_str]['balance'] = new_balance
        
        _cache.save(USERS_FILE, users)
        logger.info(f"Balance updated for user {user_id}: {current_balance} -> {new_balance}")
        
        return new_balance


def rollback_purchase(user_id: int, cost: float, stars: int, purchase_id: str):
    """Безопасный возврат баланса при ошибке покупки"""
    try:
        # Проверки безопасности
        if not user_id or not cost or cost <= 0:
            return
            
        user_id_str = str(user_id)
        with LOCK:
            users = _cache.load(USERS_FILE, {})
            if user_id_str not in users:
                return
                
            # Возвращаем баланс
            old_balance = float(users[user_id_str].get('balance', 0))
            new_balance = old_balance + float(cost)
            users[user_id_str]['balance'] = new_balance
            
            # Откатываем статистику покупок
            total_bought = users[user_id_str].get('total_bought', 0)
            if total_bought >= stars:
                users[user_id_str]['total_bought'] = total_bought - stars
            
            _cache.save(USERS_FILE, users)
            logger.info(f"Rollback success: user={user_id}, returned={cost:.4f}, balance: {old_balance:.4f} -> {new_balance:.4f}")
    except Exception as e:
        # Ошибка в rollback не должна ломать основной процесс
        logger.error(f"Rollback failed (safe): {e}")

def rollback_purchase(user_id: int, cost: float, stars: int, purchase_id: str):
    """Безопасный возврат баланса при ошибке покупки"""
    try:
        if not user_id or not cost or cost <= 0:
            return
        user_id_str = str(user_id)
        with LOCK:
            users = _cache.load(USERS_FILE, {})
            if user_id_str not in users:
                return
            old_balance = float(users[user_id_str].get("balance", 0))
            new_balance = old_balance + float(cost)
            users[user_id_str]["balance"] = new_balance
            _cache.save(USERS_FILE, users)
            logger.info(f"Rollback: returned {cost} to user {user_id}")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")



# ============================================================
# CHAT PARTNER SYSTEM - Партнёрская система для групп
# ============================================================

CHATS_FILE = os.path.join(DATA_DIR, "chats.json")
CHAT_EARNINGS_FILE = os.path.join(DATA_DIR, "chat_earnings.json")

# Процент комиссии владельцам чатов
CHAT_SPIN_COMMISSION = 40  # 40% от проигрыша в спинах
CHAT_PURCHASE_COMMISSION = 30  # 30% от наценки при покупке


def register_chat(chat_id: int, owner_id: int, title: str = "") -> Dict[str, Any]:
    """Регистрация нового чата при добавлении бота"""
    chat_id_str = str(chat_id)
    with LOCK:
        chats = _cache.load(CHATS_FILE, {})
        if chat_id_str in chats:
            # Чат уже существует - реактивируем и обновляем title
            chats[chat_id_str]["is_active"] = True
            chats[chat_id_str]["title"] = title or chats[chat_id_str].get("title", "")
            _cache.save(CHATS_FILE, chats)
            logger.info(f"Chat reactivated: {chat_id} (owner: {owner_id}, title: {title})")
        else:
            # Новый чат
            chats[chat_id_str] = {
                "id": chat_id,
                "owner_id": owner_id,
                "title": title,
                "created_at": time.time(),
                "is_active": True,
                "total_earnings": 0.0,
                "total_volume": 0.0,
                "total_spins": 0,
                "total_purchases": 0,
                "spin_earnings": 0.0,
                "purchase_earnings": 0.0,
                "members_count": 0,
                "withdrawn": 0.0
            }
            _cache.save(CHATS_FILE, chats)
            logger.info(f"Chat registered: {chat_id} (owner: {owner_id}, title: {title})")
        return chats[chat_id_str]


def get_chat(chat_id: int) -> Optional[Dict[str, Any]]:
    """Получить информацию о чате"""
    chats = _cache.load(CHATS_FILE, {})
    return chats.get(str(chat_id))


def update_chat(chat_id: int, data: Dict[str, Any]):
    """Обновить данные чата"""
    chat_id_str = str(chat_id)
    with LOCK:
        chats = _cache.load(CHATS_FILE, {})
        if chat_id_str in chats:
            chats[chat_id_str].update(data)
            _cache.save(CHATS_FILE, chats)


def deactivate_chat(chat_id: int):
    """Деактивировать чат (бот удалён)"""
    update_chat(chat_id, {"is_active": False})
    logger.info(f"Chat deactivated: {chat_id}")


def get_all_chats() -> List[Dict[str, Any]]:
    """Получить все чаты"""
    chats = _cache.load(CHATS_FILE, {})
    return list(chats.values())


def get_active_chats() -> List[Dict[str, Any]]:
    """Получить только активные чаты"""
    chats = _cache.load(CHATS_FILE, {})
    return [c for c in chats.values() if c.get("is_active", True)]


def get_owner_chats(owner_id: int) -> List[Dict[str, Any]]:
    """Получить все чаты владельца"""
    chats = _cache.load(CHATS_FILE, {})
    return [c for c in chats.values() if c.get("owner_id") == owner_id and c.get("is_active", True)]



def get_owner_all_chats(owner_id: int) -> List[Dict[str, Any]]:
    """Получить ВСЕ чаты владельца (включая неактивные) для расчёта прогресса"""
    chats = _cache.load(CHATS_FILE, {})
    return [c for c in chats.values() if c.get("owner_id") == owner_id]


def add_chat_earning(chat_id: int, amount: float, earning_type: str, 
                     user_id: int = None, details: str = ""):
    """
    Добавить заработок владельцу чата
    earning_type: 'spin' или 'purchase'
    """
    if amount <= 0:
        return
    
    chat_id_str = str(chat_id)
    with LOCK:
        # Обновляем статистику чата
        chats = _cache.load(CHATS_FILE, {})
        if chat_id_str not in chats:
            return
        
        chat = chats[chat_id_str]
        chat["total_earnings"] = chat.get("total_earnings", 0) + amount
        
        if earning_type == "spin":
            chat["spin_earnings"] = chat.get("spin_earnings", 0) + amount
            chat["total_spins"] = chat.get("total_spins", 0) + 1
        elif earning_type == "purchase":
            chat["purchase_earnings"] = chat.get("purchase_earnings", 0) + amount
            chat["total_purchases"] = chat.get("total_purchases", 0) + 1
        
        _cache.save(CHATS_FILE, chats)
        
        # Логируем заработок
        earnings = _cache.load(CHAT_EARNINGS_FILE, [])
        earnings.append({
            "chat_id": chat_id,
            "owner_id": chat["owner_id"],
            "amount": amount,
            "type": earning_type,
            "user_id": user_id,
            "details": details,
            "timestamp": time.time()
        })
        # Храним последние 10000 записей
        if len(earnings) > 10000:
            earnings = earnings[-10000:]
        _cache.save(CHAT_EARNINGS_FILE, earnings)
        
        logger.info(f"Chat earning: chat={chat_id}, type={earning_type}, amount={amount:.6f}")


def get_chat_earnings(chat_id: int = None, owner_id: int = None, 
                      limit: int = 100) -> List[Dict[str, Any]]:
    """Получить историю заработков"""
    earnings = _cache.load(CHAT_EARNINGS_FILE, [])
    
    if chat_id:
        earnings = [e for e in earnings if e.get("chat_id") == chat_id]
    if owner_id:
        earnings = [e for e in earnings if e.get("owner_id") == owner_id]
    
    return sorted(earnings, key=lambda x: x.get("timestamp", 0), reverse=True)[:limit]


def get_owner_total_earnings(owner_id: int) -> Dict[str, float]:
    """Получить общий заработок владельца по всем его чатам"""
    chats = get_owner_all_chats(owner_id)
    return {
        "total": sum(c.get("total_earnings", 0) for c in chats),
        "spin": sum(c.get("spin_earnings", 0) for c in chats),
        "purchase": sum(c.get("purchase_earnings", 0) for c in chats),
        "withdrawn": sum(c.get("withdrawn", 0) for c in chats),
        "available": sum(c.get("total_earnings", 0) - c.get("withdrawn", 0) for c in chats)
    }


def withdraw_chat_earnings(chat_id: int, amount: float) -> bool:
    """Вывод заработка владельцем чата"""
    chat = get_chat(chat_id)
    if not chat:
        return False
    
    available = chat.get("total_earnings", 0) - chat.get("withdrawn", 0)
    if amount > available:
        return False
    
    update_chat(chat_id, {"withdrawn": chat.get("withdrawn", 0) + amount})
    logger.info(f"Chat withdrawal: chat={chat_id}, amount={amount:.6f}")
    return True


def get_user_active_chat(user_id: int) -> Optional[int]:
    """
    Получить chat_id если пользователь взаимодействует из группы.
    Возвращает None если пользователь в личке.
    Эта функция будет вызываться из хендлеров.
    """
    # Реализация будет в bot.py через контекст сообщения
    return None


def calculate_spin_commission(bet: float, win: float, chat_id: int) -> float:
    """Рассчитать комиссию владельцу чата от спина"""
    if win >= bet:  # Выигрыш - нет комиссии
        return 0.0
    
    loss = bet - win  # Проигрыш пользователя
    commission = loss * (CHAT_SPIN_COMMISSION / 100)
    return round(commission, 6)


def calculate_purchase_commission(stars: int, fee_percent: float, 
                                  base_price: float, chat_id: int) -> float:
    """
    Рассчитать комиссию владельцу чата от покупки
    Комиссия = 30% от наценки (fee_percent от base_price * stars)
    """
    if fee_percent <= 0:
        return 0.0
    
    total_cost = stars * base_price * (1 + fee_percent / 100)
    markup = total_cost - (stars * base_price)  # Сумма наценки
    commission = markup * (CHAT_PURCHASE_COMMISSION / 100)
    return round(commission, 6)




# ============================================================
# PARTNER LEVELS SYSTEM - Система уровней партнёров
# ============================================================

PARTNER_LEVELS = {
    "bronze": {
        "name": "🥉 Бронза",
        "name_en": "🥉 Bronze", 
        "min_volume": 0,
        "spin_commission": 15,
        "purchase_commission": 10
    },
    "silver": {
        "name": "🥈 Серебро",
        "name_en": "🥈 Silver",
        "min_volume": 1000,
        "spin_commission": 25,
        "purchase_commission": 20
    },
    "gold": {
        "name": "🥇 Золото",
        "name_en": "🥇 Gold",
        "min_volume": 10000,
        "spin_commission": 40,
        "purchase_commission": 30
    }
}


def add_chat_volume(chat_id: int, amount: float):
    """Добавить объём к чату (для расчёта уровня)"""
    if amount <= 0:
        return
    chat_id_str = str(chat_id)
    with LOCK:
        chats = _cache.load(CHATS_FILE, {})
        if chat_id_str in chats:
            chats[chat_id_str]["total_volume"] = chats[chat_id_str].get("total_volume", 0) + amount
            _cache.save(CHATS_FILE, chats)


def calculate_spin_commission_by_level(bet: float, win: float, owner_id: int) -> float:
    """Рассчитать комиссию от спина с учётом уровня партнёра"""
    if win >= bet:
        return 0.0
    
    level_info = get_owner_level(owner_id)
    commission_percent = level_info["level"]["spin_commission"]
    
    loss = bet - win
    commission = loss * (commission_percent / 100)
    return round(commission, 6)


def calculate_purchase_commission_by_level(stars: int, fee_percent: float, 
                                           base_price: float, owner_id: int) -> float:
    """Рассчитать комиссию от покупки с учётом уровня партнёра"""
    if fee_percent <= 0:
        return 0.0
    
    level_info = get_owner_level(owner_id)
    commission_percent = level_info["level"]["purchase_commission"]
    
    total_cost = stars * base_price * (1 + fee_percent / 100)
    markup = total_cost - (stars * base_price)
    commission = markup * (commission_percent / 100)
    return round(commission, 6)


def remove_chat(chat_id: int) -> bool:
    """Удалить чат из системы"""
    chat_id_str = str(chat_id)
    with LOCK:
        chats = _cache.load(CHATS_FILE, {})
        if chat_id_str in chats:
            del chats[chat_id_str]
            _cache.save(CHATS_FILE, chats)
            logger.info(f"Chat removed: {chat_id}")
            return True
    return False


# ============================================================
# PARTNER WITHDRAWALS - Запросы на вывод
# ============================================================

WITHDRAWALS_FILE = os.path.join(DATA_DIR, "partner_withdrawals.json")

def create_withdrawal_request(owner_id: int, amount: float, wallet_address: str) -> dict:
    """Создать запрос на вывод"""
    with LOCK:
        withdrawals = _cache.load(WITHDRAWALS_FILE, [])
        
        request = {
            "id": f"wd_{int(time.time())}_{owner_id}",
            "owner_id": owner_id,
            "amount": amount,
            "wallet_address": wallet_address,
            "status": "pending",  # pending, approved, rejected, completed
            "created_at": time.time(),
            "processed_at": None,
            "tx_hash": None,
            "admin_comment": None
        }
        
        withdrawals.append(request)
        _cache.save(WITHDRAWALS_FILE, withdrawals)
        logger.info(f"Withdrawal request created: {request['id']}, amount={amount}, owner={owner_id}")
        return request


def get_withdrawal_requests(status: str = None, owner_id: int = None) -> list:
    """Получить запросы на вывод"""
    withdrawals = _cache.load(WITHDRAWALS_FILE, [])
    
    if status:
        withdrawals = [w for w in withdrawals if w.get("status") == status]
    if owner_id:
        withdrawals = [w for w in withdrawals if w.get("owner_id") == owner_id]
    
    return sorted(withdrawals, key=lambda x: x.get("created_at", 0), reverse=True)


def get_withdrawal_by_id(withdrawal_id: str) -> dict:
    """Получить запрос по ID"""
    withdrawals = _cache.load(WITHDRAWALS_FILE, [])
    for w in withdrawals:
        if w.get("id") == withdrawal_id:
            return w
    return None


def update_withdrawal_status(withdrawal_id: str, status: str, tx_hash: str = None, comment: str = None) -> bool:
    """Обновить статус запроса на вывод"""
    with LOCK:
        withdrawals = _cache.load(WITHDRAWALS_FILE, [])
        
        for w in withdrawals:
            if w.get("id") == withdrawal_id:
                w["status"] = status
                w["processed_at"] = time.time()
                if tx_hash:
                    w["tx_hash"] = tx_hash
                if comment:
                    w["admin_comment"] = comment
                
                # Если подтверждено - списываем из доступного баланса
                if status == "completed":
                    owner_id = w.get("owner_id")
                    amount = w.get("amount")
                    chats = get_owner_all_chats(owner_id)
                    # Распределяем списание по чатам
                    remaining = amount
                    for chat in chats:
                        available = chat.get("total_earnings", 0) - chat.get("withdrawn", 0)
                        if available > 0 and remaining > 0:
                            to_withdraw = min(available, remaining)
                            update_chat(chat["id"], {"withdrawn": chat.get("withdrawn", 0) + to_withdraw})
                            remaining -= to_withdraw
                
                _cache.save(WITHDRAWALS_FILE, withdrawals)
                logger.info(f"Withdrawal {withdrawal_id} updated: status={status}")
                return True
        
        return False


def get_pending_withdrawals_count() -> int:
    """Количество ожидающих запросов"""
    withdrawals = _cache.load(WITHDRAWALS_FILE, [])
    return len([w for w in withdrawals if w.get("status") == "pending"])


def get_all_partners() -> list:
    """Получить всех партнёров с их статистикой"""
    chats = _cache.load(CHATS_FILE, {})
    
    # Группируем по owner_id
    partners = {}
    for chat_id, chat in chats.items():
        owner_id = chat.get("owner_id")
        if owner_id not in partners:
            partners[owner_id] = {
                "owner_id": owner_id,
                "chats": [],
                "total_earnings": 0,
                "total_volume": 0,
                "total_withdrawn": 0
            }
        
        partners[owner_id]["chats"].append(chat)
        partners[owner_id]["total_earnings"] += chat.get("total_earnings", 0)
        partners[owner_id]["total_volume"] += chat.get("total_volume", 0)
        partners[owner_id]["total_withdrawn"] += chat.get("withdrawn", 0)
    
    # Добавляем уровень каждому партнёру
    result = []
    for owner_id, data in partners.items():
        level_info = get_owner_level(owner_id)
        data["level"] = level_info["level_key"]
        data["level_name"] = level_info["level"]["name"]
        data["available"] = data["total_earnings"] - data["total_withdrawn"]
        data["chats_count"] = len(data["chats"])
        data["total_earned"] = data["total_earnings"]
        data["withdrawn"] = data["total_withdrawn"]
        result.append(data)
    
    return sorted(result, key=lambda x: x["total_volume"], reverse=True)


def set_partner_level(owner_id: int, level_key: str) -> bool:
    """Установить уровень партнёра вручную"""
    if level_key not in PARTNER_LEVELS:
        return False
    
    with LOCK:
        chats = _cache.load(CHATS_FILE, {})
        updated = False
        for chat_id, chat in chats.items():
            if chat.get("owner_id") == owner_id:
                chat["manual_level"] = level_key
                updated = True
        
        if updated:
            _cache.save(CHATS_FILE, chats)
            logger.info(f"Partner {owner_id} level set to {level_key}")
        return updated


def adjust_partner_balance(owner_id: int, amount: float, reason: str = "") -> bool:
    """Изменить баланс партнёра (добавить/вычесть)"""
    with LOCK:
        chats = _cache.load(CHATS_FILE, {})
        # Находим первый чат партнёра для корректировки
        for chat_id, chat in chats.items():
            if chat.get("owner_id") == owner_id:
                chat["total_earnings"] = chat.get("total_earnings", 0) + amount
                chat["balance_adjustments"] = chat.get("balance_adjustments", [])
                chat["balance_adjustments"].append({
                    "amount": amount,
                    "reason": reason,
                    "timestamp": time.time()
                })
                _cache.save(CHATS_FILE, chats)
                logger.info(f"Partner {owner_id} balance adjusted by {amount}: {reason}")
                return True
        return False


def get_owner_level(owner_id: int) -> dict:
    """Получить уровень партнёра (с учётом ручной установки)"""
    chats = get_owner_all_chats(owner_id)
    total_volume = sum(c.get("total_volume", 0) for c in chats)
    
    # Проверяем ручной уровень
    manual_level = None
    for chat in chats:
        if chat.get("manual_level"):
            manual_level = chat.get("manual_level")
            break
    
    if manual_level and manual_level in PARTNER_LEVELS:
        level = PARTNER_LEVELS[manual_level]
        level_key = manual_level
    else:
        # Автоматический расчёт
        level = PARTNER_LEVELS["bronze"]
        level_key = "bronze"
        
        if total_volume >= PARTNER_LEVELS["gold"]["min_volume"]:
            level = PARTNER_LEVELS["gold"]
            level_key = "gold"
        elif total_volume >= PARTNER_LEVELS["silver"]["min_volume"]:
            level = PARTNER_LEVELS["silver"]
            level_key = "silver"
    
    # Прогресс до следующего уровня
    next_level = None
    progress = 100
    remaining = 0
    
    if level_key == "bronze":
        next_level = PARTNER_LEVELS["silver"]
        remaining = next_level["min_volume"] - total_volume
        progress = (total_volume / next_level["min_volume"]) * 100 if next_level["min_volume"] > 0 else 0
    elif level_key == "silver":
        next_level = PARTNER_LEVELS["gold"]
        remaining = next_level["min_volume"] - total_volume
        progress = (total_volume / 
                   next_level["min_volume"]) * 100
    
    return {
        "level_key": level_key,
        "level": level,
        "total_volume": total_volume,
        "next_level": next_level,
        "progress": max(0, min(progress, 100)),
        "remaining": max(remaining, 0),
        "is_manual": manual_level is not None
    }


# ============================================================
# NGR TRACKING - Net Gaming Revenue для партнёрской программы
# ============================================================

PLAYER_NGR_FILE = os.path.join(DATA_DIR, "player_ngr.json")

def get_player_ngr(user_id: int, chat_id: int) -> dict:
    """Получить NGR данные игрока в чате"""
    key = f"{user_id}_{chat_id}"
    data = _cache.load(PLAYER_NGR_FILE, {})
    return data.get(key, {
        "user_id": user_id,
        "chat_id": chat_id,
        "total_wagered": 0.0,  # Все ставки
        "total_won": 0.0,      # Все выигрыши
        "paid_ngr": 0.0        # Уже выплаченный NGR партнёру
    })


def update_player_ngr_and_calc_commission(user_id: int, chat_id: int, bet: float, win: float, owner_id: int) -> float:
    """
    Обновить NGR игрока и рассчитать комиссию партнёру.
    
    NGR = total_wagered - total_won (чистый проигрыш игрока)
    Комиссия = max(0, NGR - paid_ngr) × процент
    
    Если игрок в плюсе (NGR < 0) или NGR не вырос - комиссия 0.
    """
    key = f"{user_id}_{chat_id}"
    
    with LOCK:
        data = _cache.load(PLAYER_NGR_FILE, {})
        
        player = data.get(key, {
            "user_id": user_id,
            "chat_id": chat_id,
            "total_wagered": 0.0,
            "total_won": 0.0,
            "paid_ngr": 0.0
        })
        
        # Обновляем статистику
        player["total_wagered"] = player.get("total_wagered", 0) + bet
        player["total_won"] = player.get("total_won", 0) + win
        
        # Рассчитываем текущий NGR
        current_ngr = player["total_wagered"] - player["total_won"]
        paid_ngr = player.get("paid_ngr", 0)
        
        # Комиссия только если NGR вырос (игрок проиграл больше)
        commission = 0.0
        if current_ngr > paid_ngr:
            new_loss = current_ngr - paid_ngr
            
            # Получаем процент комиссии по уровню партнёра
            level_info = get_owner_level(owner_id)
            commission_percent = level_info["level"]["spin_commission"]
            
            commission = new_loss * (commission_percent / 100)
            
            # Обновляем paid_ngr
            player["paid_ngr"] = current_ngr
        
        data[key] = player
        _cache.save(PLAYER_NGR_FILE, data)
        
        logger.debug(f"NGR update: user={user_id}, chat={chat_id}, bet={bet}, win={win}, "
                    f"ngr={current_ngr:.4f}, paid={paid_ngr:.4f}, commission={commission:.6f}")
        
        return round(commission, 6)


def get_player_ngr_stats(user_id: int, chat_id: int) -> dict:
    """Получить статистику NGR игрока для отображения"""
    ngr_data = get_player_ngr(user_id, chat_id)
    current_ngr = ngr_data["total_wagered"] - ngr_data["total_won"]
    return {
        "total_wagered": ngr_data["total_wagered"],
        "total_won": ngr_data["total_won"],
        "ngr": current_ngr,  # Положительный = в минусе, отрицательный = в плюсе
        "player_profit": -current_ngr  # Прибыль игрока (отрицательная = проигрыш)
    }


def update_chat_volume_ngr(chat_id: int, bet: float, win: float):
    """Обновить объём чата по NGR модели (проигрыш - выигрыш)"""
    net_loss = bet - win  # Положительный при проигрыше, отрицательный при выигрыше
    chat_id_str = str(chat_id)
    with LOCK:
        chats = _cache.load(CHATS_FILE, {})
        if chat_id_str in chats:
            current_volume = chats[chat_id_str].get("total_volume", 0)
            new_volume = max(0, current_volume + net_loss)  # Не уходим в минус
            chats[chat_id_str]["total_volume"] = new_volume
            _cache.save(CHATS_FILE, chats)
            logger.debug(f"Chat volume updated: chat={chat_id}, bet={bet}, win={win}, net={net_loss}, volume={new_volume}")


# === GAME TYPE SELECTION ===
GAME_TYPE_FILE = os.path.join(DATA_DIR, "user_game_types.json")

def get_user_game_type(user_id: int) -> str:
    """Получить выбранный тип игры пользователя"""
    with LOCK:
        data = _cache.load(GAME_TYPE_FILE, {})
        return data.get(str(user_id), "slot")

def set_user_game_type(user_id: int, game_type: str):
    """Установить тип игры для пользователя"""
    valid_types = ["slot", "dice", "football", "basketball", "darts", "bowling"]
    if game_type not in valid_types:
        game_type = "slot"
    with LOCK:
        data = _cache.load(GAME_TYPE_FILE, {})
        data[str(user_id)] = game_type
        _cache.save(GAME_TYPE_FILE, data)


def record_partner_withdrawal_to_balance(owner_id: int, amount: float) -> bool:
    """Записать вывод партнёрского заработка на баланс бота"""
    chats = get_owner_all_chats(owner_id)
    if not chats:
        return False
    
    # Проверяем доступную сумму
    available = sum(c.get("total_earnings", 0) - c.get("withdrawn", 0) for c in chats)
    if amount > available:
        return False
    
    # Списываем с первого чата с доступным балансом
    remaining = amount
    for chat in chats:
        chat_available = chat.get("total_earnings", 0) - chat.get("withdrawn", 0)
        if chat_available > 0 and remaining > 0:
            to_withdraw = min(chat_available, remaining)
            update_chat(chat["id"], {"withdrawn": chat.get("withdrawn", 0) + to_withdraw})
            remaining -= to_withdraw
            if remaining <= 0:
                break
    
    logger.info(f"Partner withdrawal to balance: owner={owner_id}, amount={amount:.6f}")
    return True


def get_chat_top_by_volume(chat_id: int, period: str, limit: int = 10) -> list:
    """Топ игроков чата по объёму ставок за период"""
    import datetime
    
    with LOCK:
        spins = _cache.load(SPINS_FILE, [])
        ngr_data = _cache.load(PLAYER_NGR_FILE, {})
        users_data = _cache.load(USERS_FILE, {})
    
    volumes = {}
    
    # Для "all time" используем player_ngr (там полные данные)
    if period == "all":
        for key, data in ngr_data.items():
            if data.get("chat_id") == chat_id:
                uid = data.get("user_id")
                volumes[uid] = data.get("total_wagered", 0)
    else:
        # Для периодов используем только spins с chat_id
        now = datetime.datetime.now()
        if period == "day":
            start_time = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        elif period == "week":
            start_time = (now - datetime.timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        elif period == "month":
            start_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()
        else:
            start_time = 0
        
        for spin in spins:
            spin_chat = spin.get("chat_id")
            if spin_chat != chat_id:
                continue
            if spin.get("timestamp", 0) < start_time:
                continue
            
            uid = spin.get("user_id")
            bet = spin.get("bet", 0)
            volumes[uid] = volumes.get(uid, 0) + bet
    
    # Сортируем и формируем топ
    sorted_users = sorted(volumes.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    result = []
    for uid, volume in sorted_users:
        user_info = users_data.get(str(uid), {})
        result.append({
            "user_id": uid,
            "username": user_info.get("username", "Unknown"),
            "volume": volume
        })
    
    return result


def get_chat_top_by_balance(chat_id: int, limit: int = 10) -> list:
    """Топ игроков по балансу среди участников чата"""
    with LOCK:
        ngr_data = _cache.load(PLAYER_NGR_FILE, {})
        users_data = _cache.load(USERS_FILE, {})
    
    # Собираем user_id которые играли в этом чате (из player_ngr)
    chat_users = set()
    for key, data in ngr_data.items():
        if data.get("chat_id") == chat_id:
            chat_users.add(data.get("user_id"))
    
    # Получаем балансы только этих пользователей
    users_with_balance = []
    for uid in chat_users:
        user_info = users_data.get(str(uid), {})
        balance = user_info.get("balance", 0)
        users_with_balance.append({
            "user_id": uid,
            "username": user_info.get("username", "Unknown"),
            "balance": balance
        })
    
    # Сортируем
    sorted_users = sorted(users_with_balance, key=lambda x: x["balance"], reverse=True)[:limit]
    return sorted_users


# ==================== ДЕМО СЧЁТ ====================
DEMO_FILE = os.path.join(DATA_DIR, "demo_accounts.json")
DEMO_BALANCE_DEFAULT = 100.0
DEMO_RESET_DAYS = 7


def get_demo_account(user_id: int) -> dict:
    """Получить демо-аккаунт пользователя"""
    with LOCK:
        demos = _cache.load(DEMO_FILE, {})
        user_key = str(user_id)
        
        if user_key not in demos:
            return None
        
        return demos[user_key]


def create_demo_account(user_id: int) -> dict:
    """Создать или сбросить демо-аккаунт"""
    with LOCK:
        demos = _cache.load(DEMO_FILE, {})
        user_key = str(user_id)
        
        demos[user_key] = {
            "balance": DEMO_BALANCE_DEFAULT,
            "created_at": time.time(),
            "last_reset": time.time()
        }
        
        _cache.save(DEMO_FILE, demos)
        return demos[user_key]


def get_demo_balance(user_id: int) -> float:
    """Получить демо-баланс (с автосбросом через неделю)"""
    with LOCK:
        demos = _cache.load(DEMO_FILE, {})
        user_key = str(user_id)
        
        if user_key not in demos:
            return 0.0
        
        account = demos[user_key]
        
        # Проверяем нужен ли сброс (прошла неделя)
        last_reset = account.get("last_reset", 0)
        if time.time() - last_reset > DEMO_RESET_DAYS * 24 * 3600:
            account["balance"] = DEMO_BALANCE_DEFAULT
            account["last_reset"] = time.time()
            _cache.save(DEMO_FILE, demos)
        
        return float(account.get("balance", 0))


def update_demo_balance(user_id: int, delta: float) -> bool:
    """Изменить демо-баланс"""
    with LOCK:
        demos = _cache.load(DEMO_FILE, {})
        user_key = str(user_id)
        
        if user_key not in demos:
            return False
        
        new_balance = demos[user_key].get("balance", 0) + delta
        if new_balance < 0:
            return False
        
        demos[user_key]["balance"] = new_balance
        _cache.save(DEMO_FILE, demos)
        return True


def is_demo_mode(user_id: int) -> bool:
    """Проверить включён ли демо-режим"""
    with LOCK:
        demos = _cache.load(DEMO_FILE, {})
        user_key = str(user_id)
        return demos.get(user_key, {}).get("active", False)


def set_demo_mode(user_id: int, active: bool):
    """Включить/выключить демо-режим"""
    with LOCK:
        demos = _cache.load(DEMO_FILE, {})
        user_key = str(user_id)
        
        if user_key not in demos:
            if active:
                demos[user_key] = {
                    "balance": DEMO_BALANCE_DEFAULT,
                    "created_at": time.time(),
                    "last_reset": time.time(),
                    "active": True
                }
        else:
            demos[user_key]["active"] = active
        
        _cache.save(DEMO_FILE, demos)

# === ЗАДАНИЯ (TASKS) ===
TASKS_FILE = os.path.join(DATA_DIR, "tasks.json")

def _load_tasks() -> dict:
    if os.path.exists(TASKS_FILE):
        with open(TASKS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def _save_tasks(data: dict):
    with open(TASKS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_task_stars(user_id: int) -> int:
    """Получить заработанные звёзды за задания"""
    data = _load_tasks()
    user_data = data.get(str(user_id), {})
    return user_data.get("stars", 0)

def add_task_stars(user_id: int, amount: int):
    """Добавить звёзды за задание"""
    data = _load_tasks()
    uid = str(user_id)
    if uid not in data:
        data[uid] = {"stars": 0, "last_claim": None}
    data[uid]["stars"] = data[uid].get("stars", 0) + amount
    _save_tasks(data)

def check_daily_task_claimed(user_id: int) -> bool:
    """Проверить, получал ли пользователь награду сегодня"""
    data = _load_tasks()
    user_data = data.get(str(user_id), {})
    last_claim = user_data.get("last_claim")
    if not last_claim:
        return False
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    return last_claim == today

def set_daily_task_claimed(user_id: int):
    """Отметить получение награды сегодня"""
    data = _load_tasks()
    uid = str(user_id)
    if uid not in data:
        data[uid] = {"stars": 0}
    from datetime import datetime
    data[uid]["last_claim"] = datetime.now().strftime("%Y-%m-%d")
    _save_tasks(data)

def withdraw_task_stars(user_id: int, amount: int) -> bool:
    """Вывести звёзды (списать с баланса заданий)"""
    data = _load_tasks()
    uid = str(user_id)
    current = data.get(uid, {}).get("stars", 0)
    if current < amount:
        return False
    data[uid]["stars"] = current - amount
    _save_tasks(data)
    return True
