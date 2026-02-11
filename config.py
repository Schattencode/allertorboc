import os

from dotenv import load_dotenv

load_dotenv()

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')

# Список Telegram User ID, которые получают алерты
# В .env указывать через запятую: TELEGRAM_USER_IDS=111111,222222,333333
_raw_ids = os.getenv('TELEGRAM_USER_IDS', '')
TELEGRAM_USER_IDS = [uid.strip() for uid in _raw_ids.split(',') if uid.strip()]

# Monitoring Configuration
CHECK_INTERVAL = 30  # seconds between checks
MEMORY_HOURS = 24    # hours to keep tokens in memory

# Filters
FILTERS = {
    'min_age_minutes': 5,
    'max_age_hours': 24,
    'min_mcap': 10_000,
    'max_mcap': 10_000_000,
    'chains': ['solana'],
    'min_liquidity': 5_000,
}

# Paths
DATA_DIR = './data'
LOG_DIR = './logs'

# API Configuration
API_PROFILES_URL = 'https://api.dexscreener.com/token-profiles/latest/v1'
API_TOKENS_URL = 'https://api.dexscreener.com/latest/dex/tokens'
API_TIMEOUT = 10  # seconds
API_MAX_CONSECUTIVE_FAILURES = 5
