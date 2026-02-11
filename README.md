# DexScreener Twitter Monitor

24/7 bot that monitors DexScreener Enhanced Token Info and alerts when tokens add Twitter links.

## Features

- Monitors only PAID DexScreener tokens (`/token-profiles/latest/v1`)
- Alerts when Twitter added (immediately or up to 24h later)
- 1-2 minute alert latency (30s polling interval)
- 24-hour rolling memory window
- Configurable filters (age, market cap, chain, liquidity)
- Multi-user support — alerts go to all user IDs in `.env`
- State persistence across restarts (`data/memory.json`)
- Rotating log files

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create .env file from example
cp .env.example .env

# 3. Edit .env — вписать свой токен и user ID
nano .env

# 4. (Optional) Test API access
python test_api.py

# 5. Run
python main.py
```

## Configuration

### `.env` file

```env
# Токен бота (получить у @BotFather)
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz

# Список Telegram User ID через запятую
# Узнать свой ID: написать боту @userinfobot
TELEGRAM_USER_IDS=111111111,222222222
```

### Filters (`config.py`)

```python
FILTERS = {
    'min_age_minutes': 5,
    'max_age_hours': 24,
    'min_mcap': 10_000,
    'max_mcap': 10_000_000,
    'chains': ['solana'],
    'min_liquidity': 5_000,
}
```

## How It Works

1. Polls DexScreener `/token-profiles/latest/v1` every 30 seconds for paid tokens
2. New tokens are stored in a 24-hour rolling memory window
3. When a token adds a Twitter link, fetches market data from `/latest/dex/tokens/{address}`
4. Applies filters (chain, age, market cap, liquidity)
5. Sends a formatted Telegram alert to all users from `.env`
6. Prevents duplicate alerts via an `alerted` set persisted to disk

## Running in Background

```bash
# Option 1: nohup
nohup python main.py > output.log 2>&1 &

# Option 2: screen
screen -S dexmonitor
python main.py
# Ctrl+A, D to detach; screen -r dexmonitor to reattach

# Option 3: systemd (see spec for unit file)
```

## Troubleshooting

- **No alerts:** Check that filters match the tokens you expect. Review `logs/monitor.log`.
- **Duplicate alerts:** Inspect `data/memory.json` for corruption.
- **API errors:** Check logs; DexScreener API may be temporarily down.
- **Telegram errors:** Verify bot token and user IDs in `.env` are correct.
