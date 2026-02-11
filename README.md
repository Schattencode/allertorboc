# DexScreener Twitter Monitor

24/7 bot that monitors DexScreener Enhanced Token Info and alerts when tokens add Twitter links.

## Features

- Monitors only PAID DexScreener tokens (`/token-profiles/latest/v1`)
- Alerts when Twitter added (immediately or up to 24h later)
- 1-2 minute alert latency (30s polling interval)
- 24-hour rolling memory window
- Configurable filters (age, market cap, chain, liquidity)
- State persistence across restarts (`data/memory.json`)
- Rotating log files

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure credentials (edit config.py or set env vars)
export TELEGRAM_BOT_TOKEN="your_token"
export TELEGRAM_CHAT_ID="your_chat_id"

# Optional: test API access first
python test_api.py

# Run
python main.py
```

## Configuration

Edit `config.py` or set environment variables:

```python
TELEGRAM_BOT_TOKEN = "your_token"
TELEGRAM_CHAT_ID = "your_chat_id"

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
5. Sends a formatted Telegram alert
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
- **Telegram errors:** Verify bot token and chat ID are correct.
