#!/usr/bin/env python3
"""
DexScreener Twitter Monitor
24/7 bot that monitors tokens with PAID DexScreener Enhanced Token Info
and sends Telegram alerts when they add Twitter/X links.
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

import aiohttp

from config import (
    API_MAX_CONSECUTIVE_FAILURES,
    API_PROFILES_URL,
    API_TIMEOUT,
    API_TOKENS_URL,
    CHECK_INTERVAL,
    DATA_DIR,
    FILTERS,
    LOG_DIR,
    MEMORY_HOURS,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    """Configure logging with console and rotating file handlers."""
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger('dexscreener_monitor')
    logger.setLevel(logging.DEBUG)

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
    ))

    # File handler (rotating, 10 MB, 5 backups)
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, 'monitor.log'),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    ))

    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger


logger = setup_logging()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_twitter_url(profile):
    """
    Extract Twitter/X URL from a token profile's links array.

    Checks for type == "twitter" first, then falls back to URL matching.
    Returns the URL string or None.
    """
    links = profile.get('links') or []
    for link in links:
        link_type = (link.get('type') or '').lower()
        url = link.get('url', '')
        if link_type == 'twitter':
            return url
        if 'twitter.com' in url or 'x.com' in url:
            return url
    return None


def format_age(ms_timestamp):
    """Return a human-readable age string from a millisecond timestamp."""
    if not ms_timestamp:
        return 'Unknown'
    age_seconds = (time.time() * 1000 - ms_timestamp) / 1000
    if age_seconds < 0:
        return '0h 0m'
    hours = int(age_seconds / 3600)
    minutes = int((age_seconds % 3600) / 60)
    return f'{hours}h {minutes}m'


def format_delay(first_seen_iso, twitter_added_iso):
    """Return a human-readable delay between first_seen and twitter_added."""
    first_seen = datetime.fromisoformat(first_seen_iso)
    twitter_added = datetime.fromisoformat(twitter_added_iso)
    delay = twitter_added - first_seen
    hours = int(delay.total_seconds() / 3600)
    minutes = int((delay.total_seconds() % 3600) / 60)
    return f'{hours}h {minutes}m'

# ---------------------------------------------------------------------------
# Token Filter
# ---------------------------------------------------------------------------

class TokenFilter:
    """Apply configurable filters to token data."""

    def __init__(self, config):
        self.config = config

    def check(self, token_data):
        """
        Check whether a token passes all filters.

        Args:
            token_data: Raw response from /latest/dex/tokens/{address}

        Returns:
            (passes: bool, reason: str)
        """
        if not token_data or 'pairs' not in token_data or not token_data['pairs']:
            return False, 'No pair data available'

        pair = token_data['pairs'][0]

        # Chain filter
        chain_id = pair.get('chainId', '')
        if chain_id not in self.config['chains']:
            return False, f'Chain {chain_id} not in whitelist'

        # Age filter
        pair_created = pair.get('pairCreatedAt')
        if pair_created:
            age_minutes = (time.time() * 1000 - pair_created) / 1000 / 60

            if age_minutes < self.config['min_age_minutes']:
                return False, f'Too young: {age_minutes:.1f} minutes'

            age_hours = age_minutes / 60
            if age_hours > self.config['max_age_hours']:
                return False, f'Too old: {age_hours:.1f} hours'

        # Market cap filter
        mcap = pair.get('fdv') or pair.get('marketCap') or 0
        if mcap < self.config['min_mcap']:
            return False, f'Market cap too low: ${mcap:,.0f}'
        if mcap > self.config['max_mcap']:
            return False, f'Market cap too high: ${mcap:,.0f}'

        # Liquidity filter
        liquidity = (pair.get('liquidity') or {}).get('usd', 0) or 0
        min_liq = self.config.get('min_liquidity', 0)
        if liquidity < min_liq:
            return False, f'Liquidity too low: ${liquidity:,.0f}'

        return True, 'OK'

# ---------------------------------------------------------------------------
# DexScreener API client
# ---------------------------------------------------------------------------

class DexScreenerAPI:
    """Async client for the DexScreener public API."""

    def __init__(self):
        self.profiles_url = API_PROFILES_URL
        self.tokens_url = API_TOKENS_URL
        self.timeout = aiohttp.ClientTimeout(total=API_TIMEOUT)
        self.consecutive_failures = 0
        self.max_failures = API_MAX_CONSECUTIVE_FAILURES

    async def get_token_profiles(self):
        """
        Fetch the latest paid token profiles.
        Returns a list of profile dicts, or None on failure.
        """
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(self.profiles_url) as resp:
                    if resp.status == 200:
                        self.consecutive_failures = 0
                        data = await resp.json()
                        if isinstance(data, dict):
                            return [data] if data else []
                        return data or []
                    if resp.status == 429:
                        logger.warning('Rate limited on profiles API, backing off 60s')
                        await asyncio.sleep(60)
                        return None
                    logger.error('Profiles API returned %d', resp.status)
                    self.consecutive_failures += 1
                    return None
        except asyncio.TimeoutError:
            logger.error('Profiles API request timed out')
            self.consecutive_failures += 1
            return None
        except Exception as exc:
            logger.error('Profiles API error: %s', exc)
            self.consecutive_failures += 1
            return None

    async def get_token_data(self, token_address):
        """
        Fetch detailed token/pair data for a specific address.
        Returns the parsed JSON dict, or None on failure.
        """
        url = f'{self.tokens_url}/{token_address}'
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    logger.error('Token data API returned %d for %s', resp.status, token_address[:12])
                    return None
        except asyncio.TimeoutError:
            logger.error('Token data API timed out for %s', token_address[:12])
            return None
        except Exception as exc:
            logger.error('Token data API error for %s: %s', token_address[:12], exc)
            return None

    def is_healthy(self):
        """Return False if too many consecutive failures have occurred."""
        if self.consecutive_failures >= self.max_failures:
            logger.critical('Too many consecutive API failures (%d)!', self.consecutive_failures)
            return False
        return True

# ---------------------------------------------------------------------------
# Token Memory (24-hour rolling window)
# ---------------------------------------------------------------------------

class TokenMemory:
    """In-memory + on-disk store for paid tokens within a 24h window."""

    def __init__(self, data_dir=DATA_DIR, memory_hours=MEMORY_HOURS):
        self.data_dir = data_dir
        self.memory_hours = memory_hours
        self.tokens = {}       # {tokenAddress: dict}
        self.alerted = set()   # set of tokenAddress strings
        self.load_from_disk()

    # -- CRUD ---------------------------------------------------------------

    def add_token(self, token_info):
        self.tokens[token_info['tokenAddress']] = token_info

    def update_token(self, address, updates):
        if address in self.tokens:
            self.tokens[address].update(updates)

    def get_token(self, address):
        return self.tokens.get(address)

    def has_token(self, address):
        return address in self.tokens

    # -- Cleanup ------------------------------------------------------------

    def cleanup_old(self):
        """Remove tokens whose first_seen is older than the memory window."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.memory_hours)
        to_remove = []
        for address, token in self.tokens.items():
            try:
                first_seen = datetime.fromisoformat(token['first_seen'])
                if first_seen < cutoff:
                    to_remove.append(address)
            except (KeyError, ValueError):
                to_remove.append(address)

        for address in to_remove:
            del self.tokens[address]
            self.alerted.discard(address)

        if to_remove:
            logger.info('Cleaned up %d token(s) older than %dh', len(to_remove), self.memory_hours)

    # -- Persistence --------------------------------------------------------

    def save_to_disk(self):
        """Atomically persist state to data/memory.json."""
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            state = {
                'tokens': self.tokens,
                'alerted': list(self.alerted),
                'saved_at': datetime.now(timezone.utc).isoformat(),
            }
            tmp = os.path.join(self.data_dir, 'memory.tmp.json')
            final = os.path.join(self.data_dir, 'memory.json')
            with open(tmp, 'w') as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, final)
            logger.debug('Memory saved to disk (%d tokens)', len(self.tokens))
        except Exception as exc:
            logger.error('Failed to save memory: %s', exc)

    def load_from_disk(self):
        """Load state from data/memory.json if it exists."""
        path = os.path.join(self.data_dir, 'memory.json')
        try:
            if not os.path.exists(path):
                logger.info('No saved state found, starting fresh')
                return
            with open(path, 'r') as f:
                state = json.load(f)
            self.tokens = state.get('tokens', {})
            self.alerted = set(state.get('alerted', []))
            saved_at = state.get('saved_at', 'unknown')
            logger.info('Loaded %d tokens from disk (saved at %s)', len(self.tokens), saved_at)
        except Exception as exc:
            logger.error('Failed to load memory: %s', exc)
            logger.warning('Starting with empty memory')

# ---------------------------------------------------------------------------
# Telegram alerts
# ---------------------------------------------------------------------------

def build_alert_message(token_info, alert_type):
    """Build a Markdown-formatted Telegram alert message."""
    symbol = token_info.get('symbol', '???')
    name = token_info.get('name', 'Unknown')
    chain = token_info.get('chainId', 'unknown').upper()
    mcap = token_info.get('market_cap', 0)
    liquidity = token_info.get('liquidity_usd', 0)
    price = token_info.get('price_usd', 0)
    twitter_url = token_info.get('twitter_url', '')
    pair_address = token_info.get('pairAddress', token_info.get('tokenAddress', ''))
    created_at = token_info.get('pairCreatedAt', 0)

    token_age = format_age(created_at)

    if alert_type == 'NEW_WITH_TWITTER':
        header = '✨ NEW PAID DEXSCREENER + TWITTER'
        extra = ''
    else:
        header = '🔔 TWITTER LINK ADDED'
        first_seen = token_info.get('first_seen', '')
        twitter_added = token_info.get('twitter_added_at', '')
        if first_seen and twitter_added:
            delay = format_delay(first_seen, twitter_added)
            extra = f'⏱️ Twitter Added: {delay} after DexScreener payment\n'
        else:
            extra = ''

    msg = f'{header}\n\n'
    msg += f'*{symbol}* | {name}\n'
    msg += f'Chain: {chain}\n\n'
    msg += f'💰 Market Cap: ${mcap:,.0f}\n'
    msg += f'💧 Liquidity: ${liquidity:,.0f}\n'
    msg += f'💵 Price: ${price:.8f}\n'
    msg += f'⏰ Token Age: {token_age}\n'
    msg += extra
    msg += f'\n🐦 Twitter: {twitter_url}\n'
    msg += f'\n📈 DexScreener: https://dexscreener.com/{chain.lower()}/{pair_address}'
    return msg


async def send_telegram_alert(message):
    """Send a message to the configured Telegram chat. Returns True on success."""
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': True,
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    logger.info('Alert sent to Telegram')
                    return True
                error_text = await resp.text()
                logger.error('Telegram API error %d: %s', resp.status, error_text)
                return False
    except Exception as exc:
        logger.error('Failed to send Telegram alert: %s', exc)
        return False

# ---------------------------------------------------------------------------
# Token info builder
# ---------------------------------------------------------------------------

def build_token_info(profile, token_data, twitter_url):
    """
    Construct a token_info dict from a profile response and token data response.
    """
    pair = token_data['pairs'][0] if token_data.get('pairs') else {}
    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        'tokenAddress': profile.get('tokenAddress', ''),
        'chainId': profile.get('chainId', ''),
        'symbol': pair.get('baseToken', {}).get('symbol', '???'),
        'name': pair.get('baseToken', {}).get('name', 'Unknown'),
        'pairAddress': pair.get('pairAddress', ''),
        'pairCreatedAt': pair.get('pairCreatedAt', 0),

        # DexScreener payment tracking
        'first_seen': now_iso,
        'dex_paid': True,

        # Twitter tracking
        'twitter_url': twitter_url,
        'twitter_added_at': now_iso if twitter_url else None,
        'had_twitter_initially': twitter_url is not None,

        # Timestamps
        'last_checked': now_iso,

        # Market data
        'market_cap': pair.get('fdv') or pair.get('marketCap') or 0,
        'liquidity_usd': (pair.get('liquidity') or {}).get('usd', 0) or 0,
        'price_usd': float(pair.get('priceUsd', 0) or 0),
    }

# ---------------------------------------------------------------------------
# Core processing logic
# ---------------------------------------------------------------------------

async def process_profile(profile, api, memory, filters):
    """Process a single token profile from the profiles API."""
    token_address = profile.get('tokenAddress')
    if not token_address:
        return

    twitter_url = extract_twitter_url(profile)

    if not memory.has_token(token_address):
        # ---- NEW TOKEN ----
        token_data = await api.get_token_data(token_address)
        if not token_data:
            logger.error('Failed to fetch token data for %s', token_address[:12])
            return

        passes, reason = filters.check(token_data)
        if not passes:
            logger.info('Token %s filtered out: %s', token_address[:12], reason)
            return

        token_info = build_token_info(profile, token_data, twitter_url)
        memory.add_token(token_info)

        if twitter_url:
            await _send_alert(token_info, 'NEW_WITH_TWITTER', memory)
        else:
            logger.info('New paid token saved (no Twitter yet): %s', token_address[:12])
    else:
        # ---- EXISTING TOKEN ----
        existing = memory.get_token(token_address)
        had_twitter = existing.get('twitter_url') is not None
        has_twitter = twitter_url is not None

        if not had_twitter and has_twitter:
            # Twitter was just added!
            token_data = await api.get_token_data(token_address)

            if token_data:
                passes, reason = filters.check(token_data)
                if not passes:
                    logger.info('Token %s no longer passes filters: %s', token_address[:12], reason)
                    memory.update_token(token_address, {
                        'last_checked': datetime.now(timezone.utc).isoformat(),
                        'twitter_url': twitter_url,
                    })
                    return

                pair = token_data['pairs'][0] if token_data.get('pairs') else {}
                memory.update_token(token_address, {
                    'twitter_url': twitter_url,
                    'twitter_added_at': datetime.now(timezone.utc).isoformat(),
                    'last_checked': datetime.now(timezone.utc).isoformat(),
                    'market_cap': pair.get('fdv') or pair.get('marketCap') or 0,
                    'liquidity_usd': (pair.get('liquidity') or {}).get('usd', 0) or 0,
                    'price_usd': float(pair.get('priceUsd', 0) or 0),
                })
            else:
                # Couldn't fetch fresh data — still record the Twitter URL
                memory.update_token(token_address, {
                    'twitter_url': twitter_url,
                    'twitter_added_at': datetime.now(timezone.utc).isoformat(),
                    'last_checked': datetime.now(timezone.utc).isoformat(),
                })

            updated_info = memory.get_token(token_address)
            await _send_alert(updated_info, 'TWITTER_ADDED', memory)
        else:
            # No relevant change
            memory.update_token(token_address, {
                'last_checked': datetime.now(timezone.utc).isoformat(),
            })


async def _send_alert(token_info, alert_type, memory):
    """Send an alert if one hasn't already been sent for this token."""
    address = token_info['tokenAddress']
    if address in memory.alerted:
        logger.warning('Alert already sent for %s, skipping', address[:12])
        return

    message = build_alert_message(token_info, alert_type)
    success = await send_telegram_alert(message)
    if success:
        memory.alerted.add(address)
        memory.save_to_disk()

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def main():
    """Main monitoring loop."""
    logger.info('DexScreener Twitter Monitor starting...')
    logger.info('Filters: %s', FILTERS)
    logger.info('Check interval: %ds', CHECK_INTERVAL)

    api = DexScreenerAPI()
    memory = TokenMemory()
    token_filter = TokenFilter(FILTERS)

    iteration = 0

    while True:
        try:
            iteration += 1
            logger.info('=' * 50)
            logger.info('Iteration #%d - %s', iteration, datetime.now(timezone.utc).strftime('%H:%M:%S UTC'))
            logger.info('=' * 50)

            if not api.is_healthy():
                logger.critical('API unhealthy, pausing for 5 minutes...')
                await asyncio.sleep(300)
                # Reset counter so we try again
                api.consecutive_failures = 0
                continue

            profiles = await api.get_token_profiles()

            if profiles is None:
                logger.warning('Failed to fetch profiles, will retry next iteration')
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            logger.info('Fetched %d profile(s)', len(profiles))

            for profile in profiles:
                try:
                    await process_profile(profile, api, memory, token_filter)
                except Exception as exc:
                    logger.error('Error processing profile %s: %s',
                                 profile.get('tokenAddress', '?')[:12], exc, exc_info=True)

            # Cleanup old tokens
            memory.cleanup_old()

            # Periodic save (every 10 iterations ≈ 5 minutes)
            if iteration % 10 == 0:
                memory.save_to_disk()

            logger.info('Memory: %d token(s), %d alerted', len(memory.tokens), len(memory.alerted))

            await asyncio.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            raise
        except Exception as exc:
            logger.error('Unexpected error in main loop: %s', exc, exc_info=True)
            await asyncio.sleep(CHECK_INTERVAL)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    if not TELEGRAM_BOT_TOKEN or 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN:
        logger.error('Please configure TELEGRAM_BOT_TOKEN in config.py or as an env var!')
        sys.exit(1)

    if not TELEGRAM_CHAT_ID or 'YOUR_CHAT_ID' in TELEGRAM_CHAT_ID:
        logger.error('Please configure TELEGRAM_CHAT_ID in config.py or as an env var!')
        sys.exit(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Stopped by user')
