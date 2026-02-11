#!/usr/bin/env python3
"""
DexScreener Twitter Monitor
24/7 bot that monitors tokens with PAID DexScreener Enhanced Token Info
and sends Telegram alerts when they add Twitter/X links.
Includes interactive Telegram commands for filter management.
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
    TELEGRAM_USER_IDS,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    """Configure logging with console and rotating file handlers."""
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger('dexscreener_monitor')
    logger.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
    ))

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
    """Extract Twitter/X URL from a token profile's links array."""
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


def extract_twitter_from_pairs(token_data):
    """Extract Twitter URL from token/pair data (info.socials field)."""
    if not token_data or not token_data.get('pairs'):
        return None
    pair = token_data['pairs'][0]
    info = pair.get('info') or {}
    socials = info.get('socials') or []
    for social in socials:
        stype = (social.get('type') or '').lower()
        url = social.get('url', '')
        if stype == 'twitter':
            return url
        if 'twitter.com' in url or 'x.com' in url:
            return url
    return None

# ---------------------------------------------------------------------------
# Live Filters (mutable at runtime, persisted to disk)
# ---------------------------------------------------------------------------

FILTERS_FILE = os.path.join(DATA_DIR, 'filters.json')


def load_filters():
    """Load filters from disk, falling back to config.py defaults."""
    try:
        if os.path.exists(FILTERS_FILE):
            with open(FILTERS_FILE, 'r') as f:
                saved = json.load(f)
            logger.info('Loaded filters from disk: %s', saved)
            return saved
    except Exception as exc:
        logger.error('Failed to load filters: %s', exc)
    return dict(FILTERS)


def save_filters(filters):
    """Persist current filters to disk."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(FILTERS_FILE, 'w') as f:
            json.dump(filters, f, indent=2)
        logger.info('Filters saved to disk')
    except Exception as exc:
        logger.error('Failed to save filters: %s', exc)


# Global mutable filters dict — shared between monitor and command handler
live_filters = load_filters()

# ---------------------------------------------------------------------------
# Token Filter
# ---------------------------------------------------------------------------

class TokenFilter:
    """Apply configurable filters to token data."""

    def __init__(self, config):
        self.config = config

    def check(self, token_data):
        if not token_data or 'pairs' not in token_data or not token_data['pairs']:
            return False, 'No pair data available'

        pair = token_data['pairs'][0]

        chain_id = pair.get('chainId', '')
        if chain_id not in self.config['chains']:
            return False, f'Chain {chain_id} not in whitelist'

        pair_created = pair.get('pairCreatedAt')
        if pair_created:
            age_minutes = (time.time() * 1000 - pair_created) / 1000 / 60
            if age_minutes < self.config['min_age_minutes']:
                return False, f'Too young: {age_minutes:.1f} minutes'
            age_hours = age_minutes / 60
            if age_hours > self.config['max_age_hours']:
                return False, f'Too old: {age_hours:.1f} hours'

        mcap = pair.get('fdv') or pair.get('marketCap') or 0
        if mcap < self.config['min_mcap']:
            return False, f'Market cap too low: ${mcap:,.0f}'
        if mcap > self.config['max_mcap']:
            return False, f'Market cap too high: ${mcap:,.0f}'

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
        self.tokens = {}
        self.alerted = set()
        self.load_from_disk()

    def add_token(self, token_info):
        self.tokens[token_info['tokenAddress']] = token_info

    def update_token(self, address, updates):
        if address in self.tokens:
            self.tokens[address].update(updates)

    def get_token(self, address):
        return self.tokens.get(address)

    def has_token(self, address):
        return address in self.tokens

    def cleanup_old(self):
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

    def save_to_disk(self):
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
# Telegram: send message helper
# ---------------------------------------------------------------------------

async def _tg_send(chat_id, text, session):
    """Send a single Telegram message."""
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': True,
    }
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                err = await resp.text()
                logger.error('Telegram send error for %s (%d): %s', chat_id, resp.status, err)
    except Exception as exc:
        logger.error('Telegram send failed for %s: %s', chat_id, exc)

# ---------------------------------------------------------------------------
# Telegram alerts (broadcast to all users)
# ---------------------------------------------------------------------------

def build_alert_message(token_info, alert_type):
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
    """Broadcast alert to all users. Returns True if at least one succeeded."""
    if not TELEGRAM_USER_IDS:
        logger.error('No TELEGRAM_USER_IDS configured')
        return False

    any_success = False
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            for user_id in TELEGRAM_USER_IDS:
                url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
                payload = {
                    'chat_id': user_id,
                    'text': message,
                    'parse_mode': 'Markdown',
                    'disable_web_page_preview': True,
                }
                try:
                    async with session.post(url, json=payload) as resp:
                        if resp.status == 200:
                            logger.info('Alert sent to user %s', user_id)
                            any_success = True
                        else:
                            err = await resp.text()
                            logger.error('Telegram error for user %s (%d): %s', user_id, resp.status, err)
                except Exception as exc:
                    logger.error('Failed to send alert to user %s: %s', user_id, exc)
    except Exception as exc:
        logger.error('Telegram session error: %s', exc)
    return any_success

# ---------------------------------------------------------------------------
# Token info builder
# ---------------------------------------------------------------------------

def build_token_info(profile, token_data, twitter_url):
    pair = token_data['pairs'][0] if token_data.get('pairs') else {}
    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        'tokenAddress': profile.get('tokenAddress', ''),
        'chainId': profile.get('chainId', ''),
        'symbol': pair.get('baseToken', {}).get('symbol', '???'),
        'name': pair.get('baseToken', {}).get('name', 'Unknown'),
        'pairAddress': pair.get('pairAddress', ''),
        'pairCreatedAt': pair.get('pairCreatedAt', 0),
        'first_seen': now_iso,
        'dex_paid': True,
        'twitter_url': twitter_url,
        'twitter_added_at': now_iso if twitter_url else None,
        'had_twitter_initially': twitter_url is not None,
        'last_checked': now_iso,
        'market_cap': pair.get('fdv') or pair.get('marketCap') or 0,
        'liquidity_usd': (pair.get('liquidity') or {}).get('usd', 0) or 0,
        'price_usd': float(pair.get('priceUsd', 0) or 0),
    }

# ---------------------------------------------------------------------------
# Core processing logic
# ---------------------------------------------------------------------------

async def process_profile(profile, api, memory, token_filter):
    token_address = profile.get('tokenAddress')
    if not token_address:
        return

    twitter_url = extract_twitter_url(profile)

    if not memory.has_token(token_address):
        token_data = await api.get_token_data(token_address)
        if not token_data:
            logger.error('Failed to fetch token data for %s', token_address[:12])
            return

        passes, reason = token_filter.check(token_data)
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
        existing = memory.get_token(token_address)
        had_twitter = existing.get('twitter_url') is not None
        has_twitter = twitter_url is not None

        if not had_twitter and has_twitter:
            token_data = await api.get_token_data(token_address)
            if token_data:
                passes, reason = token_filter.check(token_data)
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
                memory.update_token(token_address, {
                    'twitter_url': twitter_url,
                    'twitter_added_at': datetime.now(timezone.utc).isoformat(),
                    'last_checked': datetime.now(timezone.utc).isoformat(),
                })
            updated_info = memory.get_token(token_address)
            await _send_alert(updated_info, 'TWITTER_ADDED', memory)
        else:
            memory.update_token(token_address, {
                'last_checked': datetime.now(timezone.utc).isoformat(),
            })


async def _send_alert(token_info, alert_type, memory):
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
# Background re-check: tokens in memory without Twitter
# ---------------------------------------------------------------------------

RECHECK_INTERVAL = 120  # seconds between re-check cycles
RECHECK_DELAY = 1       # seconds between individual token checks (rate limit)


async def recheck_loop(api, memory, token_filter):
    """Periodically re-check tokens in memory that don't have Twitter yet."""
    logger.info('Background re-check loop started (every %ds)', RECHECK_INTERVAL)

    while True:
        await asyncio.sleep(RECHECK_INTERVAL)

        try:
            no_twitter = [
                addr for addr, info in memory.tokens.items()
                if info.get('twitter_url') is None and addr not in memory.alerted
            ]

            if not no_twitter:
                logger.debug('Re-check: all tokens in memory already have Twitter or were alerted')
                continue

            logger.info('Re-checking %d token(s) without Twitter...', len(no_twitter))

            found = 0
            for address in no_twitter:
                try:
                    token_data = await api.get_token_data(address)
                    if not token_data:
                        continue

                    twitter_url = extract_twitter_from_pairs(token_data)
                    if not twitter_url:
                        await asyncio.sleep(RECHECK_DELAY)
                        continue

                    # Twitter was found!
                    logger.info('Re-check: Twitter found for %s!', address[:12])
                    found += 1

                    passes, reason = token_filter.check(token_data)
                    if not passes:
                        logger.info('Token %s no longer passes filters: %s', address[:12], reason)
                        memory.update_token(address, {
                            'twitter_url': twitter_url,
                            'last_checked': datetime.now(timezone.utc).isoformat(),
                        })
                        continue

                    pair = token_data['pairs'][0] if token_data.get('pairs') else {}
                    memory.update_token(address, {
                        'twitter_url': twitter_url,
                        'twitter_added_at': datetime.now(timezone.utc).isoformat(),
                        'last_checked': datetime.now(timezone.utc).isoformat(),
                        'market_cap': pair.get('fdv') or pair.get('marketCap') or 0,
                        'liquidity_usd': (pair.get('liquidity') or {}).get('usd', 0) or 0,
                        'price_usd': float(pair.get('priceUsd', 0) or 0),
                    })

                    updated_info = memory.get_token(address)
                    await _send_alert(updated_info, 'TWITTER_ADDED', memory)
                    await asyncio.sleep(RECHECK_DELAY)

                except Exception as exc:
                    logger.error('Re-check error for %s: %s', address[:12], exc)

            if found:
                logger.info('Re-check done: found Twitter for %d token(s)', found)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error('Re-check loop error: %s', exc)

# ---------------------------------------------------------------------------
# Telegram Command Handler
# ---------------------------------------------------------------------------

class TelegramCommandHandler:
    """Listens for Telegram commands via getUpdates and handles them."""

    def __init__(self, memory):
        self.memory = memory
        self.last_update_id = 0
        self.start_time = datetime.now(timezone.utc)

    def _is_authorized(self, user_id):
        return str(user_id) in TELEGRAM_USER_IDS

    def _format_settings(self):
        f = live_filters
        chains = ', '.join(f.get('chains', []))
        return (
            '⚙️ *Текущие настройки фильтров:*\n\n'
            f'💰 Min Market Cap: `${f["min_mcap"]:,.0f}`\n'
            f'💰 Max Market Cap: `${f["max_mcap"]:,.0f}`\n'
            f'💧 Min Liquidity: `${f.get("min_liquidity", 0):,.0f}`\n'
            f'⏰ Min Age: `{f["min_age_minutes"]} мин`\n'
            f'⏰ Max Age: `{f["max_age_hours"]} ч`\n'
            f'🔗 Chains: `{chains}`\n'
            f'🧠 Memory: `{self.memory.memory_hours} ч`'
        )

    def _format_help(self):
        return (
            '🤖 *DexScreener Twitter Monitor*\n\n'
            'Бот отслеживает токены с оплаченным DexScreener '
            'и шлёт алерт когда добавляют Twitter.\n\n'
            '*Команды:*\n'
            '/settings — текущие фильтры\n'
            '/status — статус бота\n'
            '/set\\_min\\_mcap `число` — мин. маркеткэп\n'
            '/set\\_max\\_mcap `число` — макс. маркеткэп\n'
            '/set\\_min\\_liq `число` — мин. ликвидность\n'
            '/set\\_min\\_age `минуты` — мин. возраст токена\n'
            '/set\\_max\\_age `часы` — макс. возраст токена\n'
            '/set\\_chains `chain1,chain2` — сети\n'
            '/set\\_memory `часы` — окно памяти\n'
            '/reset — сбросить фильтры по умолчанию'
        )

    async def handle_command(self, text, chat_id, session):
        """Parse and execute a command, reply to chat_id."""
        text = text.strip()
        cmd_parts = text.split(maxsplit=1)
        cmd = cmd_parts[0].lower().split('@')[0]  # strip @botname
        arg = cmd_parts[1].strip() if len(cmd_parts) > 1 else ''

        if cmd == '/start' or cmd == '/help':
            reply = self._format_help()
            await _tg_send(chat_id, reply, session)
            await _tg_send(chat_id, self._format_settings(), session)
            return

        if cmd == '/settings':
            await _tg_send(chat_id, self._format_settings(), session)
            return

        if cmd == '/status':
            uptime = datetime.now(timezone.utc) - self.start_time
            h = int(uptime.total_seconds() // 3600)
            m = int((uptime.total_seconds() % 3600) // 60)
            reply = (
                '📊 *Статус бота:*\n\n'
                f'⏱ Аптайм: `{h}ч {m}м`\n'
                f'🧠 Токенов в памяти: `{len(self.memory.tokens)}`\n'
                f'📨 Алертов отправлено: `{len(self.memory.alerted)}`\n'
                f'👥 Пользователей: `{len(TELEGRAM_USER_IDS)}`'
            )
            await _tg_send(chat_id, reply, session)
            return

        if cmd == '/reset':
            live_filters.clear()
            live_filters.update(FILTERS)
            save_filters(live_filters)
            await _tg_send(chat_id, '✅ Фильтры сброшены по умолчанию.', session)
            await _tg_send(chat_id, self._format_settings(), session)
            return

        # --- Set commands ---
        setter_map = {
            '/set_min_mcap':  ('min_mcap', float, '💰 Min Market Cap'),
            '/set_max_mcap':  ('max_mcap', float, '💰 Max Market Cap'),
            '/set_min_liq':   ('min_liquidity', float, '💧 Min Liquidity'),
            '/set_min_age':   ('min_age_minutes', float, '⏰ Min Age (мин)'),
            '/set_max_age':   ('max_age_hours', float, '⏰ Max Age (ч)'),
            '/set_memory':    ('__memory__', int, '🧠 Memory (ч)'),
        }

        if cmd in setter_map:
            key, typ, label = setter_map[cmd]
            if not arg:
                await _tg_send(chat_id, f'❌ Укажи значение.\nПример: `{cmd} 50000`', session)
                return
            try:
                value = typ(arg.replace(',', '').replace('_', ''))
            except ValueError:
                await _tg_send(chat_id, f'❌ Неверное число: `{arg}`', session)
                return

            if key == '__memory__':
                self.memory.memory_hours = int(value)
                # Also save in filters file for persistence
                live_filters['__memory_hours__'] = int(value)
            else:
                live_filters[key] = value

            save_filters(live_filters)
            await _tg_send(chat_id, f'✅ {label} = `{value}`', session)
            await _tg_send(chat_id, self._format_settings(), session)
            return

        if cmd == '/set_chains':
            if not arg:
                await _tg_send(chat_id, '❌ Укажи сети через запятую.\nПример: `/set_chains solana,base`', session)
                return
            chains = [c.strip().lower() for c in arg.split(',') if c.strip()]
            if not chains:
                await _tg_send(chat_id, '❌ Пустой список сетей.', session)
                return
            live_filters['chains'] = chains
            save_filters(live_filters)
            await _tg_send(chat_id, f'✅ Chains = `{", ".join(chains)}`', session)
            await _tg_send(chat_id, self._format_settings(), session)
            return

        # Unknown command
        await _tg_send(chat_id, '❓ Неизвестная команда. Отправь /help для списка команд.', session)

    async def poll_updates(self):
        """Long-poll Telegram getUpdates in a loop."""
        logger.info('Telegram command listener started')
        url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates'

        while True:
            try:
                params = {
                    'offset': self.last_update_id + 1,
                    'timeout': 30,
                }
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as session:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            logger.error('getUpdates returned %d', resp.status)
                            await asyncio.sleep(5)
                            continue
                        data = await resp.json()

                    if not data.get('ok'):
                        await asyncio.sleep(5)
                        continue

                    for update in data.get('result', []):
                        self.last_update_id = update['update_id']
                        msg = update.get('message')
                        if not msg:
                            continue
                        text = msg.get('text', '')
                        if not text.startswith('/'):
                            continue
                        user_id = msg.get('from', {}).get('id')
                        chat_id = msg['chat']['id']

                        if not self._is_authorized(user_id):
                            await _tg_send(chat_id, '⛔ У тебя нет доступа к этому боту.', session)
                            continue

                        await self.handle_command(text, chat_id, session)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error('Telegram poll error: %s', exc)
                await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# Monitor loop
# ---------------------------------------------------------------------------

async def monitor_loop(api, memory, token_filter):
    """Main DexScreener monitoring loop."""
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

            memory.cleanup_old()

            if iteration % 10 == 0:
                memory.save_to_disk()

            logger.info('Memory: %d token(s), %d alerted', len(memory.tokens), len(memory.alerted))
            await asyncio.sleep(CHECK_INTERVAL)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error('Unexpected error in monitor loop: %s', exc, exc_info=True)
            await asyncio.sleep(CHECK_INTERVAL)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    logger.info('DexScreener Twitter Monitor starting...')
    logger.info('Filters: %s', live_filters)
    logger.info('Check interval: %ds', CHECK_INTERVAL)

    api = DexScreenerAPI()
    memory = TokenMemory()

    # Restore memory hours from saved filters
    saved_mem_hours = live_filters.get('__memory_hours__')
    if saved_mem_hours:
        memory.memory_hours = saved_mem_hours

    token_filter = TokenFilter(live_filters)
    cmd_handler = TelegramCommandHandler(memory)

    # Send startup message to all users
    startup_msg = (
        '🚀 *Бот запущен!*\n\n'
        'Отправь /help чтобы увидеть команды.\n'
        'Отправь /settings чтобы увидеть текущие фильтры.'
    )
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
        for uid in TELEGRAM_USER_IDS:
            await _tg_send(uid, startup_msg, session)

    # Run monitor + command listener + background re-check concurrently
    await asyncio.gather(
        monitor_loop(api, memory, token_filter),
        cmd_handler.poll_updates(),
        recheck_loop(api, memory, token_filter),
    )

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    if not TELEGRAM_BOT_TOKEN:
        logger.error('TELEGRAM_BOT_TOKEN not set! Add it to .env file.')
        sys.exit(1)

    if not TELEGRAM_USER_IDS:
        logger.error('TELEGRAM_USER_IDS not set! Add user IDs to .env file.')
        sys.exit(1)

    logger.info('Sending alerts to %d user(s): %s', len(TELEGRAM_USER_IDS), ', '.join(TELEGRAM_USER_IDS))

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Stopped by user')
