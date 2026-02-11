#!/usr/bin/env python3
"""Quick manual test script for verifying DexScreener API access."""

import asyncio
import json

import aiohttp


async def test_profiles_api():
    """Test the token profiles (paid tokens) endpoint."""
    url = 'https://api.dexscreener.com/token-profiles/latest/v1'
    print('--- Token Profiles API ---')
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print(f'Status: {resp.status}')
            data = await resp.json()
            print(f'Response type: {type(data).__name__}')
            if isinstance(data, list):
                print(f'Number of profiles: {len(data)}')
                if data:
                    sample = data[0]
                    print(f'Sample profile keys: {list(sample.keys())}')
                    print(f'Sample chainId: {sample.get("chainId")}')
                    print(f'Sample tokenAddress: {sample.get("tokenAddress", "")[:20]}...')
                    links = sample.get('links', [])
                    print(f'Sample links count: {len(links)}')
                    for link in links:
                        print(f'  - {link.get("type")}: {link.get("url", "")[:60]}')
            elif isinstance(data, dict):
                print(f'Single object keys: {list(data.keys())}')
            else:
                print(f'Unexpected data: {str(data)[:200]}')


async def test_token_data_api():
    """Test the token data endpoint with a known address (Wrapped SOL)."""
    address = 'So11111111111111111111111111111111111111112'
    url = f'https://api.dexscreener.com/latest/dex/tokens/{address}'
    print('\n--- Token Data API (Wrapped SOL) ---')
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            print(f'Status: {resp.status}')
            data = await resp.json()
            pairs = data.get('pairs', [])
            print(f'Number of pairs: {len(pairs)}')
            if pairs:
                pair = pairs[0]
                print(f'Chain: {pair.get("chainId")}')
                print(f'DEX: {pair.get("dexId")}')
                base = pair.get('baseToken', {})
                print(f'Base token: {base.get("symbol")} ({base.get("name")})')
                print(f'FDV (market cap): {pair.get("fdv")}')
                print(f'Liquidity USD: {(pair.get("liquidity") or {}).get("usd")}')
                print(f'Pair created at: {pair.get("pairCreatedAt")}')


async def test_profiles_with_twitter():
    """Fetch profiles and show which ones have Twitter links."""
    url = 'https://api.dexscreener.com/token-profiles/latest/v1'
    print('\n--- Profiles With Twitter ---')
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            if not isinstance(data, list):
                data = [data] if data else []

            with_twitter = 0
            without_twitter = 0
            for profile in data:
                links = profile.get('links', [])
                twitter = None
                for link in links:
                    lt = (link.get('type') or '').lower()
                    lu = link.get('url', '')
                    if lt == 'twitter' or 'twitter.com' in lu or 'x.com' in lu:
                        twitter = lu
                        break
                if twitter:
                    with_twitter += 1
                else:
                    without_twitter += 1

            print(f'Total profiles: {len(data)}')
            print(f'With Twitter: {with_twitter}')
            print(f'Without Twitter: {without_twitter}')


if __name__ == '__main__':
    print('DexScreener API Test\n')
    asyncio.run(test_profiles_api())
    asyncio.run(test_token_data_api())
    asyncio.run(test_profiles_with_twitter())
    print('\nDone.')
