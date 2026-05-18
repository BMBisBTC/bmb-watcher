import asyncio
import json
import os
import ssl
import hashlib
import base58
import aiohttp

UPSTASH_URL = os.environ.get('KV_REST_API_URL')
UPSTASH_TOKEN = os.environ.get('KV_REST_API_TOKEN')
START_BLOCK = 38149  # 5월 1일 이자 지급 시작 블록


def address_to_scripthash(address: str) -> str:
    decoded = base58.b58decode_check(address)
    pubkey_hash = decoded[1:]
    script = bytes([0x76, 0xa9, 0x14]) + pubkey_hash + bytes([0x88, 0xac])
    sha = hashlib.sha256(script).digest()
    return sha[::-1].hex()


async def redis_get(key: str):
    headers = {'Authorization': f'Bearer {UPSTASH_TOKEN}'}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{UPSTASH_URL}/get/{key}', headers=headers) as r:
            data = await r.json()
            return data.get('result')


async def redis_set(key: str, value: str):
    headers = {'Authorization': f'Bearer {UPSTASH_TOKEN}'}
    async with aiohttp.ClientSession() as session:
        await session.get(f'{UPSTASH_URL}/set/{key}/{value}', headers=headers)


async def check_address(session, address, balance_before, ssl_ctx):
    """이자 지급 이후 잔액이 줄었는지 확인"""
    try:
        url = f'https://explorer.mobick.info/api/address/{address}'
        async with session.get(url, ssl=ssl_ctx) as r:
            data = await r.json()
            current_balance = data.get('balanceSat', 0)
            if current_balance < balance_before:
                return True, current_balance
            return False, current_balance
    except Exception as e:
        print(f'오류 {address}: {e}')
        return False, balance_before


async def main():
    print('탈락 계산 시작...')

    addresses_json = await redis_get('watcher:addresses')
    balances_json = await redis_get('watcher:balances')

    addresses = json.loads(addresses_json) if addresses_json else []
    balances = json.loads(balances_json) if balances_json else {}

    print(f'주소 {len(addresses)}개, 스냅샷 {len(balances)}개')

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    dropout_count = 0
    checked = 0
    new_balances = dict(balances)

    # 10개씩 동시에 체크
    CONCURRENT = 10
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(addresses), CONCURRENT):
            batch = addresses[i:i+CONCURRENT]
            tasks = []
            for addr in batch:
                prev = balances.get(addr, 0)
                tasks.append(check_address(session, addr, prev, ssl_ctx))

            results = await asyncio.gather(*tasks)

            for addr, (is_dropout, current_bal) in zip(batch, results):
                checked += 1
                new_balances[addr] = current_bal
                if is_dropout:
                    dropout_count += 1
                    print(f'[탈락] {addr}: {balances.get(addr,0)} → {current_bal}')

            if checked % 100 == 0:
                print(f'진행: {checked}/{len(addresses)}, 탈락: {dropout_count}')
            await asyncio.sleep(0.5)

    print(f'\n최종 탈락 수: {dropout_count}')

    # Redis 업데이트
    await redis_set('dropout:2026-05', str(dropout_count))
    await redis_set('watcher:balances', json.dumps(new_balances))
    print('Redis 업데이트 완료!')


if __name__ == '__main__':
    asyncio.run(main())
