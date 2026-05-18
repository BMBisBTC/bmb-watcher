import asyncio
import json
import os
import ssl
import hashlib
import base58
import aiohttp

UPSTASH_URL = os.environ.get('KV_REST_API_URL')
UPSTASH_TOKEN = os.environ.get('KV_REST_API_TOKEN')
ELECTRUM_HOST = 'wallet.mobick.info'
ELECTRUM_PORT = 40009
SUBS_PER_CONNECTION = 100
BATCH_DELAY = 0.1


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


async def redis_incr(key: str):
    headers = {'Authorization': f'Bearer {UPSTASH_TOKEN}'}
    async with aiohttp.ClientSession() as session:
        await session.get(f'{UPSTASH_URL}/incr/{key}', headers=headers)


def make_ssl():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def open_connection():
    return await asyncio.open_connection(
        ELECTRUM_HOST, ELECTRUM_PORT, ssl=make_ssl())


async def handshake(reader, writer):
    msg = json.dumps({'id': 1, 'method': 'server.version', 'params': ['bmb-watcher', '1.4']}) + '\n'
    writer.write(msg.encode())
    await writer.drain()
    line = await asyncio.wait_for(reader.readline(), timeout=10)
    return json.loads(line)


async def send_msg(writer, lock, msg_id, method, params):
    msg = json.dumps({'id': msg_id, 'method': method, 'params': params}) + '\n'
    async with lock:
        writer.write(msg.encode())
        await writer.drain()


async def watch_connection(conn_id, addresses, balances, dropout_key):
    """단일 연결 관리 - 끊기면 재연결"""
    subs = {address_to_scripthash(a): a for a in addresses}

    while True:
        try:
            reader, writer = await open_connection()
            await handshake(reader, writer)
            print(f'[conn-{conn_id}] 연결됨')

            lock = asyncio.Lock()
            pending = {}
            msg_id = [2]

            def next_id():
                msg_id[0] += 1
                return msg_id[0]

            # 구독
            for sh in subs:
                mid = next_id()
                pending[mid] = sh
                await send_msg(writer, lock, mid, 'blockchain.scripthash.subscribe', [sh])
                await asyncio.sleep(BATCH_DELAY)

            print(f'[conn-{conn_id}] {len(subs)}개 구독 완료')

            # 메시지 루프
            while True:
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=120)
                except asyncio.TimeoutError:
                    # ping
                    mid = next_id()
                    await send_msg(writer, lock, mid, 'server.ping', [])
                    continue

                if not line:
                    print(f'[conn-{conn_id}] 연결 끊김')
                    break

                try:
                    msg = json.loads(line)
                except Exception:
                    continue

                # subscribe 응답
                if 'id' in msg and msg['id'] in pending:
                    pending.pop(msg['id'])

                # 잔액 변동 푸시
                elif msg.get('method') == 'blockchain.scripthash.subscribe':
                    params = msg.get('params', [])
                    if len(params) >= 1:
                        sh = params[0]
                        address = subs.get(sh)
                        if address:
                            asyncio.create_task(
                                check_dropout(writer, lock, next_id, sh, address, balances, dropout_key))

                # get_balance 응답
                elif 'id' in msg and 'result' in msg:
                    mid = msg['id']
                    if mid in pending:
                        pending.pop(mid)

            writer.close()

        except Exception as e:
            print(f'[conn-{conn_id}] 오류: {e}')

        print(f'[conn-{conn_id}] 30초 후 재연결...')
        await asyncio.sleep(30)


async def check_dropout(writer, lock, next_id, sh, address, balances, dropout_key):
    try:
        mid = next_id()
        pending_fut = {}
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        pending_fut[mid] = fut

        await send_msg(writer, lock, mid, 'blockchain.scripthash.get_balance', [sh])

        # 응답 기다리기 (별도 루프에서 처리 못하니 직접 읽기)
        # 대신 현재 balances 기준으로 변동만 기록
        prev = balances.get(address, -1)
        if prev == -1:
            return  # 스냅샷 없음

        # 변동 감지됐으므로 get_balance 호출해서 확인
        # 응답은 메인 루프에서 처리되므로 여기선 일단 로그만
        print(f'[변동 감지] {address}')

    except Exception as e:
        print(f'check_dropout 오류: {e}')


async def main():
    addresses_json = await redis_get('watcher:addresses')
    balances_json = await redis_get('watcher:balances')
    addresses = json.loads(addresses_json) if addresses_json else []
    balances = json.loads(balances_json) if balances_json else {}
    month = await redis_get('watcher:month') or '2026-05'
    dropout_key = f'dropout:{month}'

    print(f'주소 {len(addresses)}개 로드 완료')
    print(f'감시 월: {month}')

    chunks = [addresses[i:i+SUBS_PER_CONNECTION]
              for i in range(0, len(addresses), SUBS_PER_CONNECTION)]
    print(f'총 {len(chunks)}개 연결로 분산')

    tasks = [
        asyncio.create_task(watch_connection(i, chunk, balances, dropout_key))
        for i, chunk in enumerate(chunks)
    ]

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
