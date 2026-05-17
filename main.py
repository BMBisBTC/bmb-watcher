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
BATCH_SIZE = 50        # 한 번에 subscribe할 개수
BATCH_DELAY = 0.1      # 배치 사이 대기 (초)


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


class ElectrumClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self._id = 0
        self._pending = {}       # id → Future
        self._subscriptions = {} # scripthash → address
        self._lock = asyncio.Lock()

    async def connect(self):
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port, ssl=ssl_ctx)
        print(f'Connected to {self.host}:{self.port}')

    def _next_id(self):
        self._id += 1
        return self._id

    async def _send(self, method, params):
        msg_id = self._next_id()
        msg = json.dumps({'id': msg_id, 'method': method, 'params': params}) + '\n'
        async with self._lock:
            self.writer.write(msg.encode())
            await self.writer.drain()
        fut = asyncio.get_event_loop().create_future()
        self._pending[msg_id] = fut
        return msg_id, fut

    async def subscribe(self, scripthash, address):
        self._subscriptions[scripthash] = address
        msg_id, fut = await self._send('blockchain.scripthash.subscribe', [scripthash])
        return msg_id, fut

    async def reader_loop(self, on_change):
        """서버에서 오는 모든 메시지 처리"""
        while True:
            line = await self.reader.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except Exception:
                continue

            # 응답 (pending future 처리)
            if 'id' in msg and msg['id'] in self._pending:
                fut = self._pending.pop(msg['id'])
                if not fut.done():
                    fut.set_result(msg.get('result'))

            # 푸시 알림 (잔액 변동)
            elif msg.get('method') == 'blockchain.scripthash.subscribe':
                params = msg.get('params', [])
                if len(params) >= 1:
                    scripthash = params[0]
                    address = self._subscriptions.get(scripthash, scripthash)
                    asyncio.create_task(on_change(address, scripthash))


async def load_addresses_and_balances():
    addresses_json = await redis_get('watcher:addresses')
    balances_json = await redis_get('watcher:balances')
    addresses = json.loads(addresses_json) if addresses_json else []
    balances = json.loads(balances_json) if balances_json else {}
    return addresses, balances


async def main():
    addresses, balances = await load_addresses_and_balances()
    month = await redis_get('watcher:month') or '2026-05'
    dropout_key = f'dropout:{month}'

    print(f'주소 {len(addresses)}개 로드 완료')
    print(f'감시 월: {month}')

    client = ElectrumClient(ELECTRUM_HOST, ELECTRUM_PORT)
    await client.connect()

    async def on_change(address, scripthash):
        """잔액 변동 감지 시 호출"""
        prev_balance = balances.get(address, 0)
        # 잔액이 줄었으면 탈락 (출금)
        # scripthash.get_balance로 현재 잔액 확인
        _, fut = await client._send('blockchain.scripthash.get_balance', [scripthash])
        try:
            result = await asyncio.wait_for(fut, timeout=10)
            if result:
                confirmed = result.get('confirmed', 0)
                if confirmed < prev_balance:
                    print(f'탈락 감지: {address} ({prev_balance} → {confirmed})')
                    await redis_incr(dropout_key)
                    balances[address] = confirmed
                    await redis_set('watcher:balances', json.dumps(balances))
        except asyncio.TimeoutError:
            print(f'타임아웃: {address}')

    # reader loop 백그라운드 실행
    reader_task = asyncio.create_task(client.reader_loop(on_change))

    # 배치로 subscribe
    print(f'Subscribe 시작 ({len(addresses)}개, 배치 {BATCH_SIZE}개씩)')
    futures = []
    for i in range(0, len(addresses), BATCH_SIZE):
        batch = addresses[i:i+BATCH_SIZE]
        for address in batch:
            sh = address_to_scripthash(address)
            msg_id, fut = await client.subscribe(sh, address)
            futures.append((address, sh, fut))
        await asyncio.sleep(BATCH_DELAY)
        print(f'  {min(i+BATCH_SIZE, len(addresses))}/{len(addresses)} 구독 완료')

    # 초기 잔액 스냅샷 수집 (balances가 비어있을 때만)
    if not balances:
        print('초기 잔액 스냅샷 수집 중...')
        for address, sh, fut in futures:
            try:
                result = await asyncio.wait_for(fut, timeout=30)
                if result is not None:
                    balances[address] = result  # scripthash 상태값 저장
            except asyncio.TimeoutError:
                pass
        await redis_set('watcher:balances', json.dumps(balances))
        print('스냅샷 저장 완료')

    print('감시 중...')
    await reader_task  # 영구 대기


if __name__ == '__main__':
    asyncio.run(main())
