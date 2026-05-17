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
SUBS_PER_CONNECTION = 100  # 연결당 구독 수 (150에서 끊겼으니 100으로 안전하게)
BATCH_DELAY = 0.05         # 구독 사이 대기


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
    def __init__(self, host, port, name=''):
        self.host = host
        self.port = port
        self.name = name
        self.reader = None
        self.writer = None
        self._id = 0
        self._pending = {}
        self._subscriptions = {}
        self._lock = asyncio.Lock()

    async def connect(self):
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port, ssl=ssl_ctx)
        print(f'[{self.name}] Connected')

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
        while True:
            try:
                line = await self.reader.readline()
                if not line:
                    print(f'[{self.name}] 연결 끊김 - 재연결 시도')
                    await self.reconnect(on_change)
                    return
                msg = json.loads(line)
            except Exception as e:
                print(f'[{self.name}] 읽기 오류: {e}')
                await asyncio.sleep(5)
                await self.reconnect(on_change)
                return

            if 'id' in msg and msg['id'] in self._pending:
                fut = self._pending.pop(msg['id'])
                if not fut.done():
                    fut.set_result(msg.get('result'))

            elif msg.get('method') == 'blockchain.scripthash.subscribe':
                params = msg.get('params', [])
                if len(params) >= 1:
                    scripthash = params[0]
                    address = self._subscriptions.get(scripthash, scripthash)
                    asyncio.create_task(on_change(address, scripthash))

    async def reconnect(self, on_change):
        print(f'[{self.name}] 재연결 중...')
        await asyncio.sleep(10)
        try:
            await self.connect()
            # 구독 복구
            subs = list(self._subscriptions.items())
            for sh, addr in subs:
                await self._send('blockchain.scripthash.subscribe', [sh])
                await asyncio.sleep(BATCH_DELAY)
            print(f'[{self.name}] 재연결 및 구독 복구 완료 ({len(subs)}개)')
            asyncio.create_task(self.reader_loop(on_change))
        except Exception as e:
            print(f'[{self.name}] 재연결 실패: {e}')
            await self.reconnect(on_change)


async def run_connection(conn_id, addresses, balances, dropout_key, on_change_callback):
    client = ElectrumClient(ELECTRUM_HOST, ELECTRUM_PORT, name=f'conn-{conn_id}')
    await client.connect()

    reader_task = asyncio.create_task(client.reader_loop(on_change_callback(client)))

    for address in addresses:
        sh = address_to_scripthash(address)
        await client.subscribe(sh, address)
        await asyncio.sleep(BATCH_DELAY)

    print(f'[conn-{conn_id}] {len(addresses)}개 구독 완료')
    await reader_task


async def main():
    addresses_json = await redis_get('watcher:addresses')
    balances_json = await redis_get('watcher:balances')
    addresses = json.loads(addresses_json) if addresses_json else []
    balances = json.loads(balances_json) if balances_json else {}
    month = await redis_get('watcher:month') or '2026-05'
    dropout_key = f'dropout:{month}'

    print(f'주소 {len(addresses)}개 로드 완료')
    print(f'감시 월: {month}')

    # 연결당 SUBS_PER_CONNECTION개씩 나누기
    chunks = [addresses[i:i+SUBS_PER_CONNECTION]
              for i in range(0, len(addresses), SUBS_PER_CONNECTION)]
    print(f'총 {len(chunks)}개 연결로 분산')

    def make_on_change(client):
        async def on_change(address, scripthash):
            prev_balance = balances.get(address, 0)
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
        return on_change

    # 모든 연결 동시 실행
    tasks = []
    for i, chunk in enumerate(chunks):
        client = ElectrumClient(ELECTRUM_HOST, ELECTRUM_PORT, name=f'conn-{i}')
        await client.connect()
        await asyncio.sleep(0.5)  # 연결 사이 간격

        on_change = make_on_change(client)
        reader_task = asyncio.create_task(client.reader_loop(on_change))

        for address in chunk:
            sh = address_to_scripthash(address)
            await client.subscribe(sh, address)
            await asyncio.sleep(BATCH_DELAY)

        print(f'[conn-{i}] {len(chunk)}개 구독 완료')
        tasks.append(reader_task)

    print('전체 구독 완료. 감시 중...')
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
