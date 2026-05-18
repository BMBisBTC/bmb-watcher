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
        async with session.get(f'{UPSTASH_URL}/incr/{key}', headers=headers) as r:
            data = await r.json()
            print(f'[Redis] dropout 카운트: {data.get("result")}')


def make_ssl():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class Connection:
    def __init__(self, conn_id, addresses, balances, dropout_key):
        self.conn_id = conn_id
        self.addresses = addresses
        self.balances = balances
        self.dropout_key = dropout_key
        self.subs = {address_to_scripthash(a): a for a in addresses}
        self.reader = None
        self.writer = None
        self.lock = asyncio.Lock()
        self._id = 1
        self.pending = {}  # id → Future

    def next_id(self):
        self._id += 1
        return self._id

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            ELECTRUM_HOST, ELECTRUM_PORT, ssl=make_ssl())
        # 핸드셰이크
        mid = self.next_id()
        msg = json.dumps({'id': mid, 'method': 'server.version', 'params': ['bmb-watcher', '1.4']}) + '\n'
        self.writer.write(msg.encode())
        await self.writer.drain()
        line = await asyncio.wait_for(self.reader.readline(), timeout=10)
        print(f'[conn-{self.conn_id}] 연결됨')

    async def send(self, method, params):
        mid = self.next_id()
        msg = json.dumps({'id': mid, 'method': method, 'params': params}) + '\n'
        fut = asyncio.get_event_loop().create_future()
        self.pending[mid] = fut
        async with self.lock:
            self.writer.write(msg.encode())
            await self.writer.drain()
        return fut

    async def subscribe_all(self):
        for sh in self.subs:
            await self.send('blockchain.scripthash.subscribe', [sh])
            await asyncio.sleep(BATCH_DELAY)
        print(f'[conn-{self.conn_id}] {len(self.subs)}개 구독 완료')

    async def get_balance(self, sh):
        fut = await self.send('blockchain.scripthash.get_balance', [sh])
        result = await asyncio.wait_for(fut, timeout=15)
        return result

    async def run(self):
        while True:
            try:
                await self.connect()
                await self.subscribe_all()
                await self.reader_loop()
            except Exception as e:
                print(f'[conn-{self.conn_id}] 오류: {e}')
            print(f'[conn-{self.conn_id}] 30초 후 재연결...')
            await asyncio.sleep(30)
            self.pending.clear()
            self._id = 1

    async def reader_loop(self):
        while True:
            try:
                line = await asyncio.wait_for(self.reader.readline(), timeout=120)
            except asyncio.TimeoutError:
                # ping
                await self.send('server.ping', [])
                continue

            if not line:
                print(f'[conn-{self.conn_id}] 연결 끊김')
                return

            try:
                msg = json.loads(line)
            except Exception:
                continue

            msg_id = msg.get('id')
            # pending future 처리
            if msg_id and msg_id in self.pending:
                fut = self.pending.pop(msg_id)
                if not fut.done():
                    fut.set_result(msg.get('result'))

            # 잔액 변동 푸시
            elif msg.get('method') == 'blockchain.scripthash.subscribe':
                params = msg.get('params', [])
                if len(params) >= 1:
                    sh = params[0]
                    address = self.subs.get(sh)
                    if address:
                        asyncio.create_task(self.handle_change(sh, address))

    async def handle_change(self, sh, address):
        try:
            prev = self.balances.get(address, -1)
            if prev == -1:
                print(f'[변동] {address} - 스냅샷 없음, 스킵')
                return

            result = await self.get_balance(sh)
            if not result:
                return

            confirmed = result.get('confirmed', 0)
            print(f'[변동] {address}: 이전={prev}, 현재={confirmed}')

            if confirmed < prev:
                print(f'[탈락] {address} ({prev} → {confirmed})')
                await redis_incr(self.dropout_key)
                self.balances[address] = confirmed
                await redis_set('watcher:balances', json.dumps(self.balances))
            else:
                # 잔액 증가 (이자 수령 등) → 스냅샷 업데이트
                self.balances[address] = confirmed

        except asyncio.TimeoutError:
            print(f'[타임아웃] {address}')
        except Exception as e:
            print(f'[오류] handle_change {address}: {e}')


async def main():
    addresses_json = await redis_get('watcher:addresses')
    balances_json = await redis_get('watcher:balances')
    addresses = json.loads(addresses_json) if addresses_json else []
    balances = json.loads(balances_json) if balances_json else {}
    month = await redis_get('watcher:month') or '2026-05'
    dropout_key = f'dropout:{month}'

    print(f'주소 {len(addresses)}개 로드 완료')
    print(f'감시 월: {month}, dropout key: {dropout_key}')
    print(f'잔액 스냅샷: {len(balances)}개')

    chunks = [addresses[i:i+SUBS_PER_CONNECTION]
              for i in range(0, len(addresses), SUBS_PER_CONNECTION)]
    print(f'총 {len(chunks)}개 연결로 분산')

    connections = [
        Connection(i, chunk, balances, dropout_key)
        for i, chunk in enumerate(chunks)
    ]

    await asyncio.gather(*[c.run() for c in connections])


if __name__ == '__main__':
    asyncio.run(main())
