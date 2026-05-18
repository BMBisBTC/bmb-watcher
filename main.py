import asyncio
import json
import os
import ssl
import hashlib
import base58
import aiohttp
from datetime import datetime, timezone, timedelta

UPSTASH_URL = os.environ.get('KV_REST_API_URL')
UPSTASH_TOKEN = os.environ.get('KV_REST_API_TOKEN')
ELECTRUM_HOST = 'wallet.mobick.info'
ELECTRUM_PORT = 40009
SUBS_PER_CONNECTION = 100
BATCH_DELAY = 0.1

PAYOUT_ADDRESS = '1BTMD8QFVfwagxAVnzscNJccgB7o8taB4P'
INTEREST_SAT = 66666667  # 0.66666667 BMB
INTEREST_TOLERANCE = 100  # satoshi 오차 허용
MIN_PAYOUT_COUNT = 100  # 이자 지급 TX 판정 최소 수령자 수
KST = timezone(timedelta(hours=9))


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


async def redis_set(key: str, value):
    headers = {'Authorization': f'Bearer {UPSTASH_TOKEN}', 'Content-Type': 'application/json'}
    async with aiohttp.ClientSession() as session:
        await session.post(f'{UPSTASH_URL}/set/{key}', headers=headers, data=json.dumps(value))


async def redis_incr(key: str):
    headers = {'Authorization': f'Bearer {UPSTASH_TOKEN}'}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{UPSTASH_URL}/incr/{key}', headers=headers) as r:
            data = await r.json()
            return data.get('result')


def make_ssl():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def fetch_tx(txid: str) -> dict:
    ssl_ctx = make_ssl()
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://explorer.mobick.info/api/tx/{txid}', ssl=ssl_ctx) as r:
            return await r.json()


async def fetch_address(address: str) -> dict:
    ssl_ctx = make_ssl()
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://explorer.mobick.info/api/address/{address}', ssl=ssl_ctx) as r:
            return await r.json()


def get_next_month(month_key: str) -> str:
    year, month = map(int, month_key.split('-'))
    if month == 12:
        return f'{year + 1}-01'
    return f'{year}-{str(month + 1).padStart(2, "0")}'


def next_month(month_key: str) -> str:
    year, month = map(int, month_key.split('-'))
    if month == 12:
        return f'{year + 1}-01'
    return f'{year}-{str(month + 1).zfill(2)}'


async def analyze_payout_tx(txid: str):
    """TX가 이자 지급 TX인지 확인하고 수령자 목록 반환"""
    try:
        tx = await fetch_tx(txid)
        vout = tx.get('vout', [])
        recipients = []
        for out in vout:
            addr = out.get('scriptPubKey', {}).get('address')
            value_sat = int(round(out.get('value', 0) * 1e8))
            if addr and addr != PAYOUT_ADDRESS:
                diff = abs(value_sat - INTEREST_SAT)
                if diff <= INTEREST_TOLERANCE:
                    recipients.append(addr)
        return tx.get('time'), recipients
    except Exception as e:
        print(f'[TX분석 오류] {txid}: {e}')
        return None, []


async def handle_payout_event(txids: list, state: dict):
    """이자 지급 TX 감지 시 처리"""
    print(f'[이자지급] TX {len(txids)}개 분석 중...')

    all_recipients = set()
    payout_txids = []
    payout_time = None

    for txid in txids:
        tx_time, recipients = await analyze_payout_tx(txid)
        if len(recipients) >= MIN_PAYOUT_COUNT:
            all_recipients.update(recipients)
            payout_txids.append(txid)
            if tx_time:
                payout_time = tx_time
            print(f'[이자지급] TX {txid[:16]}... → {len(recipients)}명')

    if not all_recipients:
        print('[이자지급] 이자 지급 TX 아님, 스킵')
        return

    recipient_list = sorted(list(all_recipients))
    count = len(recipient_list)
    print(f'[이자지급] 총 {count}명 수령 확인!')

    # 현재 월 확정
    current_month = state['month']
    kst_time = datetime.fromtimestamp(payout_time, tz=KST) if payout_time else datetime.now(KST)
    month_key = f'{kst_time.year}-{str(kst_time.month).zfill(2)}'

    # hd:months 업데이트
    months_json = await redis_get('hd:months')
    months = json.loads(months_json) if months_json else []
    new_month_entry = {
        'month': month_key,
        'count': count,
        'txids': payout_txids,
        'time': payout_time,
    }
    months.insert(0, new_month_entry)
    months = months[:6]  # 최근 6개월만 유지
    await redis_set('hd:months', months)
    print(f'[이자지급] hd:months 업데이트 완료')

    # 이전 월 탈락자 확정 (이자 못 받은 주소)
    prev_addresses = state['addresses']
    prev_set = set(prev_addresses)
    new_set = set(recipient_list)
    dropout_addresses = sorted(list(prev_set - new_set))
    dropout_count = len(dropout_addresses)
    await redis_set(f'dropout:{current_month}', str(dropout_count))
    await redis_set(f'dropout_addresses:{current_month}', dropout_addresses)
    print(f'[이자지급] {current_month} 탈락 확정: {dropout_count}명')

    # 새 월로 전환
    new_month = next_month(current_month)
    await redis_set('watcher:month', new_month)
    await redis_set('watcher:addresses', json.dumps(recipient_list))
    await redis_set('dropout:' + new_month, '0')
    await redis_set('dropout_addresses:' + new_month, [])
    print(f'[이자지급] 감시 월 전환: {current_month} → {new_month}')

    # 잔액 스냅샷 수집
    print(f'[스냅샷] {count}개 주소 잔액 수집 중...')
    new_balances = {}
    ssl_ctx = make_ssl()
    CONCURRENT = 20
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(recipient_list), CONCURRENT):
            batch = recipient_list[i:i+CONCURRENT]
            tasks = []
            for addr in batch:
                tasks.append(session.get(
                    f'https://explorer.mobick.info/api/address/{addr}',
                    ssl=ssl_ctx
                ))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for addr, resp in zip(batch, responses):
                try:
                    if isinstance(resp, Exception):
                        continue
                    data = await resp.json()
                    bal = data.get('txHistory', {}).get('balanceSat', 0)
                    new_balances[addr] = bal
                except Exception:
                    pass
            await asyncio.sleep(0.3)

    await redis_set('watcher:balances', json.dumps(new_balances))
    print(f'[스냅샷] {len(new_balances)}개 저장 완료')

    # state 업데이트
    state['month'] = new_month
    state['addresses'] = recipient_list
    state['balances'] = new_balances
    state['dropout_key'] = f'dropout:{new_month}'
    print(f'[완료] 자동 전환 완료! 이제 {new_month} 감시 시작')


class Connection:
    def __init__(self, conn_id, addresses, state):
        self.conn_id = conn_id
        self.addresses = addresses
        self.state = state
        self.subs = {address_to_scripthash(a): a for a in addresses}
        self.reader = None
        self.writer = None
        self.lock = asyncio.Lock()
        self._id = 1
        self.pending = {}

    def next_id(self):
        self._id += 1
        return self._id

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            ELECTRUM_HOST, ELECTRUM_PORT, ssl=make_ssl())
        mid = self.next_id()
        msg = json.dumps({'id': mid, 'method': 'server.version', 'params': ['bmb-watcher', '1.4']}) + '\n'
        self.writer.write(msg.encode())
        await self.writer.drain()
        await asyncio.wait_for(self.reader.readline(), timeout=10)
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
            if msg_id and msg_id in self.pending:
                fut = self.pending.pop(msg_id)
                if not fut.done():
                    fut.set_result(msg.get('result'))

            elif msg.get('method') == 'blockchain.scripthash.subscribe':
                params = msg.get('params', [])
                if len(params) >= 1:
                    sh = params[0]
                    address = self.subs.get(sh)
                    if address:
                        asyncio.create_task(self.handle_change(sh, address))

    async def handle_change(self, sh, address):
        try:
            balances = self.state['balances']
            dropout_key = self.state['dropout_key']
            prev = balances.get(address, -1)
            if prev == -1:
                return

            fut = await self.send('blockchain.scripthash.get_balance', [sh])
            result = await asyncio.wait_for(fut, timeout=15)
            if not result:
                return

            confirmed = result.get('confirmed', 0)
            if confirmed < prev:
                print(f'[탈락감지] {address} ({prev} → {confirmed})')

                # 이미 탈락 목록에 있는지 확인 (중복 카운트 방지)
                addr_key = dropout_key.replace('dropout:', 'dropout_addresses:')
                existing_raw = await redis_get(addr_key)
                existing = json.loads(existing_raw) if existing_raw else []

                if address not in existing:
                    await redis_incr(dropout_key)
                    existing.append(address)
                    await redis_set(addr_key, existing)
                    print(f'[탈락] {address} 탈락 확정 (누적 {len(existing)}명)')
                else:
                    print(f'[탈락-중복] {address} 이미 탈락 목록에 있음, 카운트 유지')

                balances[address] = confirmed
                await redis_set('watcher:balances', json.dumps(balances))
            else:
                balances[address] = confirmed

        except asyncio.TimeoutError:
            print(f'[타임아웃] {address}')
        except Exception as e:
            print(f'[오류] handle_change {address}: {e}')


class PayoutWatcher:
    """이자 지급 주소 감시 - 별도 연결"""
    def __init__(self, state):
        self.state = state
        self.payout_sh = address_to_scripthash(PAYOUT_ADDRESS)
        self.reader = None
        self.writer = None
        self.lock = asyncio.Lock()
        self._id = 1
        self.pending = {}
        self.recent_txids = set()  # 최근 감지된 TX 중복 방지

    def next_id(self):
        self._id += 1
        return self._id

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            ELECTRUM_HOST, ELECTRUM_PORT, ssl=make_ssl())
        mid = self.next_id()
        msg = json.dumps({'id': mid, 'method': 'server.version', 'params': ['bmb-watcher-payout', '1.4']}) + '\n'
        self.writer.write(msg.encode())
        await self.writer.drain()
        await asyncio.wait_for(self.reader.readline(), timeout=10)

        # 이자 지급 주소 subscribe
        sub_id = self.next_id()
        sub_msg = json.dumps({'id': sub_id, 'method': 'blockchain.scripthash.subscribe', 'params': [self.payout_sh]}) + '\n'
        self.writer.write(sub_msg.encode())
        await self.writer.drain()
        print(f'[PayoutWatcher] 이자 지급 주소 감시 시작')

    async def send(self, method, params):
        mid = self.next_id()
        msg = json.dumps({'id': mid, 'method': method, 'params': params}) + '\n'
        fut = asyncio.get_event_loop().create_future()
        self.pending[mid] = fut
        async with self.lock:
            self.writer.write(msg.encode())
            await self.writer.drain()
        return fut

    async def run(self):
        while True:
            try:
                await self.connect()
                await self.reader_loop()
            except Exception as e:
                print(f'[PayoutWatcher] 오류: {e}')
            print(f'[PayoutWatcher] 30초 후 재연결...')
            await asyncio.sleep(30)
            self.pending.clear()
            self._id = 1

    async def reader_loop(self):
        while True:
            try:
                line = await asyncio.wait_for(self.reader.readline(), timeout=120)
            except asyncio.TimeoutError:
                await self.send('server.ping', [])
                continue

            if not line:
                print(f'[PayoutWatcher] 연결 끊김')
                return

            try:
                msg = json.loads(line)
            except Exception:
                continue

            msg_id = msg.get('id')
            if msg_id and msg_id in self.pending:
                fut = self.pending.pop(msg_id)
                if not fut.done():
                    fut.set_result(msg.get('result'))

            elif msg.get('method') == 'blockchain.scripthash.subscribe':
                params = msg.get('params', [])
                if len(params) >= 1 and params[0] == self.payout_sh:
                    print(f'[PayoutWatcher] 이자 지급 주소 변동 감지!')
                    asyncio.create_task(self.check_payout())

    async def check_payout(self):
        """이자 지급 주소 변동 시 TX 확인"""
        await asyncio.sleep(30)  # TX가 완전히 처리될 때까지 대기

        try:
            # KST 기준 1일인지 확인
            now_kst = datetime.now(KST)
            if now_kst.day != 1:
                print(f'[PayoutWatcher] 1일 아님 ({now_kst.day}일), 스킵')
                return

            # 최근 TX 목록 가져오기
            fut = await self.send('blockchain.scripthash.get_history', [self.payout_sh])
            history = await asyncio.wait_for(fut, timeout=30)
            if not history:
                return

            # 최근 TX 중 새로운 것만
            recent = [h['tx_hash'] for h in history[-20:]]
            new_txids = [t for t in recent if t not in self.recent_txids]

            if not new_txids:
                return

            self.recent_txids.update(new_txids)
            print(f'[PayoutWatcher] 새 TX {len(new_txids)}개 감지')

            await asyncio.sleep(60)  # 이자 지급이 여러 TX로 나뉘므로 추가 대기
            
            # 다시 최신 TX 목록 가져오기
            fut2 = await self.send('blockchain.scripthash.get_history', [self.payout_sh])
            history2 = await asyncio.wait_for(fut2, timeout=30)
            if history2:
                all_recent = [h['tx_hash'] for h in history2[-30:]]
                new_txids = [t for t in all_recent if t not in self.recent_txids]
                self.recent_txids.update(new_txids)
                all_new = [t for t in all_recent if t in self.recent_txids or t in new_txids]
                await handle_payout_event(all_new, self.state)

        except Exception as e:
            print(f'[PayoutWatcher] check_payout 오류: {e}')


async def main():
    addresses_json = await redis_get('watcher:addresses')
    balances_json = await redis_get('watcher:balances')
    addresses = json.loads(addresses_json) if addresses_json else []
    balances = json.loads(balances_json) if balances_json else {}
    month = await redis_get('watcher:month') or '2026-05'

    print(f'주소 {len(addresses)}개 로드 완료')
    print(f'감시 월: {month}')
    print(f'잔액 스냅샷: {len(balances)}개')

    # 공유 state
    state = {
        'month': month,
        'addresses': addresses,
        'balances': balances,
        'dropout_key': f'dropout:{month}',
    }

    chunks = [addresses[i:i+SUBS_PER_CONNECTION]
              for i in range(0, len(addresses), SUBS_PER_CONNECTION)]
    print(f'총 {len(chunks)}개 연결로 분산')

    # 일반 지갑 감시 연결들
    connections = [Connection(i, chunk, state) for i, chunk in enumerate(chunks)]

    # 이자 지급 주소 감시 연결
    payout_watcher = PayoutWatcher(state)

    tasks = [c.run() for c in connections] + [payout_watcher.run()]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
