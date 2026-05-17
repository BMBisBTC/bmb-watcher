import ssl
import socket
import json
import hashlib
import time
import requests
import os

# 설정
ELECTRUM_HOST = 'wallet.mobick.info'
ELECTRUM_PORT = 40009
UPSTASH_URL = os.environ.get('KV_REST_API_URL')
UPSTASH_TOKEN = os.environ.get('KV_REST_API_TOKEN')
HIGH_DENOM_API = 'https://bmb-monitor.vercel.app/api/highdenomination'

def redis_get(key):
    r = requests.get(f"{UPSTASH_URL}/get/{key}",
                     headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"})
    data = r.json()
    return data.get('result')

def redis_set(key, value):
    requests.post(f"{UPSTASH_URL}/set/{key}/{value}",
                  headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"})

def address_to_scripthash(address):
    import base58
    decoded = base58.b58decode_check(address)
    pubkey_hash = decoded[1:]
    script = bytes([0x76, 0xa9, 0x14]) + pubkey_hash + bytes([0x88, 0xac])
    sha = hashlib.sha256(script).digest()
    return sha[::-1].hex()

def get_watched_addresses():
    try:
        r = requests.get(HIGH_DENOM_API, timeout=120)
        data = r.json()
        months = data.get('months', [])
        if not months:
            return [], ''
        latest = months[0]
        month_key = latest['month']
        txids = latest['txids']
        addresses = []
        for txid in txids:
            tx_r = requests.get(f"https://explorer.mobick.info/api/tx/{txid}", timeout=30)
            tx = tx_r.json()
            for vout in tx.get('vout', []):
                addr = vout.get('scriptPubKey', {}).get('address')
                val = vout.get('value', 0)
                if addr and abs(val - 0.66666667) < 0.0001:
                    if addr != '1BTMD8QFVfwagxAVnzscNJccgB7o8taB4P':
                        addresses.append(addr)
        return addresses, month_key
    except Exception as e:
        print(f"주소 목록 가져오기 실패: {e}")
        return [], ''

class ElectrumClient:
    def __init__(self):
        self.sock = None
        self.id = 0
        self.buffer = ''

    def connect(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.settimeout(30)
        self.sock = ctx.wrap_socket(raw, server_hostname=ELECTRUM_HOST)
        self.sock.connect((ELECTRUM_HOST, ELECTRUM_PORT))
        self.send('server.version', ['bmb-watcher', '1.4'])
        self.recv()
        print("Electrum 연결 성공!")

    def send(self, method, params=None):
        self.id += 1
        msg = json.dumps({'id': self.id, 'method': method, 'params': params or []}) + '\n'
        self.sock.send(msg.encode())
        return self.id

    def recv(self):
        while True:
            try:
                data = self.sock.recv(4096).decode()
                self.buffer += data
                if '\n' in self.buffer:
                    line, self.buffer = self.buffer.split('\n', 1)
                    return json.loads(line)
            except socket.timeout:
                return None

    def subscribe(self, scripthash):
        self.send('blockchain.scripthash.subscribe', [scripthash])

    def listen(self):
        self.sock.settimeout(60)
        while True:
            try:
                data = self.sock.recv(4096).decode()
                if not data:
                    break
                self.buffer += data
                while '\n' in self.buffer:
                    line, self.buffer = self.buffer.split('\n', 1)
                    if line.strip():
                        yield json.loads(line)
            except socket.timeout:
                self.send('server.ping', [])
                continue
            except Exception as e:
                print(f"수신 오류: {e}")
                break

def main():
    print("BMB 고액권 감시 봇 시작...")
    print("이자 지급 주소 목록 가져오는 중...")
    addresses, month_key = get_watched_addresses()
    print(f"{month_key} 기준 {len(addresses)}개 주소 감시 시작")

    if not addresses:
        print("주소 목록이 비어있습니다. 종료.")
        return

    dropout_key = f"dropout:{month_key}"
    current_dropout = redis_get(dropout_key)
    if current_dropout is None:
        redis_set(dropout_key, 0)
        print(f"Redis 초기화: {dropout_key} = 0")
    else:
        print(f"현재 탈락 수: {current_dropout}")

    print("scripthash 변환 중...")
    scripthash_to_addr = {}
    for addr in addresses:
        try:
            sh = address_to_scripthash(addr)
            scripthash_to_addr[sh] = addr
        except Exception as e:
            print(f"변환 실패 {addr}: {e}")

    print(f"{len(scripthash_to_addr)}개 scripthash 변환 완료")

    client = ElectrumClient()
    client.connect()

    print("주소 구독 중...")
    for sh in scripthash_to_addr:
        client.subscribe(sh)

    print("구독 완료! 변동 감시 중...")
    initial_states = {}

    for msg in client.listen():
        if msg.get('method') == 'blockchain.scripthash.subscribe':
            params = msg.get('params', [])
            if len(params) >= 2:
                sh = params[0]
                new_status = params[1]
                old_status = initial_states.get(sh)
                if sh not in initial_states:
                    initial_states[sh] = new_status
                    continue
                if old_status != new_status:
                    addr = scripthash_to_addr.get(sh, sh)
                    print(f"잔액 변동 감지: {addr}")
                    current = int(redis_get(dropout_key) or 0)
                    current += 1
                    redis_set(dropout_key, current)
                    print(f"탈락 카운트 업데이트: {current}")
                    initial_states[sh] = new_status

if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as e:
            print(f"오류 발생: {e}, 30초 후 재시작...")
            time.sleep(30)
