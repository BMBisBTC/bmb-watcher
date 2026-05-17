import ssl
import socket
import json
import hashlib
import time
import requests
import os

ELECTRUM_HOST = 'wallet.mobick.info'
ELECTRUM_PORT = 40009
UPSTASH_URL = os.environ.get('KV_REST_API_URL')
UPSTASH_TOKEN = os.environ.get('KV_REST_API_TOKEN')
HIGH_DENOM_API = 'https://bmb-monitor.vercel.app/api/highdenomination'

def redis_get(key):
    r = requests.get(f"{UPSTASH_URL}/get/{key}",
                     headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"})
    return r.json().get('result')

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

def get_scripthash_balance(client, scripthash):
    client.id += 1
    msg = json.dumps({'id': client.id, 'method': 'blockchain.scripthash.get_balance', 'params': [scripthash]}) + '\n'
    client.sock.send(msg.encode())
    client.buffer = ''
    start = time.time()
    while time.time() - start < 10:
        try:
            data = client.sock.recv(4096).decode()
            client.buffer += data
            if '\n' in client.buffer:
                line, client.buffer = client.buffer.split('\n', 1)
                resp = json.loads(line)
                if 'result' in resp:
                    r = resp['result']
                    return (r.get('confirmed', 0) + r.get('unconfirmed', 0))
        except socket.timeout:
            break
    return None

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
        raw.settimeout(10)
        self.sock = ctx.wrap_socket(raw, server_hostname=ELECTRUM_HOST)
        self.sock.connect((ELECTRUM_HOST, ELECTRUM_PORT))
        self.id += 1
        msg = json.dumps({'id': self.id, 'method': 'server.version', 'params': ['bmb-watcher', '1.4']}) + '\n'
        self.sock.send(msg.encode())
        time.sleep(0.5)
        self.sock.recv(4096)
        print("Electrum 연결 성공!")

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
    scripthash_list = []
    addr_map = {}
    for addr in addresses:
        try:
            sh = address_to_scripthash(addr)
            scripthash_list.append(sh)
            addr_map[sh] = addr
        except Exception as e:
            print(f"변환 실패 {addr}: {e}")

    print(f"{len(scripthash_list)}개 변환 완료, 잔액 스냅샷 시작...")

    client = ElectrumClient()
    client.connect()

    # 초기 잔액 스냅샷
    initial_balances = {}
    for i, sh in enumerate(scripthash_list):
        bal = get_scripthash_balance(client, sh)
        if bal is not None:
            initial_balances[sh] = bal
        if (i + 1) % 100 == 0:
            print(f"스냅샷 진행 중: {i+1}/{len(scripthash_list)}")
        time.sleep(0.1)

    print(f"스냅샷 완료! {len(initial_balances)}개 잔액 기록. 감시 시작...")
    dropout_count = int(redis_get(dropout_key) or 0)

    # 반복 감시
    while True:
        for sh in scripthash_list:
            bal = get_scripthash_balance(client, sh)
            if bal is None:
                continue
            old_bal = initial_balances.get(sh)
            if old_bal is not None and bal < old_bal:
                addr = addr_map.get(sh, sh)
                print(f"탈락 감지: {addr} (잔액 감소: {old_bal} → {bal})")
                dropout_count += 1
                redis_set(dropout_key, dropout_count)
                print(f"탈락 카운트: {dropout_count}")
                initial_balances[sh] = bal
            time.sleep(0.1)
        print(f"순환 완료. 현재 탈락: {dropout_count}")

if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as e:
            print(f"오류 발생: {e}, 30초 후 재시작...")
            time.sleep(30)
