import time
import requests
import os

UPSTASH_URL = os.environ.get('KV_REST_API_URL')
UPSTASH_TOKEN = os.environ.get('KV_REST_API_TOKEN')
HIGH_DENOM_API = 'https://bmb-monitor.vercel.app/api/highdenomination'
EXPLORER_API = 'https://explorer.mobick.info/api'

def redis_get(key):
    try:
        r = requests.get(f"{UPSTASH_URL}/get/{key}",
                         headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}, timeout=5)
        return r.json().get('result')
    except:
        return None

def redis_set(key, value):
    try:
        requests.post(f"{UPSTASH_URL}/set/{key}/{value}",
                      headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}, timeout=5)
    except:
        pass

def get_address_balance(address):
    try:
        r = requests.get(f"{EXPLORER_API}/address/{address}?limit=1&offset=0", timeout=10)
        data = r.json()
        return data.get('balance', None)
    except:
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
            tx_r = requests.get(f"{EXPLORER_API}/tx/{txid}", timeout=30)
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

    # 초기 잔액 스냅샷
    print("잔액 스냅샷 시작...")
    initial_balances = {}
    for i, addr in enumerate(addresses):
        bal = get_address_balance(addr)
        if bal is not None:
            initial_balances[addr] = bal
        if (i + 1) % 100 == 0:
            print(f"스냅샷: {i+1}/{len(addresses)}")
        time.sleep(0.5)

    print(f"스냅샷 완료! {len(initial_balances)}개 기록. 감시 시작...")
    dropout_count = int(redis_get(dropout_key) or 0)
    cycle = 0

    while True:
        cycle += 1
        changed = 0
        for addr in addresses:
            bal = get_address_balance(addr)
            if bal is None:
                time.sleep(1)
                continue
            old_bal = initial_balances.get(addr)
            if old_bal is not None and bal < old_bal:
                print(f"탈락 감지: {addr} ({old_bal} → {bal})")
                dropout_count += 1
                redis_set(dropout_key, dropout_count)
                print(f"탈락 카운트: {dropout_count}")
                initial_balances[addr] = bal
                changed += 1
            time.sleep(0.5)
        print(f"순환 {cycle} 완료. 탈락: {dropout_count}, 변동: {changed}")
