from kafka import KafkaConsumer
from collections import defaultdict, deque
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity-detector',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_windows = defaultdict(deque)

WINDOW_SECONDS = 60
MAX_TX = 3

print("Wykrywanie anomalii prędkości (>3 transakcje w 60s)...")
print("-" * 60)

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    now = time.time()

    window = user_windows[user_id]

    while window and (now - window[0]) > WINDOW_SECONDS:
        window.popleft()

    window.append(now)

    if len(window) > MAX_TX:
        print(
            f" ALERT | {user_id} | "
            f"{len(window)} tx w {WINDOW_SECONDS}s | "
            f"tx_id={tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}"
        )
