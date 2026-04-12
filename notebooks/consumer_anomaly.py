from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='anomaly_velocity'
)

print("Nasłuchuję na anomalię: >3 transakcje w 60s dla jednego użytkownika:")
user_times = defaultdict(deque)

for message in consumer:
    data = message.value
    user_id = data.get("user_id")
    timestamp = datetime.fromisoformat(data.get("timestamp"))
    user_times[user_id].append(timestamp)
    window_start = timestamp - timedelta(seconds=60)

    while user_times[user_id] and user_times[user_id][0] < window_start:
        user_times[user_id].popleft()

    if len(user_times[user_id]) > 3:
        print(
            f"ALERT: {user_id} | "
            f"{len(user_times[user_id])} transakcji / 60s | "
            f"ostatnia: {data.get('tx_id')} | "
            f"{data.get('store')} | "
            f"{data.get('category')}"
        )
