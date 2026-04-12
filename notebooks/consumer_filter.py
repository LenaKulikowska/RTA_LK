from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    tx = message.value

    if tx['amount'] > 3000:
        print(
            f"ALERT: {tx['tx_id']} | "
            f"{tx['amount']:.2f} PLN | "
            f"{tx['store']} | "
            f"{tx['category']}"
        )
