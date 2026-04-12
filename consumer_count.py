from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Zliczam transakcje per sklep...")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']

    store_counts[store] += 1

    if store not in total_amount:
        total_amount[store] = 0.0
    total_amount[store] += amount

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- PODSUMOWANIE ---")
        print("Sklep | Liczba | Suma (PLN) | Średnia (PLN)")
        print("-" * 50)

        for store in store_counts:
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count

            print(f"{store:10} | {count:6} | {total:10.2f} | {avg:10.2f}")

        print("-" * 50)
