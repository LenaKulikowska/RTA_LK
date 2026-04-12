from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='stats-group',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    'count': 0,
    'total': 0.0,
    'min': float('inf'),
    'max': float('-inf')
})

msg_count = 0

print("Statystyki per kategoria:")

for message in consumer:
    tx = message.value
    category = tx['category']
    amount = tx['amount']

    stats[category]['count'] += 1
    stats[category]['total'] += amount
    stats[category]['min'] = min(stats[category]['min'], amount)
    stats[category]['max'] = max(stats[category]['max'], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- STATYSTYKI KATEGORII ---")
        print("Kategoria | Liczba | Suma (PLN) | Min | Max | Średnia")
        print("-" * 70)

        for cat, data in stats.items():
            count = data['count']
            total = data['total']
            min_val = data['min']
            max_val = data['max']
            avg = total / count

            print(
                f"{cat:12} | {count:6} | {total:10.2f} | "
                f"{min_val:6.2f} | {max_val:6.2f} | {avg:8.2f}"
            )

        print("-" * 70)
