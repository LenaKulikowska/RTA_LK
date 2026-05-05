from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='enrich-group',  # inna grupa niż wcześniej!
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Wzbogacanie transakcji o risk_level...")

for message in consumer:
    data = message.value

    amount = data.get("amount", 0)

    # logika risk_level
    if amount > 3000:
        risk = "HIGH"
    elif amount > 1000:
        risk = "MEDIUM"
    else:
        risk = "LOW"

    # dodanie pola do słownika
    data["risk_level"] = risk

    # wypisanie wzbogaconego eventu
    print(data)
