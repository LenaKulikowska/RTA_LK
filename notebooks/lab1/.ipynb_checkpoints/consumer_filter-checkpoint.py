from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'lab67',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print(message)
    
# TWÓJ KOD
# Dla każdej wiadomości: sprawdź amount > 1000, jeśli tak — wypisz ALERT
