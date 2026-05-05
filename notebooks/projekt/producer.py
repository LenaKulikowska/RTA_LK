from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

# 1. KONFIGURACJA KAFKI Z ZAJĘĆ
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'transactions'

# 2. SŁOWNIKI I DANE REFERENCYJNE
HOME_COUNTRIES = [
    {"code": "PL", "lat": 52.0, "lon": 19.0, "currency": "PLN"}
]

GLOBAL_CITIES = [
    {"country": "US", "lat": 40.7128, "lon": -74.0060, "name": "Nowy Jork"},
    {"country": "JP", "lat": 35.6762, "lon": 139.6503, "name": "Tokio"},
    {"country": "AE", "lat": 25.2048, "lon": 55.2708, "name": "Dubaj"},
    {"country": "BR", "lat": -23.5505, "lon": -46.6333, "name": "Sao Paulo"},
    {"country": "GB", "lat": 51.5074, "lon": -0.1278, "name": "Londyn"}
]

MCC_CODES = {
    "groceries": 5411,    
    "gas_station": 5541,  
    "electronics": 5732,  
    "jewelry": 5094,      
    "digital_goods": 5815 
}

# Inicjalizacja puli kart
CARDS = {}
for _ in range(100):
    c_id = f"card_{random.randint(100000000, 999999999)}"
    CARDS[c_id] = random.choice(HOME_COUNTRIES)

def get_realistic_amount():
    chance = random.random()
    if chance < 0.70: return round(random.uniform(5, 80), 2)
    elif chance < 0.95: return round(random.uniform(80, 400), 2)
    else: return round(random.uniform(400, 2000), 2)

def generate_transaction(current_time, card_id, is_fraud=False, amount=None, location=None, mcc=None):
    home_data = CARDS[card_id]
    if amount is None:
        amount = get_realistic_amount()
    if location is None:
        location = {
            "country": home_data["code"],
            "lat": home_data["lat"] + random.uniform(-2.0, 2.0),
            "lon": home_data["lon"] + random.uniform(-3.0, 3.0)
        }
    if mcc is None:
        mcc = random.choice([MCC_CODES["groceries"], MCC_CODES["gas_station"], MCC_CODES["electronics"]])

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card_id,
        "timestamp": current_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "amount": amount,
        "currency": home_data["currency"],
        "mcc": mcc,
        "location_lat": round(location["lat"], 4),
        "location_lon": round(location["lon"], 4),
        "country_code": location["country"],
        "is_fraud_label": is_fraud 
    }

# 3. GŁÓWNA PĘTLA (Styl z zajęć)
current_time = datetime.now(timezone.utc)
card_list = list(CARDS.keys())

print("Rozpoczynamy wysyłanie transakcji do Kafki...")

# Ustawiamy pętlę na 2000 iteracji, żeby się sama bezpiecznie zatrzymała
for i in range(2000):
    scenario_chance = random.random()
    current_time += timedelta(seconds=random.randint(15, 120))
    events_to_send = []
    
    if scenario_chance < 0.005:
        # Impossible Travel
        bad_card = random.choice(card_list)
        foreign_city = random.choice(GLOBAL_CITIES)
        events_to_send.append(generate_transaction(current_time, bad_card, is_fraud=True, mcc=MCC_CODES["electronics"]))
        current_time += timedelta(minutes=random.randint(5, 30))
        events_to_send.append(generate_transaction(current_time, bad_card, is_fraud=True, location=foreign_city, mcc=MCC_CODES["jewelry"]))
        
    elif scenario_chance < 0.010:
        # Brute-Force
        bad_card = random.choice(card_list)
        for _ in range(5):
            events_to_send.append(generate_transaction(current_time, bad_card, is_fraud=True, amount=round(random.uniform(10, 50), 2), mcc=MCC_CODES["digital_goods"]))
            current_time += timedelta(seconds=random.randint(2, 6))
            
    elif scenario_chance < 0.015:
        # Card Testing
        bad_card = random.choice(card_list)
        events_to_send.append(generate_transaction(current_time, bad_card, is_fraud=True, amount=round(random.uniform(1.0, 3.0), 2), mcc=MCC_CODES["digital_goods"]))
        current_time += timedelta(minutes=random.randint(1, 5))
        events_to_send.append(generate_transaction(current_time, bad_card, is_fraud=True, amount=round(random.uniform(3000, 8000), 2), mcc=MCC_CODES["electronics"]))
        
    else:
        # Zwykły ruch / Anomalia
        card_id = random.choice(card_list)
        is_anomaly = random.random() < 0.005
        amount = round(random.uniform(5000, 15000), 2) if is_anomaly else None
        mcc = MCC_CODES["jewelry"] if is_anomaly else None
        events_to_send.append(generate_transaction(current_time, card_id, is_fraud=is_anomaly, amount=amount, mcc=mcc))

    # Wysyłanie paczki zdarzeń wygenerowanych w tej iteracji
    for event in events_to_send:
        producer.send(TOPIC_NAME, value=event)
        
        status = "🚨 FRAUD" if event['is_fraud_label'] else "✅ OK"
        print(f"[{i+1}] {status} | {event['transaction_id'][:8]}... | {event['amount']:.2f} PLN | {event['country_code']} | MCC: {event['mcc']}")
        
        # Opóźnienie na wzór tego z zajęć
        time.sleep(0.1) 

# Zabezpieczenie na koniec - wypchnięcie resztek z bufora i zamknięcie
producer.flush()
producer.close()
print("Zakończono wysyłanie zdarzeń.")