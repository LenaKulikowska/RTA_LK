import json
import random
import uuid
from datetime import datetime, timedelta, timezone

# Zestaw państw bazowych 
HOME_COUNTRIES = [
    {"code": "PL", "lat": 52.0, "lon": 19.0, "currency": "PLN"}
]

# Globalne miasta do scenariusza "Impossible Travel"
GLOBAL_CITIES = [
    {"country": "US", "lat": 40.7128, "lon": -74.0060, "name": "Nowy Jork"},
    {"country": "JP", "lat": 35.6762, "lon": 139.6503, "name": "Tokio"},
    {"country": "AE", "lat": 25.2048, "lon": 55.2708, "name": "Dubaj"},
    {"country": "BR", "lat": -23.5505, "lon": -46.6333, "name": "Sao Paulo"},
    {"country": "GB", "lat": 51.5074, "lon": -0.1278, "name": "Londyn"}
]

# Kody MCC (Kategorie sprzedawców)
MCC_CODES = {
    "groceries": 5411,    # Sklepy spożywcze / Supermarkety
    "gas_station": 5541,  # Stacje paliw
    "electronics": 5732,  # Elektronika
    "jewelry": 5094,      # Biżuteria
    "digital_goods": 5815 # Usługi cyfrowe / VOD / Gry
}

# Inicjalizacja puli kart z przypisanym krajem domowym (Polska)
CARDS = {}
for _ in range(100):
    c_id = f"card_{random.randint(100000000, 999999999)}"
    # random.choice wybierze teraz zawsze Polskę, ale zostawiamy logikę na wypadek rozbudowy w przyszłości
    CARDS[c_id] = random.choice(HOME_COUNTRIES)

def get_realistic_amount():
    """Generuje realistyczną kwotę zakupów na podstawie wag."""
    chance = random.random()
    if chance < 0.70:
        return round(random.uniform(5, 80), 2)     # Drobne zakupy (70%)
    elif chance < 0.95:
        return round(random.uniform(80, 400), 2)   # Średnie zakupy (25%)
    else:
        return round(random.uniform(400, 2000), 2) # Grubsze zakupy (5%)

def generate_transaction(current_time, card_id, is_fraud=False, amount=None, location=None, mcc=None):
    home_data = CARDS[card_id]
    
    if amount is None:
        amount = get_realistic_amount()
    
    if location is None:
        # Domyślnie klient płaci w swoim "domowym" kraju (dodajemy lekki szum do współrzędnych, żeby symulować różne miasta w PL)
        location = {
            "country": home_data["code"],
            "lat": home_data["lat"] + random.uniform(-2.0, 2.0),
            "lon": home_data["lon"] + random.uniform(-3.0, 3.0)
        }
        
    if mcc is None:
        # Wybór zwykłej kategorii z wykluczeniem "podejrzanych" dla zwykłego ruchu
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
        "is_fraud_label": is_fraud # Znacznik dla Ciebie do sprawdzania skuteczności modelu
    }

def generate_mock_file(filename="mock_data_pl.json", num_records=2000):
    data = []
    current_time = datetime.now(timezone.utc)
    card_list = list(CARDS.keys())
    
    print("Generowanie realistycznego zestawu transakcji dla polskiego rynku...")
    
    while len(data) < num_records:
        scenario_chance = random.random()
        current_time += timedelta(seconds=random.randint(15, 120))
        
        # Ataki stanowią poniżej 3% ruchu
        if scenario_chance < 0.005:
            # REGULA 1: Impossible Travel (0.5% szans) - Płatność w PL, a zaraz potem za granicą
            bad_card = random.choice(card_list)
            foreign_city = random.choice(GLOBAL_CITIES)
            
            # Płatność 1: W Polsce (brak przekazanej lokalizacji użyje domyślnej PL z funkcji)
            data.append(generate_transaction(current_time, bad_card, is_fraud=True, mcc=MCC_CODES["electronics"]))
            
            # Przesunięcie czasu o fizycznie niemożliwą wartość dla lotu
            current_time += timedelta(minutes=random.randint(5, 30))
            
            # Płatność 2: Za granicą
            data.append(generate_transaction(current_time, bad_card, is_fraud=True, location=foreign_city, mcc=MCC_CODES["jewelry"]))
            
        elif scenario_chance < 0.010:
            # REGULA 2: Atak Brute-Force (0.5% szans)
            bad_card = random.choice(card_list)
            for _ in range(5):
                data.append(generate_transaction(current_time, bad_card, is_fraud=True, amount=round(random.uniform(10, 50), 2), mcc=MCC_CODES["digital_goods"]))
                current_time += timedelta(seconds=random.randint(2, 6))
                
        elif scenario_chance < 0.015:
            # REGULA 4: Card Testing (0.5% szans)
            bad_card = random.choice(card_list)
            data.append(generate_transaction(current_time, bad_card, is_fraud=True, amount=round(random.uniform(1.0, 3.0), 2), mcc=MCC_CODES["digital_goods"]))
            current_time += timedelta(minutes=random.randint(1, 5))
            data.append(generate_transaction(current_time, bad_card, is_fraud=True, amount=round(random.uniform(3000, 8000), 2), mcc=MCC_CODES["electronics"]))
            
        else:
            # ZWYKŁY RUCH I ANOMALIE KWOTOWE
            card_id = random.choice(card_list)
            is_anomaly = random.random() < 0.005 # 0.5% szans na ogromną kwotę
            
            amount = round(random.uniform(5000, 15000), 2) if is_anomaly else None
            mcc = MCC_CODES["jewelry"] if is_anomaly else None
            
            data.append(generate_transaction(current_time, card_id, is_fraud=is_anomaly, amount=amount, mcc=mcc))

    # Obcięcie nadmiaru i zapis
    data = data[:num_records]
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        
    print(f"Sukces! Wygenerowano {len(data)} transakcji i zapisano jako {filename}.")

if __name__ == "__main__":
    generate_mock_file(num_records=2000)