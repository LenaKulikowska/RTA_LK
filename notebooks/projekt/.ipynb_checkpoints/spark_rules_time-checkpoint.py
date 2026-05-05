"""
=============================================================
  MODUŁ 2: Spark App 1 – Detektyw Czasowy
  Reguły: Brute-Force (Reguła 2) + Card Testing (Reguła 4)
  Autorka: Osoba B

  Kompatybilność:
    - generator.py  → wczytuje mock_data_pl.json jako dane testowe
    - producer.py   → topic: 'transactions', broker: 'broker:9092'
=============================================================

JAK URUCHOMIĆ (tryb testowy, bez Kafki):
  python spark_rules_time.py --mode test

  Opcjonalnie wskaż plik mock:
  python spark_rules_time.py --mode test --file mock_data_pl.json

JAK URUCHOMIĆ (tryb produkcyjny, z Kafką i Redisem):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
               spark_rules_time.py --mode kafka
"""

import argparse
import json
import os
from datetime import datetime, timezone
from collections import defaultdict


# =============================================================
# SEKCJA 1: KONFIGURACJA
# Zgodna z ustawieniami w producer.py
# =============================================================

DEFAULT_MOCK_FILE = "mock_data_pl.json"   # plik generowany przez generator.py

KAFKA_BROKER = "broker:9092"              # identyczny jak w producer.py
KAFKA_TOPIC  = "transactions"             # identyczny jak w producer.py


# =============================================================
# SEKCJA 2: LOGIKA SCORINGOWA (czyste funkcje Pythona)
# Działają na danych z pliku LUB z Kafki – bez różnicy.
# =============================================================

def parse_timestamp(ts_str: str) -> datetime:
    """Konwertuje '2026-05-01T20:26:36Z' → obiekt datetime."""
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def detect_brute_force(transactions: list, window_seconds: int = 60, threshold: int = 5) -> list:
    """
    REGUŁA 2: Brute-Force Attack
    Wykrywa karty z > threshold transakcji w ciągu window_seconds sekund.
    Punkty: +40

    Jak działa:
      1. Grupuje transakcje według card_id
      2. Sortuje każdą grupę chronologicznie
      3. Sliding window: dla każdego punktu startowego liczy
         ile kolejnych transakcji mieści się w oknie window_seconds
      4. Jeśli count > threshold → FRAUD

    Uwaga: generator.py produkuje 5 transakcji co 2–6 sekund,
    czyli wszystkie mieszczą się w oknie 60s.
    """
    by_card = defaultdict(list)
    for tx in transactions:
        by_card[tx["card_id"]].append(tx)

    flagged = []

    for card_id, txs in by_card.items():
        txs_sorted = sorted(txs, key=lambda x: parse_timestamp(x["timestamp"]))

        for i in range(len(txs_sorted)):
            start_time = parse_timestamp(txs_sorted[i]["timestamp"])
            count = 0

            for j in range(i, len(txs_sorted)):
                diff = (parse_timestamp(txs_sorted[j]["timestamp"]) - start_time).total_seconds()
                if diff <= window_seconds:
                    count += 1
                else:
                    break

            if count > threshold:
                flagged.append(card_id)
                break

    return list(set(flagged))


def detect_card_testing(transactions: list,
                        window_minutes: int = 10,
                        small_threshold: float = 5.0,
                        large_threshold: float = 1000.0) -> list:
    """
    REGUŁA 4: Card Testing
    Wykrywa wzorzec: mała płatność (< small_threshold zł)
    a potem duża (> large_threshold zł) w ciągu window_minutes minut.
    Punkty: +80

    Uwaga: generator.py generuje kwoty 1–3 zł (małe) i 3000–8000 zł (duże),
    więc progi 5 zł i 1000 zł w pełni je wykrywają.

    Jak działa:
      1. Grupuje transakcje według card_id
      2. Sortuje chronologicznie
      3. Dla każdej małej płatności sprawdza czy w oknie window_minutes
         pojawia się duża płatność tej samej karty
    """
    by_card = defaultdict(list)
    for tx in transactions:
        by_card[tx["card_id"]].append(tx)

    flagged = []

    for card_id, txs in by_card.items():
        txs_sorted = sorted(txs, key=lambda x: parse_timestamp(x["timestamp"]))

        for i, tx_small in enumerate(txs_sorted):
            if tx_small["amount"] >= small_threshold:
                continue

            small_time = parse_timestamp(tx_small["timestamp"])

            for tx_large in txs_sorted[i + 1:]:
                diff_min = (parse_timestamp(tx_large["timestamp"]) - small_time).total_seconds() / 60
                if diff_min > window_minutes:
                    break
                if tx_large["amount"] > large_threshold:
                    flagged.append(card_id)
                    break

            if card_id in flagged:
                break

    return list(set(flagged))


def calculate_scores(transactions: list) -> dict:
    """
    Główna funkcja scoringowa – uruchamia obie reguły i sumuje punkty.
    Zwraca: {card_id: {"score": int, "reasons": list, "blocked": bool}}
    Próg blokady: score >= 80 pkt
    """
    brute_force_cards  = detect_brute_force(transactions)
    card_testing_cards = detect_card_testing(transactions)

    all_cards = set(tx["card_id"] for tx in transactions)
    results = {}

    for card_id in all_cards:
        score   = 0
        reasons = []

        if card_id in brute_force_cards:
            score += 40
            reasons.append("Brute-Force: >5 prób w 60 sekund (+40 pkt)")

        if card_id in card_testing_cards:
            score += 80
            reasons.append("Card Testing: mała→duża płatność w 10 min (+80 pkt)")

        if score > 0:
            results[card_id] = {
                "score":   score,
                "reasons": reasons,
                "blocked": score >= 80
            }

    return results


# =============================================================
# SEKCJA 3: ZAPIS DO REDIS
# Używamy HINCRBY na 'score' żeby nie nadpisać punktów
# dodanych przez Osobę C (spark_rules_logic.py).
# =============================================================

def save_to_redis(card_id: str, score: int, reasons: list, blocked: bool):
    """
    Zapisuje wynik do Redisa jako Hash: klucz = 'fraud:{card_id}'

    Przykład w Redis CLI po uruchomieniu:
      HGETALL fraud:card_123456789
      → score:      "120"
      → blocked:    "true"
      → reasons:    "Brute-Force... | Card Testing..."
      → updated_at: "2026-05-01T20:30:00+00:00"
      → source:     "modul2"

    HINCRBY zamiast HSET na score → bezpieczne współdziałanie
    z Modułem 3 (Osoba C), który dopisuje swoje punkty do tej samej karty.
    """
    try:
        import redis
        r = redis.Redis(host="redis", port=6379, decode_responses=True)

        key = f"fraud:{card_id}"
        r.hincrby(key, "score", score)
        r.hset(key, "blocked",    str(blocked).lower())
        r.hset(key, "reasons",    " | ".join(reasons))
        r.hset(key, "updated_at", datetime.now(timezone.utc).isoformat())
        r.hset(key, "source",     "modul2")

        print(f"  ✅ Redis [{key}] score +{score} | blocked={blocked}")

    except ImportError:
        print("  ⚠️  Brak biblioteki redis. Zainstaluj: pip install redis")
    except Exception as e:
        print(f"  ❌ Redis niedostępny ({e}). Kontynuuję bez zapisu.")


# =============================================================
# SEKCJA 4: TRYB TESTOWY
# Wczytuje mock_data_pl.json wygenerowany przez generator.py.
# Działa bez Kafki i bez Redisa – idealny do pisania i testowania.
# =============================================================

def run_test_mode(mock_file: str = DEFAULT_MOCK_FILE):
    print("=" * 60)
    print("  TRYB TESTOWY – dane z mock_data_pl.json (generator.py)")
    print("=" * 60)

    if not os.path.exists(mock_file):
        print(f"\n❌ Nie znaleziono pliku: {mock_file}")
        print("   Najpierw uruchom: python generator.py")
        return

    with open(mock_file, "r", encoding="utf-8") as f:
        transactions = json.load(f)

    print(f"\n📦 Wczytano {len(transactions)} transakcji z: {mock_file}")

    # Statystyki z pliku (korzystamy z is_fraud_label z generator.py)
    fraud_labeled = sum(1 for tx in transactions if tx.get("is_fraud_label", False))
    print(f"   Oznaczonych jako fraud przez generator (is_fraud_label=true): {fraud_labeled}")
    print()

    results = calculate_scores(transactions)

    if not results:
        print("✅ Brak wykrytych anomalii przez Moduł 2.")
        return

    print(f"🚨 Moduł 2 wykrył anomalie dla {len(results)} kart:\n")

    blocked_count = 0
    for card_id, data in sorted(results.items(), key=lambda x: -x[1]["score"]):
        status = "🔴 ZABLOKOWANA" if data["blocked"] else "🟡 POD OBSERWACJĄ"
        if data["blocked"]:
            blocked_count += 1

        print(f"  Karta:  {card_id}")
        print(f"  Status: {status} | Score: {data['score']} pkt")
        for reason in data["reasons"]:
            print(f"    ↳ {reason}")

        save_to_redis(card_id, data["score"], data["reasons"], data["blocked"])
        print()

    print(f"📊 Podsumowanie Modułu 2:")
    print(f"   Kart pod obserwacją: {len(results)}")
    print(f"   Kart zablokowanych:  {blocked_count}")


# =============================================================
# SEKCJA 5: TRYB PRODUKCYJNY (PySpark Structured Streaming)
# Czyta z Kafki topic 'transactions' – identyczny jak w producer.py
# =============================================================

def run_kafka_mode():
    """
    PySpark Structured Streaming:
      Źródło:  Kafka topic 'transactions' @ broker:9092  (jak w producer.py)
      Schemat: pełny Data Contract z polami mcc i is_fraud_label
      Wyjście: Redis Hash 'fraud:{card_id}'
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            StructType, StructField,
            StringType, DoubleType, LongType, BooleanType
        )
    except ImportError:
        print("❌ PySpark nie jest zainstalowany.")
        print("   Użyj: spark-submit spark_rules_time.py --mode kafka")
        return

    print("🚀 Uruchamiam PySpark Structured Streaming...")
    print(f"   Kafka: {KAFKA_BROKER}  |  Topic: {KAFKA_TOPIC}")

    spark = SparkSession.builder \
        .appName("RTA_Modul2_DetektywCzasowy") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schemat zgodny z producer.py (wszystkie pola łącznie z mcc i is_fraud_label)
    schema = StructType([
        StructField("transaction_id", StringType(),  True),
        StructField("card_id",        StringType(),  True),
        StructField("timestamp",      StringType(),  True),
        StructField("amount",         DoubleType(),  True),
        StructField("currency",       StringType(),  True),
        StructField("mcc",            LongType(),    True),
        StructField("location_lat",   DoubleType(),  True),
        StructField("location_lon",   DoubleType(),  True),
        StructField("country_code",   StringType(),  True),
        StructField("is_fraud_label", BooleanType(), True),
    ])

    # Czytanie z Kafki – broker i topic jak w producer.py
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    transactions_df = raw_stream \
        .select(F.from_json(F.col("value").cast("string"), schema).alias("d")) \
        .select("d.*") \
        .withColumn("event_time", F.to_timestamp("timestamp"))

    # ----------------------------------------------------------
    # REGUŁA 2: Brute-Force – Tumbling Window 60 sekund
    # producer.py wysyła 5 transakcji co 2–6 sek → wszystkie w oknie 60s
    # ----------------------------------------------------------
    brute_force_df = transactions_df \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            F.window("event_time", "60 seconds"),
            F.col("card_id")
        ) \
        .agg(F.count("*").alias("tx_count")) \
        .filter(F.col("tx_count") > 5) \
        .select(
            F.col("card_id"),
            F.lit(40).alias("score_increment"),
            F.lit("Brute-Force: >5 prób w 60 sekund").alias("reason")
        )

    # ----------------------------------------------------------
    # REGUŁA 4: Card Testing – okno 10 minut
    # producer.py: małe kwoty 1–3 zł, duże 3000–8000 zł
    # Progi 5 zł i 1000 zł w pełni je pokrywają
    # ----------------------------------------------------------
    small_txs = transactions_df \
        .filter(F.col("amount") < 5.0) \
        .withWatermark("event_time", "15 minutes") \
        .select(
            F.col("card_id").alias("card_id_small"),
            F.col("event_time").alias("time_small")
        )

    large_txs = transactions_df \
        .filter(F.col("amount") > 1000.0) \
        .withWatermark("event_time", "15 minutes") \
        .select(
            F.col("card_id").alias("card_id_large"),
            F.col("event_time").alias("time_large")
        )

    card_testing_df = small_txs.join(
        large_txs,
        (F.col("card_id_small") == F.col("card_id_large")) &
        (F.col("time_large") > F.col("time_small")) &
        (F.col("time_large") <= F.col("time_small") + F.expr("INTERVAL 10 MINUTES"))
    ).select(
        F.col("card_id_small").alias("card_id"),
        F.lit(80).alias("score_increment"),
        F.lit("Card Testing: mała→duża płatność w 10 min").alias("reason")
    ).distinct()

    combined_frauds = brute_force_df.union(card_testing_df)

    def write_to_redis(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        print(f"\n[Batch {batch_id}] Anomalie: {len(rows)}")
        for row in rows:
            print(f"  🚨 {row['card_id']} | {row['reason']} | +{row['score_increment']} pkt")
            save_to_redis(
                card_id=row["card_id"],
                score=row["score_increment"],
                reasons=[row["reason"]],
                blocked=(row["score_increment"] >= 80)
            )

    query = combined_frauds.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_redis) \
        .option("checkpointLocation", "/tmp/spark_checkpoint_modul2") \
        .start()

    print(f"✅ Streaming uruchomiony. Nasłuchuję topic '{KAFKA_TOPIC}'...")
    print("   (Ctrl+C żeby zatrzymać)\n")
    query.awaitTermination()


# =============================================================
# PUNKT WEJŚCIA
# =============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RTA Moduł 2 – Detektyw Czasowy")
    parser.add_argument(
        "--mode",
        choices=["test", "kafka"],
        default="test",
        help="test = mock_data_pl.json (domyślny) | kafka = Spark Streaming"
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_MOCK_FILE,
        help=f"Ścieżka do pliku mock (domyślnie: {DEFAULT_MOCK_FILE})"
    )
    args = parser.parse_args()

    if args.mode == "test":
        run_test_mode(mock_file=args.file)
    else:
        run_kafka_mode()
