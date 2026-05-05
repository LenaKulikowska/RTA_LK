"""
test_local.py – Testowanie logiki Modułu 2 bez Kafki i Redisa
==============================================================
Wczytuje mock_data_pl.json i uruchamia tę samą logikę co spark_rules_time.py,
ale w trybie wsadowym (batch) zamiast strumieniowym.

Uruchomienie:
    python test_local.py
    python test_local.py --file inny_plik.json  (opcjonalnie)

Wymagania:
    pip install pyspark==4.0.0
"""

import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)

# =============================================================================
# PARAMETRY (identyczne jak w spark_rules_time.py)
# =============================================================================

BRUTE_FORCE_WINDOW_SECONDS  = 60
BRUTE_FORCE_MAX_ATTEMPTS    = 5
BRUTE_FORCE_SCORE           = 40

CARD_TESTING_WINDOW_MINUTES = 10
CARD_TESTING_SMALL_MAX      = 5.0
CARD_TESTING_LARGE_MIN      = 1000.0
CARD_TESTING_SCORE          = 80

FRAUD_THRESHOLD             = 80

# =============================================================================
# SCHEMAT
# =============================================================================

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("card_id",        StringType(),  True),
    StructField("timestamp",      StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("currency",       StringType(),  True),
    StructField("mcc",            StringType(),  True),
    StructField("location_lat",   DoubleType(),  True),
    StructField("location_lon",   DoubleType(),  True),
    StructField("country_code",   StringType(),  True),
    StructField("is_fraud_label", BooleanType(), True),
])

# =============================================================================
# SPARK SESSION (tryb lokalny)
# =============================================================================

spark = (
    SparkSession.builder
    .appName("RTA_Module2_LocalTest")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# WCZYTANIE DANYCH
# =============================================================================

def load_data(filepath: str):
    df = spark.read.option("multiline", "true").schema(TRANSACTION_SCHEMA).json(filepath)
    df = df.withColumn(
        "event_time",
        # Backtick-i obowiązkowe – "timestamp" jest słowem kluczowym w Spark 4.0
        F.to_timestamp(F.col("`timestamp`"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    print(f"\n✅ Wczytano {df.count()} transakcji z pliku: {filepath}")
    print(f"   Zakres czasowy: {df.agg(F.min('event_time'), F.max('event_time')).collect()[0]}")
    return df

# =============================================================================
# REGUŁA 2: BRUTE-FORCE
# =============================================================================

def detect_brute_force(df):
    """
    Grupuje transakcje w tumbling windows po 60 sek i szuka kart
    z więcej niż BRUTE_FORCE_MAX_ATTEMPTS próbami.
    """
    results = (
        df
        .groupBy(
            F.col("card_id"),
            F.window(F.col("event_time"), f"{BRUTE_FORCE_WINDOW_SECONDS} seconds")
        )
        .agg(
            F.count("transaction_id").alias("attempt_count"),
            F.max("event_time").alias("last_seen"),
            F.collect_list("amount").alias("amounts")
        )
        .filter(F.col("attempt_count") > BRUTE_FORCE_MAX_ATTEMPTS)
        .select(
            F.col("card_id"),
            F.lit("BRUTE_FORCE").alias("rule"),
            F.lit(BRUTE_FORCE_SCORE).alias("score_delta"),
            F.col("attempt_count"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("last_seen"),
            F.col("amounts")
        )
        .orderBy("last_seen")
    )
    return results

# =============================================================================
# REGUŁA 4: CARD TESTING
# =============================================================================

def detect_card_testing(df):
    """
    Sliding window 10 min (krok 1 min): szuka okien gdzie ta sama karta
    zrobiła małą (<5 PLN) i dużą (>1000 PLN) płatność.
    """
    results = (
        df
        .groupBy(
            F.col("card_id"),
            F.window(
                F.col("event_time"),
                f"{CARD_TESTING_WINDOW_MINUTES} minutes",
                "1 minutes"   # sliding step
            )
        )
        .agg(
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
            F.count("transaction_id").alias("tx_count"),
            F.max("event_time").alias("last_seen"),
            F.collect_list("amount").alias("amounts")
        )
        .filter(
            (F.col("min_amount") < CARD_TESTING_SMALL_MAX) &
            (F.col("max_amount") > CARD_TESTING_LARGE_MIN)
        )
        .select(
            F.col("card_id"),
            F.lit("CARD_TESTING").alias("rule"),
            F.lit(CARD_TESTING_SCORE).alias("score_delta"),
            F.col("tx_count").alias("attempt_count"),
            F.col("min_amount"),
            F.col("max_amount"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("last_seen"),
            F.col("amounts")
        )
        .orderBy("last_seen")
    )
    return results

# =============================================================================
# SYMULACJA LOGIKI REDISA (w pamięci, bez prawdziwego Redisa)
# =============================================================================

def simulate_redis_scoring(brute_df, card_df):
    """
    Liczy łączny score per karta i zwraca które karty przekroczą próg blokady.
    Symuluje to, co w produkcji robi HINCRBY w Redisie.
    """
    scores = {}   # card_id → {"score": int, "rules": list}

    for row in brute_df.collect():
        cid = row["card_id"]
        if cid not in scores:
            scores[cid] = {"score": 0, "rules": []}
        scores[cid]["score"]  += row["score_delta"]
        scores[cid]["rules"].append(f"BRUTE_FORCE (+{row['score_delta']})")

    for row in card_df.collect():
        cid = row["card_id"]
        if cid not in scores:
            scores[cid] = {"score": 0, "rules": []}
        scores[cid]["score"]  += row["score_delta"]
        scores[cid]["rules"].append(f"CARD_TESTING (+{row['score_delta']})")

    return scores

# =============================================================================
# WERYFIKACJA Z ETYKIETAMI (is_fraud_label)
# =============================================================================

def verify_against_labels(df, detected_cards: set):
    """
    Porównuje wykryte karty z is_fraud_label z pliku danych.
    Drukuje prostą tabelę precision/recall.
    """
    labeled = (
        df
        .filter(F.col("is_fraud_label") == True)
        .select("card_id")
        .distinct()
    )
    labeled_cards = {row["card_id"] for row in labeled.collect()}

    tp = detected_cards & labeled_cards
    fp = detected_cards - labeled_cards
    fn = labeled_cards - detected_cards

    print("\n" + "=" * 55)
    print("  WERYFIKACJA vs. is_fraud_label")
    print("=" * 55)
    print(f"  Transakcje oznaczone jako fraud:  {len(labeled_cards)} kart")
    print(f"  Nasze wykrycia (Moduł 2):         {len(detected_cards)} kart")
    print(f"  True Positives  (TP): {len(tp)}")
    print(f"  False Positives (FP): {len(fp)}")
    print(f"  False Negatives (FN): {len(fn)}")
    if detected_cards:
        precision = len(tp) / len(detected_cards) * 100
        print(f"  Precision: {precision:.1f}%")
    if labeled_cards:
        recall = len(tp) / len(labeled_cards) * 100
        print(f"  Recall:    {recall:.1f}%")
    print("=" * 55)

    if fp:
        print(f"\n  ⚠️  False Positives (wykryte, ale nie oznaczone): {fp}")
    if fn:
        print(f"  ⚠️  False Negatives (pominięte przez Moduł 2):    {fn}")
        print(f"     (Reguły 1 i 3 należą do Modułu 3 – to normalne)")

# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Test lokalny Modułu 2")
    parser.add_argument(
        "--file", default="mock_data_pl.json",
        help="Ścieżka do pliku JSON z transakcjami"
    )
    args = parser.parse_args()

    print("=" * 55)
    print("  RTA Moduł 2 – Test lokalny (bez Kafki i Redisa)")
    print("=" * 55)

    # 1. Wczytaj dane
    df = load_data(args.file)

    # 2. Uruchom detekcję
    print("\n🔍 Szukam ataków Brute-Force...")
    brute_df = detect_brute_force(df)
    brute_count = brute_df.count()

    print("\n🔍 Szukam wzorców Card Testing...")
    card_df = detect_card_testing(df)
    card_count = card_df.count()

    # 3. Wyniki Reguły 2
    print("\n" + "=" * 55)
    print(f"  REGUŁA 2 – BRUTE-FORCE: {brute_count} alertów")
    print("=" * 55)
    if brute_count > 0:
        brute_df.select(
            "card_id", "attempt_count", "window_start", "window_end", "score_delta"
        ).show(truncate=False)
    else:
        print("  Brak wykryć – prawdopodobnie za mało danych z tym wzorcem")

    # 4. Wyniki Reguły 4
    print("\n" + "=" * 55)
    print(f"  REGUŁA 4 – CARD TESTING: {card_count} alertów")
    print("=" * 55)
    if card_count > 0:
        card_df.select(
            "card_id", "min_amount", "max_amount",
            "attempt_count", "window_start", "window_end", "score_delta"
        ).show(truncate=False)
    else:
        print("  Brak wykryć – prawdopodobnie za mało danych z tym wzorcem")

    # 5. Symulacja scoringu
    scores = simulate_redis_scoring(brute_df, card_df)
    flagged = {cid for cid, v in scores.items() if v["score"] >= FRAUD_THRESHOLD}

    print("\n" + "=" * 55)
    print("  SYMULACJA REDISA – SCORING")
    print("=" * 55)
    for cid, v in sorted(scores.items(), key=lambda x: -x[1]["score"]):
        status = "🔴 ZABLOKOWANA" if v["score"] >= FRAUD_THRESHOLD else "🟡 Obserwowana"
        print(f"  {status} | {cid} | Score: {v['score']} pkt | {', '.join(v['rules'])}")

    if not scores:
        print("  Brak kart z podwyższonym ryzykiem w tym pliku danych.")

    # 6. Weryfikacja
    verify_against_labels(df, flagged)

    spark.stop()
    print("\n✅ Test zakończony.")


if __name__ == "__main__":
    main()
