"""
Moduł 2: Spark App 1 – Detektyw Czasowy
========================================
Odpowiedzialność: Reguła 2 (Brute-Force) + Reguła 4 (Card Testing)

Reguła 2 – Brute-Force:
    Więcej niż 5 transakcji tą samą kartą w oknie 60 sekund → +40 pkt

Reguła 4 – Card Testing:
    Płatność < 5 PLN, a następnie płatność > 1000 PLN tą samą kartą
    w ciągu 10 minut → +80 pkt

Wynik zapisywany do Redisa jako:
    HSET fraud:<card_id>  score <wartość>  rule <nazwa>  last_seen <timestamp>
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, BooleanType
)
import redis
import json

# =============================================================================
# KONFIGURACJA
# =============================================================================

KAFKA_BROKER   = "broker:9092"
KAFKA_TOPIC    = "transactions"
REDIS_HOST     = "redis"
REDIS_PORT     = 6379
FRAUD_THRESHOLD = 80          # powyżej tej sumy → karta ląduje jako "Oszustwo"

BRUTE_FORCE_WINDOW_SECONDS  = 60    # okno czasowe dla Reguły 2
BRUTE_FORCE_MAX_ATTEMPTS    = 5     # maks. dozwolona liczba prób w oknie
BRUTE_FORCE_SCORE           = 40

CARD_TESTING_WINDOW_MINUTES = 10    # okno czasowe dla Reguły 4
CARD_TESTING_SMALL_MAX      = 5.0   # próg "małej" płatności (PLN)
CARD_TESTING_LARGE_MIN      = 1000.0 # próg "dużej" płatności (PLN)
CARD_TESTING_SCORE          = 80

# =============================================================================
# SCHEMAT JSON (zgodny z Data Contract)
# =============================================================================

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("card_id",        StringType(),  True),
    StructField("timestamp",      StringType(),  True),   # parsujemy ręcznie do TimestampType
    # UWAGA: "timestamp" to słowo kluczowe w Spark 4.0 – w zapytaniach używamy backtick-ów: `timestamp`
    StructField("amount",         DoubleType(),  True),
    StructField("currency",       StringType(),  True),
    StructField("mcc",            StringType(),  True),
    StructField("location_lat",   DoubleType(),  True),
    StructField("location_lon",   DoubleType(),  True),
    StructField("country_code",   StringType(),  True),
    StructField("is_fraud_label", BooleanType(), True),
])

# =============================================================================
# INICJALIZACJA SPARKA
# =============================================================================

spark = (
    SparkSession.builder
    .appName("RTA_Module2_TimeDetective")
    # Potrzebne pakiety do Kafki (Spark 4.0 używa kafka-clients 3.x)
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    )
    # Redis – korzystamy z lekkiego writerForeachBatch + redis-py zamiast connectora
    .config("spark.sql.shuffle.partitions", "4")   # mała skala → ograniczamy
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# CZYTANIE STRUMIENIA Z KAFKI
# =============================================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Kafkowe pole `value` jest binarnym blokiem → dekodujemy do JSON-a
parsed_stream = (
    raw_stream
    .select(
        F.from_json(
            F.col("value").cast("string"),
            TRANSACTION_SCHEMA
        ).alias("data")
    )
    .select("data.*")
    # Konwersja stringa ISO-8601 na typ Timestamp (potrzebny do okien czasowych)
    # Backtick-i obowiązkowe – "timestamp" jest słowem kluczowym w Spark 4.0
    .withColumn("event_time", F.to_timestamp(F.col("`timestamp`"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    # Watermark: pozwala Sparkowi wyrzucić stare dane z pamięci po 5 min opóźnienia
    .withWatermark("event_time", "5 minutes")
)

# =============================================================================
# REGUŁA 2: BRUTE-FORCE (okno 60 sek, >5 prób)
# =============================================================================
# Używamy Tumbling Window – nienachodziące na siebie okna po 60 sek.
# Dla każdego okna liczymy transakcje per karta.

brute_force_alerts = (
    parsed_stream
    .groupBy(
        F.col("card_id"),
        F.window(F.col("event_time"), f"{BRUTE_FORCE_WINDOW_SECONDS} seconds")
    )
    .agg(
        F.count("transaction_id").alias("attempt_count"),
        F.max("event_time").alias("last_seen"),
        F.first("timestamp").alias("first_ts")
    )
    .filter(F.col("attempt_count") > BRUTE_FORCE_MAX_ATTEMPTS)
    .select(
        F.col("card_id"),
        F.lit("BRUTE_FORCE").alias("rule"),
        F.lit(BRUTE_FORCE_SCORE).alias("score_delta"),
        F.col("attempt_count"),
        F.col("last_seen").cast("string").alias("last_seen_str")
    )
)

# =============================================================================
# REGUŁA 4: CARD TESTING (mała płatność + duża w ciągu 10 min)
# =============================================================================
# Strategia:
#   1. W oknie 10-minutowym zbieramy MIN i MAX kwotę per karta.
#   2. Jeśli MIN < 5 PLN ORAZ MAX > 1000 PLN → wzorzec Card Testing wykryty.
# Używamy Sliding Window (krok = 1 min) żeby nie przegapić przypadku
# leżącego na granicy okna tumbling.

card_testing_alerts = (
    parsed_stream
    .groupBy(
        F.col("card_id"),
        F.window(F.col("event_time"), f"{CARD_TESTING_WINDOW_MINUTES} minutes", "1 minutes")
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
        F.count("transaction_id").alias("tx_count"),
        F.max("event_time").alias("last_seen")
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
        F.col("last_seen").cast("string").alias("last_seen_str")
    )
)

# =============================================================================
# ZAPIS DO REDISA (foreachBatch)
# =============================================================================

def write_alerts_to_redis(batch_df, batch_id):
    """
    Wywoływana przez Spark dla każdej mikro-partii wyników.
    Dla każdego alertu:
      - HINCRBY fraud:<card_id> score <delta>  → kumuluje punkty
      - HSET    fraud:<card_id> rule / last_seen / attempt_count
      - Jeśli score >= FRAUD_THRESHOLD → SADD flagged_cards <card_id>
    """
    rows = batch_df.collect()      # bezpieczne – batch jest już mały
    if not rows:
        return

    # Łączymy się z Redisem wewnątrz funkcji (połączenie nie może być serializowane)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pipe = r.pipeline()            # batch komend → jeden round-trip sieciowy

    for row in rows:
        key = f"fraud:{row['card_id']}"

        # Atomowy increment punktów
        pipe.hincrby(key, "score", row["score_delta"])

        # Aktualizacja metadanych
        pipe.hset(key, mapping={
            "card_id":       row["card_id"],
            "rule":          row["rule"],
            "last_seen":     row["last_seen_str"],
            "attempt_count": str(row["attempt_count"]),
            "score_delta":   str(row["score_delta"]),
        })

        # TTL 24h – stare wpisy same znikną
        pipe.expire(key, 86400)

        print(
            f"[Batch {batch_id}] 🚨 {row['rule']} | "
            f"Karta: {row['card_id']} | "
            f"+{row['score_delta']} pkt | "
            f"Prób: {row['attempt_count']} | "
            f"Czas: {row['last_seen_str']}"
        )

    results = pipe.execute()

    # Po wykonaniu pipelina sprawdzamy sumaryczny score i flagujemy kartę
    flagging_pipe = r.pipeline()
    for row in rows:
        key   = f"fraud:{row['card_id']}"
        score = r.hget(key, "score")
        if score and int(score) >= FRAUD_THRESHOLD:
            flagging_pipe.sadd("flagged_cards", row["card_id"])
            print(
                f"[Batch {batch_id}] 🔴 ZABLOKOWANO kartę {row['card_id']} "
                f"(score: {score})"
            )
    flagging_pipe.execute()

# =============================================================================
# URUCHOMIENIE ZAPYTAŃ STRUMIENIOWYCH
# =============================================================================

query_brute_force = (
    brute_force_alerts
    .writeStream
    .outputMode("update")          # update = emituj tylko zmienione okna
    .foreachBatch(write_alerts_to_redis)
    .option("checkpointLocation", "/tmp/checkpoints/brute_force")
    .trigger(processingTime="10 seconds")   # mikro-partia co 10 sek
    .start()
)

query_card_testing = (
    card_testing_alerts
    .writeStream
    .outputMode("update")
    .foreachBatch(write_alerts_to_redis)
    .option("checkpointLocation", "/tmp/checkpoints/card_testing")
    .trigger(processingTime="10 seconds")
    .start()
)

print("=" * 60)
print("  RTA Moduł 2 – Detektyw Czasowy uruchomiony")
print(f"  Kafka:  {KAFKA_BROKER}  →  temat: {KAFKA_TOPIC}")
print(f"  Redis:  {REDIS_HOST}:{REDIS_PORT}")
print(f"  Reguła 2 (Brute-Force):  >{BRUTE_FORCE_MAX_ATTEMPTS} prób / {BRUTE_FORCE_WINDOW_SECONDS}s  → +{BRUTE_FORCE_SCORE} pkt")
print(f"  Reguła 4 (Card Testing): <{CARD_TESTING_SMALL_MAX} PLN + >{CARD_TESTING_LARGE_MIN} PLN / {CARD_TESTING_WINDOW_MINUTES}min → +{CARD_TESTING_SCORE} pkt")
print(f"  Próg blokady: {FRAUD_THRESHOLD} pkt")
print("=" * 60)

# Czekamy aż oba strumienie zostaną zakończone (działa nieskończenie)
spark.streams.awaitAnyTermination()
