"""Spark DataFrame transformations. Pure functions of DataFrame -> DataFrame
so the same code runs in the streaming job and in batch unit tests."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import CHANNELS, DEFAULT_THRESHOLDS, SIGNAL_ORDER, FraudThresholds


def transaction_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("txn_id", T.StringType()),
            T.StructField("card_id", T.StringType()),
            T.StructField("customer_id", T.StringType()),
            T.StructField("amount", T.DoubleType()),
            T.StructField("currency", T.StringType()),
            T.StructField("merchant_id", T.StringType()),
            T.StructField("mcc", T.StringType()),
            T.StructField("channel", T.StringType()),
            T.StructField("city", T.StringType()),
            T.StructField("is_international", T.BooleanType()),
            T.StructField("event_time", T.StringType()),
        ]
    )


def parse_json(df: DataFrame) -> DataFrame:
    """Expects a string column `value` (Kafka value cast to string).

    Keeps the raw payload so quarantined rows can be replayed later.
    """
    parsed = F.from_json(F.col("value"), transaction_schema())
    return (
        df.select(F.col("value").alias("raw_value"), parsed.alias("t"))
        .select("raw_value", "t.*")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("ingest_ts", F.current_timestamp())
    )


def validity_reason() -> Column:
    """First failing rule, or null when the row is valid (mirrors schema.validate)."""
    return (
        F.when(F.col("txn_id").isNull() | (F.col("txn_id") == ""), F.lit("missing:txn_id"))
        .when(F.col("card_id").isNull() | (F.col("card_id") == ""), F.lit("missing:card_id"))
        .when(F.col("amount").isNull(), F.lit("missing:amount"))
        .when(F.col("amount") <= 0, F.lit("range:amount"))
        .when(~F.col("channel").isin(*CHANNELS) | F.col("channel").isNull(), F.lit("domain:channel"))
        .when(F.col("event_ts").isNull(), F.lit("format:event_time"))
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    tagged = df.withColumn("reject_reason", validity_reason())
    valid = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason", "raw_value")
    quarantine = tagged.filter(F.col("reject_reason").isNotNull()).select(
        "raw_value", "reject_reason", "ingest_ts", F.to_date("ingest_ts").alias("ingest_date")
    )
    return valid, quarantine


def with_partitions(df: DataFrame) -> DataFrame:
    return df.withColumn("event_date", F.to_date("event_ts"))


def fraud_signals(df: DataFrame, th: FraudThresholds = DEFAULT_THRESHOLDS) -> DataFrame:
    """Per card, per sliding window aggregates + signals + risk score.

    `df` must contain card_id, amount, channel, city, event_ts. In streaming
    the caller sets a watermark on event_ts first so windows can be finalised
    and emitted in append mode.
    """
    win = F.window("event_ts", f"{th.window_minutes} minutes", f"{th.slide_minutes} minutes")
    agg = df.groupBy(F.col("card_id"), win).agg(
        F.count(F.lit(1)).alias("txn_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        # countDistinct is not supported on streaming aggregations; collect_set is
        F.size(F.collect_set("city")).alias("distinct_cities"),
        F.sum(F.when(F.col("channel") == "ATM", 1).otherwise(0)).alias("atm_count"),
        F.sum(
            F.when((F.col("channel") == "ECOM") & (F.col("amount") < th.small_ecom_amount), 1).otherwise(0)
        ).alias("small_ecom_count"),
    )
    rules = {
        "VELOCITY": F.col("txn_count") >= th.velocity_count,
        "HIGH_VALUE": F.col("total_amount") >= th.high_value_total,
        "MULTI_CITY": F.col("distinct_cities") >= th.multi_city_count,
        "ATM_BURST": F.col("atm_count") >= th.atm_burst_count,
        "CARD_TESTING": F.col("small_ecom_count") >= th.card_testing_count,
    }
    signals = F.filter(
        F.array(*[F.when(rules[name], F.lit(name)) for name in SIGNAL_ORDER]),
        lambda x: x.isNotNull(),
    )
    score = sum((F.when(rules[name], th.weights[name]).otherwise(0) for name in SIGNAL_ORDER), F.lit(0))
    return (
        agg.select(
            "card_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "txn_count",
            "total_amount",
            "distinct_cities",
            F.col("atm_count").cast("int").alias("atm_count"),
            F.col("small_ecom_count").cast("int").alias("small_ecom_count"),
        )
        .withColumn("signals", signals)
        .withColumn("risk_score", score.cast("int"))
        .withColumn("is_flagged", F.col("risk_score") >= th.flag_score)
        .withColumn("window_date", F.to_date("window_start"))
    )
