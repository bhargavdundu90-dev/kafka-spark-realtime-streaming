"""Spark Structured Streaming job: Kafka -> validated Parquet + fraud signals.

Three sinks, each with its own checkpoint:
  <OUTPUT_PATH>/transactions   valid, de-duplicated events, partitioned by event_date
  <OUTPUT_PATH>/quarantine     records that failed the contract, with the reason
  <OUTPUT_PATH>/fraud_signals  per-card window aggregates with risk_score > 0

Locally OUTPUT_PATH is a folder (or s3a://... on MinIO); on AWS it is an S3 URI.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import DEFAULT_THRESHOLDS, FraudThresholds, JobSettings
from .transforms import fraud_signals, parse_json, split_valid, with_partitions

log = logging.getLogger("txnstream")


def build_session(app_name: str = "txnstream") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        # one commit per micro-batch; avoids partial files on S3 being picked up
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, s: JobSettings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", s.topic)
        .option("startingOffsets", s.starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
        .select(F.col("value").cast("string").alias("value"))
    )


def build_streams(
    source: DataFrame, watermark: str, th: FraudThresholds = DEFAULT_THRESHOLDS
) -> dict[str, DataFrame]:
    """Wire the logical plan. Kept separate from the sinks so tests can feed a
    file stream instead of Kafka."""
    valid, quarantine = split_valid(parse_json(source))
    deduped = (
        valid.withWatermark("event_ts", watermark)
        # producer retries can deliver the same txn twice; drop within the watermark
        .dropDuplicatesWithinWatermark(["txn_id"])
    )
    signals = fraud_signals(deduped, th).filter(F.col("risk_score") > 0)
    return {
        "transactions": with_partitions(deduped),
        "quarantine": quarantine,
        "fraud_signals": signals,
    }


PARTITION_COLS = {"transactions": "event_date", "quarantine": "ingest_date", "fraud_signals": "window_date"}


def start(s: JobSettings) -> None:
    spark = build_session()
    streams = build_streams(read_kafka(spark, s), s.watermark)
    queries = []
    for name, df in streams.items():
        q = (
            df.writeStream.queryName(name)
            .format("parquet")
            .outputMode("append")
            .partitionBy(PARTITION_COLS[name])
            .option("path", f"{s.output_path}/{name}")
            .option("checkpointLocation", f"{s.checkpoint_path}/{name}")
            .trigger(processingTime=f"{s.trigger_seconds} seconds")
            .start()
        )
        queries.append(q)
        log.info("started %s -> %s/%s", name, s.output_path, name)
    spark.streams.awaitAnyTermination()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    start(JobSettings.from_env())


if __name__ == "__main__":
    main()
