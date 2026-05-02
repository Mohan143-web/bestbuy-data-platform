"""Kafka to Delta Lake streaming pipeline.

Starts one Structured Streaming query per topic:
- Bronze: raw Kafka payloads and metadata
- Silver: typed, validated, deduplicated records
- Quarantine: malformed or contract-failing records
- Gold increments: order-level facts and revenue increments
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "data-quality"))

from rules import add_late_event_flag, apply_contract_rules  # noqa: E402
from schema_contracts import EVENT_CONTRACTS  # noqa: E402


TOPIC_SCHEMAS = {
    "orders": T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("event_ts", T.StringType()),
            T.StructField("ingestion_ts", T.StringType()),
            T.StructField("order_id", T.StringType()),
            T.StructField("user_id", T.StringType()),
            T.StructField("product_id", T.StringType()),
            T.StructField("sku", T.StringType()),
            T.StructField("store_id", T.StringType()),
            T.StructField("channel", T.StringType()),
            T.StructField("quantity", T.IntegerType()),
            T.StructField("unit_price", T.DoubleType()),
            T.StructField("order_total", T.DoubleType()),
            T.StructField("currency", T.StringType()),
            T.StructField("status", T.StringType()),
        ]
    ),
    "inventory": T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("event_ts", T.StringType()),
            T.StructField("ingestion_ts", T.StringType()),
            T.StructField("product_id", T.StringType()),
            T.StructField("sku", T.StringType()),
            T.StructField("store_id", T.StringType()),
            T.StructField("on_hand", T.IntegerType()),
            T.StructField("available_to_promise", T.IntegerType()),
            T.StructField("reorder_point", T.IntegerType()),
            T.StructField("change_quantity", T.IntegerType()),
            T.StructField("reason", T.StringType()),
        ]
    ),
    "pricing": T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("event_ts", T.StringType()),
            T.StructField("ingestion_ts", T.StringType()),
            T.StructField("product_id", T.StringType()),
            T.StructField("sku", T.StringType()),
            T.StructField("old_price", T.DoubleType()),
            T.StructField("new_price", T.DoubleType()),
            T.StructField("currency", T.StringType()),
            T.StructField("reason", T.StringType()),
        ]
    ),
    "clickstream": T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("event_ts", T.StringType()),
            T.StructField("ingestion_ts", T.StringType()),
            T.StructField("session_id", T.StringType()),
            T.StructField("user_id", T.StringType()),
            T.StructField("product_id", T.StringType()),
            T.StructField("sku", T.StringType()),
            T.StructField("page", T.StringType()),
            T.StructField("search_term", T.StringType()),
            T.StructField("device", T.StringType()),
            T.StructField("referrer", T.StringType()),
        ]
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Kafka to Delta streaming pipeline.")
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--lakehouse-root", default="lakehouse")
    parser.add_argument("--checkpoint-root", default="checkpoints")
    parser.add_argument("--topics", default="orders,inventory,pricing,clickstream")
    parser.add_argument("--available-now", action="store_true", help="Process existing data and exit.")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bestbuy-realtime-lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_stream(df, path: Path, checkpoint: Path, available_now: bool, output_mode: str = "append"):
    writer = (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", str(checkpoint))
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start(str(path))


def parsed_silver(raw_df, topic: str):
    schema = TOPIC_SCHEMAS[topic]
    parsed = raw_df.withColumn("_json", F.from_json("payload", schema))
    typed = parsed.select(
        "_json.*",
        "payload",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "_ingested_at",
    )
    typed = typed.withColumn("event_ts", F.to_timestamp("event_ts")).withColumn(
        "ingestion_ts", F.to_timestamp("ingestion_ts")
    )
    typed = apply_contract_rules(typed, topic)
    typed = add_late_event_flag(typed, topic)
    return typed


def enrich_orders(spark: SparkSession, orders_df):
    catalog_path = ROOT / "sample-data" / "product_catalog.csv"
    catalog = spark.read.option("header", True).csv(str(catalog_path))
    catalog = catalog.select(
        "product_id",
        "product_name",
        "category",
        "brand",
        F.col("cost").cast("double").alias("cost"),
        F.col("reorder_point").cast("int").alias("catalog_reorder_point"),
    )
    return orders_df.join(catalog, on="product_id", how="left").withColumn(
        "gross_margin", F.round((F.col("unit_price") - F.col("cost")) * F.col("quantity"), 2)
    )


def write_order_gold_batch(batch_df, batch_id: int, lakehouse_root: Path) -> None:
    if batch_df.isEmpty():
        return
    gold_root = lakehouse_root / "gold"
    (
        batch_df.withColumn("order_date", F.to_date("event_ts"))
        .write.format("delta")
        .mode("append")
        .save(str(gold_root / "orders_gold"))
    )
    revenue = (
        batch_df.withColumn("order_date", F.to_date("event_ts"))
        .groupBy("order_date", "product_id", "product_name", "category", "channel")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("order_total"), 2).alias("revenue"),
            F.round(F.sum("gross_margin"), 2).alias("gross_margin"),
        )
        .withColumn("_batch_id", F.lit(batch_id))
    )
    revenue.write.format("delta").mode("append").save(str(gold_root / "revenue_daily_incremental"))


def start_topic_pipeline(
    spark: SparkSession,
    topic: str,
    bootstrap_servers: str,
    lakehouse_root: Path,
    checkpoint_root: Path,
    available_now: bool,
):
    raw = kafka_stream(spark, bootstrap_servers, topic)
    queries = [
        write_stream(
            raw.withColumn("topic", F.lit(topic)),
            lakehouse_root / "bronze" / topic,
            checkpoint_root / "bronze" / topic,
            available_now,
        )
    ]

    silver = parsed_silver(raw, topic)
    contract = EVENT_CONTRACTS[topic]
    valid = (
        silver.filter(F.col("_dq_is_valid"))
        .withWatermark("event_ts", f"{contract['max_lateness_minutes']} minutes")
        .dropDuplicates(contract["dedupe_key"])
    )
    invalid = silver.filter(~F.col("_dq_is_valid"))

    if topic == "orders":
        valid = enrich_orders(spark, valid)

    queries.append(
        write_stream(
            valid,
            lakehouse_root / "silver" / topic,
            checkpoint_root / "silver" / topic,
            available_now,
        )
    )
    queries.append(
        write_stream(
            invalid,
            lakehouse_root / "quarantine" / topic,
            checkpoint_root / "quarantine" / topic,
            available_now,
        )
    )

    if topic == "orders":
        writer = (
            valid.writeStream.foreachBatch(
                lambda df, batch_id: write_order_gold_batch(df, batch_id, lakehouse_root)
            )
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint_root / "gold" / "orders"))
        )
        if available_now:
            writer = writer.trigger(availableNow=True)
        queries.append(writer.start())

    return queries


def main() -> int:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    lakehouse_root = Path(args.lakehouse_root)
    checkpoint_root = Path(args.checkpoint_root)
    topics = [topic.strip() for topic in args.topics.split(",") if topic.strip()]

    queries = []
    for topic in topics:
        queries.extend(
            start_topic_pipeline(
                spark,
                topic,
                args.bootstrap_servers,
                lakehouse_root,
                checkpoint_root,
                args.available_now,
            )
        )

    if args.available_now:
        for query in queries:
            query.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

