"""Batch rebuild of business-ready Gold Delta tables from Silver Delta data."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lakehouse-root", default="lakehouse")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bestbuy-gold-refresh")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_delta_if_exists(spark: SparkSession, path: Path):
    if not path.exists():
        return None
    return spark.read.format("delta").load(str(path))


def write_gold(df, path: Path) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))


def rebuild_orders_gold(orders, gold_root: Path) -> None:
    orders_gold = orders.withColumn("order_date", F.to_date("event_ts")).select(
        "order_id",
        "order_date",
        "event_ts",
        "user_id",
        "product_id",
        "product_name",
        "category",
        "brand",
        "store_id",
        "channel",
        "quantity",
        "unit_price",
        "order_total",
        "gross_margin",
        "status",
    )
    write_gold(orders_gold, gold_root / "orders_gold")

    revenue_daily = (
        orders_gold.groupBy("order_date", "category", "channel")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("order_total"), 2).alias("revenue"),
            F.round(F.sum("gross_margin"), 2).alias("gross_margin"),
        )
        .orderBy("order_date", "category", "channel")
    )
    write_gold(revenue_daily, gold_root / "revenue_daily")

    top_products = (
        orders_gold.groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("order_total"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy(F.desc("revenue"))
    )
    write_gold(top_products, gold_root / "top_products")


def rebuild_inventory_gold(inventory, gold_root: Path) -> None:
    latest_window = Window.partitionBy("product_id", "store_id").orderBy(F.desc("event_ts"))
    inventory_gold = (
        inventory.withColumn("_rank", F.row_number().over(latest_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn("is_low_inventory", F.col("available_to_promise") <= F.col("reorder_point"))
        .withColumn(
            "inventory_risk_level",
            F.when(F.col("available_to_promise") <= 0, F.lit("stockout"))
            .when(F.col("available_to_promise") <= F.col("reorder_point"), F.lit("reorder"))
            .otherwise(F.lit("healthy")),
        )
    )
    write_gold(inventory_gold, gold_root / "inventory_gold")
    write_gold(
        inventory_gold.filter(F.col("inventory_risk_level") != "healthy"),
        gold_root / "inventory_risk",
    )


def main() -> int:
    args = parse_args()
    lakehouse_root = Path(args.lakehouse_root)
    gold_root = lakehouse_root / "gold"
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    orders = read_delta_if_exists(spark, lakehouse_root / "silver" / "orders")
    inventory = read_delta_if_exists(spark, lakehouse_root / "silver" / "inventory")

    if orders is not None:
        rebuild_orders_gold(orders, gold_root)
    if inventory is not None:
        rebuild_inventory_gold(inventory, gold_root)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

