"""Sample distributed pipeline: synthetic e-commerce orders -> clean -> enrich -> aggregate.

All heavy work runs on the cluster's executors; only small results come back.
The transforms are pure DataFrame -> DataFrame functions so they can be unit
tested on tiny hand-written inputs (see tests/test_processing.py).

Data generation is deterministic (hash-based, no random seeds), so the same
row count always produces the same results on any cluster layout.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .errors import ConfigError

log = logging.getLogger("teamspark.pipeline")

REGIONS = ("north", "south", "east", "west")
BASE_DATE = date(2026, 1, 1)
DAYS = 90
MONEY = T.DecimalType(12, 2)

# product_id, name, category, unit_price
PRODUCTS = [
    (1, "laptop", "electronics", Decimal("1199.00")),
    (2, "phone", "electronics", Decimal("799.00")),
    (3, "headphones", "electronics", Decimal("149.50")),
    (4, "desk", "furniture", Decimal("320.00")),
    (5, "chair", "furniture", Decimal("185.25")),
    (6, "lamp", "furniture", Decimal("45.99")),
    (7, "novel", "books", Decimal("18.75")),
    (8, "textbook", "books", Decimal("92.40")),
    (9, "notebook", "books", Decimal("4.20")),
    (10, "coffee", "grocery", Decimal("12.30")),
    (11, "tea", "grocery", Decimal("8.10")),
    (12, "chocolate", "grocery", Decimal("3.65")),
]
PRODUCT_SCHEMA = "product_id int, product string, category string, unit_price decimal(10,2)"
ORDER_SCHEMA = ("order_id long, customer_id long, product_id int, region string, "
                "order_date date, quantity int, status string")


@dataclass
class PipelineResult:
    daily_revenue: DataFrame
    top_customers: DataFrame
    category_summary: DataFrame
    quality: dict[str, int]
    timings: dict[str, float]


def products_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(PRODUCTS, PRODUCT_SCHEMA)


def _h(col: str, salt: int, mod: int):
    """Deterministic pseudo-random int in [0, mod) derived from a column."""
    return F.pmod(F.hash(F.col(col), F.lit(salt)), F.lit(mod))


def generate_orders(spark: SparkSession, rows: int, partitions: int = 16,
                    customers: int | None = None) -> DataFrame:
    """Synthetic raw orders, generated in parallel on the executors.

    Includes realistic dirt: ~1% NULL quantities, ~5% cancelled orders and
    ~0.1% duplicated order rows.
    """
    if rows <= 0:
        raise ConfigError(f"rows must be positive, got {rows}")
    customers = customers or max(rows // 20, 10)
    regions = F.array(*[F.lit(r) for r in REGIONS])
    base = spark.range(0, rows, numPartitions=partitions).select(
        F.col("id").alias("order_id"),
        _h("id", 1, customers).alias("customer_id"),
        (_h("id", 2, len(PRODUCTS)) + 1).cast("int").alias("product_id"),
        F.element_at(regions, (_h("id", 3, len(REGIONS)) + 1).cast("int")).alias("region"),
        F.date_add(F.lit(BASE_DATE), _h("id", 4, DAYS).cast("int")).alias("order_date"),
        F.when(_h("id", 5, 100) == 0, F.lit(None).cast("int"))
         .otherwise((_h("id", 6, 5) + 1).cast("int")).alias("quantity"),
        F.when(_h("id", 7, 100) < 5, F.lit("cancelled")).otherwise(F.lit("completed")).alias("status"),
    )
    duplicates = base.filter(_h("order_id", 8, 1000) == 0)
    return base.unionByName(duplicates)


def clean_orders(raw: DataFrame) -> DataFrame:
    """Drop invalid quantities and cancelled orders, de-duplicate by order_id."""
    return (raw.filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
               .filter(F.col("status") == "completed")
               .dropDuplicates(["order_id"]))


def enrich_orders(orders: DataFrame, products: DataFrame) -> DataFrame:
    """Broadcast-join the small product dimension and compute exact decimal revenue."""
    return (orders.join(F.broadcast(products), "product_id", "inner")
                  .withColumn("revenue", (F.col("quantity") * F.col("unit_price")).cast(MONEY)))


def daily_revenue(enriched: DataFrame) -> DataFrame:
    return (enriched.groupBy("order_date", "region", "category")
                    .agg(F.count("order_id").alias("orders"),
                         F.sum("quantity").alias("units"),
                         F.sum("revenue").cast(MONEY).alias("revenue"),
                         F.countDistinct("customer_id").alias("customers")))


def top_customers(enriched: DataFrame, k: int = 3) -> DataFrame:
    """Top-k customers by revenue per region (ties broken by customer_id)."""
    per_customer = (enriched.groupBy("region", "customer_id")
                            .agg(F.sum("revenue").cast(MONEY).alias("revenue"),
                                 F.count("order_id").alias("orders")))
    w = Window.partitionBy("region").orderBy(F.col("revenue").desc(), F.col("customer_id").asc())
    return (per_customer.withColumn("rank", F.row_number().over(w))
                        .filter(F.col("rank") <= k))


def category_summary(daily: DataFrame) -> DataFrame:
    return (daily.groupBy("category")
                 .agg(F.sum("orders").alias("orders"),
                      F.sum("units").alias("units"),
                      F.sum("revenue").cast(MONEY).alias("revenue"))
                 .withColumn("avg_order_value", (F.col("revenue") / F.col("orders")).cast(MONEY))
                 .orderBy(F.col("revenue").desc()))


def quality_report(raw: DataFrame) -> dict[str, int]:
    """Data-quality counters computed in a single distributed pass."""
    row = raw.agg(
        F.count(F.lit(1)).alias("raw_rows"),
        F.sum(F.col("quantity").isNull().cast("int")).alias("null_quantity"),
        F.sum((F.col("status") == "cancelled").cast("int")).alias("cancelled"),
        F.countDistinct("order_id").alias("distinct_orders"),
    ).first()
    q = {k: int(row[k] or 0) for k in row.asDict()}
    q["duplicates"] = q["raw_rows"] - q["distinct_orders"]
    return q


def run_pipeline(spark: SparkSession, rows: int, partitions: int = 16, top_k: int = 3) -> PipelineResult:
    timings: dict[str, float] = {}

    t = time.monotonic()
    raw = generate_orders(spark, rows, partitions)
    quality = quality_report(raw)
    timings["generate+quality"] = time.monotonic() - t
    log.info("raw orders: %(raw_rows)s rows, %(null_quantity)s null qty, %(cancelled)s cancelled, "
             "%(duplicates)s duplicates", quality)

    t = time.monotonic()
    enriched = enrich_orders(clean_orders(raw), products_df(spark))
    daily = daily_revenue(enriched)
    # Materialize the shared intermediate once on the executors (freed when the
    # session ends); category_summary and the saved table reuse it.
    daily = daily.localCheckpoint()
    quality["clean_orders"] = int(daily.agg(F.sum("orders")).first()[0] or 0)
    timings["clean+enrich+aggregate"] = time.monotonic() - t
    log.info("clean orders: %s", quality["clean_orders"])

    return PipelineResult(
        daily_revenue=daily,
        top_customers=top_customers(enriched, top_k),
        category_summary=category_summary(daily),
        quality=quality,
        timings=timings,
    )
