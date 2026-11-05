"""Data processing tests: each pipeline transform on hand-written data, then end-to-end invariants."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from teamspark import JobTimeout, JobWatchdog, pipeline

pytestmark = pytest.mark.server

D1, D2 = date(2026, 1, 1), date(2026, 1, 2)


@pytest.fixture(scope="module")
def raw(spark):
    rows = [
        # order_id, customer_id, product_id, region, order_date, quantity, status
        (1, 10, 1, "north", D1, 2, "completed"),     # laptop 2 x 1199.00
        (2, 11, 7, "north", D1, 3, "completed"),     # novel 3 x 18.75
        (2, 11, 7, "north", D1, 3, "completed"),     # duplicate of order 2
        (3, 10, 9, "south", D2, 1, "completed"),     # notebook 1 x 4.20
        (4, 12, 4, "south", D2, None, "completed"),  # invalid: NULL quantity
        (5, 12, 4, "south", D2, 0, "completed"),     # invalid: zero quantity
        (6, 13, 2, "north", D1, 1, "cancelled"),     # cancelled
        (7, 12, 7, "south", D2, 4, "completed"),     # novel 4 x 18.75
    ]
    return spark.createDataFrame(rows, pipeline.ORDER_SCHEMA)


@pytest.fixture(scope="module")
def enriched(spark, raw):
    return pipeline.enrich_orders(pipeline.clean_orders(raw), pipeline.products_df(spark))


def test_clean_orders_drops_invalid_cancelled_and_duplicates(raw):
    ids = sorted(r.order_id for r in pipeline.clean_orders(raw).collect())
    assert ids == [1, 2, 3, 7]


def test_enrich_computes_exact_decimal_revenue(enriched):
    got = {r.order_id: (r.category, r.revenue) for r in enriched.collect()}
    assert got == {
        1: ("electronics", Decimal("2398.00")),
        2: ("books", Decimal("56.25")),
        3: ("books", Decimal("4.20")),
        7: ("books", Decimal("75.00")),
    }


def test_daily_revenue_aggregates(enriched):
    rows = {(r.order_date, r.region, r.category): (r.orders, r.units, r.revenue, r.customers)
            for r in pipeline.daily_revenue(enriched).collect()}
    assert rows == {
        (D1, "north", "electronics"): (1, 2, Decimal("2398.00"), 1),
        (D1, "north", "books"): (1, 3, Decimal("56.25"), 1),
        (D2, "south", "books"): (2, 5, Decimal("79.20"), 2),
    }


def test_top_customers_ranks_per_region(enriched):
    rows = [(r.region, r.rank, r.customer_id, r.revenue)
            for r in pipeline.top_customers(enriched, k=1).orderBy("region").collect()]
    assert rows == [("north", 1, 10, Decimal("2398.00")), ("south", 1, 12, Decimal("75.00"))]


def test_category_summary(enriched):
    rows = [(r.category, r.orders, r.revenue, r.avg_order_value)
            for r in pipeline.category_summary(pipeline.daily_revenue(enriched)).collect()]
    assert rows == [("electronics", 1, Decimal("2398.00"), Decimal("2398.00")),
                    ("books", 3, Decimal("135.45"), Decimal("45.15"))]


def test_quality_report(raw):
    assert pipeline.quality_report(raw) == {
        "raw_rows": 8, "null_quantity": 1, "cancelled": 1, "distinct_orders": 7, "duplicates": 1}


def test_generated_data_is_deterministic_and_dirty(spark):
    a = pipeline.quality_report(pipeline.generate_orders(spark, 50_000, partitions=8))
    b = pipeline.quality_report(pipeline.generate_orders(spark, 50_000, partitions=3))
    assert a == b, "generation must not depend on partitioning"
    assert a["distinct_orders"] == 50_000
    assert 0 < a["null_quantity"] < 1_000 and 0 < a["cancelled"] < 5_000 and 0 < a["duplicates"] < 200


def test_pipeline_end_to_end_invariants(spark):
    res = pipeline.run_pipeline(spark, rows=200_000, partitions=8)
    q = res.quality
    daily_total = res.daily_revenue.agg(F.sum("revenue")).first()[0]
    summary_total = res.category_summary.agg(F.sum("revenue")).first()[0]
    assert daily_total == summary_total
    assert res.category_summary.agg(F.sum("orders")).first()[0] == q["clean_orders"]
    # clean = distinct orders minus invalid/cancelled ones (which are never duplicates' only copy)
    assert q["clean_orders"] < q["distinct_orders"]
    assert res.top_customers.groupBy("region").count().filter("count != 3").count() == 0


def test_save_and_read_back_member_table(spark, settings):
    db = settings.database
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        (spark.range(100).withColumn("run_name", F.lit(settings.app_name))
              .write.mode("overwrite").saveAsTable(f"{db}.roundtrip"))
        row = spark.table(f"{db}.roundtrip").agg(F.count("*").alias("n"), F.max("run_name").alias("r")).first()
        assert (row.n, row.r) == (100, settings.app_name)
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")


@pytest.mark.slow
def test_watchdog_interrupts_long_job(spark, settings):
    with pytest.raises(JobTimeout):
        with JobWatchdog(spark, settings, timeout_s=3):
            # 10^12 rows: minutes of work, far longer than the 3 s limit.
            spark.range(0, 1_000_000_000_000, numPartitions=64).agg(F.sum(F.col("id") % 7)).collect()
    # The session stays usable after an interrupted run.
    assert spark.range(10).count() == 10
