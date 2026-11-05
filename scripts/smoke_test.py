#!/usr/bin/env python
"""TeamSpark server smoke test (server-side verification, not a client tool).

Runs a set of checks against a Spark Connect endpoint and exits non-zero on
the first failure.

    docker compose exec spark-connect python /opt/teamspark/scripts/smoke_test.py
    python smoke_test.py --remote sc://host:15002 --skip-udf   # non-3.12 client
"""
from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EXPECTED_SPARK = "4.1.4"
results: list[tuple[str, bool, str]] = []


def check(name: str):
    def deco(fn):
        def run(*a, **kw):
            t0 = time.time()
            try:
                detail = fn(*a, **kw) or ""
                results.append((name, True, f"{detail} ({time.time() - t0:.1f}s)"))
            except Exception as e:  # noqa: BLE001
                results.append((name, False, f"{type(e).__name__}: {e}"))
                raise
        return run
    return deco


@check("server version")
def t_version(spark):
    v = spark.version
    assert v == EXPECTED_SPARK, f"expected {EXPECTED_SPARK}, got {v}"
    return f"Spark {v}"


@check("distributed aggregation")
def t_agg(spark):
    n = 20_000_000
    total = spark.range(0, n, numPartitions=32).agg(F.sum("id")).first()[0]
    assert total == n * (n - 1) // 2, total
    return f"sum(0..{n:,}) correct over 32 partitions"


@check("shuffle + join")
def t_join(spark):
    a = spark.range(0, 1_000_000).withColumn("k", F.col("id") % 1000)
    b = spark.range(0, 1000).withColumnRenamed("id", "k").withColumn("v", F.lit(1))
    rows = a.join(b, "k").groupBy("k").count().count()
    assert rows == 1000, rows
    return "1000 groups"


@check("python UDF on all workers")
def t_udf(spark, min_workers):
    @F.udf("string")
    def where(_):
        import socket, sys as s
        return f"{socket.gethostname()}|py{s.version_info.major}.{s.version_info.minor}"

    rows = (spark.range(0, 2000, numPartitions=64)
            .select(where("id").alias("w")).distinct().collect())
    seen = sorted({r.w for r in rows})
    hosts = {s.split("|")[0] for s in seen}
    assert len(hosts) >= min_workers, f"tasks ran on {hosts}, expected >= {min_workers} workers"
    return ", ".join(seen)


@check("arrow / pandas round-trip")
def t_arrow(spark):
    pdf = spark.range(0, 100_000).withColumn("sq", F.col("id") * F.col("id")).toPandas()
    assert len(pdf) == 100_000 and int(pdf["sq"].iloc[-1]) == 99_999 ** 2
    back = spark.createDataFrame(pdf.head(1000)).count()
    assert back == 1000
    return "toPandas 100k rows, createDataFrame 1k rows"


@check("persistent table (hive metastore + warehouse)")
def t_table(spark, keep):
    spark.sql("CREATE DATABASE IF NOT EXISTS teamspark_smoke")
    spark.sql("DROP TABLE IF EXISTS teamspark_smoke.numbers")
    (spark.range(0, 10_000, numPartitions=8).withColumn("bucket", F.col("id") % 10)
          .write.format("parquet").partitionBy("bucket").saveAsTable("teamspark_smoke.numbers"))
    cnt = spark.table("teamspark_smoke.numbers").count()
    assert cnt == 10_000, cnt
    loc = (spark.sql("DESCRIBE TABLE EXTENDED teamspark_smoke.numbers")
                .filter("col_name = 'Location'").first().data_type)
    if not keep:
        spark.sql("DROP DATABASE teamspark_smoke CASCADE")
    return f"10k rows at {loc}" + (" (kept)" if keep else " (dropped)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--remote", default="sc://localhost:15002")
    ap.add_argument("--min-workers", type=int, default=2)
    ap.add_argument("--skip-udf", action="store_true",
                    help="skip Python UDF test (client Python != server 3.12)")
    ap.add_argument("--keep-table", action="store_true",
                    help="keep teamspark_smoke.numbers to test persistence across restarts")
    args = ap.parse_args()

    print(f"Connecting to {args.remote} ...")
    spark = SparkSession.builder.remote(args.remote).getOrCreate()
    ok = True
    try:
        t_version(spark)
        t_agg(spark)
        t_join(spark)
        if not args.skip_udf:
            t_udf(spark, args.min_workers)
        t_arrow(spark)
        t_table(spark, args.keep_table)
    except Exception:  # already recorded
        ok = False
    finally:
        spark.stop()

    print()
    for name, passed, detail in results:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}: {detail}")
    print(f"\n{'ALL CHECKS PASSED' if ok else 'SMOKE TEST FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
