#!/usr/bin/env python3
"""TeamSpark sample application: distributed sales analytics on the shared cluster.

Generates synthetic orders on the executors, cleans and enriches them, computes
daily revenue, per-region top customers and a category summary, then saves the
results to the member's own database (teamspark_<user>).

    python app/sample_app.py                          # uses SPARK_REMOTE + TEAMSPARK_USER
    python app/sample_app.py --user alice --rows 5000000
    python app/sample_app.py --remote sc://teamspark-mac:15002 --no-save

Exit codes: 0 ok, 2 config error, 3 connection error, 4 timeout, 5 job error, 130 Ctrl+C.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from teamspark import (ConfigError, JobWatchdog, Settings, TeamSparkError,  # noqa: E402
                       setup_logging, teamspark_session, translate_error)
from teamspark import pipeline  # noqa: E402

APP = "sales-pipeline"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--remote", help="Spark Connect URL (default: SPARK_REMOTE)")
    ap.add_argument("--user", help="team-member name (default: TEAMSPARK_USER or OS login)")
    ap.add_argument("--rows", type=int, default=2_000_000, help="orders to generate (default 2,000,000)")
    ap.add_argument("--partitions", type=int, default=16, help="input partitions (default 16)")
    ap.add_argument("--top", type=int, default=3, help="top customers per region (default 3)")
    ap.add_argument("--timeout", type=int, help="interrupt the run after N seconds (default 900)")
    ap.add_argument("--no-save", action="store_true", help="do not write result tables")
    ap.add_argument("--log-level", help="DEBUG, INFO, WARNING (default INFO)")
    return ap.parse_args(argv)


def save_results(spark, settings: Settings, result: pipeline.PipelineResult, log) -> list[str]:
    """Write results into the member's own database; tag rows with the run name."""
    from pyspark.sql import functions as F

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {settings.database}")
    saved = []
    for name, df in (("daily_revenue", result.daily_revenue),
                     ("top_customers", result.top_customers),
                     ("category_summary", result.category_summary)):
        table = f"{settings.database}.{name}"
        (df.withColumn("run_name", F.lit(settings.app_name))
           .write.mode("overwrite").format("parquet").saveAsTable(table))
        saved.append(table)
        log.info("saved %s", table)
    return saved


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        settings = Settings.load(APP, remote=args.remote, user=args.user,
                                 job_timeout_s=args.timeout, log_level=args.log_level)
    except TeamSparkError as e:
        print(f"ERROR: {e}", *[f"  {h}" for h in e.hints], sep="\n", file=sys.stderr)
        return e.exit_code

    log = setup_logging(settings)
    log.info("starting %s: rows=%s partitions=%s server=%s", settings.app_name, f"{args.rows:,}",
             args.partitions, settings.remote)
    t0 = time.monotonic()
    watchdog = None
    try:
        if not 0 < args.rows <= settings.max_rows:
            raise ConfigError(f"--rows must be between 1 and {settings.max_rows:,} (TEAMSPARK_MAX_ROWS)",
                              ["The cluster is shared by the team; keep sample runs modest."])
        if not 1 <= args.partitions <= 256:
            raise ConfigError("--partitions must be between 1 and 256")

        with teamspark_session(settings, logger=log) as spark:
            watchdog = JobWatchdog(spark, settings, logger=log)
            with watchdog:
                try:
                    result = pipeline.run_pipeline(spark, args.rows, args.partitions, args.top)
                    print("\nRevenue by category:")
                    result.category_summary.show(truncate=False)
                    print(f"Top {args.top} customers per region:")
                    result.top_customers.orderBy("region", "rank").show(4 * args.top, truncate=False)
                    saved = [] if args.no_save else save_results(spark, settings, result, log)
                except KeyboardInterrupt:
                    log.warning("interrupted by user, cancelling this run's jobs on the cluster")
                    watchdog.interrupt()
                    raise
                except TeamSparkError:
                    raise
                except Exception as e:  # noqa: BLE001 - translated below
                    if watchdog.fired.is_set():
                        raise  # JobWatchdog.__exit__ turns this into JobTimeout
                    raise translate_error(e) from e
    except KeyboardInterrupt:
        log.error("run %s cancelled", settings.app_name)
        return 130
    except TeamSparkError as e:
        log.error("%s", e)
        for h in e.hints:
            log.error("  hint: %s", h)
        log.error("run %s FAILED after %.1fs (exit code %s)", settings.app_name,
                  time.monotonic() - t0, e.exit_code)
        return e.exit_code

    q = result.quality
    log.info("quality: raw=%s clean=%s null_qty=%s cancelled=%s duplicates=%s",
             q["raw_rows"], q["clean_orders"], q["null_quantity"], q["cancelled"], q["duplicates"])
    log.info("timings: %s", ", ".join(f"{k}={v:.1f}s" for k, v in result.timings.items()))
    if saved:
        log.info("results: %s", ", ".join(saved))
    log.info("run %s SUCCEEDED in %.1fs", settings.app_name, time.monotonic() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
