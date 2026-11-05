"""Four team members working at the same time on the shared cluster."""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from pyspark.sql import functions as F

from teamspark import JobTimeout, JobWatchdog, Settings, pipeline, teamspark_session

pytestmark = [pytest.mark.server, pytest.mark.slow]

MEMBERS = 4


def _member_settings(test_user: str, i: int) -> Settings:
    return Settings.load("pytest-concurrency", user=f"{test_user}_m{i}", job_timeout_s=300)


def test_four_members_run_pipelines_concurrently(test_user):
    barrier = threading.Barrier(MEMBERS)

    def member(i: int) -> dict:
        s = _member_settings(test_user, i)
        with teamspark_session(s) as spark:
            barrier.wait(timeout=60)  # start the heavy work together
            t0 = time.monotonic()
            res = pipeline.run_pipeline(spark, rows=300_000, partitions=8)
            total = res.category_summary.agg(F.sum("revenue")).first()[0]
            return {"user": s.user, "session": spark.session_id, "pool": spark.conf.get("spark.scheduler.pool"),
                    "run": s.app_name, "total": total, "clean": res.quality["clean_orders"],
                    "secs": time.monotonic() - t0}

    with ThreadPoolExecutor(MEMBERS) as ex:
        results = list(ex.map(member, range(MEMBERS)))

    assert len({r["session"] for r in results}) == MEMBERS, "each member must get its own session"
    assert len({r["run"] for r in results}) == MEMBERS, "run names must be unique"
    assert all(r["pool"] == r["user"] for r in results), "each member runs in its own FAIR pool"
    assert len({(r["total"], r["clean"]) for r in results}) == 1, "same input must give same result"
    for r in results:
        print(f"{r['user']}: {r['secs']:.1f}s")


def test_sessions_are_isolated(test_user):
    a_s, b_s = _member_settings(test_user, 1), _member_settings(test_user, 2)
    with teamspark_session(a_s) as a, teamspark_session(b_s) as b:
        a.range(5).createOrReplaceTempView("only_in_a")
        a.conf.set("spark.sql.shuffle.partitions", "7")
        assert a.table("only_in_a").count() == 5
        assert not b.catalog.tableExists("only_in_a"), "temp views must not leak between members"
        assert b.conf.get("spark.sql.shuffle.partitions") != "7", "session conf must not leak"


def test_interrupting_one_member_does_not_affect_another(test_user):
    a_s, b_s = _member_settings(test_user, 1), _member_settings(test_user, 2)
    with teamspark_session(a_s) as a, teamspark_session(b_s) as b:
        errors: list[BaseException] = []

        def long_job():
            try:
                with JobWatchdog(a, a_s, timeout_s=4):
                    a.range(0, 1_000_000_000_000, numPartitions=64).agg(F.sum(F.col("id") % 7)).collect()
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        t = threading.Thread(target=long_job)
        t.start()
        time.sleep(1)
        assert b.range(0, 1_000_000, numPartitions=8).agg(F.sum("id")).first()[0] == 499_999_500_000
        t.join(timeout=120)
        assert not t.is_alive()
        assert errors and isinstance(errors[0], JobTimeout)
        assert b.range(3).count() == 3
