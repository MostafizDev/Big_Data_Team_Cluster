"""Connection tests: reachability, versions, member identity, error handling."""
from __future__ import annotations

import dataclasses

import pyspark
import pytest
from pyspark.sql import functions as F

from teamspark import ConnectionFailed, Settings, preflight, teamspark_session

pytestmark = pytest.mark.server


def test_preflight_reaches_server(settings):
    preflight(settings)  # raises ConnectionFailed with hints otherwise


def test_server_and_client_versions_match(spark):
    server = spark.version
    assert server == pyspark.__version__, f"server Spark {server} != client pyspark-client {pyspark.__version__}"


def test_session_identity_pool_and_run_name(spark, settings):
    assert f"user_id={settings.user}" in settings.remote
    assert spark.conf.get("spark.scheduler.pool") == settings.user
    assert spark.conf.get("spark.job.description") == settings.app_name
    assert settings.app_name in spark.getTags()
    assert settings.app_name.startswith(f"teamspark-{settings.user}-pytest-")


def test_run_names_are_unique(settings):
    names = {Settings.load("pytest", user=settings.user).app_name for _ in range(20)}
    assert len(names) == 20


def test_simple_dataframe_operation(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "a")], "id int, key string")
    rows = df.groupBy("key").agg(F.sum("id").alias("total")).orderBy("key").collect()
    assert [(r.key, r.total) for r in rows] == [("a", 4), ("b", 2)]


def test_python_udf_runs_on_all_workers(spark):
    @F.udf("string")
    def host(_):
        import socket
        return socket.gethostname()

    hosts = {r.h for r in spark.range(0, 400, numPartitions=40).select(host("id").alias("h")).distinct().collect()}
    assert len(hosts) >= 2, f"tasks ran only on {hosts}; expected >= 2 workers"


def test_wrong_port_raises_connection_failed_with_hints(settings):
    bad = dataclasses.replace(settings, port=15999)
    with pytest.raises(ConnectionFailed) as err:
        preflight(bad, timeout_s=3)
    assert err.value.exit_code == 3 and err.value.hints


def test_unknown_host_raises_connection_failed():
    bad = Settings.load("pytest", remote="sc://teamspark-no-such-host.invalid:15002", user="pytest")
    with pytest.raises(ConnectionFailed, match="cannot resolve"):
        with teamspark_session(bad):
            pass
