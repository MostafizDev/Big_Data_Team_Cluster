"""Spark Connect session management for team members.

    from teamspark import Settings, teamspark_session, JobWatchdog

    settings = Settings.load("my-analysis")
    with teamspark_session(settings) as spark, JobWatchdog(spark, settings):
        spark.range(10).show()

Each session:
  * connects with user_id=<member> (visible on every job in the Spark UI),
  * runs in the member's own FAIR scheduler pool (equal share of the cluster),
  * labels its jobs with the unique run name (settings.app_name),
  * is tagged with the run name so a timeout/Ctrl+C interrupts only this run.

Note: Spark Connect session tags are per thread. teamspark_session() tags the
opening thread; JobWatchdog tags the thread it is entered in. When running jobs
from extra threads, enter a JobWatchdog in that thread (or call spark.addTag).
"""
from __future__ import annotations

import logging
import socket
import threading
import time
from contextlib import contextmanager
from typing import Iterator

from .config import Settings
from .errors import ConnectionFailed, JobFailed, JobTimeout, TeamSparkError
from .logging_setup import get_logger

_UNREACHABLE_HINTS = [
    "- Is Tailscale connected on this machine? (Tailscale app / 'tailscale status')",
    "- Is the TeamSpark server running? (on the server: docker compose ps)",
    "- Does the Tailscale ACL allow you to reach tcp:15002?",
    "- Run the full diagnosis: python ~/.teamspark/verify.py",
]


def preflight(settings: Settings, timeout_s: float = 10.0) -> None:
    """Fast TCP reachability check with a clear error, before gRPC's long retry loop."""
    try:
        infos = socket.getaddrinfo(settings.host, settings.port, type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        raise ConnectionFailed(f"cannot resolve server '{settings.host}': {e}",
                               ["Check the name; for Tailscale names make sure Tailscale is connected",
                                "and MagicDNS is enabled, or use the server's 100.x.y.z IP."]) from e
    errors: list[OSError] = []
    for family, stype, proto, _, addr in {i[4]: i for i in infos}.values():
        with socket.socket(family, stype, proto) as s:
            s.settimeout(timeout_s)
            try:
                s.connect(addr)
                return
            except OSError as e:
                errors.append(e)
    refused = any(isinstance(e, ConnectionRefusedError) for e in errors)
    reason = "connection refused" if refused else f"no response ({errors[-1]})"
    raise ConnectionFailed(f"cannot reach TeamSpark at {settings.host}:{settings.port}: {reason}",
                           _UNREACHABLE_HINTS)


def translate_error(exc: BaseException) -> TeamSparkError:
    """Map Spark/gRPC exceptions to TeamSpark errors with actionable hints."""
    if isinstance(exc, TeamSparkError):
        return exc
    from pyspark.errors import AnalysisException

    first = (str(exc).strip().splitlines() or [type(exc).__name__])[0][:500]
    text = str(exc)
    if isinstance(exc, AnalysisException):
        return JobFailed(f"query error: {first}",
                         ["Check table/column names and types; the error class above names the problem."])
    if any(s in text for s in ("UNAVAILABLE", "Connection refused", "failed to connect",
                               "Socket closed", "DEADLINE_EXCEEDED")):
        return ConnectionFailed(f"lost connection to the TeamSpark server: {first}", _UNREACHABLE_HINTS)
    if "OPERATION_CANCELED" in text or "cancelled" in text.lower():
        return JobFailed(f"job was cancelled: {first}",
                         ["It may have been interrupted by you, a timeout, or a server restart."])
    if "maxResultSize" in text:
        return JobFailed(f"result too large for the shared driver: {first}",
                         ["Aggregate on the cluster or write results to a table instead of collect()/toPandas()."])
    return JobFailed(f"{type(exc).__name__}: {first}",
                     ["See the server logs: docker compose logs spark-connect (on the server)."])


@contextmanager
def teamspark_session(settings: Settings, *, logger: logging.Logger | None = None,
                      connect_timeout_s: float = 10.0) -> Iterator["SparkSession"]:  # noqa: F821
    """Open a new, isolated Spark Connect session for this team member and run."""
    from pyspark.sql import SparkSession

    log = logger or get_logger("session")
    preflight(settings, connect_timeout_s)
    t0 = time.monotonic()
    try:
        # create() (not getOrCreate) -> a fresh server-side session per run/thread.
        spark = SparkSession.builder.remote(settings.remote).create()
        server_version = spark.version
    except Exception as e:  # noqa: BLE001
        raise ConnectionFailed(f"could not open a Spark Connect session: {translate_error(e)}",
                               _UNREACHABLE_HINTS) from e
    try:
        spark.conf.set("spark.scheduler.pool", settings.pool)
        spark.conf.set("spark.job.description", settings.app_name)
        spark.addTag(settings.app_name)
        log.info("connected to %s:%s as '%s' (Spark %s, session %s, pool %s) in %.1fs",
                 settings.host, settings.port, settings.user, server_version,
                 spark.session_id, settings.pool, time.monotonic() - t0)
        yield spark
    finally:
        try:
            spark.stop()
            log.debug("session closed")
        except Exception as e:  # noqa: BLE001 - never mask the real error
            log.debug("ignoring error while closing session: %s", e)


class JobWatchdog:
    """Interrupts every operation of this run (by session tag) after settings.job_timeout_s.

    Raises JobTimeout on exit if the limit was hit, even if Spark reported the
    cancellation as a different error.
    """

    def __init__(self, spark, settings: Settings, *, timeout_s: float | None = None,
                 logger: logging.Logger | None = None):
        self.spark = spark
        self.tag = settings.app_name
        self.timeout_s = timeout_s if timeout_s is not None else settings.job_timeout_s
        self.log = logger or get_logger("watchdog")
        self.fired = threading.Event()
        self._timer: threading.Timer | None = None

    def _fire(self) -> None:
        self.fired.set()
        self.log.warning("time limit of %ss reached, interrupting run %s", self.timeout_s, self.tag)
        try:
            self.spark.interruptTag(self.tag)
        except Exception as e:  # noqa: BLE001
            self.log.error("could not interrupt run: %s", e)

    def interrupt(self) -> None:
        """Cancel this run's operations now (e.g. on Ctrl+C)."""
        try:
            self.spark.interruptTag(self.tag)
        except Exception as e:  # noqa: BLE001
            self.log.debug("interrupt failed: %s", e)

    def __enter__(self) -> "JobWatchdog":
        # Spark Connect tags are thread-local: tag the thread that will run the
        # jobs (it may differ from the thread that opened the session).
        self.spark.addTag(self.tag)
        self._timer = threading.Timer(self.timeout_s, self._fire)
        self._timer.daemon = True
        self._timer.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._timer:
            self._timer.cancel()
        if self.fired.is_set():
            raise JobTimeout(f"run {self.tag} exceeded its time limit of {self.timeout_s}s and was interrupted",
                             ["Use fewer rows, or raise the limit with --timeout / TEAMSPARK_JOB_TIMEOUT."]) from exc
        return False
