"""Shared fixtures. Server tests need SPARK_REMOTE (or TEAMSPARK_REMOTE / TEAMSPARK_HOST).

    SPARK_REMOTE=sc://localhost:15002 pytest              # on the server Mac
    pytest                                                # on a client after setup (SPARK_REMOTE is set)
    pytest -m "not server"                                # offline checks only
"""
from __future__ import annotations

import getpass
import os
from pathlib import Path

import pytest

from teamspark import Settings, normalize_name, teamspark_session

ROOT = Path(__file__).resolve().parent.parent


def _server_configured() -> bool:
    return any(os.environ.get(v) for v in ("TEAMSPARK_REMOTE", "SPARK_REMOTE", "TEAMSPARK_HOST"))


def pytest_collection_modifyitems(config, items):
    if _server_configured():
        return
    skip = pytest.mark.skip(reason="no TeamSpark server configured (set SPARK_REMOTE=sc://<server>:15002)")
    for item in items:
        if "server" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def test_user() -> str:
    """Tests run as pytest_<login> so they never touch a real member's tables."""
    return normalize_name(f"pytest_{os.environ.get('TEAMSPARK_USER') or getpass.getuser()}")


@pytest.fixture(scope="session")
def settings(test_user, tmp_path_factory) -> Settings:
    os.environ.setdefault("TEAMSPARK_LOG_DIR", str(tmp_path_factory.mktemp("logs")))
    return Settings.load("pytest", user=test_user, job_timeout_s=300)


@pytest.fixture(scope="session")
def spark(settings):
    with teamspark_session(settings) as session:
        yield session
