"""Offline tests for settings, URL handling and naming (no server needed)."""
from __future__ import annotations

import pytest

from teamspark import ConfigError, Settings, normalize_name, parse_remote


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for v in ("TEAMSPARK_REMOTE", "SPARK_REMOTE", "TEAMSPARK_HOST", "TEAMSPARK_PORT", "TEAMSPARK_USER",
              "TEAMSPARK_JOB_TIMEOUT", "TEAMSPARK_MAX_ROWS", "TEAMSPARK_LOG_LEVEL"):
        monkeypatch.delenv(v, raising=False)


@pytest.mark.parametrize("url, expected", [
    ("sc://teamspark-mac:15002", ("teamspark-mac", 15002, {})),
    ("sc://100.101.102.103:15002/;user_id=alice", ("100.101.102.103", 15002, {"user_id": "alice"})),
    ("sc://host;user_id=bob", ("host", 15002, {"user_id": "bob"})),
    ("sc://[fd7a:115c::1]:15002/;use_ssl=true", ("fd7a:115c::1", 15002, {"use_ssl": "true"})),
])
def test_parse_remote(url, expected):
    assert parse_remote(url) == expected


@pytest.mark.parametrize("url", ["", "http://x:1", "sc://", "sc://host:abc"])
def test_parse_remote_rejects_bad_urls(url):
    with pytest.raises(ConfigError):
        parse_remote(url)


@pytest.mark.parametrize("raw, name", [("Alice", "alice"), ("bob.smith@corp", "bob_smith_corp"),
                                       ("  Carol Ann ", "carol_ann"), ("42", "u_42")])
def test_normalize_name(raw, name):
    assert normalize_name(raw) == name


def test_normalize_name_rejects_empty():
    with pytest.raises(ConfigError):
        normalize_name("---")


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("SPARK_REMOTE", "sc://teamspark-mac:15002/;user_id=ignored")
    monkeypatch.setenv("TEAMSPARK_USER", "Dana")
    s = Settings.load("Sales Pipeline")
    assert s.user == "dana" and s.pool == "dana" and s.database == "teamspark_dana"
    assert s.remote == "sc://teamspark-mac:15002/;user_id=dana"
    assert s.app_name.startswith("teamspark-dana-sales-pipeline-")


def test_settings_host_port_env_and_url_user(monkeypatch):
    monkeypatch.setenv("TEAMSPARK_HOST", "100.101.102.103")
    monkeypatch.setenv("TEAMSPARK_PORT", "15003")
    s = Settings.load("x")
    assert (s.host, s.port) == ("100.101.102.103", 15003)
    s2 = Settings.load("x", remote="sc://h:15002/;user_id=erin")
    assert s2.user == "erin"


def test_settings_require_a_server():
    with pytest.raises(ConfigError, match="no TeamSpark server"):
        Settings.load("x")


def test_settings_validate_limits(monkeypatch):
    monkeypatch.setenv("SPARK_REMOTE", "sc://h:15002")
    monkeypatch.setenv("TEAMSPARK_JOB_TIMEOUT", "soon")
    with pytest.raises(ConfigError):
        Settings.load("x")
