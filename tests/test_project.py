"""Offline consistency checks across Docker, server config, client setup and docs (no server needed)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import check_secrets  # noqa: E402


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def kv(rel: str) -> dict[str, str]:
    out = {}
    for line in read(rel).splitlines():
        line = line.split("#", 1)[0].strip()
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def pins(rel: str) -> dict[str, str]:
    out = {}
    for line in read(rel).splitlines():
        line = line.split("#", 1)[0].strip()
        if "==" in line:
            name, ver = line.split("==", 1)
            out[name.strip().lower()] = ver.strip()
    return out


CLIENT = kv("setup/teamspark-client.conf")
ENV = kv(".env.example")
SPARK = CLIENT["TEAMSPARK_SPARK_VERSION"]
PY = CLIENT["TEAMSPARK_PYTHON_VERSION"]


def test_spark_version_is_consistent_everywhere():
    dockerfile = read("Dockerfile")
    assert f"apache/spark:{SPARK}-" in dockerfile, "Dockerfile base image must match client Spark version"
    assert ENV["TEAMSPARK_VERSION"].startswith(f"{SPARK}-py{PY}-"), ".env.example image tag"
    assert f"{SPARK}-py{PY}-" in read("docker-compose.yml"), "docker-compose.yml default image tag"
    assert pins("setup/requirements.txt")["pyspark-client"] == SPARK
    assert f'EXPECTED_SPARK = "{SPARK}"' in read("scripts/smoke_test.py")


def test_python_version_is_consistent():
    m = re.search(r"ARG PYTHON_VERSION=(\d+\.\d+)\.\d+", read("Dockerfile"))
    assert m and m.group(1) == PY, "server venv Python must equal the client Python minor version"
    assert f"py{PY}" in ENV["TEAMSPARK_VERSION"]
    assert sys.version_info[:2] == tuple(map(int, PY.split("."))), \
        f"run the tests with Python {PY} (the TeamSpark venv)"


def test_java_and_uv_pins():
    dockerfile = read("Dockerfile")
    assert "-java21-" in dockerfile
    assert f"ghcr.io/astral-sh/uv:{CLIENT['TEAMSPARK_UV_VERSION']}" in dockerfile, "same uv on server and clients"


def test_single_dependency_lock():
    assert "COPY setup/requirements.txt" in read("Dockerfile"), "image must install the client lock"
    assert "-r setup/requirements.txt" in read("requirements.txt")
    lock = pins("setup/requirements.txt")
    assert len(lock) >= 10 and all(re.fullmatch(r"[\w.]+", v) for v in lock.values())
    dev = pins("requirements.txt")
    assert not set(dev) & set(lock), "root requirements.txt must not re-pin lock packages"


def test_port_is_consistent():
    port = CLIENT["TEAMSPARK_DEFAULT_PORT"]
    assert ENV["CONNECT_PORT"] == port
    assert re.search(rf"spark\.connect\.grpc\.binding\.port\s+{port}", read("conf/spark-defaults.conf"))
    assert f"DEFAULT_PORT = {port}" in read("app/teamspark/config.py")


def test_cluster_cores_match_worker_count():
    compose = read("docker-compose.yml")
    workers = len(re.findall(r"^  spark-worker-\d+:", compose, re.M))
    assert workers >= 2
    assert int(ENV["SPARK_CORES_MAX"]) == workers * int(ENV["SPARK_WORKER_CORES"]), \
        "SPARK_CORES_MAX should equal workers x SPARK_WORKER_CORES"
    assert ENV["SPARK_EXECUTOR_CORES"] == ENV["SPARK_WORKER_CORES"]


def test_internal_ports_are_not_published():
    compose = read("docker-compose.yml")
    published = re.findall(r'^\s+- "([^"]+)"', compose, re.M)
    for p in published:
        assert p.startswith(("127.0.0.1:", "${CONNECT_BIND_IP}:")), f"port published on all interfaces: {p}"
    for internal in ("7077", "7078", "7079", "8081"):
        assert not any(p.endswith(f":{internal}") for p in published), f"internal port {internal} is published"
    assert ENV["CONNECT_BIND_IP"] == "127.0.0.1", ".env.example must default to localhost only"


def test_fair_scheduling_configured():
    conf = read("conf/spark-defaults.conf")
    assert re.search(r"spark\.scheduler\.mode\s+FAIR", conf)
    assert "fairscheduler.xml" in conf and (ROOT / "conf/fairscheduler.xml").exists()


@pytest.mark.parametrize("pattern", [".env", "storage/", "logs/", "__pycache__/"])
def test_gitignore(pattern):
    assert pattern in read(".gitignore").splitlines()


def test_gitattributes_keep_lf_for_shell():
    attrs = read(".gitattributes")
    assert re.search(r"^\*\.sh\s+text eol=lf", attrs, re.M)
    assert re.search(r"^Dockerfile\s+text eol=lf", attrs, re.M)


def test_no_credentials_or_private_config_committable():
    findings, n = check_secrets.scan()
    assert n > 20
    assert not findings, "\n".join(findings)


@pytest.mark.parametrize("sample", [  # fake values; lines opt out of the scan with the marker
    "tskey-auth-kAbCdEf1CNTRL-AbCdEfGhIjKlMnOpQrStUv",  # secrets-ok
    "-----BEGIN OPENSSH PRIVATE KEY-----",  # secrets-ok
    "sc://h:15002/;token=abcdef123456",  # secrets-ok
    "password = 'hunter2hunter2'",  # secrets-ok
    "server at 100.88.12.34",  # secrets-ok
    "box.tail1234.ts.net",  # secrets-ok
])
def test_secret_scanner_detects(sample):
    assert any(rx.search(sample) for _, rx in check_secrets.PATTERNS)
