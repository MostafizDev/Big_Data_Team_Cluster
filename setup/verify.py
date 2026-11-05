#!/usr/bin/env python3
"""TeamSpark client verification (macOS + Windows).

Checks, in order, and stops at the first failure with a hint:
  1. Python version and installed packages match the shared lock
  2. Remote URL is configured (SPARK_REMOTE / TEAMSPARK_REMOTE / TEAMSPARK_HOST)
  3. Tailscale status (when the server address is a tailnet address)
  4. DNS -> TCP -> gRPC reachability of the Spark Connect server
  5. Spark session + server version
  6. A simple DataFrame job executed on the cluster (+ optional Python UDF)

Usage:
    python verify.py                         # uses SPARK_REMOTE etc.
    python verify.py --remote sc://teamspark-mac:15002
Exit codes: 0 = success, 1 = check failed, 2 = configuration error.
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
USE_COLOR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None
if USE_COLOR and os.name == "nt":
    os.system("")  # enables ANSI escape processing in Windows consoles


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text


def ok(msg: str) -> None:
    print(f"  {_c('32', '[ OK ]')} {msg}")


def warn(msg: str) -> None:
    print(f"  {_c('33', '[WARN]')} {msg}")


def info(msg: str) -> None:
    print(f"  [INFO] {msg}")


class CheckFailed(Exception):
    def __init__(self, msg: str, hints: list[str] | None = None, code: int = 1):
        super().__init__(msg)
        self.hints = hints or []
        self.code = code


# --------------------------------------------------------------------------- config
def load_conf() -> dict[str, str]:
    conf: dict[str, str] = {}
    path = HERE / "teamspark-client.conf"
    if not path.exists():
        raise CheckFailed(f"missing {path}", ["Re-run the setup script."], code=2)
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            conf[k.strip()] = v.strip()
    return conf


def resolve_remote(cli: str | None, default_port: str) -> tuple[str, str]:
    """Return (url, source)."""
    if cli:
        return cli, "--remote"
    for var in ("TEAMSPARK_REMOTE", "SPARK_REMOTE"):
        if os.environ.get(var):
            return os.environ[var], var
    host = os.environ.get("TEAMSPARK_HOST")
    if host:
        port = os.environ.get("TEAMSPARK_PORT") or default_port
        return f"sc://{host}:{port}", "TEAMSPARK_HOST/TEAMSPARK_PORT"
    raise CheckFailed(
        "no Spark Connect server configured",
        ["Run the setup script with the server host, e.g. './setup.sh --host teamspark-mac'",
         "or set SPARK_REMOTE=sc://<server>:15002, or pass --remote."],
        code=2,
    )


def parse_remote(url: str, default_port: str) -> tuple[str, int]:
    if not url.startswith("sc://"):
        raise CheckFailed(f"invalid remote URL '{url}'",
                          ["It must look like sc://<host>:<port>, e.g. sc://teamspark-mac:15002"], code=2)
    hostport = url[5:].split("/", 1)[0].split(";", 1)[0]
    if hostport.startswith("["):  # [ipv6]:port
        host, _, rest = hostport[1:].partition("]")
        port = rest.lstrip(":") or default_port
    else:
        host, _, port = hostport.partition(":")
        port = port or default_port
    if not host or not port.isdigit():
        raise CheckFailed(f"invalid remote URL '{url}'", ["Expected sc://<host>:<port>"], code=2)
    return host, int(port)


def is_tailnet_address(host: str) -> bool:
    try:
        return ipaddress.ip_address(host) in ipaddress.ip_network("100.64.0.0/10")
    except ValueError:
        h = host.lower().rstrip(".")
        # *.ts.net = MagicDNS FQDN; a bare dot-less name is usually a MagicDNS short name.
        return h.endswith(".ts.net") or ("." not in h and h != "localhost")


# --------------------------------------------------------------------------- checks
def check_python(conf: dict[str, str]) -> bool:
    want = conf["TEAMSPARK_PYTHON_VERSION"]
    have = f"{sys.version_info.major}.{sys.version_info.minor}"
    if have == want:
        ok(f"Python {sys.version.split()[0]} ({sys.executable})")
        return True
    warn(f"Python {have} != server Python {want}: DataFrame/SQL work, Python UDFs will fail")
    return False


def check_packages(conf: dict[str, str]) -> None:
    from importlib import metadata

    req = HERE / "requirements.txt"
    pins = []
    for line in req.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if "==" in line:
            name, ver = line.split("==", 1)
            pins.append((name.strip(), ver.strip()))
    bad = []
    for name, ver in pins:
        try:
            got = metadata.version(name)
        except metadata.PackageNotFoundError:
            got = "missing"
        if got != ver:
            bad.append(f"{name}: installed {got}, expected {ver}")
    if bad:
        raise CheckFailed("installed packages do not match setup/requirements.txt",
                          bad + ["Re-run the setup script with --force to rebuild the environment."])
    ok(f"{len(pins)} pinned packages match the lock (pyspark-client {metadata.version('pyspark-client')})")

    # Guard against a full local Spark: only the thin Connect client is allowed.
    try:
        full = metadata.version("pyspark")
        raise CheckFailed(f"full 'pyspark' {full} is installed in this environment",
                          ["It ships a local Spark/JVM and conflicts with pyspark-client.",
                           "Re-run the setup script with --force."])
    except metadata.PackageNotFoundError:
        pass
    import pyspark

    if (Path(pyspark.__file__).parent / "jars").exists():
        raise CheckFailed("pyspark in this environment bundles Spark jars (local Spark)",
                          ["Re-run the setup script with --force."])
    ok("client-only install: no local Spark, no Java required")


def find_tailscale() -> str | None:
    candidates = [shutil.which("tailscale"),
                  "/Applications/Tailscale.app/Contents/MacOS/Tailscale",
                  r"C:\Program Files\Tailscale\tailscale.exe"]
    return next((c for c in candidates if c and Path(c).exists()), None)


def check_tailscale(host: str) -> None:
    tailnet = is_tailnet_address(host)
    exe = find_tailscale()
    if not exe:
        if tailnet:
            warn(f"'{host}' looks like a Tailscale address but the Tailscale CLI was not found; "
                 "make sure Tailscale is installed and connected")
        else:
            info("Tailscale CLI not found (fine if the server is reachable directly)")
        return
    try:
        out = subprocess.run([exe, "status", "--json"], capture_output=True, text=True, timeout=10)
        status = json.loads(out.stdout or "{}")
    except (OSError, subprocess.SubprocessError, ValueError) as e:
        warn(f"could not read Tailscale status: {e}")
        return
    state = status.get("BackendState", "unknown")
    if state == "Running":
        me = (status.get("Self") or {}).get("DNSName", "").rstrip(".")
        ok(f"Tailscale connected{f' as {me}' if me else ''}")
    elif tailnet:
        raise CheckFailed(f"Tailscale is not connected (state: {state})",
                          ["Open the Tailscale app and log in / connect, then re-run verify.",
                           "CLI: 'tailscale up'"])
    else:
        info(f"Tailscale state: {state} (server address is not a tailnet address)")


def check_network(host: str, port: int, timeout: float) -> None:
    tailnet = is_tailnet_address(host)
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        addrs = sorted({a[4][0] for a in infos})
        # Many hosts resolve to the same address twice (e.g. per protocol); try each once.
        infos = list({a[4]: a for a in infos}.values())
    except socket.gaierror as e:
        hints = ["Check the host name for typos."]
        if tailnet:
            hints += ["For Tailscale names: make sure Tailscale is connected and MagicDNS is enabled",
                      "(admin console -> DNS), or use the server's 100.x.y.z IP instead."]
        raise CheckFailed(f"cannot resolve '{host}': {e}", hints)
    ok(f"DNS {host} -> {', '.join(addrs)}")

    # Try every resolved address (IPv4 and IPv6) and report the most telling error:
    # "refused" on any address beats "unreachable"/"timeout" on another.
    t0 = time.time()
    errors: list[OSError] = []
    for family, stype, proto, _, sockaddr in infos:
        s = socket.socket(family, stype, proto)
        s.settimeout(timeout)
        try:
            s.connect(sockaddr)
            errors = []
            break
        except OSError as e:
            errors.append(e)
        finally:
            s.close()
    try:
        if errors:
            raise next((e for e in errors if isinstance(e, ConnectionRefusedError)),
                       next((e for e in errors if isinstance(e, (socket.timeout, TimeoutError))), errors[0]))
    except ConnectionRefusedError:
        raise CheckFailed(f"connection refused by {host}:{port}",
                          ["The server machine is reachable but nothing accepts on this port:",
                           "- the TeamSpark server may be stopped ('docker compose ps' on the server)",
                           "- or Spark Connect is not exposed to the tailnet "
                           "('tailscale serve status' on the server, see README)",
                           "- or the port is wrong."])
    except (socket.timeout, TimeoutError):
        raise CheckFailed(f"timed out connecting to {host}:{port} after {timeout:.0f}s",
                          ["No response from the server:",
                           "- this machine or the server is not connected to Tailscale",
                           "- a Tailscale ACL does not allow you to reach tcp:" + str(port),
                           "- the server Mac is asleep or offline, or the address is wrong."])
    except OSError as e:
        raise CheckFailed(f"cannot connect to {host}:{port}: {e}",
                          ["Check network/VPN connectivity (Tailscale) and the server address."])
    ok(f"TCP {host}:{port} reachable ({(time.time() - t0) * 1000:.0f} ms)")


def check_grpc(url: str, host: str, port: int, timeout: float) -> None:
    if "use_ssl=true" in url.lower():
        info("TLS URL: skipping plain gRPC probe")
        return
    import grpc

    target = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    channel = grpc.insecure_channel(target)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout)
    except grpc.FutureTimeoutError:
        raise CheckFailed(f"{host}:{port} accepts TCP but does not speak gRPC",
                          ["Something other than Spark Connect is listening on this port,",
                           "or the Spark Connect server is still starting. Wait 1 minute and retry."])
    finally:
        channel.close()
    ok("gRPC handshake with Spark Connect")


def check_spark(url: str, conf: dict[str, str], run_udf: bool):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    try:
        spark = SparkSession.builder.remote(url).getOrCreate()
        server = spark.version
    except Exception as e:  # noqa: BLE001
        raise CheckFailed(f"Spark session failed: {type(e).__name__}: {str(e).splitlines()[0][:300]}",
                          ["The server is reachable but rejected the session.",
                           "Check the server logs: 'docker compose logs spark-connect'."])
    try:
        want = conf["TEAMSPARK_SPARK_VERSION"]
        if server.split(".")[:2] != want.split(".")[:2]:
            raise CheckFailed(f"server runs Spark {server}, this client is built for {want}",
                              ["Client and server Spark minor versions must match.",
                               "Update setup/teamspark-client.conf + requirements.txt to the server version."])
        (ok if server == want else warn)(f"Spark session open, server Spark {server}")

        t0 = time.time()
        rows = [("alice", "eng", 120), ("bob", "eng", 100), ("carol", "ops", 90),
                ("dan", "ops", 110), ("erin", "sales", 95)]
        df = spark.createDataFrame(rows, "name string, team string, score int")
        result = (df.groupBy("team")
                    .agg(F.count("*").alias("members"), F.avg("score").alias("avg_score"))
                    .orderBy("team").collect())
        got = [(r.team, r.members, float(r.avg_score)) for r in result]
        expected = [("eng", 2, 110.0), ("ops", 2, 100.0), ("sales", 1, 95.0)]
        if got != expected:
            raise CheckFailed(f"DataFrame groupBy returned {got}, expected {expected}")
        total = spark.range(0, 1_000_000, numPartitions=8).agg(F.sum("id")).first()[0]
        if total != 499_999_500_000:
            raise CheckFailed(f"distributed sum returned {total}")
        ok(f"DataFrame groupBy/agg + distributed sum over 8 partitions ({time.time() - t0:.1f}s)")
        print()
        df.groupBy("team").agg(F.count("*").alias("members"), F.avg("score").alias("avg_score")) \
          .orderBy("team").show()

        if run_udf:
            @F.udf("string")
            def where(_):
                import socket as s
                import sys as y
                return f"{s.gethostname()} (py{y.version_info.major}.{y.version_info.minor})"

            seen = sorted({r.w for r in spark.range(0, 64, numPartitions=16)
                           .select(where("id").alias("w")).distinct().collect()})
            ok(f"Python UDF executed on cluster workers: {', '.join(seen)}")
        else:
            info("Python UDF check skipped")
    finally:
        spark.stop()


# --------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description="Verify the TeamSpark client connection.")
    ap.add_argument("--remote", help="Spark Connect URL, e.g. sc://teamspark-mac:15002")
    ap.add_argument("--timeout", type=float, default=10.0, help="network timeout in seconds")
    ap.add_argument("--skip-udf", action="store_true", help="skip the Python UDF check")
    args = ap.parse_args()

    print("TeamSpark client verification")
    print("=" * 40)
    try:
        conf = load_conf()
        py_match = check_python(conf)
        check_packages(conf)
        url, source = resolve_remote(args.remote, conf["TEAMSPARK_DEFAULT_PORT"])
        host, port = parse_remote(url, conf["TEAMSPARK_DEFAULT_PORT"])
        ok(f"server {url} (from {source})")
        member = next((p.split("=", 1)[1] for p in url.split(";") if p.startswith("user_id=")), None)
        if member:
            ok(f"team member: {member}")
        else:
            warn("no user_id in the URL: your jobs will not be attributed to you "
                 "(re-run setup with --user / -User)")
        check_tailscale(host)
        check_network(host, port, args.timeout)
        check_grpc(url, host, port, args.timeout)
        check_spark(url, conf, run_udf=py_match and not args.skip_udf)
    except CheckFailed as e:
        print(f"  {_c('31', '[FAIL]')} {e}")
        for h in e.hints:
            print(f"         {h}")
        print(f"\n{_c('31', 'ERROR')}: TeamSpark verification failed.")
        return e.code
    except KeyboardInterrupt:
        print("\ninterrupted")
        return 130

    print(f"{_c('32', 'SUCCESS')}: connected to TeamSpark at {url}")
    print("In Python:  from pyspark.sql import SparkSession; "
          "spark = SparkSession.builder.getOrCreate()   # uses SPARK_REMOTE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
