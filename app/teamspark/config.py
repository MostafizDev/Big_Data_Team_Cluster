"""TeamSpark client settings: server URL, team-member identity, unique run names, limits.

Everything comes from environment variables (written by setup/setup.sh or
setup/setup.ps1) and can be overridden per call:

    TEAMSPARK_REMOTE / SPARK_REMOTE   sc://host:15002[/;user_id=alice]
    TEAMSPARK_HOST + TEAMSPARK_PORT   alternative to a full URL
    TEAMSPARK_USER                    team-member name (default: OS login)
    TEAMSPARK_JOB_TIMEOUT             seconds before a run is interrupted (default 900)
    TEAMSPARK_MAX_ROWS                upper bound for generated/processed rows (default 50M)
    TEAMSPARK_LOG_LEVEL               DEBUG | INFO | WARNING (default INFO)
    TEAMSPARK_LOG_DIR                 directory for log files (default ./logs)
"""
from __future__ import annotations

import getpass
import os
import re
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .errors import ConfigError

DEFAULT_PORT = 15002
DEFAULT_TIMEOUT_S = 900
DEFAULT_MAX_ROWS = 50_000_000

_USER_BAD_CHARS = re.compile(r"[^a-z0-9_]+")
_APP_BAD_CHARS = re.compile(r"[^a-z0-9_-]+")


def normalize_name(raw: str, *, what: str = "user name", max_len: int = 32) -> str:
    """Lower-case, [a-z0-9_] only: safe as a pool name, tag, and database suffix."""
    name = _USER_BAD_CHARS.sub("_", raw.strip().lower()).strip("_")[:max_len]
    if not name:
        raise ConfigError(f"invalid {what}: {raw!r}",
                          ["Use letters, digits and '_' (e.g. 'alice' or 'bob_smith')."])
    if name[0].isdigit():
        name = f"u_{name}"[:max_len]
    return name


def parse_remote(url: str) -> tuple[str, int, dict[str, str]]:
    """Split sc://host:port/;k=v;k2=v2 into (host, port, params)."""
    if not url or not url.startswith("sc://"):
        raise ConfigError(f"invalid Spark Connect URL: {url!r}",
                          ["It must look like sc://<host>:<port>, e.g. sc://teamspark-mac:15002"])
    rest = url[len("sc://"):]
    hostport, _, tail = rest.partition("/")
    if ";" in hostport:  # tolerate sc://host:port;k=v
        hostport, _, extra = hostport.partition(";")
        tail = f";{extra}"
    if hostport.startswith("["):
        host, _, port_s = hostport[1:].partition("]")
        port_s = port_s.lstrip(":")
    else:
        host, _, port_s = hostport.partition(":")
    if not host or (port_s and not port_s.isdigit()):
        raise ConfigError(f"invalid Spark Connect URL: {url!r}", ["Expected sc://<host>:<port>"])
    params = {}
    for part in tail.lstrip(";").split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            params[k.strip()] = v.strip()
    return host, int(port_s or DEFAULT_PORT), params


def build_remote(host: str, port: int, params: dict[str, str]) -> str:
    h = f"[{host}]" if ":" in host else host
    url = f"sc://{h}:{port}"
    if params:
        url += "/;" + ";".join(f"{k}={v}" for k, v in params.items())
    return url


def _env_remote() -> str | None:
    for var in ("TEAMSPARK_REMOTE", "SPARK_REMOTE"):
        if os.environ.get(var):
            return os.environ[var]
    if os.environ.get("TEAMSPARK_HOST"):
        return f"sc://{os.environ['TEAMSPARK_HOST']}:{os.environ.get('TEAMSPARK_PORT') or DEFAULT_PORT}"
    return None


def _env_int(var: str, default: int) -> int:
    raw = os.environ.get(var)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{var} must be an integer, got {raw!r}") from None
    if value <= 0:
        raise ConfigError(f"{var} must be positive, got {value}")
    return value


def new_run_id() -> str:
    """UTC timestamp + random suffix: unique even when one member starts runs in parallel."""
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


@dataclass(frozen=True)
class Settings:
    remote: str                 # full URL incl. user_id, e.g. sc://teamspark-mac:15002/;user_id=alice
    host: str
    port: int
    user: str                   # normalized team-member name
    app: str                    # logical application name, e.g. "sales-pipeline"
    run_id: str = field(default_factory=new_run_id)
    job_timeout_s: int = DEFAULT_TIMEOUT_S
    max_rows: int = DEFAULT_MAX_ROWS
    log_level: str = "INFO"
    log_dir: str = "logs"

    @property
    def app_name(self) -> str:
        """Unique, human-readable run name: teamspark-<user>-<app>-<run_id>.

        Shown as the job description in the Spark UI, used as the session tag
        (for interrupting this run only) and recorded in output tables.
        """
        return f"teamspark-{self.user}-{self.app}-{self.run_id}".lower()

    @property
    def pool(self) -> str:
        """FAIR scheduler pool: one per team member."""
        return self.user

    @property
    def database(self) -> str:
        """Per-member database so members never overwrite each other's tables."""
        return f"teamspark_{self.user}"

    @classmethod
    def load(cls, app: str, *, remote: str | None = None, user: str | None = None,
             job_timeout_s: int | None = None, max_rows: int | None = None,
             log_level: str | None = None) -> "Settings":
        url = remote or _env_remote()
        if not url:
            raise ConfigError("no TeamSpark server configured",
                              ["Run setup/setup.sh (macOS) or setup\\setup.ps1 (Windows),",
                               "or set SPARK_REMOTE=sc://<server>:15002, or pass --remote."])
        host, port, params = parse_remote(url)
        raw_user = (user or os.environ.get("TEAMSPARK_USER") or params.get("user_id")
                    or getpass.getuser())
        member = normalize_name(raw_user)
        params["user_id"] = member  # recorded by the server on every job of this session

        app_norm = _APP_BAD_CHARS.sub("-", app.strip().lower()).strip("-")[:40]
        if not app_norm:
            raise ConfigError(f"invalid application name: {app!r}")

        level = (log_level or os.environ.get("TEAMSPARK_LOG_LEVEL") or "INFO").upper()
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            raise ConfigError(f"invalid log level {level!r}", ["Use DEBUG, INFO, WARNING or ERROR."])

        return cls(
            remote=build_remote(host, port, params),
            host=host,
            port=port,
            user=member,
            app=app_norm,
            job_timeout_s=job_timeout_s or _env_int("TEAMSPARK_JOB_TIMEOUT", DEFAULT_TIMEOUT_S),
            max_rows=max_rows or _env_int("TEAMSPARK_MAX_ROWS", DEFAULT_MAX_ROWS),
            log_level=level,
            log_dir=os.environ.get("TEAMSPARK_LOG_DIR", "logs"),
        )
