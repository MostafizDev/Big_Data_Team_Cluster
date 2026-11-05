"""Logging for TeamSpark applications: console + rotating file, every line tagged with the run name."""
from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

from .config import Settings

LOGGER_NAME = "teamspark"
_FORMAT = "%(asctime)s %(levelname)-7s [%(app_name)s] %(name)s: %(message)s"


class _RunContext(logging.Filter):
    def __init__(self, app_name: str):
        super().__init__()
        self.app_name = app_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.app_name = self.app_name
        return True


def setup_logging(settings: Settings, *, to_file: bool = True) -> logging.Logger:
    """Configure the 'teamspark' logger once per process; returns it.

    Log file: <log_dir>/<app_name>.log (one file per run, rotated at 5 MB).
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(settings.log_level)
    logger.propagate = False
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()

    ctx = _RunContext(settings.app_name)
    fmt = logging.Formatter(_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S%z")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.addFilter(ctx)
    logger.addHandler(console)

    if to_file:
        log_dir = Path(settings.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            log_dir / f"{settings.app_name}.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        fh.addFilter(ctx)
        logger.addHandler(fh)
        logger.debug("logging to %s", fh.baseFilename)

    # The Spark Connect client logs retries etc. at INFO/DEBUG; keep it quiet unless debugging.
    logging.getLogger("pyspark").setLevel("DEBUG" if settings.log_level == "DEBUG" else "WARNING")
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    return logging.getLogger(f"{LOGGER_NAME}.{name}" if name else LOGGER_NAME)
