"""TeamSpark client library: settings, logging, sessions and the sample pipeline."""
from .config import Settings, normalize_name, parse_remote
from .errors import ConfigError, ConnectionFailed, JobFailed, JobTimeout, TeamSparkError
from .logging_setup import get_logger, setup_logging
from .session import JobWatchdog, preflight, teamspark_session, translate_error

__all__ = [
    "Settings", "normalize_name", "parse_remote",
    "TeamSparkError", "ConfigError", "ConnectionFailed", "JobFailed", "JobTimeout",
    "setup_logging", "get_logger",
    "teamspark_session", "JobWatchdog", "preflight", "translate_error",
]
