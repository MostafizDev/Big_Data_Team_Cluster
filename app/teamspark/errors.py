"""TeamSpark error types. Each carries a process exit code and user-facing hints."""
from __future__ import annotations


class TeamSparkError(Exception):
    """Base class: message for the log, hints for the user, exit code for scripts."""

    exit_code = 1

    def __init__(self, message: str, hints: list[str] | tuple[str, ...] = ()):
        super().__init__(message)
        self.hints = list(hints)


class ConfigError(TeamSparkError):
    """Missing or invalid configuration (server URL, user name, limits)."""

    exit_code = 2


class ConnectionFailed(TeamSparkError):
    """The Spark Connect server could not be reached or the session was lost."""

    exit_code = 3


class JobTimeout(TeamSparkError):
    """A job exceeded the configured time limit and was interrupted."""

    exit_code = 4


class JobFailed(TeamSparkError):
    """A Spark query or job failed on the server."""

    exit_code = 5
