"""Logging configuration for jobhist."""

import logging
import sys

# Default log format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Module-level cache for loggers
_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get a logger with standard configuration.

    Creates a logger with a console handler if one doesn't exist.
    Loggers are cached to avoid creating duplicate handlers.

    Args:
        name: Logger name (typically __name__ of the calling module)
        level: Logging level (default: INFO)

    Returns:
        Configured Logger instance
    """
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Only add handler if logger doesn't have one
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    _loggers[name] = logger
    return logger


def configure_logging(level: int = logging.INFO, format_str: str | None = None):
    """Configure root logging for the application.

    This sets up logging for all job_history modules.

    Args:
        level: Logging level for job_history loggers
        format_str: Custom format string (optional)
    """
    fmt = format_str or LOG_FORMAT
    logging.basicConfig(level=level, format=fmt, stream=sys.stderr)

    # Also configure job_history namespace
    qhist_logger = logging.getLogger("job_history")
    qhist_logger.setLevel(level)
