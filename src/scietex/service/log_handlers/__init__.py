"""Logging utilities for ``scietex.service``.

Provides ``LoggerStatus`` for tracking async handler states and
``parse_logging_level()`` for converting string or integer logging
level specifications to ``logging`` module constants.
"""

import logging
from enum import Enum

DEFAULT_LOGGING_LEVEL: int = logging.DEBUG


class LoggerStatus(Enum):
    """Lifecycle states of an async logging handler."""

    #: The handler is not running.
    STOPPED = "Stopped"
    #: The handler is actively processing log messages.
    RUNNING = "Running"
    #: The handler failed to start and will be retried.
    FAILED = "Failed"


def parse_logging_level(level: int | str | None) -> int:
    """Parse a logging level specification into a ``logging`` constant.

    ``None`` or an unrecognized value falls back to
    ``DEFAULT_LOGGING_LEVEL``.

    Args:
        level: Logging level as a string or integer. Supported string values:
            - DEBUG: ``'D'``, ``'DBG'``, ``'DEBUG'``, ``logging.DEBUG``
            - INFO: ``'I'``, ``'INF'``, ``'INFO'``, ``'INFORMATION'``, ``logging.INFO``
            - WARNING: ``'W'``, ``'WRN'``, ``'WARN'``, ``'WARNING'``, ``logging.WARNING``
            - ERROR: ``'E'``, ``'ERR'``, ``'ERROR'``, ``logging.ERROR``
            - CRITICAL: ``'C'``, ``'CRT'``, ``'CRIT'``, ``'CRITICAL'``, ``logging.CRITICAL``
            - FATAL: ``'F'``, ``'FTL'``, ``'FAT'``, ``'FATAL'``, ``logging.FATAL``
    """
    if level in ("D", "DBG", "DEBUG", logging.DEBUG):
        logging_level = logging.DEBUG
    elif level in ("I", "INF", "INFO", "INFORMATION", logging.INFO):
        logging_level = logging.INFO
    elif level in ("W", "WRN", "WARN", "WARNING", logging.WARNING):
        logging_level = logging.WARNING
    elif level in ("E", "ERR", "ERROR", logging.ERROR):
        logging_level = logging.ERROR
    elif level in ("C", "CRT", "CRIT", "CRITICAL", logging.CRITICAL):
        logging_level = logging.CRITICAL
    elif level in ("F", "FTL", "FAT", "FATAL", logging.FATAL):
        logging_level = logging.FATAL
    else:
        logging_level = DEFAULT_LOGGING_LEVEL
    return logging_level


__all__ = [
    "DEFAULT_LOGGING_LEVEL",
    "LoggerStatus",
    "parse_logging_level",
]
