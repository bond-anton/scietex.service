from typing import Union
from datetime import datetime, timezone
import logging


class CustomConsoleFormatter(logging.Formatter):
    """Console formatter for log records"""

    def __init__(
        self,
        service_name: str,
        worker_id: Union[int, None] = None,
        fmt: Union[str, None] = None,
        datefmt: Union[str, None] = None,
    ) -> None:
        if worker_id is None:
            worker_id = 1
        if fmt is None:
            fmt = "%(utc_timestamp)s - %(levelname)s - [%(worker_name)s] - %(message)s"
        super().__init__(fmt, datefmt)
        self.worker_name: str = f"{service_name}:{worker_id}"

    def format(self, record: logging.LogRecord) -> str:
        # Add the worker_id to the log record
        record.worker_name = self.worker_name

        record.utc_timestamp = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S %Z"
        )
        # Convert the log level to a 3-letter abbreviation
        record.levelname = self.level_abbreviation(record.levelno)

        # Call the parent class's format method to do the actual formatting
        return super().format(record)

    @staticmethod
    def level_abbreviation(log_level: int) -> str:
        """Map logging levels to 3-letter abbreviations"""
        level_map = {
            logging.DEBUG: "DBG",
            logging.INFO: "INF",
            logging.WARNING: "WRN",
            logging.ERROR: "ERR",
            logging.CRITICAL: "CRT",
            logging.FATAL: "FTL",
        }
        return level_map.get(log_level, f"{log_level:03d}")
