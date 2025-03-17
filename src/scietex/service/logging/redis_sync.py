from typing import Union
from datetime import datetime, timezone
import logging
import redis


class RedisSyncStreamHandler(logging.Handler):
    """Stream handler to send logs to Redis."""

    def __init__(
        self,
        worker_name: str,
        connection_conf: Union[dict, None] = None,
        stream_name: str = "worker_logs",
    ) -> None:
        super().__init__()
        self.worker_name: str = worker_name
        if connection_conf is None:
            connection_conf = {"host": "localhost", "port": 6379, "db": 0}
        self.redis_client: redis.client.Redis = redis.StrictRedis(
            **connection_conf, decode_responses=True
        )
        self.stream_name: str = stream_name

    def emit(self, record):
        """Emit log record to redis stream."""
        # pylint: disable=broad-exception-caught
        try:
            # Create log entry as a dictionary with the required fields
            log_entry = {
                "worker_name": self.worker_name,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "log_level": record.levelname,
                "log_message": record.getMessage(),
            }

            # Add the log entry to the Redis stream
            self.redis_client.xadd(self.stream_name, log_entry)

        except redis.exceptions.ConnectionError as conn_err:
            print(f"ConnectionError while logging to Redis: {conn_err}")

        except redis.exceptions.DataError as data_err:
            print(f"DataError while logging to Redis: {data_err}")

        except Exception as e:
            # Log any other exception types not anticipated but avoid masking the error completely
            print(f"Unexpected error while logging to Redis: {e}")


class RedisSyncLogFormatter(logging.Formatter):
    """
    Custom formatter to include worker name, timestamp (UTC), log level, and log message

    def format(self, record):
        return super().format(record)

    """
