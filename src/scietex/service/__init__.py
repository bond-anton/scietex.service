"""This module provides classes for implementation of simple daemons working in a background."""

__version__ = "0.0.2"

from .basic_async_worker import BasicAsyncWorker
from .redis_async_worker import RedisConfig, RedisWorker
