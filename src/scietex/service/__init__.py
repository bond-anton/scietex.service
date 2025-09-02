"""This module provides classes for implementation of simple daemons working in a background."""

from .version import __version__

from .basic_async_worker import BasicAsyncWorker

try:
    from .redis_async_worker import RedisWorker
except ImportError:
    pass
try:
    from .valkey_async_worker import ValkeyWorker
except ImportError:
    pass
