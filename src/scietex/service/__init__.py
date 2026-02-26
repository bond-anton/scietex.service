"""This module provides classes for implementation of simple daemons working in a background."""

from .version import __version__
from .basic_async_worker import BasicAsyncWorker

__all__ = ["__version__", "BasicAsyncWorker"]

try:
    from .redis_async_worker import RedisWorker, RedisConfig

    __all__ += ["RedisWorker", "RedisConfig"]
except ImportError:
    pass
try:
    from .valkey_async_worker import (
        ValkeyWorker,
        ValkeyNode,
        ValkeyUserCredentials,
        ValkeyBackoffStrategy,
        ValkeyBaseConfig,
    )

    __all__ += [
        "ValkeyWorker",
        "ValkeyNode",
        "ValkeyUserCredentials",
        "ValkeyBackoffStrategy",
        "ValkeyBaseConfig",
    ]
except ImportError:
    pass
