"""This module provides classes for implementation of simple daemons working in a background."""

from .version import __version__
from .basic_async_worker import BasicAsyncWorker
from .async_tasks_processor import AsyncTaskProcessor

__all__ = ["__version__", "BasicAsyncWorker", "AsyncTaskProcessor"]

try:
    from .valkey import (
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
