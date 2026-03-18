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
except Exception:
    # If importing Valkey support fails for any reason (missing glide,
    # runtime errors during module import, etc.), swallow the exception so
    # the package remains importable without Valkey installed.
    # We intentionally catch broad Exception because import-time errors
    # inside `scietex.service.valkey` (not only ImportError) should not
    # prevent the rest of the package from loading.
    pass
