"""This module provides classes for implementation of simple daemons working in a background."""

from .async_tasks_processor import AsyncTaskProcessor
from .basic_async_worker import BasicAsyncWorker
from .version import __version__

__all__ = ["__version__", "BasicAsyncWorker", "AsyncTaskProcessor"]

try:
    from .valkey import (
        ValkeyAdvancedConfig,
        ValkeyBackoffStrategy,
        ValkeyBaseConfig,
        ValkeyConfig,
        ValkeyNode,
        ValkeyTlsAdvancedConfiguration,
        ValkeyUserCredentials,
        ValkeyWorker,
    )

    __all__ += [
        "ValkeyWorker",
        "ValkeyNode",
        "ValkeyUserCredentials",
        "ValkeyBackoffStrategy",
        "ValkeyBaseConfig",
        "ValkeyConfig",
        "ValkeyAdvancedConfig",
        "ValkeyTlsAdvancedConfiguration",
    ]
except Exception:
    # If importing Valkey support fails for any reason (missing glide,
    # runtime errors during module import, etc.), swallow the exception so
    # the package remains importable without Valkey installed.
    # We intentionally catch broad Exception because import-time errors
    # inside `scietex.service.valkey` (not only ImportError) should not
    # prevent the rest of the package from loading.
    pass
