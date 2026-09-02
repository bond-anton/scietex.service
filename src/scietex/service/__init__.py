"""scietex.service — Async worker framework for building background daemon services.

Core classes:
    - ``BasicAsyncWorker``: Base async worker with signal handling, logging,
      heartbeat and watchdog managers, and graceful shutdown support.
    - ``AsyncTaskProcessor``: Extends ``BasicAsyncWorker`` with a task queue,
      concurrent task processing, handler dispatch, and timeout monitoring.
    - ``ValkeyWorker``: Extends ``AsyncTaskProcessor`` with Valkey (Redis)
      integration via the ``glide`` client for distributed task queues.
      (Requires ``scietex.service[valkey]`` extra.)

Module-level exports:
    ``__version__``, ``BasicAsyncWorker``, ``AsyncTaskProcessor``, and
    optionally ``ValkeyWorker`` and its configuration classes.
"""

from .async_tasks_processor import AsyncTaskProcessor
from .basic_async_worker import BasicAsyncWorker
from .manager import Manager
from .version import __version__

__all__ = ["__version__", "AsyncTaskProcessor", "BasicAsyncWorker", "Manager"]

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
