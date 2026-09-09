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

The ``VALKEY_AVAILABLE`` flag reports whether the Valkey surface could be
imported at package load time.
"""

import logging

from .basic_worker import BasicAsyncWorker
from .config import TaskProcessorConfig, WorkerConfig
from .manager import Manager
from .task_processor import AsyncTaskProcessor
from .version import __version__

__all__ = [
    "__version__",
    "AsyncTaskProcessor",
    "BasicAsyncWorker",
    "Manager",
    "TaskProcessorConfig",
    "WorkerConfig",
]

VALKEY_AVAILABLE = False
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
        ValkeyWorkerConfig,
    )

    VALKEY_AVAILABLE = True

    __all__ += [
        "ValkeyWorker",
        "ValkeyNode",
        "ValkeyUserCredentials",
        "ValkeyBackoffStrategy",
        "ValkeyBaseConfig",
        "ValkeyConfig",
        "ValkeyAdvancedConfig",
        "ValkeyTlsAdvancedConfiguration",
        "ValkeyWorkerConfig",
    ]
except ImportError:
    # If the Valkey dependency (glide) is missing, swallow the ImportError so
    # the package remains importable without the valkey extra installed.
    # Real bugs in the valkey module or a broken glide install must not be
    # hidden: they raise non-ImportError exceptions that propagate.
    logging.getLogger(__name__).warning(
        "Valkey support unavailable: install the 'valkey' extra (scietex.service[valkey]) to enable ValkeyWorker."
    )
