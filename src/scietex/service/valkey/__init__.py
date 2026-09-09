"""Valkey-backed async worker for ``scietex.service``.

Provides :class:`ValkeyWorker` (extends :class:`~scietex.service.task_processor.AsyncTaskProcessor`)
for task processing backed by a Valkey/Redis stream, along with
configuration schemas (:mod:`config`) and data
schemas (:mod:`schemas`).

Requires the optional ``valkey-glide`` dependency.

Public exports:
    - :class:`ValkeyWorker` — Async worker with Valkey stream support.
    - :class:`ValkeyWorkerConfig` — Worker configuration (service identity,
      task queue, and Valkey-specific fields).
    - :class:`ValkeyConfig` — Top-level configuration schema.
    - :class:`ValkeyBaseConfig` — Basic connection settings.
    - :class:`ValkeyAdvancedConfig` — Advanced connection settings.
    - :class:`ValkeyNode` — Single server node definition.
    - :class:`ValkeyUserCredentials` — Authentication credentials.
    - :class:`ValkeyBackoffStrategy` — Reconnection backoff settings.
    - :class:`ValkeyTlsAdvancedConfiguration` — TLS settings.
    - :func:`read_valkey_config` — YAML config loader.
    - :func:`generate_glide_config` — Schema-to-glide converter.
    - :func:`purge_task_stream` — Standalone task-stream purge utility.
"""

from .config import (
    ValkeyAdvancedConfig,
    ValkeyBackoffStrategy,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyTlsAdvancedConfiguration,
    ValkeyUserCredentials,
    ValkeyWorkerConfig,
)
from .purge import purge_task_stream
from .worker import ValkeyWorker

__all__ = [
    "ValkeyNode",
    "ValkeyUserCredentials",
    "ValkeyBackoffStrategy",
    "ValkeyTlsAdvancedConfiguration",
    "ValkeyAdvancedConfig",
    "ValkeyBaseConfig",
    "ValkeyConfig",
    "ValkeyWorkerConfig",
    "ValkeyWorker",
    "purge_task_stream",
]
