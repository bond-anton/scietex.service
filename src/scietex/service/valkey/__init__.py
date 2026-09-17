"""Valkey-backed async worker for ``scietex.service``.

Provides :class:`ValkeyWorker` (extends :class:`~scietex.service.task_processor.TaskProcessor`)
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
    - :class:`ValkeyPubSubConfig` — PubSub control-message subscription settings.
    - :class:`ValkeyNode` — Single server node definition.
    - :class:`ValkeyUserCredentials` — Authentication credentials.
    - :class:`ValkeyBackoffStrategy` — Reconnection backoff settings.
    - :class:`ValkeyTlsAdvancedConfiguration` — TLS settings.
    - :class:`PurgeResult` — Outcome of :func:`purge_task_stream` (entries
      purged and any errors).
    - :func:`purge_task_stream` — Standalone task-stream purge utility.

The YAML loader (:func:`read_valkey_config`) and the schema-to-glide converter
(:func:`generate_glide_config`) live in :mod:`scietex.service.valkey.config`
and are not re-exported here.
"""

from .config import (
    ValkeyAdvancedConfig,
    ValkeyBackoffStrategy,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyPubSubConfig,
    ValkeyTlsAdvancedConfiguration,
    ValkeyUserCredentials,
    ValkeyWorkerConfig,
)
from .purge import PurgeResult, purge_task_stream
from .worker import ValkeyWorker

__all__ = [
    "ValkeyNode",
    "ValkeyUserCredentials",
    "ValkeyBackoffStrategy",
    "ValkeyTlsAdvancedConfiguration",
    "ValkeyAdvancedConfig",
    "ValkeyPubSubConfig",
    "ValkeyBaseConfig",
    "ValkeyConfig",
    "ValkeyWorkerConfig",
    "ValkeyWorker",
    "PurgeResult",
    "purge_task_stream",
]
