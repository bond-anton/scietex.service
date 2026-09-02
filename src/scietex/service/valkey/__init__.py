"""Valkey-backed async worker for ``scietex.service``.

Provides :class:`ValkeyWorker` (extends :class:`~scietex.service.async_tasks_processor.AsyncTaskProcessor`)
for task processing backed by a Valkey/Redis stream, along with
configuration schemas (:mod:`valkey_config`) and data
schemas (:mod:`schemas`).

Requires the optional ``valkey-glide`` dependency.

Public exports:
    - :class:`ValkeyWorker` — Async worker with Valkey stream support.
    - :class:`ValkeyConfig` — Top-level configuration schema.
    - :class:`ValkeyBaseConfig` — Basic connection settings.
    - :class:`ValkeyAdvancedConfig` — Advanced connection settings.
    - :class:`ValkeyNode` — Single server node definition.
    - :class:`ValkeyUserCredentials` — Authentication credentials.
    - :class:`ValkeyBackoffStrategy` — Reconnection backoff settings.
    - :class:`ValkeyTlsAdvancedConfiguration` — TLS settings.
    - :func:`read_valkey_config` — YAML config loader.
    - :func:`generate_glide_config` — Schema-to-glide converter.
"""

from .valkey_async_worker import ValkeyWorker
from .valkey_config import (
    ValkeyAdvancedConfig,
    ValkeyBackoffStrategy,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyTlsAdvancedConfiguration,
    ValkeyUserCredentials,
)

__all__ = [
    "ValkeyNode",
    "ValkeyUserCredentials",
    "ValkeyBackoffStrategy",
    "ValkeyTlsAdvancedConfiguration",
    "ValkeyAdvancedConfig",
    "ValkeyBaseConfig",
    "ValkeyConfig",
    "ValkeyWorker",
]
