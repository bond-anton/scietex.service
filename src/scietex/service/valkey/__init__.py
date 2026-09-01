"""Valkey-backed async worker for ``scietex.service``.

Provides ``ValkeyWorker`` (extends ``AsyncTaskProcessor``) for task
processing backed by a Valkey/Redis stream, along with configuration
schemas and a YAML config loader.

Requires the optional ``valkey-glide`` dependency.
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
