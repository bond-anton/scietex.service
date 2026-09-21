"""Client-side worker registry and watcher.

Read-only view over the worker heartbeats published by ``ValkeyWorker`` and
``MqttWorker``. A :class:`WorkerWatcher` owns a
:class:`~scietex.service.client.registry.WorkerRegistry` and a swappable
backend, and exposes a snapshot plus an async change stream.

The core (registry, watcher, event types) has no optional dependencies. The
concrete backends live in their transport packages and require the matching
extra:

- :class:`~scietex.service.valkey.watch.PollingBackend` — Valkey SCAN
  (``scietex.service[valkey]``).
- :class:`~scietex.service.mqtt.watch.SubscribeBackend` — MQTT wildcard
  subscription (``scietex.service[mqtt]``).

Public exports:
    - :class:`WorkerWatcher` — snapshot + change stream over worker heartbeats.
    - :class:`WorkerRegistry` — in-memory registry keyed by ``instance_id``.
    - :class:`WorkerRecord` — a worker's last-known state.
    - :class:`WorkerEvent` — a single observed change.
    - :class:`WorkerEventKind` — the kind of change.
    - :class:`WatchBackend` — the delivery-mechanism Protocol.
"""

from .registry import WorkerRecord, WorkerRegistry
from .watcher import WatchBackend, WorkerEvent, WorkerEventKind, WorkerWatcher

__all__ = [
    "WatchBackend",
    "WorkerEvent",
    "WorkerEventKind",
    "WorkerRecord",
    "WorkerRegistry",
    "WorkerWatcher",
]
