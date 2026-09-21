"""Transport-agnostic heartbeat schema for worker liveness.

The :class:`Heartbeat` struct is the single wire shape every broker-backed
worker publishes to signal liveness, so ``ValkeyWorker`` and ``MqttWorker``
cannot drift on the payload. It is msgpack-encoded; the field order below is
the wire order and must not be reordered without a version bump.

Serialized as msgpack, the struct is stored per transport as:

- ``ValkeyWorker``: at ``scietex:{service}:{instance_id}:status`` with a TTL of
  ``2 × heartbeat_interval`` seconds.
- ``MqttWorker``: retained on ``scietex/{service}/workers/{instance_id}``.
"""

from datetime import datetime, timezone
from typing import Literal

import msgspec

__all__ = ["Heartbeat"]


class Heartbeat(msgspec.Struct, frozen=True):
    """Worker liveness marker published by both broker-backed workers.

    Attributes:
        service: Name of the publishing service.
        instance_id: Unique identifier of the worker instance.
        status: Current worker status — ``"active"`` or ``"inactive"``.
        heartbeat_interval: Interval in seconds between heartbeats.
        start_time: UTC timestamp when the worker started.
        timestamp: UTC timestamp of this heartbeat entry (defaults to
            ``datetime.now(timezone.utc)`` at construction time).
    """

    service: str
    instance_id: str
    status: Literal["active", "inactive"]
    heartbeat_interval: float
    start_time: datetime
    timestamp: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
