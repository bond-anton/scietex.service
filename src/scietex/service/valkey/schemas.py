"""Valkey-specific data schemas for ``scietex.service``.

Defines structured data types used by :class:`~scietex.service.valkey.ValkeyWorker`,
including :class:`Heartbeat` for worker status publishing.

All schemas are :class:`msgspec.Struct` subclasses with ``frozen=True``
for immutability.
"""

from datetime import datetime, timezone
from typing import Literal

import msgspec


class Heartbeat(msgspec.Struct, frozen=True):
    """Heartbeat data published by :class:`~scietex.service.valkey.ValkeyWorker` to track worker status.

    Serialized as msgpack and stored at a key like
    ``scietex:{service_name}:{worker_id}:status`` with a TTL set to
    twice the heartbeat interval.

    Attributes:
        service: Name of the publishing service.
        worker_id: Unique identifier of the worker instance.
        status: Current worker status — ``"active"`` or ``"inactive"``.
        heartbeat_interval: Interval in seconds between heartbeats.
        start_time: UTC timestamp when the worker started.
        timestamp: UTC timestamp of this heartbeat entry (defaults to
            ``datetime.now(timezone.utc)`` at construction time).
    """

    service: str
    worker_id: int
    status: Literal["active", "inactive"]
    heartbeat_interval: float
    start_time: datetime
    timestamp: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
