"""Transport-agnostic heartbeat schema for worker liveness.

The :class:`Heartbeat` struct is the single wire shape every broker-backed
worker publishes to signal liveness, so ``ValkeyWorker`` and ``MqttWorker``
cannot drift on the payload. It is msgpack-encoded as a map, so fields are
keyed by name: reordering them does not change the wire bytes. Adding a
required field does — a payload written before that field existed fails to
decode, which is how the v5 ``ttl`` addition rejects pre-v5 heartbeats.

Serialized as msgpack, the struct is stored per transport as:

- ``ValkeyWorker``: at ``scietex:{service}:{instance_id}:status`` with a TTL of
  ``Heartbeat.ttl`` seconds.
- ``MqttWorker``: retained on ``scietex/{service}/workers/{instance_id}`` with
  a ``MessageExpiryInterval`` of ``Heartbeat.ttl`` seconds.

``ttl`` is carried in the payload rather than derived by the reader: the
producer owns the TTL policy (its own interval and grace multiplier), so
encoding it keeps the two transports from drifting and spares every client
from re-deriving it.
"""

from datetime import datetime, timezone
from typing import Literal

import msgspec

__all__ = ["Heartbeat"]


class Heartbeat(msgspec.Struct, frozen=True):
    """Worker liveness marker published by both broker-backed workers.

    Args:
        service: Name of the publishing service.
        instance_id: Unique identifier of the worker instance.
        status: Current worker status — ``"active"`` or ``"inactive"``.
        heartbeat_interval: Interval in seconds between heartbeats.
        start_time: UTC timestamp when the worker started.
        ttl: Lifetime in seconds of this entry, resolved by the producer from
            its configured TTL (or the ``2 ×``/``10 ×`` interval multipliers).
            Readers evict the entry once ``ttl`` has elapsed since
            ``timestamp``; the broker enforces the same bound independently.
        timestamp: UTC timestamp of this heartbeat entry (defaults to
            ``datetime.now(timezone.utc)`` at construction time).
    """

    service: str
    instance_id: str
    status: Literal["active", "inactive"]
    heartbeat_interval: float
    start_time: datetime
    # Required, no default: a pre-v5 heartbeat (no ttl) must fail decode rather
    # than silently adopt a default that may not match the producer's interval.
    # Must stay above ``timestamp`` — a required field cannot follow the
    # ``default_factory`` field.
    ttl: float
    timestamp: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
