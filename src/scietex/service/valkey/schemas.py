"""Valkey-specific data schemas for ``scietex.service``.

Defines structured data types used by ``ValkeyWorker``, including
``Heartbeat`` for worker status publishing.
"""

from datetime import datetime, timezone
from typing import Literal

import msgspec


class Heartbeat(msgspec.Struct, frozen=True):
    """Heartbeat data published by ``ValkeyWorker`` to track worker status.

    Serialized as msgpack and stored at a key like
    ``scietex:{service_name}:{worker_id}:status`` with a TTL set to
    twice the heartbeat interval.
    """

    service: str
    worker_id: int
    status: Literal["active", "inactive"]
    heartbeat_interval: float
    start_time: datetime
    timestamp: datetime = datetime.now(timezone.utc)
