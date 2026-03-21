"""Data schemas."""

from datetime import datetime, timezone
from typing import Literal

import msgspec


class Heartbeat(msgspec.Struct, frozen=True):
    """Heartbeat status data schema."""

    service: str
    worker_id: int
    status: Literal["active", "inactive"]
    heartbeat_interval: int
    start_time: datetime | None = None
    timestamp: datetime = datetime.now(timezone.utc)
