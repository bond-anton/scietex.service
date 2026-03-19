from typing import Literal
from datetime import datetime, timezone
import msgspec


class Heartbeat(msgspec.Struct, frozen=True):
    service: str
    worker_id: int
    status: Literal["active", "inactive"]
    heartbeat_interval: int
    timestamp: datetime = datetime.now(timezone.utc)
