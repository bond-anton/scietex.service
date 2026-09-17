"""Back-compat re-export of the connection-health supervisor.

``TransportHealth`` moved to :mod:`scietex.service.health` (AR-089), where it
serves as the transport-agnostic connection-health supervisor shared by every
transport (``ValkeyWorker`` today; MQTT/Kafka later). This module re-exports the
same names so existing imports of
``scietex.service.valkey.health.TransportHealth`` keep working.
"""

from ..health import DEFAULT_TRANSPORT_DOWN_THRESHOLD_SECONDS, TransportHealth

__all__ = [
    "DEFAULT_TRANSPORT_DOWN_THRESHOLD_SECONDS",
    "TransportHealth",
]
