"""MQTT retained-topic implementation of the core ``ConfigSource`` protocol.

The retained topic ``scietex/{service}/config`` is the source of truth for
remote config. MQTT has no cross-topic atomicity, so the whole envelope lives
in a single retained message: retained = state, delivered to every subscriber
on SUBACK. Unlike Valkey's ``GET``, the retained payload cannot be read on
demand — it arrives as a message — so the source records the latest payload
received on the topic as an in-memory snapshot and exposes a bounded
``wait_for_snapshot`` for startup.

``store`` publishes the effective config back as a retained message carrying an
MQTT 5 message-expiry (``config_ttl``) so a stale marker ages out of the broker
instead of persisting forever, mirroring ``status_ttl``
(:meth:`scietex.service.mqtt.transport.MqttTransport._publish_status`).
"""

import asyncio
import logging

from ._aiomqtt import PacketTypes, Properties
from .transport import MqttPublish

__all__ = ["MqttConfigSource"]


class MqttConfigSource:
    """Retained-topic ``ConfigSource``: snapshot + publish on the config topic.

    ``record`` is called by the worker's message loop for every message on the
    config topic and keeps the newest payload; ``wait_for_snapshot`` blocks
    (bounded) until the retained message arrives after SUBACK, so startup is
    deterministic without hanging a broker that holds no retained config.
    ``load`` returns the recorded snapshot without touching the network, and
    ``store`` publishes the effective config back as a retained message with an
    optional MQTT 5 message-expiry (``ttl``).
    """

    def __init__(
        self,
        *,
        topic: str,
        qos: int,
        ttl: int | None,
        publish: MqttPublish,
        logger: logging.Logger,
    ) -> None:
        self._topic = topic
        self._qos = qos
        self._ttl = ttl
        self._publish = publish
        self._logger = logger

        self._snapshot: bytes | None = None
        self._event = asyncio.Event()

    def record(self, payload: bytes) -> None:
        """Store the latest config-topic payload and signal any waiter.

        Called by the message loop on each config-topic message; the newest
        payload wins (a retained topic holds exactly one desired state).
        """
        self._snapshot = payload
        self._event.set()

    async def wait_for_snapshot(self, timeout: float) -> bytes | None:
        """Await the retained snapshot for at most ``timeout`` seconds.

        Returns the recorded payload once it arrives, or ``None`` on timeout —
        it never raises, so a broker without a retained config cannot fail
        startup.
        """
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        return self._snapshot

    async def load(self) -> bytes | None:
        """Return the recorded snapshot, or ``None`` if none has arrived yet."""
        return self._snapshot

    async def store(self, envelope: bytes) -> None:
        """Publish the effective config back as a retained message.

        Retained = state, so the marker persists for later subscribers. When
        ``ttl`` is set the publish carries an MQTT 5 message-expiry property so
        a stale marker ages out (mirroring ``status_ttl``); ``ttl=None``
        publishes no properties.
        """
        properties = None
        if self._ttl is not None:
            properties = Properties(PacketTypes.PUBLISH)
            properties.MessageExpiryInterval = self._ttl
        await self._publish(self._topic, envelope, self._qos, retain=True, properties=properties)
