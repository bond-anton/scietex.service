"""MQTT subscribe backend for the worker watcher.

Subscribes to the retained heartbeat topics (``scietex/{service}/workers/+``)
and buffers every message it receives. This is the push implementation of
:class:`~scietex.service.client.watcher.WatchBackend`: the broker delivers
retained state on subscribe and live updates thereafter, so the watcher sees
changes without polling the broker.

Requires the optional ``aiomqtt`` dependency.
"""

from __future__ import annotations

import asyncio
import logging

from ..client.backends import decode_heartbeat
from ..client.watcher import WatchBackend
from ..heartbeat import Heartbeat
from ._aiomqtt import Client, MqttError

__all__ = ["SubscribeBackend"]

#: QoS for the heartbeat subscription. Retained liveness is published at QoS 1,
#: so the subscription matches it.
_SUBSCRIBE_QOS: int = 1


class SubscribeBackend(WatchBackend):
    """Feeds a watcher from an MQTT wildcard subscription.

    Args:
        client: A connected ``aiomqtt.Client``.
        service_name: The service whose workers to watch; scopes the
            subscription to ``scietex/{service}/workers/+``.
        logger: Optional logger for delivery failures.
    """

    def __init__(self, client: Client, service_name: str, *, logger: logging.Logger | None = None) -> None:
        self._client = client
        self._topic = f"scietex/{service_name}/workers/+"
        self._logger = logger or logging.getLogger(__name__)
        self._buffer: list[Heartbeat] = []
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Subscribe and begin buffering messages in the background.

        Must be awaited before the first :meth:`poll`; the watcher does not
        start the backend itself, so the caller controls when the subscription
        is live.
        """
        await self._client.subscribe(self._topic, qos=_SUBSCRIBE_QOS)
        self._task = asyncio.create_task(self._consume())

    async def _consume(self) -> None:
        """Drain the client's message iterator into the buffer.

        A broker drop ends the iterator; the backend then stops buffering and
        the watcher's records expire on their own TTL, which is the correct
        degradation (a client that cannot see heartbeats must not keep
        reporting workers as live).
        """
        try:
            async for message in self._client.messages:
                heartbeat = decode_heartbeat(message.payload)
                if heartbeat is not None:
                    self._buffer.append(heartbeat)
        except MqttError as exc:
            self._logger.log(logging.WARNING, "Heartbeat subscription ended: %s", exc)

    async def poll(self) -> list[Heartbeat]:
        """Return the heartbeats buffered since the previous call.

        Returns:
            The buffered heartbeats, and clears the buffer.
        """
        buffered, self._buffer = self._buffer, []
        return buffered

    async def close(self) -> None:
        """Cancel the background consumer and unsubscribe."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        try:
            await self._client.unsubscribe(self._topic)
        except MqttError as exc:
            # Best-effort: the client may already be closing.
            self._logger.log(logging.DEBUG, "Unsubscribe from %s failed: %s", self._topic, exc)
