"""Streams a mosquitto broker's ``$SYS`` topic tree into a snapshot.

Independent of the workers: it owns its own client and connects on app start.
``$SYS`` is published retained at QoS 2 and refreshed every ``sys_interval``
seconds (10 by default), so the panel shows the last value and marks it stale
once it ages past the freshness threshold.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

import aiomqtt
import msgspec

from examples.textual.broker_parsing import parse_sys_message
from examples.textual.broker_snapshot import MqttBrokerSnapshot
from scietex.service.config import prepare_conf_dir
from scietex.service.mqtt.config import MqttConfig, read_mqtt_config
from scietex.service.mqtt.worker import _create_client

MQTT_RECONNECT_DELAY = 5.0
MQTT_STOP_TIMEOUT = 5.0
SYS_TOPIC = "$SYS/#"

ClientFactory = Callable[[MqttConfig], Awaitable[aiomqtt.Client]]


class MqttBrokerMonitor:
    """Subscribes to ``$SYS/#`` and folds messages into a snapshot."""

    def __init__(
        self,
        config: MqttConfig | None = None,
        *,
        conf_dir: Path | None = None,
        client_factory: ClientFactory | None = None,
        reconnect_delay: float = MQTT_RECONNECT_DELAY,
        sys_topic: str = SYS_TOPIC,
    ) -> None:
        self._config = config
        self._conf_dir = conf_dir
        self._client_factory = client_factory
        self._reconnect_delay = reconnect_delay
        self._sys_topic = sys_topic
        self._values: dict[str, object] = {}
        self._connected = False
        self._error: str | None = None
        self._received_at = 0.0
        self._task: asyncio.Task[None] | None = None

    def snapshot(self) -> MqttBrokerSnapshot:
        return msgspec.structs.replace(
            MqttBrokerSnapshot(
                connected=self._connected,
                error=self._error,
                received_at=self._received_at,
            ),
            **self._values,
        )

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=MQTT_STOP_TIMEOUT)
        except (asyncio.CancelledError, TimeoutError):
            # Cancellation is the expected path; a timeout means the client is
            # stuck on an unreachable broker and must not block shutdown.
            pass

    def _ingest(self, topic: str, payload: bytes) -> None:
        """Fold one ``$SYS`` message into the accumulated values."""
        parsed = parse_sys_message(topic, payload)
        if parsed is None:
            return
        field, value = parsed
        self._values[field] = value
        self._received_at = time.monotonic()

    async def _run(self) -> None:
        while True:
            client = None
            try:
                client = await self._connect()
                await client.subscribe(self._sys_topic, qos=0)
                self._connected = True
                self._error = None
                async for message in client.messages:
                    self._ingest(str(message.topic), bytes(message.payload))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._connected = False
                self._error = str(exc)
            finally:
                await self._close(client)
            await asyncio.sleep(self._reconnect_delay)

    async def _connect(self) -> aiomqtt.Client:
        config = self._config
        if config is None:
            try:
                config = read_mqtt_config(prepare_conf_dir(self._conf_dir), create_default=False)
            except RuntimeError:
                # No config file on disk: fall back to the same localhost
                # defaults a worker would use, without writing a file.
                config = MqttConfig()
        factory = self._client_factory or _create_client
        return await factory(config)

    async def _close(self, client: aiomqtt.Client | None) -> None:
        if client is None:
            return
        try:
            # aiomqtt has no aclose; exiting the async context disconnects.
            await client.__aexit__(None, None, None)
        except Exception:
            # The connection is already unusable; nothing to recover here.
            pass
