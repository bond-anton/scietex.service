"""Task producers that publish demo tasks straight to the brokers.

A producer is the mirror image of a worker: instead of consuming tasks it
enqueues them, one XADD per task on Valkey and one publish per task on MQTT.
It talks to the brokers directly (no worker, no transport) because the point of
the PRODUCERS pane is to drive the workers from outside, exactly as an external
client would.

Each producer owns two independent broker switches. With both on, every emitted
task goes to both brokers; with neither on, nothing is published and the
emission is a no-op. The task name is fixed per producer (``fast_task`` /
``slow_task``) so the emitted work lands on the matching demo handler.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from uuid import uuid4

import aiomqtt
import msgspec

from scietex.service.config import prepare_conf_dir
from scietex.service.mqtt.config import MqttConfig, read_mqtt_config
from scietex.service.mqtt.worker import _create_client
from scietex.service.task_handler.schemas import TaskData, TaskTimeout
from scietex.service.task_handler.wire import encode_task_envelope
from scietex.service.valkey._glide import GlideClient, GlideClientConfiguration
from scietex.service.valkey.config import ValkeyConfig, generate_glide_config, read_valkey_config

#: Field name of the task payload inside a Valkey stream entry. Mirrors
#: ``valkey.transport.TASK_FIELD``; one entry per task, never batched, because
#: the worker's per-entry ack/delete bookkeeping assumes one task per entry id.
TASK_FIELD = b"task"

#: How long a producer waits for a broker client to close before giving up.
PRODUCER_STOP_TIMEOUT = 5.0

#: Bounds on the interval and timeout inputs, in milliseconds. The lower bound
#: keeps the emit loop from spinning the event loop; the upper bound is a sanity
#: ceiling, not a protocol limit.
MIN_INTERVAL_MS = 10
MAX_INTERVAL_MS = 600_000
MIN_TIMEOUT_MS = 1
MAX_TIMEOUT_MS = 600_000

#: Bounds on the batch size. The interval floor caps a single-task loop at
#: ``1000 / MIN_INTERVAL_MS`` tasks/s, so throughput above that comes from
#: emitting several tasks per tick rather than from a shorter interval. The
#: upper bound keeps one tick from monopolising the event loop.
MIN_BATCH_SIZE = 1
MAX_BATCH_SIZE = 1000

#: Default interval, timeout, and batch size for a fresh producer card.
DEFAULT_INTERVAL_MS = 1000
DEFAULT_TIMEOUT_MS = 1000
DEFAULT_BATCH_SIZE = 1

ValkeyClientFactory = Callable[[GlideClientConfiguration], Awaitable[GlideClient]]
MqttClientFactory = Callable[[MqttConfig], Awaitable[aiomqtt.Client]]


class TaskProducer:
    """Publishes demo tasks to Valkey and/or MQTT on demand or on an interval.

    The producer holds no worker and no transport: it opens its own broker
    clients lazily on first use and keeps them for the lifetime of the app. A
    broker that is switched off is never contacted, so a producer with only the
    Valkey switch on works without an MQTT client at all.
    """

    def __init__(
        self,
        task_name: str,
        *,
        service_name: str = "service",
        valkey_config: ValkeyConfig | None = None,
        mqtt_config: MqttConfig | None = None,
        conf_dir: Path | None = None,
        valkey_client_factory: ValkeyClientFactory | None = None,
        mqtt_client_factory: MqttClientFactory | None = None,
    ) -> None:
        self._task_name = task_name
        self._service_name = service_name
        self._valkey_config = valkey_config
        self._mqtt_config = mqtt_config
        self._conf_dir = conf_dir
        self._valkey_client_factory = valkey_client_factory
        self._mqtt_client_factory = mqtt_client_factory
        self._valkey_client: GlideClient | None = None
        self._mqtt_client: aiomqtt.Client | None = None
        self._valkey_enabled = False
        self._mqtt_enabled = False
        self._interval_ms = DEFAULT_INTERVAL_MS
        self._timeout_ms = DEFAULT_TIMEOUT_MS
        self._batch_size = DEFAULT_BATCH_SIZE
        self._emitted = 0
        self._last_error: str | None = None
        self._run_task: asyncio.Task[None] | None = None

    @property
    def task_name(self) -> str:
        return self._task_name

    @property
    def emitted(self) -> int:
        """Cumulative count of tasks published since the producer was created."""
        return self._emitted

    @property
    def last_error(self) -> str | None:
        """The most recent publish failure, or ``None`` if the last emit succeeded."""
        return self._last_error

    @property
    def running(self) -> bool:
        """Whether the interval loop is currently emitting."""
        return self._run_task is not None and not self._run_task.done()

    @property
    def valkey_enabled(self) -> bool:
        return self._valkey_enabled

    @property
    def mqtt_enabled(self) -> bool:
        return self._mqtt_enabled

    @property
    def batch_size(self) -> int:
        """How many tasks one emit publishes to each enabled broker."""
        return self._batch_size

    def set_valkey_enabled(self, enabled: bool) -> None:
        self._valkey_enabled = enabled

    def set_mqtt_enabled(self, enabled: bool) -> None:
        self._mqtt_enabled = enabled

    def set_interval_ms(self, interval_ms: int) -> None:
        self._interval_ms = max(MIN_INTERVAL_MS, min(interval_ms, MAX_INTERVAL_MS))

    def set_timeout_ms(self, timeout_ms: int) -> None:
        self._timeout_ms = max(MIN_TIMEOUT_MS, min(timeout_ms, MAX_TIMEOUT_MS))

    def set_batch_size(self, batch_size: int) -> None:
        self._batch_size = max(MIN_BATCH_SIZE, min(batch_size, MAX_BATCH_SIZE))

    def _build_task(self) -> TaskData:
        """Build one task with the producer's configured timeout.

        The timeout action is ``requeue`` so a task that outlives its timeout is
        retried rather than dropped, matching the schema default.
        """
        return TaskData(
            task_id=str(uuid4()),
            task=self._task_name,
            timeout=TaskTimeout(timeout=self._timeout_ms / 1000.0, timeout_action="requeue"),
        )

    async def emit(self) -> int:
        """Publish ``batch_size`` tasks to every enabled broker; return the count.

        A broker that is switched off is skipped entirely. A publish failure is
        recorded in ``last_error`` and does not abort the other broker, so one
        unreachable broker cannot silence a healthy one. A failure part-way
        through a batch keeps the tasks already published and stops that
        broker's batch, so the returned count reflects what actually landed.
        """
        if not self._valkey_enabled and not self._mqtt_enabled:
            return 0
        published = 0
        errors: list[str] = []
        if self._valkey_enabled:
            try:
                for _ in range(self._batch_size):
                    await self._publish_valkey(self._build_task())
                    published += 1
            except Exception as exc:
                errors.append(f"valkey: {exc}")
                await self._drop_valkey_client()
        if self._mqtt_enabled:
            try:
                for _ in range(self._batch_size):
                    await self._publish_mqtt(self._build_task())
                    published += 1
            except Exception as exc:
                errors.append(f"mqtt: {exc}")
                await self._drop_mqtt_client()
        self._emitted += published
        self._last_error = "; ".join(errors) if errors else None
        return published

    async def _publish_valkey(self, task: TaskData) -> None:
        client = await self._ensure_valkey_client()
        stream = f"scietex:{self._service_name}:tasks"
        await client.xadd(stream, [(TASK_FIELD, encode_task_envelope(task))])

    async def _publish_mqtt(self, task: TaskData) -> None:
        client = await self._ensure_mqtt_client()
        topic = f"scietex/{self._service_name}/tasks"
        await client.publish(topic, encode_task_envelope(task), qos=2)

    async def _ensure_valkey_client(self) -> GlideClient:
        if self._valkey_client is not None:
            return self._valkey_client
        config = self._valkey_config
        if config is None:
            config = read_valkey_config(self._resolve_conf_dir(), create_default=True)
        if self._valkey_client_factory is not None:
            client = await self._valkey_client_factory(generate_glide_config(config, service_name=self._service_name))
        else:
            client = await GlideClient.create(generate_glide_config(config, service_name=self._service_name))
        self._valkey_client = client
        return client

    async def _ensure_mqtt_client(self) -> aiomqtt.Client:
        if self._mqtt_client is not None:
            return self._mqtt_client
        config = self._mqtt_config
        if config is None:
            config = read_mqtt_config(self._resolve_conf_dir(), create_default=True)
        if self._mqtt_client_factory is not None:
            client = await self._mqtt_client_factory(config)
        else:
            client = await _create_client(config)
        self._mqtt_client = client
        return client

    def _resolve_conf_dir(self) -> Path:
        return prepare_conf_dir(self._conf_dir)

    async def _drop_valkey_client(self) -> None:
        client, self._valkey_client = self._valkey_client, None
        if client is None:
            return
        try:
            await client.close()
        except Exception:
            # A client that already failed to publish may also fail to close;
            # dropping the reference is what matters, not the close outcome.
            pass

    async def _drop_mqtt_client(self) -> None:
        client, self._mqtt_client = self._mqtt_client, None
        if client is None:
            return
        try:
            await client.__aexit__(None, None, None)
        except Exception:
            # See _drop_valkey_client: the reference drop is the point.
            pass

    def start(self) -> None:
        """Start the interval loop, emitting one task every ``interval_ms``."""
        if self.running:
            return
        self._run_task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            await self.emit()
            await asyncio.sleep(self._interval_ms / 1000.0)

    async def stop_loop(self) -> None:
        """Stop the interval loop, keeping the broker clients open.

        The subprocess handle exposes the same verb so the app can drive either
        producer through one surface; here it is the loop-only half of
        :meth:`stop`.
        """
        task, self._run_task = self._run_task, None
        if task is None:
            return
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=PRODUCER_STOP_TIMEOUT)
        except (asyncio.CancelledError, TimeoutError):
            # Cancellation is the expected path; a timeout means the loop is
            # stuck on an unreachable broker and must not block shutdown.
            pass

    async def stop(self) -> None:
        """Stop the interval loop and close both broker clients."""
        await self.stop_loop()
        await self._drop_valkey_client()
        await self._drop_mqtt_client()

    def snapshot(self) -> "ProducerSnapshot":
        """A point-in-time view of the producer for the card to render."""
        return ProducerSnapshot(
            emitted=self._emitted,
            running=self.running,
            valkey_enabled=self._valkey_enabled,
            mqtt_enabled=self._mqtt_enabled,
            batch_size=self._batch_size,
            last_error=self._last_error,
            received_at=time.monotonic(),
        )


class ProducerSnapshot(msgspec.Struct, frozen=True):
    """Immutable producer state pushed to a card on each poll tick."""

    emitted: int = 0
    running: bool = False
    valkey_enabled: bool = False
    mqtt_enabled: bool = False
    batch_size: int = DEFAULT_BATCH_SIZE
    last_error: str | None = None
    received_at: float = 0.0
