"""Tests for the MQTT worker (``MqttWorker``)."""

import asyncio
import logging
import time
from datetime import datetime, timezone
from uuid import uuid4

import msgspec
import pytest

import scietex.service.mqtt.worker as mod
from scietex.service.health import TransportHealth
from scietex.service.heartbeat import Heartbeat
from scietex.service.mqtt._aiomqtt import MqttError, PacketTypes, Properties
from scietex.service.mqtt.config import MqttConfig, MqttWorkerConfig
from scietex.service.mqtt.inbox import FileMqttInbox, MemoryInbox
from scietex.service.mqtt.transport import MqttTransport
from scietex.service.mqtt.worker import MqttWorker
from scietex.service.task_handler.schemas import TaskData, TaskEnvelope, TaskResult
from scietex.service.task_handler.wire import encode_task_envelope

_LOGGER = "test_worker"

# Sentinel fed into a FakeClient's message queue to simulate a broker drop: the
# message iterator raises MqttError, mirroring aiomqtt's disconnect behavior.
_DISCONNECT = object()


class _FakeMessage:
    """Minimal aiomqtt ``Message``: a payload plus a topic."""

    def __init__(self, payload, topic="scietex/svc/tasks"):
        self.topic = topic
        self.payload = payload
        self.qos = 2
        self.retain = False
        self.mid = 0
        self.properties = None


class _FakeMessages:
    """Async iterator over a fake client's inbound message queue."""

    def __init__(self, queue):
        self._queue = queue

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._queue.get()
        if item is _DISCONNECT:
            raise MqttError("Connection closed")
        return item


class FakeClient:
    """Minimal aiomqtt ``Client`` recording publish/subscribe without a broker."""

    def __init__(self):
        self._queue = asyncio.Queue()
        self.published = []
        self.subscriptions = []
        self.closed = False
        self.publish_error = None
        self.subscribe_error = None

    @property
    def messages(self):
        return _FakeMessages(self._queue)

    def feed(self, message):
        self._queue.put_nowait(message)

    def feed_disconnect(self):
        """Simulate a broker drop: the message iterator raises MqttError next."""
        self._queue.put_nowait(_DISCONNECT)

    async def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((topic, payload, qos, retain, properties))

    async def subscribe(self, topic, qos=0, *args, **kwargs):
        if self.subscribe_error is not None:
            raise self.subscribe_error
        self.subscriptions.append((topic, qos))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.closed = True


class _FakeHandler(logging.Handler):
    """Stand-in for ``AsyncMqttHandler`` in connection tests.

    The real handler's ``start_logging`` spawns a worker that connects to a
    live broker; this fake records construction arguments and start/stop calls
    without opening a connection, so tests stay deterministic.
    """

    def __init__(self, topic, *, mqtt_config=None, qos=0, retain=False, client=None, **kwargs):
        super().__init__()
        self.topic = topic
        self.mqtt_config = mqtt_config
        self.qos = qos
        self.retain = retain
        self.client = client
        self.logging_running_event = asyncio.Event()
        self.start_calls = 0
        self.stop_calls = 0

    async def start_logging(self):
        self.start_calls += 1
        self.logging_running_event.set()

    async def stop_logging(self):
        self.stop_calls += 1
        self.logging_running_event.clear()

    def emit(self, record):
        # Drop records: the fake is here to record construction/start/stop, not
        # to deliver logs.
        pass


def _patch_handler(monkeypatch):
    """Swap the real logging handler for a :class:`_FakeHandler` so connect()
    never opens a second connection."""
    monkeypatch.setattr(mod, "AsyncMqttHandler", _FakeHandler)
    return mod


def _make_worker(tmp_path, *, inbox_backend="file", **config_kwargs):
    """Build a worker with an explicit MQTT config and a tmp_path-backed inbox."""
    return MqttWorker(
        MqttWorkerConfig(
            service_name="svc",
            mqtt_config=MqttConfig(),
            inbox_backend=inbox_backend,
            inbox_path=str(tmp_path / "inbox"),
            **config_kwargs,
        )
    )


def _health() -> TransportHealth:
    async def _reconnect() -> None:
        return None

    return TransportHealth(
        reconnect=_reconnect,
        is_connected=lambda: True,
        logger=logging.getLogger(_LOGGER),
    )


def _transport(inbox) -> MqttTransport:
    """Build an ``MqttTransport`` over a real inbox with a recording publisher."""

    async def publish(topic, payload, qos, *, retain=False, properties=None):
        return None

    return MqttTransport(
        config=MqttWorkerConfig(service_name="svc"),
        service_name="svc",
        topic="scietex/svc/tasks",
        inbox=inbox,
        health=_health(),
        publish=publish,
        logger=logging.getLogger(_LOGGER),
    )


def test_construction_is_side_effect_free(monkeypatch, tmp_path):
    """With no explicit config, ``mqtt.yml`` is not read (or written) at
    construction; the read is deferred to first connect (AR-066)."""
    monkeypatch.setenv("SCIETEX_CONFIG_DIR", str(tmp_path))

    worker = MqttWorker()

    assert worker.mqtt_config is None
    assert not (tmp_path / "mqtt.yml").exists()


@pytest.mark.asyncio
async def test_initialize_refuses_when_file_inbox_unbuildable(tmp_path):
    """inbox_backend="file" with an unbuildable path (an existing file) must
    refuse to start rather than silently drop at-least-once (design §10 #3)."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    worker = MqttWorker(
        MqttWorkerConfig(
            service_name="svc",
            mqtt_config=MqttConfig(),
            inbox_backend="file",
            inbox_path=str(blocker),
        )
    )

    assert await worker.initialize() is False
    assert worker.client is None


@pytest.mark.asyncio
async def test_initialize_none_backend_proceeds(monkeypatch):
    """inbox_backend="none" is the explicit at-most-once opt-out; startup
    connects, subscribes, and builds no inbox."""
    _patch_handler(monkeypatch)
    fake = FakeClient()

    async def factory(cfg):
        return fake

    worker = MqttWorker(
        MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig(), inbox_backend="none"),
        client_factory=factory,
    )

    assert await worker.initialize() is True
    assert worker.client is fake
    assert fake.subscriptions == [("scietex/svc/tasks", 2), ("scietex/svc/config", 1)]
    assert worker._inbox is None

    await worker._stop_message_loop()
    await worker.disconnect()


@pytest.mark.asyncio
async def test_initialize_defers_recovery_to_first_fetch(monkeypatch, tmp_path):
    """Recovery of a previous run's non-terminal inbox entries no longer runs
    eagerly in initialize(): the shared RecoverableTransport guard owns it and
    runs on the first fetch, so initialize leaves the inbox untouched."""
    _patch_handler(monkeypatch)
    fake = FakeClient()

    async def factory(cfg):
        return fake

    worker = MqttWorker(
        MqttWorkerConfig(
            service_name="svc",
            mqtt_config=MqttConfig(),
            inbox_backend="file",
            inbox_path=str(tmp_path / "inbox"),
            config_startup_timeout=0.0,
        ),
        client_factory=factory,
    )
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email")
    await worker._inbox.put(task_id, task_data)

    assert await worker.initialize() is True
    assert worker.dequeue_task() is None

    assert await worker.fetch_tasks() is True
    assert worker.dequeue_task() == task_data

    await worker.cleanup()


@pytest.mark.asyncio
async def test_message_persists_without_enqueueing(tmp_path):
    """A message carrying the task-id user property is persisted to the inbox
    but NOT enqueued directly; the transport's fetch() drains it (design §3.2)."""
    worker = _make_worker(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b'{"to":"a@b.c"}')
    message = _FakeMessage(encode_task_envelope(task_data))

    await worker._handle_message(message)

    assert await worker._inbox.pending() == [task_data]
    assert worker.dequeue_task() is None


@pytest.mark.asyncio
async def test_fetch_drains_persisted_message_exactly_once(tmp_path):
    """After the loop persists a message, one transport.fetch() enqueues it
    exactly once; a second fetch does not re-enqueue it (double-delivery fix)."""
    worker = _make_worker(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b'{"to":"a@b.c"}')
    message = _FakeMessage(encode_task_envelope(task_data))

    await worker._handle_message(message)

    assert await worker._mqtt_transport.fetch(worker) is True
    assert worker.dequeue_task() == task_data
    assert worker.dequeue_task() is None

    assert await worker._mqtt_transport.fetch(worker) is False
    assert worker.dequeue_task() is None


@pytest.mark.asyncio
async def test_none_backend_message_is_buffered_and_drained(tmp_path):
    """inbox_backend="none" must still deliver: the message loop buffers the
    message in the in-memory adapter and the transport's fetch drains it.

    Regression: the adapter was a no-op, so the at-most-once opt-out silently
    dropped every task and the worker never processed anything."""
    worker = _make_worker(tmp_path, inbox_backend="none")
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b'{"to":"a@b.c"}')
    message = _FakeMessage(encode_task_envelope(task_data))

    await worker._handle_message(message)

    assert worker._inbox is None
    assert await worker._mqtt_transport.fetch(worker) is True
    assert worker.dequeue_task() == task_data
    assert worker.dequeue_task() is None


@pytest.mark.asyncio
async def test_memory_backend_is_alias_for_none(tmp_path):
    """inbox_backend="memory" selects the same at-most-once path as "none":
    no durable inbox, and the transport receives a MemoryInbox."""
    worker = _make_worker(tmp_path, inbox_backend="memory")

    assert worker._inbox is None
    assert isinstance(worker._intake_inbox, MemoryInbox)


@pytest.mark.asyncio
async def test_memory_backend_message_is_buffered_and_drained(tmp_path):
    """inbox_backend="memory" must deliver: the message loop buffers the
    message in the MemoryInbox and the transport's fetch drains it."""
    worker = _make_worker(tmp_path, inbox_backend="memory")
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b'{"to":"a@b.c"}')
    message = _FakeMessage(encode_task_envelope(task_data))

    await worker._handle_message(message)

    assert await worker._mqtt_transport.fetch(worker) is True
    assert worker.dequeue_task() == task_data
    assert worker.dequeue_task() is None


@pytest.mark.asyncio
async def test_message_without_task_id_is_skipped(tmp_path, caplog):
    """A message whose payload lacks a task_id (a pre-v5 wire payload) is
    skipped with a warning; the loop never crashes."""
    worker = _make_worker(tmp_path)
    # A pre-v5 envelope wraps a TaskData payload without the (now required) id,
    # so the inner decode fails and intake skips it.
    envelope = TaskEnvelope(version=1, data=msgspec.msgpack.encode({"task": "send_email"}))
    message = _FakeMessage(msgspec.msgpack.encode(envelope))

    with caplog.at_level(logging.WARNING):
        await worker._handle_message(message)

    assert worker.task_queue_empty()
    assert await worker._inbox.pending() == []
    assert any("undecodable envelope" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_message_with_bad_envelope_is_skipped(tmp_path, caplog):
    """A message whose payload is not a decodable envelope is skipped with a
    warning; the loop never crashes."""
    worker = _make_worker(tmp_path)
    message = _FakeMessage(b"not-an-envelope")

    with caplog.at_level(logging.WARNING):
        await worker._handle_message(message)

    assert worker.task_queue_empty()
    assert await worker._inbox.pending() == []
    assert any("undecodable envelope" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_message_loop_exits_on_cancellation(monkeypatch):
    """The message loop task cancels cleanly during shutdown."""
    _patch_handler(monkeypatch)
    fake = FakeClient()

    async def factory(cfg):
        return fake

    worker = MqttWorker(
        MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()),
        client_factory=factory,
    )
    await worker.connect()
    assert worker._message_task is not None

    await worker._stop_message_loop()

    assert worker._message_task is None


@pytest.mark.asyncio
async def test_reconnect_resubscribes_and_restarts_loop(monkeypatch):
    """A reconnect after the loop exits on MqttError re-subscribes and starts a
    fresh message loop (intake is restored without a full worker restart)."""
    _patch_handler(monkeypatch)
    clients = []

    async def factory(cfg):
        client = FakeClient()
        clients.append(client)
        return client

    worker = MqttWorker(
        MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()),
        client_factory=factory,
    )
    assert await worker.connect() is True
    first_loop = worker._message_task
    assert first_loop is not None
    assert clients[0].subscriptions == [("scietex/svc/tasks", 2), ("scietex/svc/config", 1)]

    # Simulate a broker drop: the loop exits on MqttError and reports to health.
    clients[0].feed_disconnect()
    await first_loop
    assert worker.transport_health.degraded is True

    await worker._reconnect()

    assert len(clients) == 2
    assert clients[1].subscriptions == [("scietex/svc/tasks", 2), ("scietex/svc/config", 1)]
    assert worker._message_task is not None
    assert worker._message_task is not first_loop
    assert not worker._message_task.done()

    await worker._stop_message_loop()
    await worker.disconnect()


@pytest.mark.asyncio
async def test_start_intake_does_not_double_start_loop(monkeypatch):
    """Calling _start_intake twice does not create a second message-loop task."""
    _patch_handler(monkeypatch)
    fake = FakeClient()

    async def factory(cfg):
        return fake

    worker = MqttWorker(
        MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()),
        client_factory=factory,
    )
    await worker.connect()
    first_loop = worker._message_task
    assert first_loop is not None

    assert await worker._start_intake() is True

    assert worker._message_task is first_loop

    await worker._stop_message_loop()
    await worker.disconnect()


@pytest.mark.asyncio
async def test_retryable_error_does_not_tombstone(tmp_path):
    """A retryable error leaves the inbox entry non-terminal so the re-published
    retry copy (same task id) is accepted, not suppressed by a tombstone
    (AR-077b mirror)."""
    inbox = FileMqttInbox(tmp_path / "inbox", logger=logging.getLogger(_LOGGER))
    transport = _transport(inbox)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email")

    await inbox.put(task_id, task_data)
    await transport.ack(task_data, TaskResult(status="error", retryable=True))

    assert await inbox.pending() == [task_data]
    # The retry copy is accepted rather than skipped as a duplicate.
    await inbox.put(task_id, task_data)
    assert await inbox.pending() == [task_data]


@pytest.mark.asyncio
async def test_terminal_error_still_tombstones(tmp_path):
    """A non-retryable error still tombstones the entry (the guard is narrow)."""
    inbox = FileMqttInbox(tmp_path / "inbox", logger=logging.getLogger(_LOGGER))
    transport = _transport(inbox)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email")

    await inbox.put(task_id, task_data)
    await transport.ack(task_data, TaskResult(status="error", retryable=False))

    assert await inbox.pending() == []


@pytest.mark.asyncio
async def test_heartbeat_publishes_retained_on_registry_topic():
    """heartbeat publishes a retained msgpack Heartbeat on the registry topic."""
    fake = FakeClient()
    worker = MqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()))
    worker._client = fake
    start = datetime.now(timezone.utc)
    worker._lifecycle.start_time = start

    await worker.heartbeat()

    assert len(fake.published) == 1
    topic, payload, qos, retain, _ = fake.published[0]
    assert topic == f"scietex/svc/workers/{worker.instance_id}"
    assert qos == 1
    assert retain is True
    decoded = msgspec.msgpack.decode(payload, type=Heartbeat)
    assert decoded.service == "svc"
    assert decoded.instance_id == worker.instance_id
    assert decoded.status == "active"
    assert decoded.heartbeat_interval == worker.heartbeat_interval
    assert isinstance(decoded.start_time, datetime)
    assert isinstance(decoded.timestamp, datetime)
    # Byte-identity parity: the MQTT payload is the exact msgpack encoding of
    # the shared Heartbeat struct, matching what Valkey publishes field-for-field.
    reference = Heartbeat(
        service="svc",
        instance_id=worker.instance_id,
        status="active",
        heartbeat_interval=worker.heartbeat_interval,
        start_time=start,
        timestamp=decoded.timestamp,
    )
    assert msgspec.msgpack.encode(reference) == payload


@pytest.mark.asyncio
async def test_heartbeat_failure_reports_to_health():
    """A failed heartbeat publish is reported to TransportHealth, not raised."""
    fake = FakeClient()
    fake.publish_error = MqttError("boom")
    worker = MqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()))
    worker._client = fake
    worker._lifecycle.start_time = datetime.now(timezone.utc)

    await worker.heartbeat()

    assert worker.transport_health.degraded is True
    assert worker.transport_health.last_error == "boom"


@pytest.mark.asyncio
async def test_register_and_unregister_publish_and_clear():
    """_register_instance publishes the retained marker; _unregister_instance
    clears it with an empty retained payload."""
    fake = FakeClient()
    worker = MqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()))
    worker._client = fake

    await worker._register_instance()
    await worker._unregister_instance()

    assert len(fake.published) == 2
    register_topic, register_payload, _, register_retain, _ = fake.published[0]
    assert register_topic == f"scietex/svc/workers/{worker.instance_id}"
    assert register_retain is True
    assert msgspec.msgpack.decode(register_payload)["status"] == "active"

    unregister_topic, unregister_payload, _, unregister_retain, _ = fake.published[1]
    assert unregister_topic == f"scietex/svc/workers/{worker.instance_id}"
    assert unregister_retain is True
    assert unregister_payload is None  # empty retained payload clears the marker


@pytest.mark.asyncio
async def test_register_instance_failure_is_best_effort(caplog):
    """A failed registration logs WARNING and reports to health, never raising."""
    fake = FakeClient()
    fake.publish_error = MqttError("boom")
    worker = MqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()))
    worker._client = fake

    with caplog.at_level(logging.WARNING):
        await worker._register_instance()

    assert worker.transport_health.degraded is True
    assert any("Failed to register instance" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_watchdog_refreshes_then_recovers(monkeypatch):
    """watchdog refreshes leases before health.recover (parity with Valkey)."""
    worker = MqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()))
    calls = []

    async def refresh_leases():
        calls.append("refresh")

    async def recover():
        calls.append("recover")

    monkeypatch.setattr(worker._mqtt_transport, "refresh_leases", refresh_leases)
    monkeypatch.setattr(worker._health, "recover", recover)

    await worker.watchdog()

    assert calls == ["refresh", "recover"]


@pytest.mark.asyncio
async def test_watchdog_logs_critical_report(caplog):
    """A connection down past the threshold surfaces one CRITICAL report."""
    worker = MqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()))
    # Simulate a sustained outage without requesting a reconnect, so recover()
    # no-ops and critical_report() fires.
    worker._health._down_since = time.monotonic() - 9999
    worker._health._failure_count = 3
    worker._health._last_error = "boom"
    worker._health._degraded = True
    worker._health._reported_critical = False

    with caplog.at_level(logging.CRITICAL):
        await worker.watchdog()

    messages = [r.getMessage() for r in caplog.records]
    assert any("down for" in m for m in messages)
    assert any(m.startswith("MQTT connection down") for m in messages), "report must name the MQTT backend"


@pytest.mark.asyncio
async def test_watchdog_prunes_inbox_once_per_interval(tmp_path, monkeypatch):
    """watchdog calls inbox.prune_expired() on its first tick, throttled to one
    call per INBOX_PRUNE_INTERVAL so the tombstone scan does not run every
    1s watchdog tick (AR-115)."""
    worker = _make_worker(tmp_path)
    calls = []

    async def prune_expired():
        calls.append("prune")

    monkeypatch.setattr(worker._inbox, "prune_expired", prune_expired)
    worker._next_inbox_prune = 0.0

    await worker.watchdog()
    await worker.watchdog()

    assert calls == ["prune"]
    assert worker._next_inbox_prune > 0.0


@pytest.mark.asyncio
async def test_watchdog_prunes_not_without_durable_inbox(tmp_path):
    """inbox_backend="none" (worker._inbox is None) never attempts a prune: the
    at-most-once opt-out has no durable files to reclaim (AR-115)."""
    worker = _make_worker(tmp_path, inbox_backend="none")

    await worker.watchdog()

    assert worker._inbox is None
    assert worker._next_inbox_prune == 0.0


@pytest.mark.asyncio
async def test_cleanup_stops_loop_handler_and_disconnects(monkeypatch):
    """cleanup stops the message loop, stops the log handler, and disconnects."""
    _patch_handler(monkeypatch)
    fake = FakeClient()

    async def factory(cfg):
        return fake

    worker = MqttWorker(
        MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig()),
        client_factory=factory,
    )
    await worker.connect()
    handler = worker._mqtt_logger_handler
    assert handler is not None
    assert handler.start_calls == 1
    assert worker._message_task is not None

    await worker.cleanup()

    assert worker.client is None
    assert fake.closed is True
    assert handler.stop_calls == 1
    assert worker._message_task is None


def test_status_topic_prefix_resolved_at_construction(tmp_path):
    """The transport receives the ``{service}``-substituted status_topic_prefix,
    resolved once at construction exactly as task_topic is (design §13.2)."""
    worker = _make_worker(tmp_path)

    assert worker._status_topic_prefix == "scietex/svc/tasks"
    assert worker._mqtt_transport._status_topic_prefix == "scietex/svc/tasks"


@pytest.mark.asyncio
async def test_status_publish_disabled_suppresses_publishes(tmp_path):
    """status_publish_enabled=False makes on_progress a no-op and skips the
    running/terminal status publishes, so nothing reaches the broker (design
    §13.6)."""
    fake = FakeClient()
    worker = _make_worker(tmp_path, inbox_backend="none", status_publish_enabled=False)
    worker._client = fake
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email")

    await worker._mqtt_transport.on_progress(task_id, 42.0)
    await worker._mqtt_transport.on_started(task_data)
    await worker._mqtt_transport.ack(task_data, TaskResult(status="success"))

    assert fake.published == []


@pytest.mark.asyncio
async def test_publish_forwards_retain_flag():
    """The worker's _publish forwards retain to client.publish, so status
    (retained) and progress/requeue (non-retained) honor the seam's flag; when
    no properties are passed, ``None`` is forwarded."""
    fake = FakeClient()
    worker = MqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig(), inbox_backend="none"))
    worker._client = fake

    await worker._publish("topic/status", b"st", qos=1, retain=True)
    await worker._publish("topic/progress", b"pg", qos=0, retain=False)

    assert fake.published == [
        ("topic/status", b"st", 1, True, None),
        ("topic/progress", b"pg", 0, False, None),
    ]


@pytest.mark.asyncio
async def test_publish_forwards_properties():
    """The worker's _publish forwards the MQTT 5 properties to client.publish,
    so a retained status can carry a message-expiry interval."""
    fake = FakeClient()
    worker = MqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=MqttConfig(), inbox_backend="none"))
    worker._client = fake
    props = Properties(PacketTypes.PUBLISH)
    props.MessageExpiryInterval = 60

    await worker._publish("t", b"x", qos=1, retain=True, properties=props)

    assert fake.published == [("t", b"x", 1, True, props)]
