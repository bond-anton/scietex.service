"""Tests for the MQTT transport (``MqttTransport``)."""

import logging
from uuid import UUID, uuid4

import pytest

from scietex.service.health import TransportHealth
from scietex.service.mqtt.config import MqttWorkerConfig
from scietex.service.mqtt.transport import MqttTransport
from scietex.service.task_handler.schemas import TaskData
from scietex.service.task_handler.wire import encode_task_envelope

_LOGGER = "test_transport"
_TOPIC = "scietex/svc/tasks"


class FakeInbox:
    """In-memory ``MqttInbox`` for transport tests.

    Mirrors ``FileMqttInbox`` semantics (``put`` persists a pending entry,
    ``mark_terminal`` tombstones it) while recording every call so tests can
    assert on the transport's interaction without touching the filesystem.
    """

    def __init__(self) -> None:
        self._entries: dict[UUID, TaskData] = {}
        self._terminal: set[UUID] = set()
        self.put_calls: list[UUID] = []
        self.mark_in_flight_calls: list[UUID] = []
        self.mark_terminal_calls: list[UUID] = []
        self.pending_calls = 0
        self.recover_calls = 0

    def seed(self, *entries: tuple[UUID, TaskData]) -> None:
        """Pre-populate non-terminal entries, oldest first (insertion order)."""
        for task_id, task_data in entries:
            self._entries[task_id] = task_data

    async def put(self, task_id: UUID, task_data: TaskData) -> None:
        self.put_calls.append(task_id)
        if task_id in self._terminal:
            return  # duplicate delivery of an already-terminal task
        self._entries[task_id] = task_data

    async def mark_in_flight(self, task_id: UUID) -> None:
        self.mark_in_flight_calls.append(task_id)

    async def mark_terminal(self, task_id: UUID) -> None:
        self.mark_terminal_calls.append(task_id)
        self._terminal.add(task_id)
        self._entries.pop(task_id, None)

    async def pending(self) -> list[tuple[UUID, TaskData]]:
        self.pending_calls += 1
        return self._non_terminal()

    async def recover(self) -> list[tuple[UUID, TaskData]]:
        self.recover_calls += 1
        return self._non_terminal()

    def _non_terminal(self) -> list[tuple[UUID, TaskData]]:
        return [(task_id, data) for task_id, data in self._entries.items() if task_id not in self._terminal]


class FakeSink:
    """A minimal ``TaskSink`` that records enqueued tasks and can reject ids."""

    def __init__(self, *, full: bool = False, reject: set[UUID] | None = None) -> None:
        self.full = full
        self.reject: set[UUID] = reject or set()
        self.items: list[tuple[UUID, TaskData]] = []

    def task_queue_full(self) -> bool:
        return self.full

    def enqueue_task(self, task_id: UUID, task_data: TaskData) -> bool:
        if task_id in self.reject:
            return False
        self.items.append((task_id, task_data))
        return True


def _health() -> TransportHealth:
    async def _reconnect() -> None:
        return None

    return TransportHealth(
        reconnect=_reconnect,
        is_connected=lambda: True,
        logger=logging.getLogger(_LOGGER),
    )


def _transport(
    inbox: FakeInbox | None = None,
    *,
    task_qos: int = 2,
    topic: str = _TOPIC,
) -> tuple[MqttTransport, FakeInbox, list[tuple[str, bytes, int]]]:
    """Build a ``MqttTransport`` with a fake inbox and a recording publisher."""
    inbox = inbox if inbox is not None else FakeInbox()
    published: list[tuple[str, bytes, int]] = []

    async def publish(topic: str, payload: bytes, qos: int) -> None:
        published.append((topic, payload, qos))

    config = MqttWorkerConfig(service_name="svc", task_qos=task_qos)
    transport = MqttTransport(
        config=config,
        service_name="svc",
        topic=topic,
        inbox=inbox,
        health=_health(),
        publish=publish,
        logger=logging.getLogger(_LOGGER),
    )
    return transport, inbox, published


@pytest.mark.asyncio
async def test_fetch_drains_inbox_and_reports():
    """fetch drains pending entries into the sink in order and reports True;
    a second fetch does not re-enqueue already-handed-over tasks."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task="a"), TaskData(task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    transport, _, _ = _transport(inbox)
    transport.recovered = True  # skip recovery; exercise the drain path only

    sink = FakeSink()
    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, d1), (t2, d2)]

    assert await transport.fetch(sink) is False
    assert sink.items == [(t1, d1), (t2, d2)]


@pytest.mark.asyncio
async def test_fetch_returns_false_when_inbox_empty():
    """fetch reports False when the inbox holds no non-terminal entries."""
    transport, _, _ = _transport()
    transport.recovered = True

    assert await transport.fetch(FakeSink()) is False


@pytest.mark.asyncio
async def test_fetch_respects_backpressure_and_does_not_lose_rejected_task():
    """A full or rejecting sink stops the drain; the rejected task stays pending
    and is redelivered on the next fetch instead of being lost."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task="a"), TaskData(task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    transport, _, _ = _transport(inbox)
    transport.recovered = True

    full_sink = FakeSink(full=True)
    assert await transport.fetch(full_sink) is False
    assert full_sink.items == []

    reject_sink = FakeSink(reject={t1})
    assert await transport.fetch(reject_sink) is False
    assert reject_sink.items == []

    reject_sink.reject.clear()
    assert await transport.fetch(reject_sink) is True
    assert reject_sink.items == [(t1, d1), (t2, d2)]


@pytest.mark.asyncio
async def test_first_fetch_triggers_recovery_once():
    """The first fetch runs recovery and marks it done; later fetches do not
    re-run recovery."""
    t1 = uuid4()
    d1 = TaskData(task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, _, _ = _transport(inbox)
    assert transport.recovered is False

    sink = FakeSink()
    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, d1)]
    assert transport.recovered is True
    assert inbox.recover_calls == 1

    assert await transport.fetch(sink) is False
    assert inbox.recover_calls == 1


@pytest.mark.asyncio
async def test_fetch_keeps_recovery_pending_when_incomplete():
    """A queue-full interruption leaves recovery incomplete, so the next fetch
    retries the remainder (AR-051 mirror)."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task="a"), TaskData(task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    transport, _, _ = _transport(inbox)

    sink = FakeSink(reject={t2})
    assert await transport.fetch(sink) is True  # t1 accepted during recovery
    assert transport.recovered is False
    assert sink.items == [(t1, d1)]

    sink.reject.clear()
    assert await transport.fetch(sink) is True  # retry recovery: t2 accepted
    assert transport.recovered is True
    assert sink.items == [(t1, d1), (t2, d2)]


@pytest.mark.asyncio
async def test_requeue_publishes_encoded_envelope_and_leaves_entry_pending():
    """requeue publishes the versioned envelope at task_qos and leaves the inbox
    entry non-terminal (not marked terminal)."""
    t1 = uuid4()
    d1 = TaskData(task="a", payload=b"{}")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, used_inbox, published = _transport(inbox, task_qos=1)

    await transport.requeue(t1, d1)

    assert published == [(_TOPIC, encode_task_envelope(d1), 1)]
    assert used_inbox.mark_terminal_calls == []
    assert await used_inbox.pending() == [(t1, d1)]


@pytest.mark.asyncio
async def test_release_does_not_publish():
    """release drops the in-process claim without publishing or marking terminal."""
    t1 = uuid4()
    d1 = TaskData(task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, used_inbox, published = _transport(inbox)

    await transport.release(t1)

    assert published == []
    assert used_inbox.mark_terminal_calls == []
    assert await used_inbox.pending() == [(t1, d1)]


@pytest.mark.asyncio
async def test_on_started_marks_in_flight():
    """on_started marks the inbox entry in-flight."""
    t1 = uuid4()
    d1 = TaskData(task="a")
    inbox = FakeInbox()
    transport, used_inbox, _ = _transport(inbox)

    await transport.on_started(t1, d1)

    assert used_inbox.mark_in_flight_calls == [t1]


@pytest.mark.asyncio
async def test_ack_marks_terminal():
    """ack marks the inbox entry terminal and releases the in-process claim."""
    t1 = uuid4()
    d1 = TaskData(task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, used_inbox, _ = _transport(inbox)

    await transport.ack(t1, d1, None)

    assert used_inbox.mark_terminal_calls == [t1]
    assert await used_inbox.pending() == []


@pytest.mark.asyncio
async def test_on_progress_is_noop():
    """on_progress neither publishes nor mutates the inbox (design §10 #5)."""
    t1 = uuid4()
    inbox = FakeInbox()
    transport, used_inbox, published = _transport(inbox)

    await transport.on_progress(t1, 42.0)

    assert published == []
    assert used_inbox.mark_terminal_calls == []
    assert used_inbox.mark_in_flight_calls == []


@pytest.mark.asyncio
async def test_on_drain_leaves_entry_pending_and_does_not_reenqueue():
    """on_drain leaves the inbox entry non-terminal and neither publishes nor
    marks it terminal, so a restart redelivers it."""
    t1 = uuid4()
    d1 = TaskData(task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, used_inbox, published = _transport(inbox)

    await transport.on_drain(t1, d1)

    assert published == []
    assert used_inbox.mark_terminal_calls == []
    assert await used_inbox.pending() == [(t1, d1)]


@pytest.mark.asyncio
async def test_recover_pending_tasks_replays_oldest_first():
    """recover_pending_tasks enqueues entries in inbox order and reports a
    complete, productive recovery."""
    older, newer = uuid4(), uuid4()
    d_old, d_new = TaskData(task="old"), TaskData(task="new")
    inbox = FakeInbox()
    inbox.seed((older, d_old), (newer, d_new))
    transport, _, _ = _transport(inbox)

    sink = FakeSink()
    complete, enqueued = await transport.recover_pending_tasks(sink)

    assert (complete, enqueued) == (True, True)
    assert sink.items == [(older, d_old), (newer, d_new)]


@pytest.mark.asyncio
async def test_recover_pending_tasks_reports_incomplete_on_queue_full():
    """recover_pending_tasks stops at a rejected entry and reports recovery
    incomplete so the remainder is retried."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task="a"), TaskData(task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    transport, _, _ = _transport(inbox)

    sink = FakeSink(reject={t2})
    complete, enqueued = await transport.recover_pending_tasks(sink)

    assert complete is False
    assert enqueued is True
    assert sink.items == [(t1, d1)]


@pytest.mark.asyncio
async def test_refresh_leases_is_noop():
    """refresh_leases is a documented no-op (the file inbox has no leases)."""
    transport, _, _ = _transport()

    await transport.refresh_leases()
