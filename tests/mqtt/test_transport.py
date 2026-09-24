"""Tests for the MQTT transport (``MqttTransport``)."""

import logging
import time
from collections.abc import Callable
from uuid import UUID, uuid4

import msgspec
import pytest

from scietex.service.health import TransportHealth
from scietex.service.mqtt._aiomqtt import MqttError, Properties
from scietex.service.mqtt.config import MqttWorkerConfig
from scietex.service.mqtt.transport import MqttPublish, MqttTransport
from scietex.service.task_handler import TASK_CANCEL_TASK_NAME
from scietex.service.task_handler.schemas import TaskData, TaskProgress, TaskResult, TaskStatus, task_data_id
from scietex.service.task_handler.wire import decode_task_envelope, encode_task_envelope

_LOGGER = "test_transport"
_TOPIC = "scietex/svc/tasks"


class FakeClock:
    """Injectable monotonic clock with a manually advanced offset."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def _status_topic(task_id: UUID) -> str:
    """Per-task status topic for the default ``status_topic_prefix``."""
    return f"{_TOPIC}/{task_id}/status"


def _progress_topic(task_id: UUID) -> str:
    """Per-task progress topic for the default ``status_topic_prefix``."""
    return f"{_TOPIC}/{task_id}/progress"


def _owner_topic(task_id: UUID) -> str:
    """Per-task retained owner-marker topic (design §10.1)."""
    return f"{_TOPIC}/{task_id}/owner"


class FakeInbox:
    """In-memory ``MqttInbox`` for transport tests.

    Mirrors the ``MqttInbox`` Protocol semantics (``put`` persists a pending
    entry, ``mark_terminal`` tombstones it) while recording every call so tests
    can assert on the transport's interaction without touching the filesystem.
    """

    def __init__(self) -> None:
        self._entries: dict[UUID, TaskData] = {}
        self._terminal: set[UUID] = set()
        self.put_calls: list[UUID] = []
        self.mark_in_flight_calls: list[UUID] = []
        self.mark_terminal_calls: list[UUID] = []
        self.pending_calls = 0
        self.recover_calls = 0
        self.claim_calls: list[UUID] = []
        self.release_calls: list[UUID] = []
        self.refresh_calls: list[list[UUID]] = []
        #: Ids a peer already owns; ``claim`` returns False for these.
        self.peer_claimed: set[UUID] = set()
        #: Ids this fake currently holds a claim on.
        self.claimed: set[UUID] = set()

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
        self.claimed.discard(task_id)

    async def pending(self) -> list[TaskData]:
        self.pending_calls += 1
        return self._non_terminal()

    async def recover(self) -> list[TaskData]:
        self.recover_calls += 1
        return self._non_terminal()

    async def claim(self, task_id: UUID) -> bool:
        self.claim_calls.append(task_id)
        if task_id in self.peer_claimed:
            return False
        self.claimed.add(task_id)
        return True

    async def release(self, task_id: UUID) -> None:
        self.release_calls.append(task_id)
        self.claimed.discard(task_id)

    async def refresh(self, task_ids) -> None:
        self.refresh_calls.append(list(task_ids))

    async def prune_expired(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def _non_terminal(self) -> list[TaskData]:
        return [data for task_id, data in self._entries.items() if task_id not in self._terminal]


class FakeSink:
    """A minimal ``TaskSink`` that records enqueued tasks and can reject ids."""

    def __init__(self, *, full: bool = False, reject: set[UUID] | None = None) -> None:
        self.full = full
        self.reject: set[UUID] = reject or set()
        self.items: list[tuple[UUID, TaskData]] = []
        self.control_items: list[tuple[UUID, TaskData]] = []

    def task_queue_full(self) -> bool:
        return self.full

    def enqueue_task(self, task_data: TaskData) -> bool:
        task_id = task_data_id(task_data)
        if task_id in self.reject:
            return False
        self.items.append((task_id, task_data))
        return True

    def enqueue_control_task(self, task_data: TaskData) -> bool:
        task_id = task_data_id(task_data)
        if task_id in self.reject:
            return False
        self.control_items.append((task_id, task_data))
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
    control_inbox: FakeInbox | None = None,
    task_qos: int = 2,
    topic: str = _TOPIC,
    clock: Callable[[], float] | None = None,
    health: TransportHealth | None = None,
    publish: MqttPublish | None = None,
    **config_kwargs,
) -> tuple[MqttTransport, FakeInbox, list[tuple[str, bytes, int, bool, Properties | None]]]:
    """Build a ``MqttTransport`` with a fake data inbox and a recording publisher.

    The recording publisher captures ``(topic, payload, qos, retain, properties)``;
    extra ``config_kwargs`` override ``MqttWorkerConfig`` fields (e.g. the
    throttling thresholds), and ``clock``/``health``/``publish`` are injectable
    seams. ``control_inbox`` defaults to a fresh :class:`FakeInbox`; tests that
    assert on the control drain pass their own and reach it via
    ``transport._control_inbox``.
    """
    inbox = inbox if inbox is not None else FakeInbox()
    control_inbox = control_inbox if control_inbox is not None else FakeInbox()
    published: list[tuple[str, bytes, int, bool, Properties | None]] = []

    async def record(
        topic: str,
        payload: bytes,
        qos: int,
        *,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> None:
        published.append((topic, payload, qos, retain, properties))

    config = MqttWorkerConfig(service_name="svc", task_qos=task_qos, **config_kwargs)
    transport = MqttTransport(
        config=config,
        service_name="svc",
        topic=topic,
        inbox=inbox,
        control_inbox=control_inbox,
        health=health if health is not None else _health(),
        publish=publish if publish is not None else record,
        logger=logging.getLogger(_LOGGER),
        instance_id="worker-1",
        clock=clock if clock is not None else time.monotonic,
    )
    return transport, inbox, published


@pytest.mark.asyncio
async def test_fetch_drains_inbox_and_reports():
    """fetch drains pending entries into the sink in order and reports True;
    a second fetch does not re-enqueue already-handed-over tasks."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task_id=str(t1), task="a"), TaskData(task_id=str(t2), task="b")
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
    d1, d2 = TaskData(task_id=str(t1), task="a"), TaskData(task_id=str(t2), task="b")
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
async def test_fetch_skips_peer_claimed_entry_and_enqueues_next():
    """A data entry a peer already owns (claim returns False) is skipped without
    setting backpressure; the next claimable entry is still enqueued."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task_id=str(t1), task="a"), TaskData(task_id=str(t2), task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    inbox.peer_claimed.add(t1)
    transport, _, _ = _transport(inbox)
    transport.recovered = True

    sink = FakeSink()
    assert await transport.fetch(sink) is True
    assert sink.items == [(t2, d2)]
    assert t1 in inbox.claim_calls
    assert t1 not in inbox.claimed


@pytest.mark.asyncio
async def test_fetch_releases_claim_when_sink_rejects():
    """A claim taken before a rejected enqueue is released, so the entry is not
    stranded under a claim the worker never handed over."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, _, _ = _transport(inbox)
    transport.recovered = True

    sink = FakeSink(reject={t1})
    assert await transport.fetch(sink) is False
    assert t1 in inbox.claim_calls
    assert t1 in inbox.release_calls
    assert t1 not in inbox.claimed


@pytest.mark.asyncio
async def test_recover_skips_peer_claimed_entry():
    """Recovery applies the same claim gate: a peer-owned entry is skipped."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task_id=str(t1), task="a"), TaskData(task_id=str(t2), task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    inbox.peer_claimed.add(t1)
    transport, _, _ = _transport(inbox)

    sink = FakeSink()
    complete, enqueued = await transport.recover_pending_tasks(sink)
    assert complete is True
    assert enqueued is True
    assert sink.items == [(t2, d2)]


@pytest.mark.asyncio
async def test_requeue_releases_claim():
    """requeue releases the cross-process claim so the re-published copy is
    immediately claimable."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, _, _ = _transport(inbox)
    transport.recovered = True
    await transport.fetch(FakeSink())
    assert t1 in inbox.claimed

    await transport.requeue(d1)

    assert t1 in inbox.release_calls
    assert t1 not in inbox.claimed


@pytest.mark.asyncio
async def test_on_drain_releases_claim():
    """on_drain releases the cross-process claim for a data task."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, _, _ = _transport(inbox)
    transport.recovered = True
    await transport.fetch(FakeSink())
    assert t1 in inbox.claimed

    await transport.on_drain(d1)

    assert t1 in inbox.release_calls
    assert t1 not in inbox.claimed


@pytest.mark.asyncio
async def test_refresh_leases_refreshes_enqueued_data_ids():
    """refresh_leases renews the claims on this worker's enqueued data tasks."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, _, _ = _transport(inbox)
    transport.recovered = True
    await transport.fetch(FakeSink())

    await transport.refresh_leases()

    assert inbox.refresh_calls == [[t1]]


@pytest.mark.asyncio
async def test_fetch_delivers_control_inbox_when_data_lane_full():
    """A control entry in the control inbox is enqueued even when the data lane
    is full: the control drain never consults data backpressure (design §5.1)."""
    t_ctrl = uuid4()
    d_ctrl = TaskData(task_id=str(t_ctrl), task=TASK_CANCEL_TASK_NAME)
    control_inbox = FakeInbox()
    control_inbox.seed((t_ctrl, d_ctrl))
    transport, _, _ = _transport(control_inbox=control_inbox)
    transport.recovered = True  # skip recovery; exercise the control drain only

    sink = FakeSink(full=True)
    assert await transport.fetch(sink) is True
    assert sink.control_items == [(t_ctrl, d_ctrl)]
    assert sink.items == []


@pytest.mark.asyncio
async def test_ack_control_marks_control_inbox_terminal():
    """ack on a control entry marks the *control* inbox terminal, not the data
    inbox: the owning inbox is chosen by which enqueued set holds the id."""
    t_ctrl = uuid4()
    d_ctrl = TaskData(task_id=str(t_ctrl), task=TASK_CANCEL_TASK_NAME)
    control_inbox = FakeInbox()
    control_inbox.seed((t_ctrl, d_ctrl))
    data_inbox = FakeInbox()
    transport, _, _ = _transport(inbox=data_inbox, control_inbox=control_inbox)
    transport.recovered = True

    sink = FakeSink()
    assert await transport.fetch(sink) is True
    await transport.ack(d_ctrl, None)

    assert control_inbox.mark_terminal_calls == [t_ctrl]
    assert data_inbox.mark_terminal_calls == []


@pytest.mark.asyncio
async def test_recover_pending_tasks_replays_control_inbox_entry():
    """recover_pending_tasks replays a non-terminal control entry from the
    control inbox (control recovery is not subject to data backpressure)."""
    t_ctrl = uuid4()
    d_ctrl = TaskData(task_id=str(t_ctrl), task=TASK_CANCEL_TASK_NAME)
    control_inbox = FakeInbox()
    control_inbox.seed((t_ctrl, d_ctrl))
    transport, _, _ = _transport(control_inbox=control_inbox)

    sink = FakeSink()
    complete, enqueued = await transport.recover_pending_tasks(sink)

    assert (complete, enqueued) == (True, True)
    assert sink.control_items == [(t_ctrl, d_ctrl)]


@pytest.mark.asyncio
async def test_fetch_does_not_double_deliver_control_inbox_entry():
    """A control entry in the control inbox is delivered once: the control drain
    records it in ``_control_enqueued`` so a repeat poll does not re-enqueue it,
    and the data drain never sees it (it is not in the data inbox)."""
    t_ctrl = uuid4()
    d_ctrl = TaskData(task_id=str(t_ctrl), task=TASK_CANCEL_TASK_NAME)
    control_inbox = FakeInbox()
    control_inbox.seed((t_ctrl, d_ctrl))
    transport, _, _ = _transport(control_inbox=control_inbox)
    transport.recovered = True

    sink = FakeSink()
    assert await transport.fetch(sink) is True
    assert sink.control_items == [(t_ctrl, d_ctrl)]

    assert await transport.fetch(sink) is False
    assert sink.control_items == [(t_ctrl, d_ctrl)]


@pytest.mark.asyncio
async def test_first_fetch_triggers_recovery_once():
    """The first fetch runs recovery and marks it done; later fetches do not
    re-run recovery."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
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
    d1, d2 = TaskData(task_id=str(t1), task="a"), TaskData(task_id=str(t2), task="b")
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
    d1 = TaskData(task_id=str(t1), task="a", payload=b"{}")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, used_inbox, published = _transport(inbox, task_qos=1)

    await transport.requeue(d1)

    # The envelope re-publish carries no user property: the task id now travels
    # inside the encoded TaskData payload, so the worker's message loop decodes
    # the id from the envelope rather than from an MQTT 5 user property. The
    # follow-up ``queued`` status is retained and carries the message-expiry
    # property, and the retained owner marker follows it (design §10.1).
    assert len(published) == 3
    topic, payload, qos, retain, envelope_properties = published[0]
    assert (topic, payload, qos, retain) == (_TOPIC, encode_task_envelope(d1), 1, False)
    assert envelope_properties is None  # no user property on the re-published copy
    assert decode_task_envelope(payload) == d1  # the id travels inside the payload
    queued_properties = published[1][4]
    assert queued_properties is not None
    assert queued_properties.MessageExpiryInterval == 86400
    assert published[2][0] == _owner_topic(t1)
    assert used_inbox.mark_terminal_calls == []
    assert await used_inbox.pending() == [d1]


@pytest.mark.asyncio
async def test_on_started_marks_in_flight():
    """on_started marks the inbox entry in-flight."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    transport, used_inbox, _ = _transport(inbox)

    await transport.on_started(d1)

    assert used_inbox.mark_in_flight_calls == [t1]


@pytest.mark.asyncio
async def test_ack_marks_terminal():
    """ack marks the inbox entry terminal and releases the in-process claim."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, used_inbox, _ = _transport(inbox)

    await transport.ack(d1, None)

    assert used_inbox.mark_terminal_calls == [t1]
    assert await used_inbox.pending() == []


@pytest.mark.asyncio
async def test_on_progress_publishes_progress_tick():
    """on_progress publishes a non-retained QoS 0 ``TaskProgress`` to the
    per-task progress topic (replacing the pre-addendum no-op)."""
    t1 = uuid4()
    transport, _, published = _transport()

    await transport.on_progress(t1, 42.0)

    assert len(published) == 1
    topic, payload, qos, retain, _ = published[0]
    assert topic == _progress_topic(t1)
    assert qos == 0
    assert retain is False
    assert msgspec.msgpack.decode(payload, type=TaskProgress) == TaskProgress(progress=True, value=42.0)


@pytest.mark.asyncio
async def test_progress_publish_has_no_expiry():
    """Progress ticks publish with no properties (never retained, never expired)."""
    t1 = uuid4()
    transport, _, published = _transport()

    await transport.on_progress(t1, 42.0)

    assert len(published) == 1
    assert published[0][4] is None


@pytest.mark.asyncio
async def test_on_drain_leaves_entry_pending_and_does_not_reenqueue():
    """on_drain leaves the inbox entry non-terminal and neither publishes nor
    marks it terminal, so a restart redelivers it."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, used_inbox, published = _transport(inbox)

    await transport.on_drain(d1)

    assert published == []
    assert used_inbox.mark_terminal_calls == []
    assert await used_inbox.pending() == [d1]


@pytest.mark.asyncio
async def test_recover_pending_tasks_replays_oldest_first():
    """recover_pending_tasks enqueues entries in inbox order and reports a
    complete, productive recovery."""
    older, newer = uuid4(), uuid4()
    d_old, d_new = TaskData(task_id=str(older), task="old"), TaskData(task_id=str(newer), task="new")
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
    d1, d2 = TaskData(task_id=str(t1), task="a"), TaskData(task_id=str(t2), task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    transport, _, _ = _transport(inbox)

    sink = FakeSink(reject={t2})
    complete, enqueued = await transport.recover_pending_tasks(sink)

    assert complete is False
    assert enqueued is True
    assert sink.items == [(t1, d1)]


@pytest.mark.asyncio
async def test_refresh_leases_delegates_to_inbox():
    """refresh_leases delegates to the inbox's refresh over the enqueued set
    (a no-op for the file/memory backends, a lease renewal for sqlite)."""
    inbox = FakeInbox()
    transport, _, _ = _transport(inbox)

    await transport.refresh_leases()

    assert inbox.refresh_calls == [[]]


@pytest.mark.asyncio
async def test_on_started_publishes_running_status():
    """on_started publishes a retained QoS 1 ``running`` TaskStatus to the
    per-task status topic."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    transport, _, published = _transport()

    await transport.on_started(d1)

    assert len(published) == 1
    topic, payload, qos, retain, _ = published[0]
    assert topic == _status_topic(t1)
    assert qos == 1
    assert retain is True
    status = msgspec.msgpack.decode(payload, type=TaskStatus)
    assert status.status == "running"
    assert status.task_id == str(t1)
    assert status.service == "svc"
    assert status.task == "a"
    assert status.result is None
    assert status.data is None
    assert status.error == ""
    assert status.error_code == ""
    assert status.progress == TaskProgress()
    assert status.created_at == status.updated_at


@pytest.mark.asyncio
async def test_status_publish_carries_message_expiry():
    """A retained status publish carries an MQTT 5 message-expiry property set
    to ``status_ttl``, so the broker ages out the per-task marker."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    transport, _, published = _transport(status_ttl=3600)

    await transport.on_started(d1)

    assert len(published) == 1
    properties = published[0][4]
    assert properties is not None
    assert properties.MessageExpiryInterval == 3600


@pytest.mark.asyncio
async def test_status_publish_without_ttl_has_no_expiry():
    """``status_ttl=None`` publishes the retained status with no properties, so
    no message expiry is set."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    transport, _, published = _transport(status_ttl=None)

    await transport.on_started(d1)

    assert len(published) == 1
    assert published[0][4] is None


@pytest.mark.asyncio
async def test_ack_success_publishes_completed_with_result():
    """ack with a success TaskResult publishes ``completed`` with ``result`` set
    to the result payload."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    transport, _, published = _transport()

    await transport.ack(d1, TaskResult(status="success", payload=b"done"))

    assert len(published) == 1
    topic, payload, qos, retain, _ = published[0]
    assert topic == _status_topic(t1)
    assert qos == 1
    assert retain is True
    status = msgspec.msgpack.decode(payload, type=TaskStatus)
    assert status.status == "completed"
    assert status.result == b"done"
    assert status.error == ""
    assert status.error_code == ""


@pytest.mark.asyncio
async def test_ack_non_retryable_error_publishes_failed():
    """ack with a non-retryable error publishes ``failed`` with ``error`` and
    ``error_code`` populated."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    transport, _, published = _transport()

    await transport.ack(d1, TaskResult(status="error", error="boom", error_code="PERMANENT"))

    assert len(published) == 1
    _, payload, _, _, _ = published[0]
    status = msgspec.msgpack.decode(payload, type=TaskStatus)
    assert status.status == "failed"
    assert status.error == "boom"
    assert status.error_code == "PERMANENT"
    assert status.result is None
    assert status.data is None


@pytest.mark.asyncio
async def test_ack_deliberate_cancel_publishes_cancelled_with_data():
    """ack with ``cancel_reason="deliberate"`` publishes ``cancelled`` with the
    original TaskData embedded in ``data``."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a", payload=b"{}")
    transport, _, published = _transport()

    await transport.ack(d1, None, cancel_reason="deliberate")

    assert len(published) == 1
    _, payload, _, _, _ = published[0]
    status = msgspec.msgpack.decode(payload, type=TaskStatus)
    assert status.status == "cancelled"
    assert status.data == d1
    assert status.error == "canceled"
    assert status.error_code == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_reason", ["timeout", "shutdown"])
async def test_ack_timeout_or_shutdown_cancel_publishes_failed(cancel_reason):
    """ack with a timeout/shutdown cancel reason publishes ``failed`` (not
    ``cancelled``) with no ``data``."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    transport, _, published = _transport()

    await transport.ack(d1, None, cancel_reason=cancel_reason)

    assert len(published) == 1
    _, payload, _, _, _ = published[0]
    status = msgspec.msgpack.decode(payload, type=TaskStatus)
    assert status.status == "failed"
    assert status.data is None
    assert status.error == "canceled"


@pytest.mark.asyncio
async def test_ack_retryable_error_publishes_nothing_and_leaves_entry_non_terminal():
    """A retryable-error ack publishes no status and leaves the inbox entry
    non-terminal (AR-077b mirror): ``requeue`` already re-published the task
    under the same id, so a tombstone would suppress the retry copy."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, used_inbox, published = _transport(inbox)

    await transport.ack(d1, TaskResult(status="error", error="transient", retryable=True))

    assert published == []
    assert used_inbox.mark_terminal_calls == []
    assert await used_inbox.pending() == [d1]


@pytest.mark.asyncio
async def test_requeue_publishes_queued_status_after_envelope():
    """requeue publishes a retained QoS 1 ``queued`` status in addition to the
    non-retained envelope, advertising the task's return to the source queue."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    transport, _, published = _transport()

    await transport.requeue(d1)

    assert len(published) == 3
    envelope_topic, _, _, envelope_retain, _ = published[0]
    assert envelope_topic == _TOPIC
    assert envelope_retain is False
    topic, payload, qos, retain, _ = published[1]
    assert topic == _status_topic(t1)
    assert qos == 1
    assert retain is True
    assert msgspec.msgpack.decode(payload, type=TaskStatus).status == "queued"
    assert published[2][0] == _owner_topic(t1)


@pytest.mark.asyncio
async def test_fetch_publishes_queued_once_per_accepted_task():
    """fetch publishes a retained QoS 1 ``queued`` status once per accepted task
    and never republishes on a repeat poll."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task_id=str(t1), task="a"), TaskData(task_id=str(t2), task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    transport, _, published = _transport(inbox)
    transport.recovered = True  # skip recovery; exercise the drain path only

    sink = FakeSink()
    assert await transport.fetch(sink) is True
    # Each accepted task publishes its ``queued`` status followed by its retained
    # owner marker (design §10.1).
    assert [p[0] for p in published] == [
        _status_topic(t1),
        _owner_topic(t1),
        _status_topic(t2),
        _owner_topic(t2),
    ]
    for topic, payload, qos, retain, _ in published:
        assert (qos, retain) == (1, True)
        if topic.endswith("/status"):
            assert msgspec.msgpack.decode(payload, type=TaskStatus).status == "queued"

    assert await transport.fetch(sink) is False
    assert len(published) == 4  # repeat poll republishes nothing


@pytest.mark.asyncio
async def test_recovery_publishes_queued_for_replayed_tasks():
    """recover_pending_tasks publishes ``queued`` once per recovered task, so a
    task redelivered after a restart re-advertises itself."""
    t1, t2 = uuid4(), uuid4()
    d1, d2 = TaskData(task_id=str(t1), task="a"), TaskData(task_id=str(t2), task="b")
    inbox = FakeInbox()
    inbox.seed((t1, d1), (t2, d2))
    transport, _, published = _transport(inbox)

    sink = FakeSink()
    complete, enqueued = await transport.recover_pending_tasks(sink)

    assert (complete, enqueued) == (True, True)
    assert [p[0] for p in published] == [
        _status_topic(t1),
        _owner_topic(t1),
        _status_topic(t2),
        _owner_topic(t2),
    ]
    for topic, payload, qos, retain, _ in published:
        assert (qos, retain) == (1, True)
        if topic.endswith("/status"):
            assert msgspec.msgpack.decode(payload, type=TaskStatus).status == "queued"


@pytest.mark.asyncio
async def test_queued_status_publishes_retained_owner_marker():
    """The ``queued`` status is followed by a retained owner marker carrying the
    worker's instance_id, so a submitter can resolve the task's owner (design
    §10.1)."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    inbox = FakeInbox()
    inbox.seed((t1, d1))
    transport, _, published = _transport(inbox)
    transport.recovered = True  # skip recovery; exercise the drain path only

    assert await transport.fetch(FakeSink()) is True

    owner = [p for p in published if p[0] == _owner_topic(t1)]
    assert len(owner) == 1
    topic, payload, qos, retain, properties = owner[0]
    assert topic == _owner_topic(t1)
    assert payload == b"worker-1"
    assert (qos, retain) == (1, True)
    assert properties is not None
    assert properties.MessageExpiryInterval == 86400


@pytest.mark.asyncio
async def test_publish_status_failure_is_reported_not_raised(caplog):
    """A status publish that raises MqttError is logged at WARNING, reported to
    health, and never raised into the hook."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    health = _health()

    async def failing_publish(
        topic: str,
        payload: bytes,
        qos: int,
        *,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> None:
        raise MqttError("boom")

    transport, _, _ = _transport(health=health, publish=failing_publish)

    with caplog.at_level(logging.WARNING):
        await transport.on_started(d1)

    assert health.degraded is True
    assert health.last_error == "boom"
    assert any("Failed to publish running status" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
async def test_publish_progress_failure_is_reported_not_raised(caplog):
    """A progress publish that raises MqttError is logged at DEBUG, reported to
    health, and never raised into ``on_progress``."""
    t1 = uuid4()
    health = _health()

    async def failing_publish(
        topic: str,
        payload: bytes,
        qos: int,
        *,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> None:
        raise MqttError("boom")

    transport, _, _ = _transport(health=health, publish=failing_publish)

    with caplog.at_level(logging.DEBUG):
        await transport.on_progress(t1, 42.0)

    assert health.degraded is True
    assert health.last_error == "boom"
    assert any("Failed to publish progress" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
async def test_on_progress_first_tick_publishes_then_coalesces():
    """The first progress tick always publishes; later ticks inside the interval
    coalesce to pending and publish nothing."""
    t1 = uuid4()
    clock = FakeClock()
    transport, _, published = _transport(clock=clock, progress_min_interval=1.0)

    await transport.on_progress(t1, 10.0)
    assert len(published) == 1
    topic, payload, qos, retain, _ = published[0]
    assert topic == _progress_topic(t1)
    assert qos == 0
    assert retain is False
    assert msgspec.msgpack.decode(payload, type=TaskProgress) == TaskProgress(progress=True, value=10.0)

    await transport.on_progress(t1, 20.0)  # inside interval
    await transport.on_progress(t1, 30.0)  # inside interval
    assert len(published) == 1


@pytest.mark.asyncio
async def test_on_progress_publishes_after_interval_elapses():
    """Advancing the clock past the interval makes the next tick publish (with
    the newest value) instead of coalescing."""
    t1 = uuid4()
    clock = FakeClock()
    transport, _, published = _transport(clock=clock, progress_min_interval=1.0)

    await transport.on_progress(t1, 10.0)
    await transport.on_progress(t1, 20.0)
    await transport.on_progress(t1, 30.0)  # coalesced, never published
    assert len(published) == 1

    clock.now = 1.5
    await transport.on_progress(t1, 40.0)
    assert len(published) == 2
    assert msgspec.msgpack.decode(published[1][1], type=TaskProgress) == TaskProgress(progress=True, value=40.0)


@pytest.mark.asyncio
async def test_on_progress_delta_forces_publish_inside_interval():
    """A large value jump (``progress_min_delta``) publishes even while the
    interval threshold is unmet."""
    t1 = uuid4()
    clock = FakeClock()
    transport, _, published = _transport(clock=clock, progress_min_interval=10.0, progress_min_delta=5.0)

    await transport.on_progress(t1, 0.0)
    await transport.on_progress(t1, 1.0)  # delta 1.0 < 5.0: coalesce
    assert len(published) == 1

    await transport.on_progress(t1, 10.0)  # delta 10.0 >= 5.0: publish
    assert len(published) == 2
    assert msgspec.msgpack.decode(published[1][1], type=TaskProgress) == TaskProgress(progress=True, value=10.0)


@pytest.mark.asyncio
async def test_on_progress_both_thresholds_zero_publishes_every_call():
    """Both thresholds disabled (0) publishes every call, throttling off."""
    t1 = uuid4()
    clock = FakeClock()
    transport, _, published = _transport(clock=clock, progress_min_interval=0.0, progress_min_delta=0.0)

    await transport.on_progress(t1, 1.0)
    await transport.on_progress(t1, 2.0)
    await transport.on_progress(t1, 3.0)

    assert len(published) == 3
    values = [msgspec.msgpack.decode(p[1], type=TaskProgress).value for p in published]
    assert values == [1.0, 2.0, 3.0]


@pytest.mark.asyncio
async def test_ack_flushes_pending_progress_before_terminal_status():
    """ack flushes a coalesced pending value to the progress topic immediately
    before the terminal status, then drops the throttle state."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    clock = FakeClock()
    transport, _, published = _transport(clock=clock, progress_min_interval=1.0)

    await transport.on_progress(t1, 10.0)  # publishes (first tick)
    await transport.on_progress(t1, 20.0)  # coalesces to pending
    assert len(published) == 1

    await transport.ack(d1, TaskResult(status="success", payload=b"ok"))

    assert [p[0] for p in published] == [_progress_topic(t1), _progress_topic(t1), _status_topic(t1)]
    assert msgspec.msgpack.decode(published[1][1], type=TaskProgress) == TaskProgress(progress=True, value=20.0)
    assert msgspec.msgpack.decode(published[2][1], type=TaskStatus).status == "completed"


@pytest.mark.asyncio
async def test_requeue_drops_pending_progress_without_flushing():
    """requeue drops the throttle state without flushing the pending tick: the
    fresh run must not inherit a stale progress value."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    clock = FakeClock()
    transport, _, published = _transport(clock=clock, progress_min_interval=1.0)

    await transport.on_progress(t1, 10.0)  # publishes (first tick)
    await transport.on_progress(t1, 20.0)  # coalesces to pending
    assert [p[0] for p in published] == [_progress_topic(t1)]

    await transport.requeue(d1)

    # Envelope + queued status + owner marker go out, but the pending tick is not
    # flushed.
    assert [p[0] for p in published] == [_progress_topic(t1), _TOPIC, _status_topic(t1), _owner_topic(t1)]

    await transport.on_progress(t1, 5.0)  # state dropped: publishes as a fresh first tick
    assert [p[0] for p in published] == [
        _progress_topic(t1),
        _TOPIC,
        _status_topic(t1),
        _owner_topic(t1),
        _progress_topic(t1),
    ]


@pytest.mark.asyncio
async def test_on_drain_drops_pending_progress():
    """on_drain drops the throttle state without flushing the pending tick
    and publishes nothing."""
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    clock = FakeClock()
    transport, _, published = _transport(clock=clock, progress_min_interval=1.0)

    await transport.on_progress(t1, 10.0)  # publishes (first tick)
    await transport.on_progress(t1, 20.0)  # coalesces to pending
    assert [p[0] for p in published] == [_progress_topic(t1)]

    await transport.on_drain(d1)

    # on_drain publishes neither the pending tick nor anything else.
    assert [p[0] for p in published] == [_progress_topic(t1)]

    await transport.on_progress(t1, 5.0)  # state dropped: publishes as a fresh first tick
    assert [p[0] for p in published] == [_progress_topic(t1), _progress_topic(t1)]
