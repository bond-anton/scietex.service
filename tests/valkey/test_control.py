"""ValkeyTransport control-stream intake tests (AR-123 §4).

Exercises the group-less ``XREAD`` intake path for the per-worker directed
control stream and the service-scoped broadcast control stream: enqueue,
no-lease, misroute rejection, deferral, per-stream cursors, fan-out, and the
ack/requeue/on_started/on_drain control branches. Retention/TTL (step 6) is out
of scope here.
"""

from collections.abc import Callable
from uuid import UUID

import pytest

from scietex.service import ValkeyWorker
from scietex.service.task_handler import CONFIG_APPLY_TASK_NAME, TASK_CANCEL_TASK_NAME
from scietex.service.task_handler.schemas import TaskData, TaskResult, TaskTimeout
from scietex.service.task_handler.wire import encode_task_envelope
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig

from ._helpers import DummyClient, _entry


class _Sink:
    """Minimal ``TaskSink``: records accepted tasks and rejects on demand.

    ``accept`` is either a constant bool or a per-task predicate, so a test can
    reject data while still accepting control (or vice versa).
    """

    def __init__(self, accept: bool | Callable[[TaskData], bool] = True):
        self.enqueued: list[TaskData] = []
        self._accept = accept

    def _accepts(self, task_data: TaskData) -> bool:
        return self._accept(task_data) if callable(self._accept) else self._accept

    def enqueue_task(self, task_data: TaskData) -> bool:
        if self._accepts(task_data):
            self.enqueued.append(task_data)
            return True
        return False

    def enqueue_control_task(self, task_data: TaskData) -> bool:
        if self._accepts(task_data):
            self.enqueued.append(task_data)
            return True
        return False

    def task_queue_full(self) -> bool:
        return False


def _control_task(task_id: UUID) -> TaskData:
    """Build a directed control ``TaskData`` (a ``cancel_task`` command)."""
    return TaskData(
        task_id=str(task_id),
        task=TASK_CANCEL_TASK_NAME,
        payload=b"",
        timeout=TaskTimeout(timeout=None, timeout_action="discard"),
        canceled_action="discard",
    )


def _broadcast_task(task_id: UUID) -> TaskData:
    """Build a broadcast control ``TaskData`` (a ``config:apply`` command)."""
    return TaskData(
        task_id=str(task_id),
        task=CONFIG_APPLY_TASK_NAME,
        payload=b"",
        timeout=TaskTimeout(timeout=None, timeout_action="discard"),
        canceled_action="discard",
    )


def _control_worker(client: DummyClient, **config) -> ValkeyWorker:
    """Build a ValkeyWorker with an injected client, ready to fetch."""
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig(), **config))
    worker._client = client
    worker._transport.recovered = True  # skip recovery; exercise the control read only
    return worker


@pytest.mark.asyncio
async def test_directed_read_enqueues_control_task_without_lease():
    """A control envelope on the directed stream is enqueued, mapped to its
    (stream, entry id), and never leased (AR-123 §4.5/§4.6)."""
    control_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = _control_task(control_id)
    client = DummyClient(xread_results=[_entry(b"1-0", encode_task_envelope(task_data)), None])
    worker = _control_worker(client)
    sink = _Sink()

    enqueued = await worker._transport.fetch(sink)

    assert enqueued is True
    assert sink.enqueued == [task_data]
    assert worker._control_entry_ids[control_id] == (worker._control_stream_name, b"1-0")
    # No lease write for control: the only key writes would come from lease/status
    # stores, and neither runs during fetch for a control entry.
    assert client.sets == []


@pytest.mark.asyncio
async def test_non_control_entry_on_control_stream_is_enqueued_as_control():
    """A task read from a control stream is delivered to the control lane
    regardless of its task type: the lane is decided by the channel, not by
    classifying the payload, so nothing is skipped at the transport."""
    data_id = UUID("11111111-1111-1111-1111-111111111111")
    data_task = TaskData(task_id=str(data_id), task="dummy", payload=b"{}")
    client = DummyClient(xread_results=[_entry(b"1-0", encode_task_envelope(data_task)), None])
    worker = _control_worker(client)
    sink = _Sink()

    enqueued = await worker._transport.fetch(sink)

    assert enqueued is True
    assert sink.enqueued == [data_task]
    assert worker._control_entry_ids[data_id] == (worker._control_stream_name, b"1-0")


@pytest.mark.asyncio
async def test_control_lane_full_defers_then_retries():
    """A rejected control entry is held in the bounded deferred buffer and
    retried on the next poll once the lane accepts it."""
    control_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = _control_task(control_id)
    client = DummyClient(xread_results=[_entry(b"1-0", encode_task_envelope(task_data)), None])
    worker = _control_worker(client)
    sink = _Sink(accept=False)

    enqueued = await worker._transport.fetch(sink)

    assert enqueued is False
    assert sink.enqueued == []
    assert len(worker._transport._control_deferred) == 1

    # Lane frees up; the next fetch flushes the deferred entry (no new read).
    sink._accept = True
    client.xread_results = [None, None]
    enqueued = await worker._transport.fetch(sink)

    assert enqueued is True
    assert len(worker._transport._control_deferred) == 0
    assert sink.enqueued == [task_data]
    assert worker._control_entry_ids[control_id] == (worker._control_stream_name, b"1-0")


@pytest.mark.asyncio
async def test_ack_control_xdels_entry_without_xack_or_lease_delete():
    """A control ack XDELs the recorded entry and writes a terminal record, with
    no XACK (no group) and no lease delete (AR-123 §4.5)."""
    control_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = _control_task(control_id)
    client = DummyClient()
    worker = _control_worker(client)
    worker._control_entry_ids[control_id] = (worker._control_stream_name, b"1-0")

    await worker.on_task_completed(task_data, TaskResult(status="success"))

    assert client.deleted == [(worker._control_stream_name, [b"1-0"])]
    assert client.acked == []
    assert client.deleted_keys == []


@pytest.mark.asyncio
async def test_requeue_control_is_a_noop():
    """requeue() never re-publishes a control command (AR-123 §4.7): no XADD."""
    control_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = _control_task(control_id)
    client = DummyClient()
    worker = _control_worker(client)
    worker._control_entry_ids[control_id] = (worker._control_stream_name, b"1-0")

    await worker._transport.requeue(task_data)

    assert client.added == []


@pytest.mark.asyncio
async def test_on_started_control_writes_running_record_but_no_lease():
    """on_started() publishes the running record for a control task but skips
    the lease write (AR-123 §4.5/§4.6)."""
    control_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = _control_task(control_id)
    client = DummyClient()
    worker = _control_worker(client)
    worker._control_entry_ids[control_id] = (worker._control_stream_name, b"1-0")

    await worker.on_task_started(task_data)

    assert len(client.sets) == 1
    key, _value, _expiry = client.sets[0]
    assert key == worker._task_status.key(control_id)
    assert key != worker._task_lease.key(control_id)


@pytest.mark.asyncio
async def test_on_drain_control_does_not_delete_lease():
    """on_drain() releases no lease for a control entry (AR-123 §4.6)."""
    control_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = _control_task(control_id)
    client = DummyClient()
    worker = _control_worker(client)
    worker._control_entry_ids[control_id] = (worker._control_stream_name, b"1-0")

    await worker._transport.on_drain(task_data)

    assert client.deleted_keys == []


@pytest.mark.asyncio
async def test_control_read_happens_when_data_lane_is_full():
    """Control is polled even when the data deferred buffer is at capacity (so
    the data read is skipped): the control entry is still enqueued (AR-123 §4.4)."""
    control_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = _control_task(control_id)
    client = DummyClient(xread_results=[_entry(b"1-0", encode_task_envelope(task_data)), None])
    worker = _control_worker(client, task_fetch_batch_size=2)
    # Seed the data deferred buffer to capacity; a sink that rejects data but
    # accepts control keeps it full, so the data read is skipped.
    for i in range(2):
        d_id = UUID(f"aaaaaaaa-aaaa-aaaa-aaaa-{i:012d}")
        worker._transport._deferred.append((d_id, TaskData(task_id=str(d_id), task="dummy", payload=b"{}"), b"1-0"))
    sink = _Sink(accept=lambda t: t.task == TASK_CANCEL_TASK_NAME)

    enqueued = await worker._transport.fetch(sink)

    assert enqueued is True
    assert sink.enqueued == [task_data]
    assert client.xreadgroup_calls == [], "data read must be skipped while the deferred buffer is full"
    assert client.xread_calls, "control read must still happen despite data backpressure"


@pytest.mark.asyncio
async def test_control_cursor_advances_to_last_entry_id():
    """The in-memory cursor advances from ``$`` to the last entry id seen, so the
    next read resumes after it (AR-123 §4.2)."""
    first_id = UUID("11111111-1111-1111-1111-111111111111")
    second_id = UUID("22222222-2222-2222-2222-222222222222")
    client = DummyClient(xread_results=[_entry(b"1-0", encode_task_envelope(_control_task(first_id))), None])
    worker = _control_worker(client)
    sink = _Sink()

    await worker._transport.fetch(sink)

    client.xread_results = [_entry(b"2-0", encode_task_envelope(_control_task(second_id))), None]
    await worker._transport.fetch(sink)

    directed_calls = [c for c in client.xread_calls if worker._control_stream_name in c[0]]
    assert directed_calls[0][0] == {worker._control_stream_name: "$"}
    assert directed_calls[1][0] == {worker._control_stream_name: b"1-0"}


@pytest.mark.asyncio
async def test_broadcast_read_enqueues_control_task():
    """A control envelope on the service-scoped broadcast stream is enqueued and
    mapped to its (broadcast stream, entry id) pair (AR-123 §4.4)."""
    control_id = UUID("33333333-3333-3333-3333-333333333333")
    task_data = _broadcast_task(control_id)
    client = DummyClient(xread_results=[None, _entry(b"7-0", encode_task_envelope(task_data))])
    worker = _control_worker(client)
    sink = _Sink()

    enqueued = await worker._transport.fetch(sink)

    assert enqueued is True
    assert sink.enqueued == [task_data]
    assert worker._control_entry_ids[control_id] == (worker._control_broadcast_stream_name, b"7-0")
    assert client.sets == []


@pytest.mark.asyncio
async def test_broadcast_read_uses_broadcast_stream_name():
    """The broadcast read issues ``XREAD`` against the broadcast stream with a
    ``$``-seeded cursor (AR-123 §4.2/§4.4)."""
    control_id = UUID("33333333-3333-3333-3333-333333333333")
    task_data = _broadcast_task(control_id)
    client = DummyClient(xread_results=[None, _entry(b"7-0", encode_task_envelope(task_data))])
    worker = _control_worker(client)
    sink = _Sink()

    await worker._transport.fetch(sink)

    broadcast_calls = [c for c in client.xread_calls if worker._control_broadcast_stream_name in c[0]]
    assert broadcast_calls[0][0] == {worker._control_broadcast_stream_name: "$"}


@pytest.mark.asyncio
async def test_two_workers_each_read_same_broadcast_entry():
    """Two workers reading the broadcast stream with plain ``XREAD`` both receive
    the same entry — a shared consumer group could not fan it out (AR-123 §4.1)."""
    control_id = UUID("44444444-4444-4444-4444-444444444444")
    task_data = _broadcast_task(control_id)
    entry = _entry(b"7-0", encode_task_envelope(task_data))
    worker_a = _control_worker(DummyClient(xread_results=[None, entry]))
    worker_b = _control_worker(DummyClient(xread_results=[None, entry]))
    sink_a, sink_b = _Sink(), _Sink()

    await worker_a._transport.fetch(sink_a)
    await worker_b._transport.fetch(sink_b)

    assert sink_a.enqueued == [task_data]
    assert sink_b.enqueued == [task_data]


@pytest.mark.asyncio
async def test_broadcast_lane_full_defers_then_retries():
    """A rejected broadcast entry is held in the broadcast deferred buffer and
    retried on the next poll once the lane accepts it (AR-123 §4.4)."""
    control_id = UUID("33333333-3333-3333-3333-333333333333")
    task_data = _broadcast_task(control_id)
    client = DummyClient(xread_results=[None, _entry(b"7-0", encode_task_envelope(task_data))])
    worker = _control_worker(client)
    sink = _Sink(accept=False)

    enqueued = await worker._transport.fetch(sink)

    assert enqueued is False
    assert sink.enqueued == []
    assert len(worker._transport._broadcast_deferred) == 1
    assert len(worker._transport._control_deferred) == 0

    # Lane frees up; the next fetch flushes the broadcast deferred entry.
    sink._accept = True
    client.xread_results = [None, None]
    enqueued = await worker._transport.fetch(sink)

    assert enqueued is True
    assert len(worker._transport._broadcast_deferred) == 0
    assert sink.enqueued == [task_data]
    assert worker._control_entry_ids[control_id] == (worker._control_broadcast_stream_name, b"7-0")


@pytest.mark.asyncio
async def test_directed_and_broadcast_cursors_are_independent():
    """Advancing one control stream's cursor leaves the other's untouched
    (AR-123 §4.4): the directed and broadcast reads hold separate positions."""
    directed_id = UUID("11111111-1111-1111-1111-111111111111")
    broadcast_id = UUID("33333333-3333-3333-3333-333333333333")
    client = DummyClient(xread_results=[_entry(b"1-0", encode_task_envelope(_control_task(directed_id))), None])
    worker = _control_worker(client)
    sink = _Sink()

    await worker._transport.fetch(sink)
    assert worker._transport._control_cursor == b"1-0"
    assert worker._transport._broadcast_cursor == "$"

    client.xread_results = [None, _entry(b"9-0", encode_task_envelope(_broadcast_task(broadcast_id)))]
    await worker._transport.fetch(sink)

    assert worker._transport._control_cursor == b"1-0"
    assert worker._transport._broadcast_cursor == b"9-0"
