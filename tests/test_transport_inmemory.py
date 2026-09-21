"""InMemoryTransport unit tests (AR-072): drain, backpressure, requeue, drain policy, and no-op hooks."""

import logging
from uuid import UUID, uuid4

import pytest

from scietex.service.task_handler import CANCEL_TASK_TYPE
from scietex.service.task_handler.schemas import TaskData, task_data_id
from scietex.service.transport import InMemoryTransport, RecoverableTransport, TaskSink


def _logger() -> logging.Logger:
    return logging.getLogger("test_transport_inmemory")


class FakeSink:
    """A minimal ``TaskSink`` that records enqueued tasks and can be told to
    report full or reject a specific task id."""

    def __init__(self, *, full: bool = False, reject: set[UUID] | None = None) -> None:
        self.full = full
        self.reject: set[UUID] = reject or set()
        self.items: list[tuple[UUID, TaskData]] = []

    def task_queue_full(self) -> bool:
        return self.full

    def enqueue_task(self, task_data: TaskData) -> bool:
        task_id = task_data_id(task_data)
        if task_id in self.reject:
            return False
        self.items.append((task_id, task_data))
        return True


@pytest.mark.asyncio
async def test_fetch_drains_submitted_tasks_and_reports():
    """fetch drains submitted tasks into the sink in order and returns True;
    an empty transport reports False."""
    transport = InMemoryTransport(logger=_logger())
    sink = FakeSink()
    t1, t2 = uuid4(), uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    d2 = TaskData(task_id=str(t2), task="b")
    transport.submit(d1)
    transport.submit(d2)

    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, d1), (t2, d2)]
    assert await transport.fetch(sink) is False


@pytest.mark.asyncio
async def test_fetch_stops_at_queue_full_and_preserves_order_on_reject():
    """fetch drains nothing from a full sink, and when enqueue_task rejects a
    task the rejected task is put back at the front, preserving order."""
    transport = InMemoryTransport(logger=_logger())
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    d2 = TaskData(task_id=str(t2), task="b")
    d3 = TaskData(task_id=str(t3), task="c")
    transport.submit(d1)
    transport.submit(d2)
    transport.submit(d3)

    # A full sink accepts nothing and reports nothing enqueued.
    full_sink = FakeSink(full=True)
    assert await transport.fetch(full_sink) is False
    assert full_sink.items == []

    # Rejecting t2: t1 is enqueued, t2 is put back, the drain stops before t3.
    sink = FakeSink(reject={t2})
    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, d1)]

    # Clearing the rejection lets the next fetch re-deliver t2 first, then t3.
    sink.reject.clear()
    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, d1), (t2, d2), (t3, d3)]


@pytest.mark.asyncio
async def test_requeue_redelivers_on_next_fetch():
    """requeue re-appends a task so the next fetch re-delivers it."""
    transport = InMemoryTransport(logger=_logger())
    sink = FakeSink()
    t1 = uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    transport.submit(d1)
    await transport.fetch(sink)
    assert sink.items == [(t1, d1)]

    await transport.requeue(d1)
    sink.items.clear()
    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, d1)]


@pytest.mark.asyncio
async def test_on_drain_requeues_iff_canceled_action_is_requeue():
    """on_drain returns a task to the queue only when canceled_action is
    'requeue'; a 'discard' task is dropped."""
    transport = InMemoryTransport(logger=_logger())
    sink = FakeSink()
    t1, t2 = uuid4(), uuid4()
    requeue_data = TaskData(task_id=str(t1), task="a", canceled_action="requeue")
    discard_data = TaskData(task_id=str(t2), task="b", canceled_action="discard")

    await transport.on_drain(requeue_data)
    await transport.on_drain(discard_data)

    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, requeue_data)]


@pytest.mark.asyncio
async def test_started_ack_progress_are_noops():
    """on_started/ack/on_progress are no-ops for the in-memory transport:
    they neither enqueue nor raise."""
    transport = InMemoryTransport(logger=_logger())
    t1 = uuid4()
    data = TaskData(task_id=str(t1), task="a")

    await transport.on_started(data)
    await transport.ack(data, None)
    await transport.on_progress(t1, 42.0)

    assert await transport.fetch(FakeSink()) is False


@pytest.mark.asyncio
async def test_refresh_leases_is_noop():
    """refresh_leases does not raise and changes nothing observable: the
    in-memory transport holds no lease to renew."""
    transport = InMemoryTransport(logger=_logger())
    t1 = uuid4()
    data = TaskData(task_id=str(t1), task="a")
    transport.submit(data)

    await transport.refresh_leases()

    assert await transport.fetch(FakeSink()) is True


@pytest.mark.asyncio
async def test_recover_pending_tasks_returns_complete_empty():
    """recover_pending_tasks reports recovery complete with nothing enqueued,
    since the in-memory transport has no cross-restart state to re-deliver."""
    transport = InMemoryTransport(logger=_logger())

    recovered, enqueued = await transport.recover_pending_tasks(FakeSink())

    assert recovered is True
    assert enqueued is False


class _CountedRecoveryTransport(RecoverableTransport):
    """A ``RecoverableTransport`` double that pops one scripted result per call."""

    def __init__(self, *results: tuple[bool, bool]) -> None:
        self._results: list[tuple[bool, bool]] = list(results)
        self.calls: int = 0

    async def recover_pending_tasks(self, sink: TaskSink) -> tuple[bool, bool]:
        self.calls += 1
        return self._results.pop(0)


@pytest.mark.asyncio
async def test_ensure_recovered_runs_once_and_sets_flag():
    """ensure_recovered returns the enqueued signal and sets recovered on a
    complete recovery; a second call is a no-op."""
    transport = _CountedRecoveryTransport((True, True))
    sink = FakeSink()

    assert await transport.ensure_recovered(sink) is True
    assert transport.recovered is True
    assert await transport.ensure_recovered(sink) is False
    assert transport.calls == 1


@pytest.mark.asyncio
async def test_ensure_recovered_retries_when_incomplete():
    """An incomplete recovery leaves recovered unset; the next call retries and
    completes it."""
    transport = _CountedRecoveryTransport((False, False), (True, True))
    sink = FakeSink()

    assert await transport.ensure_recovered(sink) is False
    assert transport.recovered is False
    assert await transport.ensure_recovered(sink) is True
    assert transport.recovered is True
    assert transport.calls == 2


@pytest.mark.asyncio
async def test_ensure_recovered_returns_enqueued_on_incomplete():
    """An incomplete recovery still reports its enqueued signal without setting
    the recovered flag."""
    transport = _CountedRecoveryTransport((False, True))
    sink = FakeSink()

    assert await transport.ensure_recovered(sink) is True
    assert transport.recovered is False


@pytest.mark.asyncio
async def test_fetch_delivers_control_behind_full_data_lane():
    """A control command queued behind a full data lane is still delivered,
    while the data tasks stay pending (AR-108)."""
    transport = InMemoryTransport(logger=_logger())
    t1, t2 = uuid4(), uuid4()
    data = TaskData(task_id=str(t1), task="a")
    control = TaskData(task_id=str(t2), task=CANCEL_TASK_TYPE)
    transport.submit(data)
    transport.submit(control)

    sink = FakeSink(full=True)
    assert await transport.fetch(sink) is True
    assert sink.items == [(t2, control)]
    assert list(transport._pending) == [data]


@pytest.mark.asyncio
async def test_fetch_preserves_data_fifo_when_control_interleaved():
    """With pending order [d1, c1, d2] and a full data lane, fetch delivers c1
    and leaves data FIFO order [d1, d2] intact (AR-108)."""
    transport = InMemoryTransport(logger=_logger())
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    d1 = TaskData(task_id=str(t1), task="a")
    c1 = TaskData(task_id=str(t2), task=CANCEL_TASK_TYPE)
    d2 = TaskData(task_id=str(t3), task="b")
    transport.submit(d1)
    transport.submit(c1)
    transport.submit(d2)

    sink = FakeSink(full=True)
    assert await transport.fetch(sink) is True
    assert sink.items == [(t2, c1)]
    assert list(transport._pending) == [d1, d2]
