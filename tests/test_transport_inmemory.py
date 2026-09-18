"""InMemoryTransport unit tests (AR-072): drain, backpressure, requeue, drain policy, and no-op hooks."""

import logging
from uuid import UUID, uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData
from scietex.service.transport import InMemoryTransport


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

    def enqueue_task(self, task_id: UUID, task_data: TaskData) -> bool:
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
    d1, d2 = TaskData(task="a"), TaskData(task="b")
    transport.submit(t1, d1)
    transport.submit(t2, d2)

    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, d1), (t2, d2)]
    assert await transport.fetch(sink) is False


@pytest.mark.asyncio
async def test_fetch_stops_at_queue_full_and_preserves_order_on_reject():
    """fetch drains nothing from a full sink, and when enqueue_task rejects a
    task the rejected task is put back at the front, preserving order."""
    transport = InMemoryTransport(logger=_logger())
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    d1, d2, d3 = TaskData(task="a"), TaskData(task="b"), TaskData(task="c")
    transport.submit(t1, d1)
    transport.submit(t2, d2)
    transport.submit(t3, d3)

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
    d1 = TaskData(task="a")
    transport.submit(t1, d1)
    await transport.fetch(sink)
    assert sink.items == [(t1, d1)]

    await transport.requeue(t1, d1)
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
    requeue_data = TaskData(task="a", canceled_action="requeue")
    discard_data = TaskData(task="b", canceled_action="discard")

    await transport.on_drain(t1, requeue_data)
    await transport.on_drain(t2, discard_data)

    assert await transport.fetch(sink) is True
    assert sink.items == [(t1, requeue_data)]


@pytest.mark.asyncio
async def test_started_ack_progress_are_noops():
    """on_started/ack/on_progress are no-ops for the in-memory transport:
    they neither enqueue nor raise."""
    transport = InMemoryTransport(logger=_logger())
    t1 = uuid4()
    data = TaskData(task="a")

    await transport.on_started(t1, data)
    await transport.ack(t1, data, None)
    await transport.on_progress(t1, 42.0)

    assert await transport.fetch(FakeSink()) is False


@pytest.mark.asyncio
async def test_refresh_leases_is_noop():
    """refresh_leases does not raise and changes nothing observable: the
    in-memory transport holds no lease to renew."""
    transport = InMemoryTransport(logger=_logger())
    t1 = uuid4()
    data = TaskData(task="a")
    transport.submit(t1, data)

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
