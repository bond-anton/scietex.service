"""TaskProcessor task_metrics() snapshot wiring tests."""

import asyncio
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData

from ._helpers import DemoProcessor, DummyHandler, NeverFinishesHandler


@pytest.mark.asyncio
async def test_task_metrics_reports_queue_depth():
    """task_metrics() reflects the pending queue depth before processing."""
    proc = DemoProcessor()

    snapshot = proc.task_metrics()
    assert snapshot.queue_depth == 0
    assert snapshot.running == 0
    assert snapshot.rate == 0.0
    assert snapshot.total == 0

    proc.enqueue_task(TaskData(task_id=str(uuid4()), task="dummy", payload=b"{}"))

    snapshot = proc.task_metrics()
    assert snapshot.queue_depth == 1
    assert snapshot.running == 0
    assert snapshot.total == 0


@pytest.mark.asyncio
async def test_task_metrics_reports_running_count():
    """task_metrics() reports a dispatched-but-unfinished task as running."""
    proc = DemoProcessor()
    proc.add_task_handler(NeverFinishesHandler)
    await proc._start_task_handler("NeverFinishesHandler")
    await proc.start()
    try:
        proc.enqueue_task(TaskData(task_id=str(uuid4()), task="never", payload=b"{}"))
        for _ in range(100):
            if proc.running_tasks:
                break
            await asyncio.sleep(0.01)

        snapshot = proc.task_metrics()
        assert snapshot.running == 1
        assert snapshot.queue_depth == 0
        assert snapshot.total == 0
        assert snapshot.rate == 0.0
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_task_metrics_counts_completed_task():
    """A completed task increments total and produces a positive rate."""
    proc = DemoProcessor()
    proc.add_task_handler(DummyHandler)
    await proc._start_task_handler("DummyHandler")
    await proc.start()
    try:
        proc.enqueue_task(TaskData(task_id=str(uuid4()), task="dummy", payload=b"{}"))
        for _ in range(100):
            if proc.task_metrics().total == 1:
                break
            await asyncio.sleep(0.01)

        snapshot = proc.task_metrics()
        assert snapshot.total == 1
        assert snapshot.running == 0
        assert snapshot.queue_depth == 0
        assert snapshot.rate > 0.0
    finally:
        await proc.exit()
        await proc.events["exit"].wait()
