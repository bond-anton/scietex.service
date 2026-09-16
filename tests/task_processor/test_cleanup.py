"""TaskProcessor cleanup drain semantics tests (durable vs non-durable transport)."""

from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData

from ._helpers import DemoProcessor, DurableProcessor


@pytest.mark.asyncio
async def test_cleanup_drain_requeues_queued_tasks_for_non_durable_transport():
    """cleanup must requeue queued-but-undispatched tasks for a non-durable
    (in-memory) transport: nothing keeps them pending, so dropping them would
    silently lose work on shutdown (AR-041)."""
    proc = DemoProcessor()
    t_id = uuid4()
    proc.enqueue_task(t_id, TaskData(task="dummy", payload=b"{}"))
    await proc.cleanup()
    assert any(tid == t_id for tid, _ in proc.requeued)
    assert proc.task_queue_empty()


@pytest.mark.asyncio
async def test_cleanup_drain_does_not_requeue_queued_tasks_for_durable_transport():
    """cleanup must NOT requeue queued-but-undispatched tasks for a durable
    transport: their entries stay pending and are redelivered on restart, so
    an XADD here would duplicate them (AR-041)."""
    proc = DurableProcessor()
    t_id = uuid4()
    proc.enqueue_task(t_id, TaskData(task="dummy", payload=b"{}"))
    await proc.cleanup()
    assert not any(tid == t_id for tid, _ in proc.requeued)
    assert proc.task_queue_empty()
