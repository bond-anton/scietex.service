"""TaskProcessor handle_task completion hooks, retry/requeue, and hook-order tests."""

import asyncio
import gc
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData

from ._helpers import (
    DemoProcessor,
    DummyHandler,
    ExplodingSupportsHandler,
    OrderRecordingProcessor,
    PermanentErrorHandler,
    RaisingHandler,
    RecordingProcessor,
    RequeueRecordingProcessor,
    RetryableErrorHandler,
)


@pytest.mark.asyncio
async def test_task_manager_consumes_handler_exception_without_leaking():
    """An exception raised outside process_task's try/except (e.g. in a
    handler's supports()) must be caught by handle_task so it does not surface
    as an unretrieved task exception (AR-010)."""

    loop = asyncio.get_running_loop()
    leaked: list[str] = []
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, ctx: leaked.append(str(ctx.get("message", ""))))
    proc = DemoProcessor()
    proc.add_task_handler(ExplodingSupportsHandler)
    # Start the handler first so it is ready before the managers consume the
    # task; otherwise _find_task_handler would see an empty registry and the
    # exploding supports() path would never run.
    await proc._start_task_handler("ExplodingSupportsHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="exploding", payload=b"{}"))
        # Wait until task_manager has consumed the task from the queue.
        for _ in range(100):
            if proc.task_queue_empty():
                break
            await asyncio.sleep(0.01)
        # Let the spawned handle_task coroutine run to completion.
        await asyncio.sleep(0.05)
        # Force GC so any unretrieved task exception is reported deterministically.
        gc.collect()
        await asyncio.sleep(0.05)
        assert not any("never retrieved" in m for m in leaked), f"leaked: {leaked}"
    finally:
        loop.set_exception_handler(old_handler)
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_handle_task_invokes_completion_hook():
    """handle_task must invoke on_task_completed with the final result (AR-005)."""
    proc = RecordingProcessor()
    proc.add_task_handler(DummyHandler)
    await proc._start_task_handler("DummyHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="dummy", payload=b'{"value": 5}'))
        for _ in range(100):
            if proc.completed:
                break
            await asyncio.sleep(0.01)
        assert len(proc.completed) == 1
        cid, cdata, cresult = proc.completed[0]
        assert cid == t_id
        assert cdata.task == "dummy"
        assert cresult.status == "success"
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_handle_task_requeues_retryable_error_before_ack():
    """A retryable error result must be requeued via return_task_to_queue
    before on_task_completed acks the transport entry (AR-022 v4)."""
    proc = RequeueRecordingProcessor()
    proc.add_task_handler(RetryableErrorHandler)
    await proc._start_task_handler("RetryableErrorHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="retryable_err", payload=b"{}"))
        for _ in range(100):
            if proc.completed:
                break
            await asyncio.sleep(0.01)
        assert len(proc.completed) == 1
        assert any(tid == t_id for tid, _ in proc.requeued)
        _, _, cresult = proc.completed[0]
        assert cresult.status == "error"
        assert cresult.retryable is True
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_handle_task_drops_permanent_error_without_requeue():
    """A permanent error result must be acked+dropped, not requeued (AR-022 v4)."""
    proc = RequeueRecordingProcessor()
    proc.add_task_handler(PermanentErrorHandler)
    await proc._start_task_handler("PermanentErrorHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="permanent_err", payload=b"{}"))
        for _ in range(100):
            if proc.completed:
                break
            await asyncio.sleep(0.01)
        assert len(proc.completed) == 1
        assert not any(tid == t_id for tid, _ in proc.requeued)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_handle_task_does_not_requeue_raised_handler():
    """A handler that raises is permanent: handle_task must not requeue it
    (AR-022 v4)."""
    proc = RequeueRecordingProcessor()
    proc.add_task_handler(RaisingHandler)
    await proc._start_task_handler("RaisingHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="raiser", payload=b"{}"))
        for _ in range(100):
            if proc.completed:
                break
            await asyncio.sleep(0.01)
        assert len(proc.completed) == 1
        assert not any(tid == t_id for tid, _ in proc.requeued)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_handle_task_calls_on_task_started_before_process_task():
    """handle_task must invoke on_task_started before process_task, so the
    running tracking record is published before the handler runs (AR-005)."""
    proc = OrderRecordingProcessor()
    proc.add_task_handler(DummyHandler)
    await proc._start_task_handler("DummyHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="dummy", payload=b"{}"))
        for _ in range(100):
            if len(proc.call_order) >= 2:
                break
            await asyncio.sleep(0.01)
        assert proc.call_order == ["started", "process"]
    finally:
        await proc.exit()
        await proc.events["exit"].wait()
