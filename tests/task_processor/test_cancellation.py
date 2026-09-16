"""TaskProcessor cancel_task outcome tests."""

import asyncio
from uuid import uuid4

import msgspec
import pytest

from scietex.service.config import TaskProcessorConfig
from scietex.service.task_handler.cancel import CancelTaskResponse
from scietex.service.task_handler.schemas import TaskData

from ._helpers import (
    CancelRecordingProcessor,
    SelfCancelHandler,
    SlowHandler,
    StubbornHandler,
    _cancel_payload,
)


@pytest.mark.asyncio
async def test_cancel_task_cancels_running_target():
    """A cancel_task request stops a running target and reports success."""
    proc = CancelRecordingProcessor(TaskProcessorConfig(task_cancellation_timeout=0.5))
    proc.add_task_handler(SlowHandler)
    await proc._start_task_handler("SlowHandler")
    await proc.start()
    try:
        target_id = uuid4()
        proc.enqueue_task(target_id, TaskData(task="slow", payload=b"{}"))
        for _ in range(100):
            if target_id in proc.running_tasks:
                break
            await asyncio.sleep(0.01)
        assert target_id in proc.running_tasks

        cancel_id = uuid4()
        proc.enqueue_task(cancel_id, TaskData(task="cancel_task", payload=_cancel_payload(target_id)))
        for _ in range(200):
            if any(tid == cancel_id for tid, *_ in proc.completed):
                break
            await asyncio.sleep(0.01)

        cancel_calls = [c for c in proc.completed if c[0] == cancel_id]
        assert len(cancel_calls) == 1
        assert cancel_calls[0][2].status == "success"
        response = msgspec.msgpack.decode(cancel_calls[0][2].payload, type=CancelTaskResponse)
        assert response.outcome == "cancelled"

        target_calls = [c for c in proc.completed if c[0] == target_id]
        assert len(target_calls) == 1
        assert target_calls[0][3] == "deliberate"
        assert target_calls[0][2] is None
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_cancel_task_unknown_target_returns_not_running():
    """Cancelling an unknown task id yields a non-retryable error result."""
    proc = CancelRecordingProcessor()
    await proc.start()
    try:
        cancel_id = uuid4()
        proc.enqueue_task(cancel_id, TaskData(task="cancel_task", payload=_cancel_payload(uuid4())))
        for _ in range(200):
            if any(tid == cancel_id for tid, *_ in proc.completed):
                break
            await asyncio.sleep(0.01)

        cancel_calls = [c for c in proc.completed if c[0] == cancel_id]
        assert len(cancel_calls) == 1
        result = cancel_calls[0][2]
        assert result.status == "error"
        assert result.error_code == "TASK_NOT_RUNNING"
        assert result.retryable is False
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_cancel_task_malformed_payload_returns_invalid_code():
    """A payload that is not a CancelTaskRequest yields INVALID_CANCEL_PAYLOAD."""
    proc = CancelRecordingProcessor()
    await proc.start()
    try:
        cancel_id = uuid4()
        proc.enqueue_task(cancel_id, TaskData(task="cancel_task", payload=b"not-msgpack"))
        for _ in range(200):
            if any(tid == cancel_id for tid, *_ in proc.completed):
                break
            await asyncio.sleep(0.01)

        cancel_calls = [c for c in proc.completed if c[0] == cancel_id]
        assert len(cancel_calls) == 1
        result = cancel_calls[0][2]
        assert result.status == "error"
        assert result.error_code == "INVALID_CANCEL_PAYLOAD"
        assert result.retryable is False
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_cancel_task_stubborn_target_reports_ignored():
    """A target that swallows cancellation yields CANCEL_IGNORED and is not
    requeued (requeueing would run it twice)."""
    proc = CancelRecordingProcessor(TaskProcessorConfig(task_cancellation_timeout=0.1))
    proc.add_task_handler(StubbornHandler)
    await proc._start_task_handler("StubbornHandler")
    await proc.start()
    try:
        target_id = uuid4()
        proc.enqueue_task(target_id, TaskData(task="stubborn", payload=b"{}"))
        for _ in range(100):
            if target_id in proc.running_tasks:
                break
            await asyncio.sleep(0.01)

        cancel_id = uuid4()
        proc.enqueue_task(cancel_id, TaskData(task="cancel_task", payload=_cancel_payload(target_id)))
        for _ in range(200):
            if any(tid == cancel_id for tid, *_ in proc.completed):
                break
            await asyncio.sleep(0.01)

        cancel_calls = [c for c in proc.completed if c[0] == cancel_id]
        assert len(cancel_calls) == 1
        result = cancel_calls[0][2]
        assert result.status == "error"
        assert result.error_code == "CANCEL_IGNORED"
        assert result.retryable is False
        assert not any(tid == target_id for tid, _ in proc.requeued)
        # Let the stubborn handler finish so no dangling task remains.
        await asyncio.sleep(0.5)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_cancel_task_removes_queued_target():
    """A queued-but-undispatched target is removed and reported cancelled,
    without ever being processed."""
    proc = CancelRecordingProcessor(TaskProcessorConfig(max_concurrent_tasks=1))
    proc.add_task_handler(SlowHandler)
    await proc._start_task_handler("SlowHandler")
    await proc.start()
    try:
        # Occupy the single concurrency slot so the next task stays queued.
        blocker_id = uuid4()
        proc.enqueue_task(blocker_id, TaskData(task="slow", payload=b"{}"))
        for _ in range(100):
            if blocker_id in proc.running_tasks:
                break
            await asyncio.sleep(0.01)

        target_id = uuid4()
        proc.enqueue_task(target_id, TaskData(task="slow", payload=b"{}"))
        assert not proc.task_queue_empty()

        # Cancel the queued target directly through the callback (the cancel
        # task itself would also queue behind the blocker).
        outcome = await proc._cancel_task(target_id)
        assert outcome == "cancelled"
        assert proc.task_queue_empty()

        target_calls = [c for c in proc.completed if c[0] == target_id]
        assert len(target_calls) == 1
        assert target_calls[0][2] is None
        assert target_calls[0][3] == "deliberate"
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_cancel_task_self_cancel_rejected():
    """A task cannot cancel itself: the callback reports not_running."""
    proc = CancelRecordingProcessor()
    proc.add_task_handler(SelfCancelHandler, cancel=proc._cancel_task)
    await proc._start_task_handler("SelfCancelHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="self_cancel", payload=b"{}"))
        for _ in range(200):
            if any(tid == t_id for tid, *_ in proc.completed):
                break
            await asyncio.sleep(0.01)

        calls = [c for c in proc.completed if c[0] == t_id]
        assert len(calls) == 1
        assert calls[0][2].payload == b"not_running"
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_cancel_task_does_not_requeue_deliberate_cancel():
    """A deliberate cancel never auto-requeues, even with canceled_action=requeue."""
    proc = CancelRecordingProcessor(TaskProcessorConfig(task_cancellation_timeout=0.5))
    proc.add_task_handler(SlowHandler)
    await proc._start_task_handler("SlowHandler")
    await proc.start()
    try:
        target_id = uuid4()
        proc.enqueue_task(
            target_id,
            TaskData(task="slow", payload=b"{}", canceled_action="requeue"),
        )
        for _ in range(100):
            if target_id in proc.running_tasks:
                break
            await asyncio.sleep(0.01)

        outcome = await proc._cancel_task(target_id)
        assert outcome == "cancelled"
        assert not any(tid == target_id for tid, _ in proc.requeued)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()
