"""TaskProcessor control-lane priority tests (AR-108)."""

import asyncio
from uuid import uuid4

import msgspec
import pytest

from scietex.service.config import TaskProcessorConfig
from scietex.service.task_handler.cancel import CancelTaskResponse
from scietex.service.task_handler.schemas import TaskData

from ._helpers import (
    CancelRecordingProcessor,
    DemoProcessor,
    SlowHandler,
    _cancel_payload,
)


@pytest.mark.asyncio
async def test_enqueue_routes_control_and_data_to_separate_lanes():
    """enqueue_control_task sends control-plane commands to the control lane and
    enqueue_task sends data tasks to the data lane, without blocking (AR-108)."""
    proc = CancelRecordingProcessor()
    control_id = uuid4()
    data_id = uuid4()

    assert proc.enqueue_control_task(
        TaskData(task_id=str(control_id), task="cancel_task", payload=_cancel_payload(uuid4()))
    )
    assert proc.enqueue_task(TaskData(task_id=str(data_id), task="dummy", payload=b"{}"))

    # Both lanes hold their task; neither was dropped.
    assert not proc.control_queue_empty()
    assert not proc.task_queue_empty()

    # dequeue_task drains the data lane only: the data task leaves while the
    # control command stays in its own lane.
    dequeued = proc.dequeue_task()
    assert dequeued is not None
    assert dequeued.task_id == str(data_id)
    assert dequeued.task == "dummy"
    assert proc.task_queue_empty()
    assert not proc.control_queue_empty()


@pytest.mark.asyncio
async def test_cancel_task_bypasses_saturated_data_plane():
    """With the single data slot held by a slow task, an enqueued cancel_task
    still runs on the control lane and removes a queued data target (AR-108)."""
    proc = CancelRecordingProcessor(TaskProcessorConfig(max_concurrent_tasks=1))
    proc.add_task_handler(SlowHandler)
    await proc._start_task_handler("SlowHandler")
    await proc.start()
    try:
        # Occupy the single data slot so the next data task stays queued.
        blocker_id = uuid4()
        proc.enqueue_task(TaskData(task_id=str(blocker_id), task="slow", payload=b"{}"))
        for _ in range(100):
            if blocker_id in proc.running_tasks:
                break
            await asyncio.sleep(0.01)
        assert blocker_id in proc.running_tasks

        target_id = uuid4()
        proc.enqueue_task(TaskData(task_id=str(target_id), task="slow", payload=b"{}"))
        assert not proc.task_queue_empty()

        # The cancel travels the control lane and completes while the data slot
        # is still occupied, removing the queued target.
        cancel_id = uuid4()
        proc.enqueue_control_task(
            TaskData(task_id=str(cancel_id), task="cancel_task", payload=_cancel_payload(target_id))
        )
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
        assert target_calls[0][2] is None
        assert target_calls[0][3] == "deliberate"
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_task_queue_manager_polls_when_data_full_but_control_has_room():
    """The intake gate polls fetch_tasks while the data queue is full but the
    control lane still has room, so control commands are not starved (AR-108)."""
    proc = DemoProcessor(TaskProcessorConfig(queue_size=1))
    proc.enqueue_task(TaskData(task_id=str(uuid4()), task="dummy", payload=b"{}"))
    assert proc.task_queue_full()
    assert not proc.control_queue_full()

    calls = 0

    async def spy_fetch():
        nonlocal calls
        calls += 1
        return True

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(proc, "fetch_tasks", spy_fetch)
    try:
        await proc.task_queue_manager()
    finally:
        monkeypatch.undo()

    assert calls == 1
