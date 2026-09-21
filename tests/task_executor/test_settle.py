"""TaskExecutor._settle / _handle_task terminal-state tests."""

import asyncio
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import CANCEL_TASK_TYPE, TaskData, TaskResult, task_data_id
from scietex.service.task_lifecycle import TaskLifecycle

from ._helpers import Recording, build_executor, register_finished


@pytest.mark.asyncio
async def test_settle_removes_tracker_and_acks_result():
    """_settle drops the tracker and acks the result through on_completed."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="dummy")
    result = TaskResult(status="success")
    queue.put_nowait(task_data)
    await register_finished(lifecycle, task_id, task_data)

    await executor._settle(task_data, result)

    assert task_id not in lifecycle.trackers()
    assert recording.completed == [(task_id, task_data, result, None)]


@pytest.mark.asyncio
async def test_settle_consumes_cancel_reason():
    """_settle pops and forwards the recorded cancel reason to the ack hook."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="dummy")
    queue.put_nowait(task_data)
    await register_finished(lifecycle, task_id, task_data)
    lifecycle.mark_cancelled(task_id, "deliberate")

    await executor._settle(task_data, None)

    _, _, acked, cancel_reason = recording.completed[0]
    assert acked is None
    assert cancel_reason == "deliberate"
    assert lifecycle.take_cancel_reason(task_id) is None


@pytest.mark.asyncio
async def test_settle_ack_failure_is_logged_not_raised():
    """A transport ack failure must not raise out of settle."""

    class RaisingRecording(Recording):
        async def on_completed(self, task_data, result, *, cancel_reason=None):
            self.completed.append((task_data_id(task_data), task_data, result, cancel_reason))
            raise RuntimeError("ack boom")

    recording = RaisingRecording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="dummy")
    queue.put_nowait(task_data)
    await register_finished(lifecycle, task_id, task_data)

    await executor._settle(task_data, TaskResult(status="success"))

    assert task_id not in lifecycle.trackers()


@pytest.mark.asyncio
async def test_execute_swallows_exception():
    """A handler exception is swallowed: the ack sees result None."""
    recording = Recording(process_result=ValueError("boom"))
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="raiser")
    queue.put_nowait(task_data)

    await executor._handle_task(task_data)

    assert recording.completed == [(task_id, task_data, None, None)]


@pytest.mark.asyncio
async def test_execute_propagates_cancelled_error():
    """_execute catches Exception, not BaseException, so CancelledError escapes
    while the settle step still runs and acks None."""

    class CancelRecording(Recording):
        async def process_task(self, task_data, *, control=False):
            self.processed.append((task_data_id(task_data), task_data))
            raise asyncio.CancelledError()

    recording = CancelRecording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="cancelled")
    queue.put_nowait(task_data)

    with pytest.raises(asyncio.CancelledError):
        await executor._handle_task(task_data)

    assert recording.completed == [(task_id, task_data, None, None)]


@pytest.mark.asyncio
async def test_settle_balances_control_lane():
    """_settle calls task_done on the control lane, not the data lane (AR-108)."""
    recording = Recording()
    queue = asyncio.Queue()
    control_queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, control_queue=control_queue, lifecycle=lifecycle)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task=CANCEL_TASK_TYPE)
    control_queue.put_nowait(task_data)
    await register_finished(lifecycle, task_id, task_data, control=True)
    executor._control_running.add(task_id)

    await executor._settle(task_data, TaskResult(status="success"))

    await asyncio.wait_for(control_queue.join(), timeout=0.1)
    assert task_id not in executor._control_running
