"""TaskExecutor.run_once tests: dequeue, concurrency gate, and idle timeout."""

import asyncio
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import CANCEL_TASK_TYPE, TaskData
from scietex.service.task_lifecycle import TaskLifecycle

from ._helpers import Recording, build_executor, make_settings, register_finished, register_running


@pytest.mark.asyncio
async def test_run_once_dequeues_and_dispatches():
    """run_once dequeues a task, registers a tracker, and dispatches it."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle)
    task_id = uuid4()
    task_data = TaskData(task="dummy")
    await queue.put((task_id, task_data))

    await executor.run_once()

    trackers = lifecycle.trackers()
    assert task_id in trackers
    await trackers[task_id].worker_task
    assert recording.started == [(task_id, task_data)]
    assert recording.completed
    assert task_id not in lifecycle.trackers()


@pytest.mark.asyncio
async def test_run_once_respects_concurrency_limit():
    """run_once does not dequeue when the running set is at max_concurrent_tasks."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(max_concurrent_tasks=1)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)

    blocker_id = uuid4()
    await register_finished(lifecycle, blocker_id, TaskData(task="blocker"))

    task_id = uuid4()
    task_data = TaskData(task="dummy")
    await queue.put((task_id, task_data))

    await executor.run_once()

    assert task_id not in lifecycle.trackers()
    assert not queue.empty()
    assert not recording.started


@pytest.mark.asyncio
async def test_run_once_times_out_on_empty_queue():
    """run_once returns without dispatching when the queue is empty."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_queue_fetch_timeout=0.05)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)

    await executor.run_once()

    assert not lifecycle.trackers()
    assert not recording.started


@pytest.mark.asyncio
async def test_run_once_admits_control_when_data_budget_full():
    """A saturated data plane still admits control-plane commands (AR-108)."""
    recording = Recording()
    queue = asyncio.Queue()
    control_queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(max_concurrent_tasks=1)
    executor = build_executor(
        recording,
        queue=queue,
        control_queue=control_queue,
        lifecycle=lifecycle,
        settings=settings,
    )

    blocker_id = uuid4()
    register_running(lifecycle, blocker_id, TaskData(task="blocker"))

    control_id = uuid4()
    control_data = TaskData(task=CANCEL_TASK_TYPE)
    await control_queue.put((control_id, control_data))

    data_id = uuid4()
    data_data = TaskData(task="dummy")
    await queue.put((data_id, data_data))

    await executor.run_once()

    assert control_id in lifecycle.trackers()
    assert control_id in executor._control_running
    assert data_id not in lifecycle.trackers()
    assert not queue.empty()

    # Clean up the admitted control task and the data blocker.
    await lifecycle.trackers()[control_id].worker_task
    blocker_tracker = lifecycle.trackers()[blocker_id]
    blocker_tracker.worker_task.cancel()
    await asyncio.gather(blocker_tracker.worker_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_control_concurrency_ceiling():
    """A control task is not admitted when the control lane is at capacity."""
    recording = Recording()
    control_queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(
        recording,
        control_queue=control_queue,
        lifecycle=lifecycle,
        control_concurrency=1,
    )

    running_id = uuid4()
    running_tracker = register_running(lifecycle, running_id, TaskData(task=CANCEL_TASK_TYPE))
    executor._control_running.add(running_id)

    queued_id = uuid4()
    queued_data = TaskData(task=CANCEL_TASK_TYPE)
    await control_queue.put((queued_id, queued_data))

    await executor.run_once()

    assert queued_id not in lifecycle.trackers()
    assert not control_queue.empty()

    running_tracker.worker_task.cancel()
    await asyncio.gather(running_tracker.worker_task, return_exceptions=True)
