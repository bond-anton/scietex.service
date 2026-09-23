"""TaskExecutor.run_once tests: dequeue, concurrency gate, and idle timeout."""

import asyncio
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TASK_CANCEL_TASK_NAME, TaskData, TaskResult, task_data_id
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
    task_data = TaskData(task_id=str(task_id), task="dummy")
    await queue.put(task_data)

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
    await register_finished(lifecycle, blocker_id, TaskData(task_id=str(blocker_id), task="blocker"))

    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="dummy")
    await queue.put(task_data)

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
    register_running(lifecycle, blocker_id, TaskData(task_id=str(blocker_id), task="blocker"))

    control_id = uuid4()
    control_data = TaskData(task_id=str(control_id), task=TASK_CANCEL_TASK_NAME)
    await control_queue.put(control_data)

    data_id = uuid4()
    data_data = TaskData(task_id=str(data_id), task="dummy")
    await queue.put(data_data)

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
    running_tracker = register_running(
        lifecycle, running_id, TaskData(task_id=str(running_id), task=TASK_CANCEL_TASK_NAME)
    )
    executor._control_running.add(running_id)

    queued_id = uuid4()
    queued_data = TaskData(task_id=str(queued_id), task=TASK_CANCEL_TASK_NAME)
    await control_queue.put(queued_data)

    await executor.run_once()

    assert queued_id not in lifecycle.trackers()
    assert not control_queue.empty()

    running_tracker.worker_task.cancel()
    await asyncio.gather(running_tracker.worker_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_run_once_refills_all_free_slots():
    """One iteration dispatches up to the free-slot count, not just one task."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(max_concurrent_tasks=3)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)

    ids = [uuid4() for _ in range(3)]
    for task_id in ids:
        await queue.put(TaskData(task_id=str(task_id), task="dummy"))

    await executor.run_once()

    # The old single-dispatch loop registered exactly one tracker here.
    assert set(lifecycle.trackers()) == set(ids)
    for tracker in list(lifecycle.trackers().values()):
        await tracker.worker_task


@pytest.mark.asyncio
async def test_run_once_batch_stops_at_the_free_slot_count():
    """A batch never exceeds the free-slot budget, leaving the surplus queued."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(max_concurrent_tasks=2)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)

    ids = [uuid4() for _ in range(5)]
    for task_id in ids:
        await queue.put(TaskData(task_id=str(task_id), task="dummy"))

    await executor.run_once()

    assert len(lifecycle.trackers()) == 2
    assert queue.qsize() == 3
    for tracker in list(lifecycle.trackers().values()):
        await tracker.worker_task


@pytest.mark.asyncio
async def test_full_gate_wakes_on_settle_instead_of_sleeping():
    """A parked iteration wakes when a slot frees, without waiting the sleep timeout."""
    gate = asyncio.Event()

    class GatedRecording(Recording):
        async def process_task(self, task_data, *, control=False):
            self.processed.append((task_data_id(task_data), task_data))
            await gate.wait()
            return TaskResult(status="success")

    recording = GatedRecording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    # A 30s sleep timeout makes a regression fail loudly rather than pass slowly.
    settings = make_settings(max_concurrent_tasks=1, task_manager_sleep_time=30.0, task_queue_fetch_timeout=5.0)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)

    first_id = uuid4()
    await queue.put(TaskData(task_id=str(first_id), task="gated"))
    await executor.run_once()
    assert first_id in lifecycle.trackers()

    second_id = uuid4()
    await queue.put(TaskData(task_id=str(second_id), task="gated"))
    parked = asyncio.create_task(executor.run_once())
    await asyncio.sleep(0)
    assert not parked.done()

    gate.set()
    await asyncio.wait_for(parked, timeout=1.0)
    assert second_id not in lifecycle.trackers()

    await executor.run_once()
    assert second_id in lifecycle.trackers()
    for tracker in list(lifecycle.trackers().values()):
        await tracker.worker_task


@pytest.mark.asyncio
async def test_control_admitted_before_data_batch():
    """Control wins the iteration even when data slots are free and data is queued."""
    recording = Recording()
    queue = asyncio.Queue()
    control_queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, control_queue=control_queue, lifecycle=lifecycle)

    control_id = uuid4()
    await control_queue.put(TaskData(task_id=str(control_id), task=TASK_CANCEL_TASK_NAME))
    data_id = uuid4()
    await queue.put(TaskData(task_id=str(data_id), task="dummy"))

    await executor.run_once()

    assert control_id in lifecycle.trackers()
    assert data_id not in lifecycle.trackers()
    assert not queue.empty()

    await lifecycle.trackers()[control_id].worker_task
