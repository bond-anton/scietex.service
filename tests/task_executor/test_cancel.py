"""TaskExecutor.cancel and shutdown tests: running/queued cancellation and drain."""

import asyncio
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TASK_CANCEL_TASK_NAME, TaskData
from scietex.service.task_lifecycle import TaskLifecycle

from ._helpers import Recording, build_executor, make_settings, register_running


@pytest.mark.asyncio
async def test_cancel_running_target_returns_cancelled():
    """cancel stops a running target and records the deliberate reason."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_cancellation_timeout=0.1)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)
    target_id = uuid4()
    task_data = TaskData(task_id=str(target_id), task="slow")
    tracker = register_running(lifecycle, target_id, task_data)

    outcome = await executor.cancel(target_id)

    assert outcome == "cancelled"
    assert tracker.worker_task.done()
    assert lifecycle.take_cancel_reason(target_id) == "deliberate"


@pytest.mark.asyncio
async def test_cancel_queued_target_returns_cancelled():
    """cancel removes a queued target and acks it terminal with cancel_reason."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle)
    target_id = uuid4()
    task_data = TaskData(task_id=str(target_id), task="dummy")
    await queue.put(task_data)

    outcome = await executor.cancel(target_id)

    assert outcome == "cancelled"
    assert queue.empty()
    assert recording.completed == [(target_id, task_data, None, "deliberate")]


@pytest.mark.asyncio
async def test_queued_cancel_clears_timeout_budget():
    """cancel of a queued target clears any seeded timeout-requeue budget."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle)
    target_id = uuid4()
    task_data = TaskData(task_id=str(target_id), task="dummy")
    await queue.put(task_data)
    executor._timeout_requeues = {target_id: 3}

    outcome = await executor.cancel(target_id)

    assert outcome == "cancelled"
    assert executor._timeout_requeues == {}


@pytest.mark.asyncio
async def test_queued_cancel_clears_retry_budget():
    """cancel of a queued target clears any seeded error-path retry budget,
    without touching another live task's budget (AR-122)."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    retry_attempts = {}
    executor = build_executor(
        recording,
        queue=queue,
        lifecycle=lifecycle,
        retry_attempts=retry_attempts,
    )
    target_id = uuid4()
    other_id = uuid4()
    task_data = TaskData(task_id=str(target_id), task="dummy")
    await queue.put(task_data)
    # Simulate a retryable error that requeued this id: budget == 1.
    retry_attempts[target_id] = 1
    retry_attempts[other_id] = 1

    outcome = await executor.cancel(target_id)

    assert outcome == "cancelled"
    assert queue.empty()
    assert retry_attempts == {other_id: 1}  # target cleared, other untouched
    assert recording.completed == [(target_id, task_data, None, "deliberate")]


@pytest.mark.asyncio
async def test_cancel_unknown_target_returns_not_running():
    """cancel of an unknown id yields not_running with no ack."""
    recording = Recording()
    executor = build_executor(recording)

    outcome = await executor.cancel(uuid4())

    assert outcome == "not_running"
    assert not recording.completed


@pytest.mark.asyncio
async def test_cancel_removes_queued_control_task():
    """cancel removes a control-lane task and acks it terminal (AR-108)."""
    recording = Recording()
    queue = asyncio.Queue()
    control_queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(
        recording,
        queue=queue,
        control_queue=control_queue,
        lifecycle=lifecycle,
    )
    target_id = uuid4()
    task_data = TaskData(task_id=str(target_id), task=TASK_CANCEL_TASK_NAME)
    await control_queue.put(task_data)

    outcome = await executor.cancel(target_id)

    assert outcome == "cancelled"
    assert control_queue.empty()
    assert recording.completed == [(target_id, task_data, None, "deliberate")]


@pytest.mark.asyncio
async def test_shutdown_drains_cancels_and_clears_budget():
    """shutdown drains queued tasks, cancels+requeues running tasks, and clears
    the retry budget."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    retry_attempts = {uuid4(): 1}
    settings = make_settings(task_cancellation_timeout=0.1)
    executor = build_executor(
        recording,
        queue=queue,
        lifecycle=lifecycle,
        retry_attempts=retry_attempts,
        settings=settings,
    )

    queued_id = uuid4()
    queued_data = TaskData(task_id=str(queued_id), task="queued", canceled_action="requeue")
    await queue.put(queued_data)

    running_id = uuid4()
    running_data = TaskData(task_id=str(running_id), task="running", canceled_action="requeue")
    running_tracker = register_running(lifecycle, running_id, running_data)

    await executor.shutdown()

    assert queue.empty()
    assert recording.drained == [(queued_id, queued_data)]
    assert any(tid == running_id for tid, _ in recording.requeued)
    assert running_tracker.worker_task.done()
    assert retry_attempts == {}


@pytest.mark.asyncio
async def test_shutdown_does_not_requeue_discarded_running_task():
    """shutdown cancels a running task but does not requeue a discard action."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_cancellation_timeout=0.1)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)
    running_id = uuid4()
    running_data = TaskData(task_id=str(running_id), task="running", canceled_action="discard")
    running_tracker = register_running(lifecycle, running_id, running_data)

    await executor.shutdown()

    assert running_tracker.worker_task.done()
    assert not recording.requeued


@pytest.mark.asyncio
async def test_shutdown_drains_control_lane():
    """shutdown drains a queued control-lane task through on_drain (AR-108)."""
    recording = Recording()
    queue = asyncio.Queue()
    control_queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    executor = build_executor(
        recording,
        queue=queue,
        control_queue=control_queue,
        lifecycle=lifecycle,
    )
    control_id = uuid4()
    control_data = TaskData(task_id=str(control_id), task=TASK_CANCEL_TASK_NAME)
    await control_queue.put(control_data)

    await executor.shutdown()

    assert control_queue.empty()
    assert recording.drained == [(control_id, control_data)]
