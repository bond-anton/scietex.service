"""TaskExecutor.watchdog tests: timeout detection and requeue/discard by action."""

import asyncio
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData, TaskTimeout
from scietex.service.task_lifecycle import TaskLifecycle

from ._helpers import Recording, build_executor, make_settings, register_running


@pytest.mark.asyncio
async def test_watchdog_requeues_timed_out_task():
    """A timed-out running task with timeout_action="requeue" is requeued and
    untracked, with the timeout reason preserved for the eventual ack."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_cancellation_timeout=0.1)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)
    task_id = uuid4()
    task_data = TaskData(task="slow", timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"))
    tracker = register_running(lifecycle, task_id, task_data, started=-100.0)

    await executor.watchdog()

    assert tracker.worker_task.done()
    assert any(tid == task_id for tid, _ in recording.requeued)
    assert task_id not in lifecycle.trackers()
    assert lifecycle.take_cancel_reason(task_id) == "timeout"


@pytest.mark.asyncio
async def test_watchdog_discards_timed_out_task():
    """A timed-out task with timeout_action="discard" is not requeued."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_cancellation_timeout=0.1)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)
    task_id = uuid4()
    task_data = TaskData(task="slow", timeout=TaskTimeout(timeout=0.1, timeout_action="discard"))
    tracker = register_running(lifecycle, task_id, task_data, started=-100.0)

    await executor.watchdog()

    assert tracker.worker_task.done()
    assert not recording.requeued
    assert task_id not in lifecycle.trackers()


@pytest.mark.asyncio
async def test_watchdog_uses_configured_task_timeout_when_per_task_none():
    """A task with timeout=None falls back to the configured task_timeout."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_timeout=0.1, task_cancellation_timeout=0.1)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)
    task_id = uuid4()
    task_data = TaskData(task="slow", timeout=TaskTimeout(timeout=None, timeout_action="requeue"))
    register_running(lifecycle, task_id, task_data, started=-100.0)

    await executor.watchdog()

    assert any(tid == task_id for tid, _ in recording.requeued)


@pytest.mark.asyncio
async def test_watchdog_ignores_non_positive_timeout():
    """timeout <= 0 means no timeout: the watchdog never cancels the task."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_timeout=3.0)
    executor = build_executor(recording, queue=queue, lifecycle=lifecycle, settings=settings)
    task_id = uuid4()
    task_data = TaskData(task="never", timeout=TaskTimeout(timeout=0, timeout_action="requeue"))
    tracker = register_running(lifecycle, task_id, task_data, started=-100.0)

    await executor.watchdog()

    assert not tracker.worker_task.done()
    assert not recording.requeued
    assert task_id in lifecycle.trackers()

    # Clean up the still-running blocker task.
    tracker.worker_task.cancel()
    await asyncio.gather(tracker.worker_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_watchdog_requeues_until_ceiling_then_terminates():
    """A max_timeout_requeues=2 budget requeues a task twice, then terminates
    it on the third timeout without redelivering and pops the budget key."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_cancellation_timeout=0.1)
    executor = build_executor(
        recording,
        queue=queue,
        lifecycle=lifecycle,
        settings=settings,
        max_timeout_requeues=2,
    )
    task_id = uuid4()
    task_data = TaskData(task="slow", timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"))

    # First redelivery: under budget, requeue and bump to 1.
    register_running(lifecycle, task_id, task_data, started=-100.0)
    await executor.watchdog()
    assert len(recording.requeued) == 1
    assert executor._timeout_requeues == {task_id: 1}

    # Second redelivery: still under budget, requeue and bump to 2.
    register_running(lifecycle, task_id, task_data, started=-100.0)
    await executor.watchdog()
    assert len(recording.requeued) == 2
    assert executor._timeout_requeues == {task_id: 2}

    # Third redelivery: ceiling hit, no requeue, budget key popped.
    register_running(lifecycle, task_id, task_data, started=-100.0)
    await executor.watchdog()
    assert len(recording.requeued) == 2
    assert executor._timeout_requeues == {}


@pytest.mark.asyncio
async def test_watchdog_zero_ceiling_never_requeues():
    """max_timeout_requeues=0 disables timeout requeue: the task is not
    redelivered and the budget stays empty."""
    recording = Recording()
    queue = asyncio.Queue()
    lifecycle = TaskLifecycle()
    settings = make_settings(task_cancellation_timeout=0.1)
    executor = build_executor(
        recording,
        queue=queue,
        lifecycle=lifecycle,
        settings=settings,
        max_timeout_requeues=0,
    )
    task_id = uuid4()
    task_data = TaskData(task="slow", timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"))
    tracker = register_running(lifecycle, task_id, task_data, started=-100.0)

    await executor.watchdog()

    assert tracker.worker_task.done()
    assert not recording.requeued
    assert executor._timeout_requeues == {}
    assert task_id not in lifecycle.trackers()
