"""TaskProcessor watchdog timeout and requeue tests."""

import asyncio
from uuid import uuid4

import pytest

from scietex.service.config import TaskProcessorConfig
from scietex.service.task_handler.schemas import TaskData, TaskTimeout

from ._helpers import (
    CancelRecordingProcessor,
    DemoProcessor,
    NeverFinishesHandler,
    RetryCycleProcessor,
    SlowHandler,
    StubbornHandler,
)


@pytest.mark.asyncio
async def test_watchdog_requeues_timed_out_task():
    proc = DemoProcessor()
    proc.add_task_handler(SlowHandler)

    # start managers (task_manager, task_queue_manager, watchdog)
    await proc.start()

    # push a task that will timeout quickly
    t_id = uuid4()
    proc.enqueue_task(
        t_id,
        TaskData(
            task="slow",
            payload=b'{"value": 5}',
            timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"),
        ),
    )

    # allow some time for task_manager to pick up and watchdog to act
    # Need to wait longer than watchdog sleep interval to ensure it runs at least once
    await asyncio.sleep(1.5)

    # task should have been requeued by watchdog
    assert any(tid == t_id for tid, _ in proc.requeued)

    # stop processor and wait for full shutdown so no background task leaks
    # into the event-loop teardown (stop() alone is fire-and-forget).
    await proc.exit()
    await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_watchdog_uses_configured_task_timeout_when_per_task_none():
    """When a task's own timeout is None, the watchdog uses the configured
    task_timeout (not the 3s default) as the cancellation deadline (AR-062)."""
    proc = DemoProcessor(TaskProcessorConfig(task_timeout=0.1, watchdog_interval=0.05))
    proc.add_task_handler(SlowHandler)
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(
            t_id,
            TaskData(
                task="slow",
                payload=b"{}",
                timeout=TaskTimeout(timeout=None, timeout_action="requeue"),
            ),
        )
        # The 3s default would not have cancelled SlowHandler (2s) yet, but the
        # configured 0.1s deadline does, so the task is requeued well under 2s.
        await asyncio.sleep(1.0)
        assert any(tid == t_id for tid, _ in proc.requeued)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_watchdog_does_not_requeue_when_handler_ignores_cancellation():
    """A handler that swallows CancelledError must not be requeued by the
    watchdog: it is still running, so requeueing would run it twice (AR-005).
    A short task_cancellation_timeout (config, AR-062) keeps the test fast."""
    proc = DemoProcessor(TaskProcessorConfig(task_cancellation_timeout=0.1))
    proc.add_task_handler(StubbornHandler)
    await proc._start_task_handler("StubbornHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(
            t_id,
            TaskData(
                task="stubborn",
                payload=b"{}",
                timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"),
            ),
        )
        # Wait past the watchdog interval (default 1s) plus the cancel wait so
        # the watchdog has acted and decided not to requeue.
        await asyncio.sleep(1.6)
        assert not any(tid == t_id for tid, _ in proc.requeued)
        # Let the stubborn handler finish so no dangling task remains.
        await asyncio.sleep(0.5)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_watchdog_ignored_cancellation_preserves_timeout_reason():
    """When a handler ignores watchdog cancellation, the tracker is removed but
    the 'timeout' reason survives to the eventual ack (AR-088): the handler's
    terminal on_task_completed must receive cancel_reason='timeout' even though
    the watchdog already stopped tracking the task."""
    proc = CancelRecordingProcessor(TaskProcessorConfig(task_cancellation_timeout=0.1))
    proc.add_task_handler(StubbornHandler)
    await proc._start_task_handler("StubbornHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(
            t_id,
            TaskData(
                task="stubborn",
                payload=b"{}",
                timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"),
            ),
        )
        # StubbornHandler: cancel at t~1s -> swallow + 0.3s -> finish ~1.4s.
        for _ in range(300):
            if any(tid == t_id for tid, *_ in proc.completed):
                break
            await asyncio.sleep(0.01)
        calls = [c for c in proc.completed if c[0] == t_id]
        assert len(calls) == 1
        assert calls[0][3] == "timeout"
        assert not any(tid == t_id for tid, _ in proc.requeued)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_watchdog_ignores_non_positive_timeout():
    """timeout <= 0 means 'no timeout': the watchdog must never cancel the
    task (AR-034)."""
    proc = DemoProcessor(TaskProcessorConfig(watchdog_interval=0.05))
    proc.add_task_handler(NeverFinishesHandler)
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(
            t_id,
            TaskData(
                task="never",
                payload=b"{}",
                timeout=TaskTimeout(timeout=0, timeout_action="requeue"),
            ),
        )
        # Wait until the task has been dispatched, then let the watchdog run
        # several times (interval 50ms). The task must still be running and
        # must not have been requeued.
        for _ in range(100):
            if t_id in proc.running_tasks:
                break
            await asyncio.sleep(0.01)
        assert t_id in proc.running_tasks
        await asyncio.sleep(0.3)
        assert t_id in proc.running_tasks
        assert not any(tid == t_id for tid, _ in proc.requeued)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_watchdog_ignores_non_positive_configured_task_timeout():
    """A configured task_timeout <= 0 means 'no timeout': when a task's own
    timeout is None, the watchdog must never cancel it (AR-062 preserves the
    unbounded sentinel and the is-not-None resolution of 0)."""
    proc = DemoProcessor(TaskProcessorConfig(task_timeout=0, watchdog_interval=0.05))
    proc.add_task_handler(NeverFinishesHandler)
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(
            t_id,
            TaskData(
                task="never",
                payload=b"{}",
                timeout=TaskTimeout(timeout=None, timeout_action="requeue"),
            ),
        )
        for _ in range(100):
            if t_id in proc.running_tasks:
                break
            await asyncio.sleep(0.01)
        assert t_id in proc.running_tasks
        await asyncio.sleep(0.3)
        assert t_id in proc.running_tasks
        assert not any(tid == t_id for tid, _ in proc.requeued)
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_watchdog_bounds_timeout_requeue_loop():
    """A task whose handler hangs forever is requeued exactly once by the
    watchdog, then terminated without redelivery (AR-104): the timeout-driven
    requeue loop is bounded end to end through the full manager pipeline.

    The first timeout requeues (budget 0 -> 1) and the transport redelivers;
    the second timeout hits the max_timeout_requeues=1 ceiling and terminates
    without redelivering. Two on_task_completed calls prove the redelivery
    round-trip actually ran, so the test would fail under an unbounded loop."""
    proc = RetryCycleProcessor(
        TaskProcessorConfig(
            task_timeout=0.1,
            watchdog_interval=0.05,
            task_cancellation_timeout=0.1,
            max_timeout_requeues=1,
        )
    )
    proc.add_task_handler(NeverFinishesHandler)
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(
            t_id,
            TaskData(
                task="never",
                payload=b"{}",
                timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"),
            ),
        )
        # Wait for the redelivery round-trip to complete (two completions), then
        # a generous extra window so a buggy third redelivery would surface.
        for _ in range(500):
            if len(proc.completed) >= 2:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(1.0)
        assert len(proc.requeued) == 1
        assert proc.requeued[0][0] == t_id
        assert len(proc.completed) == 2
    finally:
        await proc.exit()
        await proc.events["exit"].wait()
