"""TaskProcessor task-manager fetch timeout and queue-manager backoff tests."""

import asyncio

import pytest

import scietex.service.task_processor as mod
from scietex.service.config import TaskProcessorConfig

from ._helpers import DemoProcessor, ReportingProcessor


@pytest.mark.asyncio
async def test_task_manager_uses_configured_queue_fetch_timeout():
    """task_manager passes the configured task_queue_fetch_timeout to the
    wait_for that guards the queue get (AR-062)."""

    proc = DemoProcessor(TaskProcessorConfig(task_queue_fetch_timeout=0.25))
    observed: list = []
    real_wait_for = mod.asyncio.wait_for

    async def spy_wait_for(coro, timeout=None):
        observed.append(timeout)
        return await real_wait_for(coro, timeout=timeout)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(mod.asyncio, "wait_for", spy_wait_for)
    try:
        mgr = asyncio.create_task(proc.task_manager())
        # The queue is empty, so task_manager blocks in wait_for(get(), ...)
        # for the configured timeout; wait until the spy has recorded it.
        for _ in range(100):
            if observed:
                break
            await asyncio.sleep(0.01)
        mgr.cancel()
        try:
            await mgr
        except asyncio.CancelledError:
            pass
    finally:
        monkeypatch.undo()

    assert observed == [0.25], "task_manager must use the configured task_queue_fetch_timeout"


@pytest.mark.asyncio
async def test_task_queue_manager_skips_sleep_after_productive_fetch():
    """task_queue_manager must not sleep after a fetch_tasks that reports it
    enqueued work, so a backlog drains back-to-back (AR-042)."""
    proc = ReportingProcessor(TaskProcessorConfig(task_queue_manager_sleep_time=0.01), fetch_result=True)
    backoff_delays: list = []
    real_sleep = asyncio.sleep

    async def spy_sleep(delay):
        if delay == proc.task_queue_manager_sleep_time:
            backoff_delays.append(delay)
        await real_sleep(delay)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(mod.asyncio, "sleep", spy_sleep)
    try:
        mgr = asyncio.create_task(proc.task_queue_manager())
        await asyncio.sleep(0.05)
        mgr.cancel()
        try:
            await mgr
        except asyncio.CancelledError:
            pass
    finally:
        monkeypatch.undo()

    assert backoff_delays == [], "a productive fetch must not trigger the idle backoff sleep"


@pytest.mark.asyncio
async def test_task_queue_manager_sleeps_after_empty_fetch():
    """task_queue_manager must sleep after a fetch_tasks that reports nothing
    enqueued, to avoid busy-polling an empty source (AR-042)."""
    proc = ReportingProcessor(TaskProcessorConfig(task_queue_manager_sleep_time=0.01), fetch_result=False)
    backoff_delays: list = []
    real_sleep = asyncio.sleep

    async def spy_sleep(delay):
        if delay == proc.task_queue_manager_sleep_time:
            backoff_delays.append(delay)
        await real_sleep(delay)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(mod.asyncio, "sleep", spy_sleep)
    try:
        mgr = asyncio.create_task(proc.task_queue_manager())
        await asyncio.sleep(0.05)
        mgr.cancel()
        try:
            await mgr
        except asyncio.CancelledError:
            pass
    finally:
        monkeypatch.undo()

    assert backoff_delays, "an empty fetch must trigger the idle backoff sleep"
