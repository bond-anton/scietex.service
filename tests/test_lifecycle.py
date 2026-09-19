"""Tests for ``WorkerLifecycle`` state-machine and event bookkeeping (AR-087)."""

import asyncio
from datetime import datetime, timezone
from typing import cast

import pytest

from scietex.service.basic_worker import BasicWorker, ServiceStatus
from scietex.service.lifecycle import InvalidStateTransition, WorkerLifecycle


class _StubWorker:
    """Minimal stand-in for ``BasicWorker`` used by ``WorkerLifecycle``.

    Provides only the ``exit()`` hook that ``request_exit()`` spawns; the hook
    records the call, marks exit requested, and blocks until released so the
    pending stop task can be observed.
    """

    def __init__(self) -> None:
        self.lifecycle: WorkerLifecycle | None = None
        self.exit_calls = 0
        self.block = asyncio.Event()

    async def exit(self) -> None:
        self.exit_calls += 1
        assert self.lifecycle is not None
        self.lifecycle.events["exit_requested"].set()
        await self.block.wait()


def test_initial_state_is_stopped():
    """A fresh lifecycle starts STOPPED with no start time and unset events."""
    lifecycle = WorkerLifecycle(cast(BasicWorker, _StubWorker()))

    assert lifecycle.state == ServiceStatus.STOPPED
    assert lifecycle.start_time is None
    assert not lifecycle.events["exit_requested"].is_set()
    assert not lifecycle.events["exit"].is_set()


def test_state_and_start_time_reflect_internal_values():
    """The read-only properties surface the lifecycle's current bookkeeping."""
    lifecycle = WorkerLifecycle(cast(BasicWorker, _StubWorker()))

    start = datetime.now(timezone.utc)
    lifecycle.transition(ServiceStatus.STARTING)
    lifecycle.transition(ServiceStatus.RUNNING)
    lifecycle._start_time = start

    assert lifecycle.state == ServiceStatus.RUNNING
    assert lifecycle.start_time == start


def test_force_stopped_clears_state_and_events():
    """force_stopped() lands the lifecycle in STOPPED and finalizes exit events."""
    lifecycle = WorkerLifecycle(cast(BasicWorker, _StubWorker()))

    lifecycle.transition(ServiceStatus.STARTING)
    lifecycle.transition(ServiceStatus.RUNNING)
    lifecycle._start_time = datetime.now(timezone.utc)
    lifecycle._events["exit_requested"].set()

    lifecycle.force_stopped()

    assert lifecycle.state == ServiceStatus.STOPPED
    assert lifecycle.start_time is None
    assert not lifecycle.events["exit_requested"].is_set()
    assert lifecycle.events["exit"].is_set()


def test_force_stopped_without_exit_leaves_exit_unset():
    """Without a requested exit, force_stopped() must not set the exit event."""
    lifecycle = WorkerLifecycle(cast(BasicWorker, _StubWorker()))

    lifecycle.transition(ServiceStatus.STARTING)
    lifecycle.transition(ServiceStatus.STOPPING)
    lifecycle.force_stopped()

    assert lifecycle.state == ServiceStatus.STOPPED
    assert not lifecycle.events["exit_requested"].is_set()
    assert not lifecycle.events["exit"].is_set()


def test_events_mapping_is_read_only():
    """The events mapping rejects mutation while its Event values stay mutable."""
    lifecycle = WorkerLifecycle(cast(BasicWorker, _StubWorker()))

    with pytest.raises(TypeError):
        lifecycle.events["new_event"] = asyncio.Event()


@pytest.mark.asyncio
async def test_request_exit_spawns_single_stop_task():
    """A second request_exit() while a stop task is pending must not duplicate it."""
    worker = _StubWorker()
    lifecycle = WorkerLifecycle(cast(BasicWorker, worker))
    worker.lifecycle = lifecycle

    lifecycle.request_exit()
    # Yield so the spawned StopTask runs far enough to record the call and block.
    await asyncio.sleep(0)
    assert worker.exit_calls == 1
    assert lifecycle.events["exit_requested"].is_set()

    # A second request while the stop task is pending must not spawn another.
    lifecycle.request_exit()
    await asyncio.sleep(0)
    assert worker.exit_calls == 1

    # Release the pending stop task so the test ends with no dangling task.
    worker.block.set()
    stop_task = lifecycle._stop_task
    assert stop_task is not None
    await stop_task


@pytest.mark.asyncio
async def test_wait_until_stopped_returns_once_stopped():
    """The stopped-wait event returns only after the state reaches STOPPED."""
    lifecycle = WorkerLifecycle(cast(BasicWorker, _StubWorker()))

    lifecycle.transition(ServiceStatus.STARTING)
    lifecycle.transition(ServiceStatus.RUNNING)
    lifecycle.transition(ServiceStatus.STOPPING)
    wait_task = asyncio.create_task(lifecycle._wait_until_stopped())

    # The task must still be pending while the state is not STOPPED.
    await asyncio.sleep(0)
    assert not wait_task.done()

    lifecycle.transition(ServiceStatus.STOPPED)
    await asyncio.wait_for(wait_task, timeout=1.0)


def test_transition_rejects_illegal_edge():
    """An illegal edge raises InvalidStateTransition and leaves state unchanged."""
    lifecycle = WorkerLifecycle(cast(BasicWorker, _StubWorker()))

    with pytest.raises(InvalidStateTransition):
        lifecycle.transition(ServiceStatus.RUNNING)

    assert lifecycle.state is ServiceStatus.STOPPED


@pytest.mark.asyncio
async def test_transition_to_stopped_wakes_waiters():
    """transition(STOPPED) wakes a waiter blocked in _wait_until_stopped()."""
    lifecycle = WorkerLifecycle(cast(BasicWorker, _StubWorker()))

    lifecycle.transition(ServiceStatus.STARTING)
    lifecycle.transition(ServiceStatus.RUNNING)
    lifecycle.transition(ServiceStatus.STOPPING)
    wait_task = asyncio.create_task(lifecycle._wait_until_stopped())
    await asyncio.sleep(0)
    assert not wait_task.done()

    lifecycle.transition(ServiceStatus.STOPPED)
    await asyncio.wait_for(wait_task, timeout=1.0)
