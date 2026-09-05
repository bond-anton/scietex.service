"""Tests for manager restart (AR-001) and manager discovery/binding (AR-002)."""

import asyncio

import pytest

from scietex.service.basic_async_worker import BasicAsyncWorker, ServiceStatus
from scietex.service.manager import Manager


class FlakyWorker(BasicAsyncWorker):
    """Worker whose manager fails a fixed number of times then succeeds."""

    def __init__(self, failures_before_success: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.failures_before_success = failures_before_success
        self.attempts = 0

    @Manager(name="Flaky")
    async def _flaky_manager(self) -> None:
        self.attempts += 1
        if self.attempts <= self.failures_before_success:
            raise RuntimeError("boom")
        await asyncio.sleep(0.05)


class AlwaysFailingWorker(BasicAsyncWorker):
    """Worker whose manager always raises."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.attempts = 0

    @Manager(name="Doomed")
    async def _doomed_manager(self) -> None:
        self.attempts += 1
        raise RuntimeError("always fails")


class BaseWorker(BasicAsyncWorker):
    @Manager(name="Shared")
    async def _shared_manager(self) -> None:
        self.base_ran = True
        await asyncio.sleep(0.05)


class DerivedWorker(BaseWorker):
    @Manager(name="Shared")
    async def _shared_manager(self) -> None:
        self.derived_ran = True
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_manager_retries_after_error_without_deadlock():
    """A manager that fails once must be retried in the same task, not deadlock."""
    worker = FlakyWorker(failures_before_success=1, manager_max_retries=5, manager_restart_backoff=0.01)
    await worker.start()
    try:
        # Give the manager time to fail once and be retried successfully.
        for _ in range(50):
            if worker.attempts >= 2:
                break
            await asyncio.sleep(0.05)
        assert worker.attempts >= 2, f"expected retry, got {worker.attempts} attempts"
        assert worker.state == ServiceStatus.RUNNING
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_manager_gives_up_and_removes_stale_task_entry():
    """A manager that exhausts retries must give up and clear its task entry."""
    worker = AlwaysFailingWorker(manager_max_retries=2, manager_restart_backoff=0.01)
    await worker.start()
    try:
        # Wait long enough for the manager to fail 3 times (2 retries + initial).
        for _ in range(100):
            if worker.attempts >= 3:
                break
            await asyncio.sleep(0.05)
        assert worker.attempts >= 3, f"expected give-up after retries, got {worker.attempts} attempts"
        # The task must have removed itself from tracking (no stale entry).
        assert "Doomed" not in worker._BasicAsyncWorker__manager_tasks
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_manager_can_restart_after_giving_up():
    """After a manager gives up, it must be restartable (no stale bookkeeping)."""
    worker = AlwaysFailingWorker(manager_max_retries=1, manager_restart_backoff=0.01)
    await worker.start()
    try:
        for _ in range(100):
            if worker.attempts >= 2:
                break
            await asyncio.sleep(0.05)
        assert worker.attempts >= 2
        assert "Doomed" not in worker._BasicAsyncWorker__manager_tasks

        # A fresh start of the same manager must succeed (task entry was cleared).
        manager = AlwaysFailingWorker.__dict__["_doomed_manager"]
        await worker._start_manager("Doomed", manager)
        assert "Doomed" in worker._BasicAsyncWorker__manager_tasks
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_subclass_manager_override_wins():
    """Most-derived manager definition must win over a base-class override."""
    worker = DerivedWorker()
    # Discovery must yield the derived manager only (base is shadowed).
    names = [name for name, _ in worker._iter_manager_definitions()]
    assert names.count("Shared") == 1
    await worker.start()
    try:
        for _ in range(50):
            if getattr(worker, "derived_ran", False):
                break
            await asyncio.sleep(0.05)
        assert getattr(worker, "derived_ran", False), "derived manager should have run"
        assert not getattr(worker, "base_ran", False), "base manager should not run when overridden"
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_manager_decorated_method_is_callable():
    """A @Manager-decorated method must remain callable as a normal bound method."""
    worker = BasicAsyncWorker()
    # Accessing the attribute on an instance must return a bound coroutine function.
    bound = worker._heartbeat_manager
    assert callable(bound)
    # The class attribute must still be the Manager instance for discovery.
    assert isinstance(BasicAsyncWorker.__dict__["_heartbeat_manager"], Manager)
