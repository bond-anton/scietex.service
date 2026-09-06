"""Tests for manager restart (AR-001) and manager discovery/binding (AR-002)."""

import asyncio
from unittest.mock import patch

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


class CleanupRaisingWorker(BasicAsyncWorker):
    """Worker whose manager cleanup raises."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cleaned_up = False

    async def _raise_on_cleanup(worker) -> None:
        worker.cleaned_up = True
        raise RuntimeError("cleanup boom")

    @Manager(name="Messy", cleanup=_raise_on_cleanup)
    async def _messy_manager(self) -> None:
        await asyncio.sleep(0.05)


class CancellationIgnoringWorker(BasicAsyncWorker):
    """Worker whose manager ignores cancellation until told to stop."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ignore_cancellation = True

    @Manager(name="Stubborn")
    async def _stubborn_manager(self) -> None:
        while True:
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                # Swallow cancellation until the test flips the flag.
                if not self.ignore_cancellation:
                    raise


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
        assert "Doomed" not in worker._manager_runtime.tasks
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
        assert "Doomed" not in worker._manager_runtime.tasks

        # A fresh start of the same manager must succeed (task entry was cleared).
        manager = AlwaysFailingWorker.__dict__["_doomed_manager"]
        await worker._start_manager("Doomed", manager)
        assert "Doomed" in worker._manager_runtime.tasks
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


@pytest.mark.asyncio
async def test_cleanup_raising_manager_still_removed_from_tracking():
    """A manager whose cleanup raises must still be removed from task tracking."""
    worker = CleanupRaisingWorker()
    await worker.start()
    try:
        for _ in range(50):
            if "Messy" in worker._manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Messy" in worker._manager_runtime.tasks

        # Cleanup raises, which must propagate out of stop_manager, but the
        # task must still remove itself from tracking.
        with pytest.raises(RuntimeError):
            await worker._stop_manager("Messy")
        assert worker.cleaned_up
        assert "Messy" not in worker._manager_runtime.tasks
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_cancellation_ignoring_manager_stays_tracked_after_timeout():
    """A manager that ignores cancellation must stay tracked after stop times out."""
    worker = CancellationIgnoringWorker()
    await worker.start()
    try:
        for _ in range(50):
            if "Stubborn" in worker._manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Stubborn" in worker._manager_runtime.tasks

        original_task = worker._manager_runtime.tasks["Stubborn"]

        # On Python 3.12+ wait_for awaits the task to finish on timeout, so a
        # manager that ignores cancellation would hang the real call. Simulate
        # the shutdown timeout so stop_manager observes a still-running task.
        with patch("asyncio.wait_for", side_effect=TimeoutError):
            await worker._stop_manager("Stubborn")

        # The still-running task must remain tracked.
        assert "Stubborn" in worker._manager_runtime.tasks
        assert worker._manager_runtime.tasks["Stubborn"] is original_task

        # A restart attempt must not double-spawn the same name.
        manager = CancellationIgnoringWorker.__dict__["_stubborn_manager"]
        await worker._start_manager("Stubborn", manager)
        assert worker._manager_runtime.tasks["Stubborn"] is original_task
    finally:
        # Flip the flag so the manager honors the next cancellation and the
        # test cleans up without leaving a pending task.
        worker.ignore_cancellation = False
        if "Stubborn" in worker._manager_runtime.tasks:
            await worker._stop_manager("Stubborn")
        await worker.stop()
