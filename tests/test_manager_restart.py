"""Tests for manager restart (AR-001) and manager discovery/binding (AR-002)."""

import asyncio
import logging
from unittest.mock import patch

import pytest

from scietex.service.basic_worker import BasicWorker, ServiceStatus
from scietex.service.config import WorkerConfig
from scietex.service.manager import Manager, ManagerStatus


class FlakyWorker(BasicWorker):
    """Worker whose manager fails a fixed number of times then succeeds."""

    def __init__(self, config: WorkerConfig | None = None, *, failures_before_success: int = 1):
        super().__init__(config)
        self.failures_before_success = failures_before_success
        self.attempts = 0

    @Manager(name="Flaky")
    async def _flaky_manager(self) -> None:
        self.attempts += 1
        if self.attempts <= self.failures_before_success:
            raise RuntimeError("boom")
        await asyncio.sleep(0.05)


class AlwaysFailingWorker(BasicWorker):
    """Worker whose manager always raises."""

    def __init__(self, config: WorkerConfig | None = None):
        super().__init__(config)
        self.attempts = 0

    @Manager(name="Doomed")
    async def _doomed_manager(self) -> None:
        self.attempts += 1
        raise RuntimeError("always fails")


class BaseWorker(BasicWorker):
    @Manager(name="Shared")
    async def _shared_manager(self) -> None:
        self.base_ran = True
        await asyncio.sleep(0.05)


class DerivedWorker(BaseWorker):
    @Manager(name="Shared")
    async def _shared_manager(self) -> None:
        self.derived_ran = True
        await asyncio.sleep(0.05)


class CleanupRaisingWorker(BasicWorker):
    """Worker whose manager cleanup raises."""

    def __init__(self, config: WorkerConfig | None = None):
        super().__init__(config)
        self.cleaned_up = False

    async def _raise_on_cleanup(worker) -> None:
        worker.cleaned_up = True
        raise RuntimeError("cleanup boom")

    @Manager(name="Messy", cleanup=_raise_on_cleanup)
    async def _messy_manager(self) -> None:
        await asyncio.sleep(0.05)


class CleanupWorker(BasicWorker):
    """Worker whose manager cleanup callable runs on a clean shutdown."""

    def __init__(self, config: WorkerConfig | None = None):
        super().__init__(config)
        self.cleaned_up = False

    async def _record_cleanup(worker) -> None:
        worker.cleaned_up = True

    @Manager(name="Neat", cleanup=_record_cleanup)
    async def _neat_manager(self) -> None:
        while True:
            await asyncio.sleep(0.05)


class CancellationIgnoringWorker(BasicWorker):
    """Worker whose manager ignores cancellation until told to stop."""

    def __init__(self, config: WorkerConfig | None = None):
        super().__init__(config)
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


class RunLoopWorker(BasicWorker):
    """Worker whose manager runs a long-lived loop."""

    @Manager(name="Loop")
    async def _loop_manager(self) -> None:
        while True:
            await asyncio.sleep(0.05)


class DuplicateNameWorker(BasicWorker):
    """Worker whose two unrelated managers pick the same ``name=`` (AR-068)."""

    @Manager(name="Dup")
    async def _dup_first(self) -> None:
        self.first_ran = True
        await asyncio.sleep(0.05)

    @Manager(name="Dup")
    async def _dup_second(self) -> None:
        self.second_ran = True
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_manager_status_reports_running_and_stopped():
    """The manager enum status must reflect RUNNING while alive and STOPPED after."""
    worker = RunLoopWorker()
    await worker.start()
    try:
        # Wait until the manager task is tracked so its loop has begun.
        for _ in range(50):
            if "Loop" in worker._manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Loop" in worker._manager_runtime.tasks
        assert worker._manager_runtime.statuses["Loop"] == ManagerStatus.RUNNING
    finally:
        await worker.stop()
    # The task marks itself STOPPED in its finally block, which may complete
    # asynchronously after stop() returns; poll until it lands.
    for _ in range(50):
        if worker._manager_runtime.statuses.get("Loop") == ManagerStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker._manager_runtime.statuses.get("Loop") == ManagerStatus.STOPPED


@pytest.mark.asyncio
async def test_manager_retries_after_error_without_deadlock():
    """A manager that fails once must be retried in the same task, not deadlock."""
    worker = FlakyWorker(WorkerConfig(manager_max_retries=5, manager_restart_backoff=0.01), failures_before_success=1)
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
    worker = AlwaysFailingWorker(WorkerConfig(manager_max_retries=2, manager_restart_backoff=0.01))
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
    worker = AlwaysFailingWorker(WorkerConfig(manager_max_retries=1, manager_restart_backoff=0.01))
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
        await worker._manager_runtime.start_manager("Doomed", manager)
        assert "Doomed" in worker._manager_runtime.tasks
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_manager_exhausting_retries_ends_failed():
    """A manager that exhausts its retry budget must end FAILED, not STOPPED (AR-063)."""
    worker = AlwaysFailingWorker(WorkerConfig(manager_max_retries=2, manager_restart_backoff=0.01))
    await worker.start()
    try:
        # Wait for the manager to give up (3 failures: initial + 2 retries) and
        # reach its terminal FAILED state.
        for _ in range(100):
            if worker._manager_runtime.statuses.get("Doomed") == ManagerStatus.FAILED:
                break
            await asyncio.sleep(0.05)
        assert worker._manager_runtime.statuses.get("Doomed") == ManagerStatus.FAILED
        assert worker._manager_runtime.errors.get("Doomed") is not None
        # The failure must be surfaced to the worker's public accessor.
        assert "Doomed" in worker.failed_managers
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_cleanly_stopped_manager_not_failed():
    """A cleanly-stopped manager must end STOPPED, never FAILED (AR-063)."""
    worker = RunLoopWorker()
    await worker.start()
    try:
        for _ in range(50):
            if "Loop" in worker._manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Loop" in worker._manager_runtime.tasks
    finally:
        await worker.stop()
    # Poll until the task lands in its terminal state.
    for _ in range(50):
        status = worker._manager_runtime.statuses.get("Loop")
        if status in (ManagerStatus.STOPPED, ManagerStatus.FAILED):
            break
        await asyncio.sleep(0.05)
    assert worker._manager_runtime.statuses.get("Loop") == ManagerStatus.STOPPED
    assert "Loop" not in worker.failed_managers


@pytest.mark.asyncio
async def test_subclass_manager_override_wins():
    """Most-derived manager definition must win over a base-class override."""
    worker = DerivedWorker()
    # Discovery must yield the derived manager only (base is shadowed).
    names = [name for name, _ in worker._manager_runtime.iter_manager_definitions()]
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
    worker = BasicWorker()
    # Accessing the attribute on an instance must return a bound coroutine function.
    bound = worker._heartbeat_manager
    assert callable(bound)
    # The class attribute must still be the Manager instance for discovery.
    assert isinstance(BasicWorker.__dict__["_heartbeat_manager"], Manager)


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
            await worker._manager_runtime.stop_manager("Messy")
        assert worker.cleaned_up
        assert "Messy" not in worker._manager_runtime.tasks
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_cleanup_callable_runs_on_clean_shutdown():
    """A @Manager(cleanup=...) callable must run on a clean (non-raising) shutdown."""
    worker = CleanupWorker()
    await worker.start()
    try:
        for _ in range(50):
            if "Neat" in worker._manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Neat" in worker._manager_runtime.tasks
    finally:
        await worker.stop()
    # stop() spawns _shutdown asynchronously; the cleanup callable runs inside
    # the cancelled manager task's finally block, so poll until it records.
    for _ in range(50):
        if worker.cleaned_up:
            break
        await asyncio.sleep(0.05)
    assert worker.cleaned_up


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
        # Use asyncio.TimeoutError (not the builtin): on 3.10 it is a distinct
        # subclass, so the builtin would not be caught by stop_manager.
        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            await worker._manager_runtime.stop_manager("Stubborn")

        # The still-running task must remain tracked.
        assert "Stubborn" in worker._manager_runtime.tasks
        assert worker._manager_runtime.tasks["Stubborn"] is original_task

        # A restart attempt must not double-spawn the same name.
        manager = CancellationIgnoringWorker.__dict__["_stubborn_manager"]
        await worker._manager_runtime.start_manager("Stubborn", manager)
        assert worker._manager_runtime.tasks["Stubborn"] is original_task
    finally:
        # Flip the flag so the manager honors the next cancellation and the
        # test cleans up without leaving a pending task.
        worker.ignore_cancellation = False
        if "Stubborn" in worker._manager_runtime.tasks:
            await worker._manager_runtime.stop_manager("Stubborn")
        await worker.stop()


@pytest.mark.asyncio
async def test_manager_name_collision_logs_warning(caplog):
    """A manager name collision must log a WARNING and keep the first definition (AR-068)."""
    worker = DuplicateNameWorker()
    with caplog.at_level(logging.WARNING):
        names = [name for name, _ in worker._manager_runtime.iter_manager_definitions()]
    # Dedup semantics unchanged: only the first "Dup" definition is yielded.
    assert names.count("Dup") == 1
    # The collision is surfaced, not silently dropped.
    assert any("collides" in record.getMessage() and "Dup" in record.getMessage() for record in caplog.records)
