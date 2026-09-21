"""Tests for manager restart (AR-001) and manager discovery/binding (AR-002)."""

import asyncio
import logging
from unittest.mock import patch

import pytest

from scietex.service.basic_worker import BasicWorker, ServiceStatus
from scietex.service.config import WorkerConfig
from scietex.service.manager import (
    MANAGER_REGISTRY_ATTR,
    Manager,
    ManagerDefinition,
    ManagerStatus,
    register_manager,
)


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
            if "Loop" in worker.manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Loop" in worker.manager_runtime.tasks
        assert worker.manager_runtime.statuses["Loop"] == ManagerStatus.RUNNING
    finally:
        await worker.stop()
    # The task marks itself STOPPED in its finally block, which may complete
    # asynchronously after stop() returns; poll until it lands.
    for _ in range(50):
        if worker.manager_runtime.statuses.get("Loop") == ManagerStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker.manager_runtime.statuses.get("Loop") == ManagerStatus.STOPPED


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
        assert "Doomed" not in worker.manager_runtime.tasks
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
        assert "Doomed" not in worker.manager_runtime.tasks

        # A fresh start of the same manager must succeed (task entry was cleared).
        manager = AlwaysFailingWorker.__dict__["_doomed_manager"]
        await worker.manager_runtime.start_manager("Doomed", manager)
        assert "Doomed" in worker.manager_runtime.tasks
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
            if worker.manager_runtime.statuses.get("Doomed") == ManagerStatus.FAILED:
                break
            await asyncio.sleep(0.05)
        assert worker.manager_runtime.statuses.get("Doomed") == ManagerStatus.FAILED
        assert worker.manager_runtime.errors.get("Doomed") is not None
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
            if "Loop" in worker.manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Loop" in worker.manager_runtime.tasks
    finally:
        await worker.stop()
    # Poll until the task lands in its terminal state.
    for _ in range(50):
        status = worker.manager_runtime.statuses.get("Loop")
        if status in (ManagerStatus.STOPPED, ManagerStatus.FAILED):
            break
        await asyncio.sleep(0.05)
    assert worker.manager_runtime.statuses.get("Loop") == ManagerStatus.STOPPED
    assert "Loop" not in worker.failed_managers


@pytest.mark.asyncio
async def test_subclass_manager_override_wins():
    """Most-derived manager definition must win over a base-class override."""
    worker = DerivedWorker()
    # Discovery must yield the derived manager only (base is shadowed).
    names = [name for name, _ in worker.manager_runtime.iter_manager_definitions()]
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
            if "Messy" in worker.manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Messy" in worker.manager_runtime.tasks

        # Cleanup raises, which must propagate out of stop_manager, but the
        # task must still remove itself from tracking.
        with pytest.raises(RuntimeError):
            await worker.manager_runtime.stop_manager("Messy")
        assert worker.cleaned_up
        assert "Messy" not in worker.manager_runtime.tasks
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_cleanup_callable_runs_on_clean_shutdown():
    """A @Manager(cleanup=...) callable must run on a clean (non-raising) shutdown."""
    worker = CleanupWorker()
    await worker.start()
    try:
        for _ in range(50):
            if "Neat" in worker.manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Neat" in worker.manager_runtime.tasks
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
            if "Stubborn" in worker.manager_runtime.tasks:
                break
            await asyncio.sleep(0.05)
        assert "Stubborn" in worker.manager_runtime.tasks

        original_task = worker.manager_runtime.tasks["Stubborn"]

        # On Python 3.12+ wait_for awaits the task to finish on timeout, so a
        # manager that ignores cancellation would hang the real call. Simulate
        # the shutdown timeout so stop_manager observes a still-running task.
        # Use asyncio.TimeoutError (not the builtin): on 3.10 it is a distinct
        # subclass, so the builtin would not be caught by stop_manager.
        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            await worker.manager_runtime.stop_manager("Stubborn")

        # The still-running task must remain tracked.
        assert "Stubborn" in worker.manager_runtime.tasks
        assert worker.manager_runtime.tasks["Stubborn"] is original_task

        # A restart attempt must not double-spawn the same name.
        manager = CancellationIgnoringWorker.__dict__["_stubborn_manager"]
        await worker.manager_runtime.start_manager("Stubborn", manager)
        assert worker.manager_runtime.tasks["Stubborn"] is original_task
    finally:
        # Flip the flag so the manager honors the next cancellation and the
        # test cleans up without leaving a pending task.
        worker.ignore_cancellation = False
        if "Stubborn" in worker.manager_runtime.tasks:
            await worker.manager_runtime.stop_manager("Stubborn")
        await worker.stop()


@pytest.mark.asyncio
async def test_manager_name_collision_logs_warning(caplog):
    """A manager name collision must log a WARNING and keep the first definition (AR-068)."""
    worker = DuplicateNameWorker()
    with caplog.at_level(logging.WARNING):
        names = [name for name, _ in worker.manager_runtime.iter_manager_definitions()]
    # Dedup semantics unchanged: only the first "Dup" definition is yielded.
    assert names.count("Dup") == 1
    # The collision is surfaced, not silently dropped.
    assert any("collides" in record.getMessage() and "Dup" in record.getMessage() for record in caplog.records)


def test_manager_requires_explicit_name():
    """Constructing a Manager with no name must raise TypeError (AR-086)."""
    with pytest.raises(TypeError):
        Manager()


@pytest.mark.parametrize("bad_name", [None, "", "   "])
def test_manager_rejects_blank_or_non_string_name(bad_name):
    """Manager must reject a non-string or blank name (AR-086)."""
    with pytest.raises(TypeError):
        Manager(bad_name)


def test_bare_manager_decorator_rejects_function_as_name():
    """The bare @Manager form must reject the decorated function passed as name (AR-086)."""

    async def _method(self) -> None:
        await asyncio.sleep(0.05)

    with pytest.raises(TypeError):
        Manager(_method)


def test_manager_call_decorator_requires_name():
    """The @Manager() form must raise before the method is even passed (AR-086)."""

    async def _method(self) -> None:
        await asyncio.sleep(0.05)

    with pytest.raises(TypeError):
        Manager()(_method)


def test_register_manager_requires_name_keyword():
    """register_manager must require the name keyword (AR-086)."""

    class W(BasicWorker):
        async def _extra(self) -> None: ...

    with pytest.raises(TypeError):
        register_manager(W, W._extra)


def test_register_manager_rejects_blank_name():
    """register_manager must reject a blank name (AR-086)."""

    class W(BasicWorker):
        async def _extra(self) -> None: ...

    with pytest.raises(TypeError):
        register_manager(W, W._extra, name="")


def test_manager_name_stable_across_attribute_rename():
    """Manager identity must be its explicit name, not the attribute name (AR-086)."""

    async def _loop(self) -> None:
        await asyncio.sleep(0.05)

    manager = Manager(name="Stable")
    manager(_loop)
    Renamed = type("Renamed", (BasicWorker,), {"_renamed": manager})

    # type.__new__ invokes __set_name__, so owner/attribute_name are recorded
    # even though the attribute name differs from the manager's identity.
    assert manager.attribute_name == "_renamed"
    assert manager.owner is Renamed
    names = [name for name, _ in Renamed().manager_runtime.iter_manager_definitions()]
    assert "Stable" in names


def test_same_name_different_attribute_names_yield_same_name():
    """Two classes sharing name= under different attributes yield the same name (AR-086)."""

    class First(BasicWorker):
        @Manager(name="SharedName")
        async def _alpha(self) -> None:
            await asyncio.sleep(0.05)

    class Second(BasicWorker):
        @Manager(name="SharedName")
        async def _beta(self) -> None:
            await asyncio.sleep(0.05)

    assert "SharedName" in [name for name, _ in First().manager_runtime.iter_manager_definitions()]
    assert "SharedName" in [name for name, _ in Second().manager_runtime.iter_manager_definitions()]
    assert First.__dict__["_alpha"].attribute_name == "_alpha"
    assert Second.__dict__["_beta"].attribute_name == "_beta"


def test_manager_registry_isolation_between_subclasses():
    """Each subclass must own an independent registry; managers never leak across subclasses (AR-086)."""

    class A(BasicWorker):
        @Manager(name="OnlyA")
        async def _a(self) -> None:
            await asyncio.sleep(0.05)

    class B(BasicWorker):
        @Manager(name="OnlyB")
        async def _b(self) -> None:
            await asyncio.sleep(0.05)

    a_names = {name for name, _ in A().manager_runtime.iter_manager_definitions()}
    b_names = {name for name, _ in B().manager_runtime.iter_manager_definitions()}
    assert "OnlyA" in a_names and "OnlyA" not in b_names
    assert "OnlyB" in b_names and "OnlyB" not in a_names
    assert A.__dict__[MANAGER_REGISTRY_ATTR] is not B.__dict__[MANAGER_REGISTRY_ATTR]


def test_manager_definition_order_preserved():
    """Managers must be recorded in class-body order (AR-086)."""

    class Ordered(BasicWorker):
        @Manager(name="First")
        async def _one(self) -> None:
            await asyncio.sleep(0.05)

        @Manager(name="Second")
        async def _two(self) -> None:
            await asyncio.sleep(0.05)

    assert [manager.name for manager in Ordered.__dict__[MANAGER_REGISTRY_ATTR]] == ["First", "Second"]


def test_manager_metadata_captured_on_basic_worker():
    """The base worker's heartbeat manager must capture owner/attribute_name/name (AR-086)."""
    manager = BasicWorker.__dict__["_heartbeat_manager"]
    assert manager.owner is BasicWorker
    assert manager.attribute_name == "_heartbeat_manager"
    assert manager.name == "Heartbeat"


def test_registry_entries_are_definitions_with_yielded_names():
    """Each discovered entry must be a ManagerDefinition whose name matches the yielded name (AR-086)."""
    worker = BasicWorker()
    for name, manager in worker.manager_runtime.iter_manager_definitions():
        assert isinstance(manager, ManagerDefinition)
        assert manager.name == name


def test_manager_alias_dedup_in_registry():
    """A Manager aliased under two attribute names must appear once in the registry (AR-086)."""

    async def _loop(self) -> None:
        await asyncio.sleep(0.05)

    manager = Manager(name="Aliased")
    manager(_loop)
    Aliased = type("Aliased", (BasicWorker,), {"_first": manager, "_second": manager})

    registry = Aliased.__dict__[MANAGER_REGISTRY_ATTR]
    assert len(registry) == 1
    assert isinstance(registry[0], ManagerDefinition)
    assert Aliased.__dict__["_first"] is manager and Aliased.__dict__["_second"] is manager
    names = [name for name, _ in Aliased().manager_runtime.iter_manager_definitions()]
    assert names.count("Aliased") == 1


@pytest.mark.asyncio
async def test_register_manager_post_creation_discovered_and_executed():
    """A manager registered after class creation must be discovered and run (AR-086)."""

    class W(BasicWorker):
        def __init__(self, config: WorkerConfig | None = None):
            super().__init__(config)
            self.custom_ran = False

        async def _custom(self) -> None:
            self.custom_ran = True
            await asyncio.sleep(0.05)

    register_manager(W, W._custom, name="Custom")
    worker = W()
    assert "Custom" in [name for name, _ in worker.manager_runtime.iter_manager_definitions()]
    await worker.start()
    try:
        for _ in range(50):
            if worker.custom_ran:
                break
            await asyncio.sleep(0.05)
        assert worker.custom_ran, "registered manager should have run"
    finally:
        await worker.stop()


def test_register_manager_attribute_name_binding():
    """attribute_name binds a callable attribute while name stays the identity (AR-086)."""

    class W(BasicWorker):
        async def _custom_impl(self) -> None:
            await asyncio.sleep(0.05)

    manager = register_manager(W, W._custom_impl, name="Custom", attribute_name="_custom")
    worker = W()
    assert manager.name == "Custom"
    assert W.__dict__["_custom"] is manager
    assert callable(worker._custom)
    assert "Custom" in [name for name, _ in worker.manager_runtime.iter_manager_definitions()]


def test_register_manager_replace_upserts_in_place(caplog):
    """replace=True must replace the same-named manager in place, preserving order (AR-086)."""

    class W(BasicWorker):
        async def _alpha(self) -> None: ...
        async def _beta(self) -> None: ...
        async def _beta_two(self) -> None: ...

    register_manager(W, W._alpha, name="Alpha")
    register_manager(W, W._beta, name="Beta")
    register_manager(W, W._beta_two, name="Beta")

    registry = W.__dict__[MANAGER_REGISTRY_ATTR]
    assert [manager.name for manager in registry] == ["Alpha", "Beta"]
    assert registry[1].method is W._beta_two

    worker = W()
    with caplog.at_level(logging.WARNING):
        names = [name for name, _ in worker.manager_runtime.iter_manager_definitions()]
    assert names.count("Beta") == 1
    assert not any("collides" in record.getMessage() for record in caplog.records)


def test_register_manager_append_duplicates_collide(caplog):
    """replace=False must append, surfacing a discovery-time collision warning (AR-086)."""

    class W(BasicWorker):
        async def _first(self) -> None: ...
        async def _second(self) -> None: ...

    register_manager(W, W._first, name="Dup")
    register_manager(W, W._second, name="Dup", replace=False)

    assert [manager.name for manager in W.__dict__[MANAGER_REGISTRY_ATTR]] == ["Dup", "Dup"]
    worker = W()
    with caplog.at_level(logging.WARNING):
        names = [name for name, _ in worker.manager_runtime.iter_manager_definitions()]
    assert names.count("Dup") == 1
    assert any("collides" in record.getMessage() and "Dup" in record.getMessage() for record in caplog.records)


def test_register_manager_on_base_does_not_shadow_subclass(caplog):
    """A base registration after a subclass exists must not shadow the subclass override (AR-086)."""

    class Base(BasicWorker):
        @Manager(name="Shared")
        async def _base_shared(self) -> None:
            await asyncio.sleep(0.05)

    class Derived(Base):
        @Manager(name="Shared")
        async def _derived_shared(self) -> None:
            await asyncio.sleep(0.05)

    async def _extra(self) -> None:
        await asyncio.sleep(0.05)

    register_manager(Base, _extra, name="Shared", replace=False)

    worker = Derived()
    with caplog.at_level(logging.WARNING):
        pairs = list(worker.manager_runtime.iter_manager_definitions())
    names = [name for name, _ in pairs]
    assert names.count("Shared") == 1
    assert pairs[names.index("Shared")][1] is Derived.__dict__["_derived_shared"].definition
    assert any("collides" in record.getMessage() and "Shared" in record.getMessage() for record in caplog.records)


def test_register_manager_rejects_non_type_owner():
    """register_manager must reject a non-class owner (AR-086)."""

    async def _extra(self) -> None:
        await asyncio.sleep(0.05)

    worker = BasicWorker()
    with pytest.raises(TypeError):
        register_manager(worker, _extra, name="Bad")


def test_register_manager_entry_lives_in_owner_registry():
    """A registered manager must live in the owner's registry alongside decorator entries (AR-086)."""

    class W(BasicWorker):
        @Manager(name="Decorated")
        async def _decorated(self) -> None:
            await asyncio.sleep(0.05)

        async def _extra(self) -> None:
            await asyncio.sleep(0.05)

    manager = register_manager(W, W._extra, name="Custom")
    assert manager.definition in W.__dict__[MANAGER_REGISTRY_ATTR]
    names = [name for name, _ in W().manager_runtime.iter_manager_definitions()]
    assert "Decorated" in names and "Custom" in names


def test_exact_name_shadows_and_typo_yields_two(caplog):
    """An exact name= shadow hides the base manager; a typo yields two distinct managers (AR-086)."""

    class Base(BasicWorker):
        @Manager(name="Exact")
        async def _base(self) -> None:
            await asyncio.sleep(0.05)

    class Exact(Base):
        @Manager(name="Exact")
        async def _derived(self) -> None:
            await asyncio.sleep(0.05)

    class Typo(Base):
        @Manager(name="Exactt")
        async def _derived(self) -> None:
            await asyncio.sleep(0.05)

    # Exact override: only the derived definition wins (base is shadowed).
    exact = Exact()
    pairs = list(exact.manager_runtime.iter_manager_definitions())
    names = [name for name, _ in pairs]
    assert names.count("Exact") == 1
    assert pairs[names.index("Exact")][1] is Exact.__dict__["_derived"].definition

    # Typo: two distinct managers with no collision warning.
    caplog.clear()
    typo = Typo()
    with caplog.at_level(logging.WARNING):
        typo_names = [name for name, _ in typo.manager_runtime.iter_manager_definitions()]
    assert typo_names.count("Exact") == 1 and typo_names.count("Exactt") == 1
    assert not any("collides" in record.getMessage() for record in caplog.records)


class ShadowBase(BasicWorker):
    """Base worker with a manager bound to ``_shared_manager`` (AR-086 shadowing)."""

    @Manager(name="Shadow")
    async def _shared_manager(self) -> None:
        await asyncio.sleep(0.05)


class ShadowPlain(ShadowBase):
    """Redefines ``_shared_manager`` as a plain method without re-decorating."""

    async def _shared_manager(self) -> None:
        await asyncio.sleep(0.05)


class ShadowDisabled(ShadowBase):
    """Assigns ``None`` over the base manager attribute (does not disable it)."""

    _shared_manager = None


class ShadowRegistered(ShadowBase):
    """Shadows with a plain method, then registers it under the same name."""

    async def _shared_manager(self) -> None:
        await asyncio.sleep(0.05)


shadow_registered_manager = register_manager(ShadowRegistered, ShadowRegistered._shared_manager, name="Shadow")


class ShadowRegisteredBound(ShadowBase):
    """Shadows with a plain method, then registers it with an ``attribute_name`` binding."""

    async def _shared_manager(self) -> None:
        await asyncio.sleep(0.05)


shadow_registered_bound_manager = register_manager(
    ShadowRegisteredBound,
    ShadowRegisteredBound._shared_manager,
    name="Shadow",
    attribute_name="_shared_manager",
)


class CoincidenceBase(BasicWorker):
    """Base worker whose manager attribute name coincides with a subclass method name."""

    @Manager(name="TaskManager")
    async def task_manager(self) -> None:
        await asyncio.sleep(0.05)


class CoincidencePlain(CoincidenceBase):
    """Redefines ``task_manager`` as a plain method without re-decorating."""

    async def task_manager(self) -> None:
        await asyncio.sleep(0.05)


def test_plain_manager_attribute_shadow_warns(caplog):
    """A plain attribute shadowing a base manager must log an advisory WARNING (AR-086)."""
    worker = ShadowPlain()
    with caplog.at_level(logging.WARNING):
        list(worker.manager_runtime.iter_manager_definitions())
    assert any(
        "shadows" in record.getMessage() and "_shared_manager" in record.getMessage() for record in caplog.records
    )
    assert not any("collides" in record.getMessage() for record in caplog.records)


def test_plain_manager_shadow_does_not_change_discovery(caplog):
    """A shadowing plain attribute must not change what discovery yields (AR-086)."""
    worker = ShadowPlain()
    with caplog.at_level(logging.WARNING):
        pairs = list(worker.manager_runtime.iter_manager_definitions())
    names = [name for name, _ in pairs]
    # The base manager still runs: exactly one "Shadow" yield, the base definition.
    assert names.count("Shadow") == 1
    assert pairs[names.index("Shadow")][1] is ShadowBase.__dict__["_shared_manager"].definition


def test_shadow_then_register_manager_does_not_warn(caplog):
    """Registering the shadowing method must silence the advisory warning (AR-086)."""
    worker = ShadowRegistered()
    with caplog.at_level(logging.WARNING):
        pairs = list(worker.manager_runtime.iter_manager_definitions())
    names = [name for name, _ in pairs]
    assert not any("shadows" in record.getMessage() for record in caplog.records)
    assert names.count("Shadow") == 1
    assert pairs[names.index("Shadow")][1] is shadow_registered_manager.definition


def test_shadow_then_register_manager_with_attribute_name_does_not_warn(caplog):
    """Registering with an attribute_name binding must also silence the warning (AR-086)."""
    worker = ShadowRegisteredBound()
    with caplog.at_level(logging.WARNING):
        pairs = list(worker.manager_runtime.iter_manager_definitions())
    names = [name for name, _ in pairs]
    assert not any("shadows" in record.getMessage() for record in caplog.records)
    assert names.count("Shadow") == 1
    assert pairs[names.index("Shadow")][1] is shadow_registered_bound_manager.definition


def test_plain_manager_none_assignment_warns(caplog):
    """Assigning None over a base manager attribute must still warn (AR-086)."""
    worker = ShadowDisabled()
    with caplog.at_level(logging.WARNING):
        pairs = list(worker.manager_runtime.iter_manager_definitions())
    names = [name for name, _ in pairs]
    assert any(
        "shadows" in record.getMessage() and "_shared_manager" in record.getMessage() for record in caplog.records
    )
    # None does not disable the base manager: it still runs.
    assert names.count("Shadow") == 1
    assert pairs[names.index("Shadow")][1] is ShadowBase.__dict__["_shared_manager"].definition


def test_manager_attribute_name_coincidence_warns(caplog):
    """A name coincidence between a manager attribute and a plain method must warn (AR-086)."""
    worker = CoincidencePlain()
    with caplog.at_level(logging.WARNING):
        list(worker.manager_runtime.iter_manager_definitions())
    assert any("shadows" in record.getMessage() and "task_manager" in record.getMessage() for record in caplog.records)


def test_decorated_manager_override_still_warns_nothing(caplog):
    """A re-decorated override must not trigger the shadowing warning (AR-086)."""
    worker = DerivedWorker()
    with caplog.at_level(logging.WARNING):
        names = [name for name, _ in worker.manager_runtime.iter_manager_definitions()]
    assert not any("shadows" in record.getMessage() for record in caplog.records)
    assert names.count("Shared") == 1


def test_registry_holds_manager_definition_values():
    """The registry stores ManagerDefinition values, never Manager instances (AR-107)."""

    class W(BasicWorker):
        @Manager(name="Decorated")
        async def _decorated(self) -> None:
            await asyncio.sleep(0.05)

    registry = W.__dict__[MANAGER_REGISTRY_ATTR]
    assert registry, "expected at least one registry entry"
    assert all(isinstance(entry, ManagerDefinition) for entry in registry)
    assert all(not isinstance(entry, Manager) for entry in registry)


def test_decorator_and_register_manager_produce_equivalent_definitions():
    """Both registration paths produce equivalent ManagerDefinition values (AR-107)."""

    async def _impl(self) -> None:
        await asyncio.sleep(0.05)

    async def _cleanup(worker) -> None:
        await asyncio.sleep(0.05)

    # Decorator path: build the Manager and let type.__new__ trigger __set_name__.
    decorator = Manager(name="Eq", cleanup=_cleanup)
    decorator(_impl)
    Decorated = type("Decorated", (BasicWorker,), {"_impl": decorator})

    # register_manager path: an explicit entry on an otherwise empty class.
    Registered = type("Registered", (BasicWorker,), {})
    registered = register_manager(Registered, _impl, name="Eq", cleanup=_cleanup, attribute_name="_impl")

    decorated = Decorated.__dict__["_impl"].definition
    explicit = registered.definition

    assert isinstance(decorated, ManagerDefinition)
    assert isinstance(explicit, ManagerDefinition)
    assert decorated.name == explicit.name == "Eq"
    assert decorated.method is explicit.method is _impl
    assert decorated.cleanup is explicit.cleanup is _cleanup
    assert decorated.attribute_name == explicit.attribute_name == "_impl"
    assert decorated.owner is Decorated
    assert explicit.owner is Registered


def test_manager_alias_yields_exactly_one_definition():
    """A Manager aliased under two attribute names yields exactly one ManagerDefinition (AR-107)."""

    async def _loop(self) -> None:
        await asyncio.sleep(0.05)

    manager = Manager(name="Aliased")
    manager(_loop)
    Aliased = type("Aliased", (BasicWorker,), {"_first": manager, "_second": manager})

    registry = Aliased.__dict__[MANAGER_REGISTRY_ATTR]
    assert len(registry) == 1
    assert isinstance(registry[0], ManagerDefinition)
    assert registry[0] is manager.definition


def test_subclass_does_not_mutate_or_inherit_base_registry():
    """A subclass must not mutate or inherit a base class's registry (AR-107)."""

    class Base(BasicWorker):
        @Manager(name="BaseOnly")
        async def _base(self) -> None:
            await asyncio.sleep(0.05)

    class Sub(Base):
        @Manager(name="SubOnly")
        async def _sub(self) -> None:
            await asyncio.sleep(0.05)

    base_registry = Base.__dict__[MANAGER_REGISTRY_ATTR]
    sub_registry = Sub.__dict__[MANAGER_REGISTRY_ATTR]

    assert sub_registry is not base_registry
    assert [entry.name for entry in base_registry] == ["BaseOnly"]
    assert [entry.name for entry in sub_registry] == ["SubOnly"]
    assert "BaseOnly" not in [entry.name for entry in sub_registry]
