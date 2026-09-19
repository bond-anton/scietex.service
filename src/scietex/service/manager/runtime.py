"""Manager runtime component for ``scietex.service``.

Provides ``ManagerRuntime``, which owns manager discovery, start/stop
bookkeeping, and the restart-on-error loop used by ``BasicWorker``.
"""

import asyncio
import logging
from collections.abc import Generator
from typing import TYPE_CHECKING

from . import MANAGER_REGISTRY_ATTR, Manager, ManagerDefinition, ManagerStatus

if TYPE_CHECKING:
    from ..basic_worker import BasicWorker


class ManagerRuntime:
    """Owns manager discovery, lifecycle bookkeeping, and restart-on-error loops.

    Extracted from ``BasicWorker`` (AR-003) so the worker delegates its
    manager-loop machinery here while keeping its public API and subclass
    hooks stable. Config is read off the worker's public properties, which
    remain the single source of truth for clamped values.
    """

    def __init__(self, worker: "BasicWorker") -> None:
        """Initialize the manager runtime with a back-reference to its worker.

        Args:
            worker: The owning ``BasicWorker`` instance, providing the
                logger and clamped config values used by the manager loops.
        """
        self.worker: BasicWorker = worker
        self.statuses: dict[str, ManagerStatus] = {}
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.errors: dict[str, Exception | None] = {}

    @property
    def failed_managers(self) -> list[str]:
        """Names of managers that exhausted their retry budget and gave up.

        Returns:
            List of manager names whose status is ``ManagerStatus.FAILED``.
            The recorded exception for each is available in ``self.errors``.
        """
        return [name for name, status in self.statuses.items() if status is ManagerStatus.FAILED]

    def iter_manager_definitions(self) -> Generator[tuple[str, ManagerDefinition]]:
        """
        Iterate over all registered managers from the worker's class MRO.

        Managers are yielded most-derived-first so that a subclass override
        of a same-named manager shadows the base definition. Each manager
        name is yielded at most once. When two managers independently pick
        the same ``name=``, a WARNING is logged and the first (most-derived)
        definition wins; the later one is skipped, never silently dropped.

        Discovery reads each class's own ``MANAGER_REGISTRY_ATTR`` registry
        (populated by ``_record_definition``, shared by ``Manager.__set_name__``
        and ``register_manager``), so a manager's identity is its explicit
        ``name``. Reading from the class's own ``__dict__`` only means a
        subclass never inherits or mutates a base class's registry list.

        A WARNING is also logged when a class redefines a name that a base
        class bound as a manager attribute without re-decorating it (AR-086
        failure mode 2). Such a plain attribute produces no registry entry, so
        discovery falls through to the base manager and the override never
        runs. The shadow is reported only when the shadowing value is neither
        a ``Manager`` instance nor the method of an already-registered manager
        (so a legitimate re-decorated override or a shadow-then-
        ``register_manager`` is not reported). The warning is advisory: the
        yielded ``(name, manager)`` pairs and their order are unaffected.

        Yields:
            Tuple of (manager_name, manager) for each Manager recorded in
            the class hierarchy, processed from most-derived to base classes.
        """
        mro = type(self.worker).__mro__

        # AR-086 failure mode 2: a class in the MRO may redefine a name that
        # a base class bound as a manager attribute, without re-decorating.
        # That leaves a plain function/attribute in the class __dict__, no
        # registry entry on the subclass, and discovery silently runs the
        # base manager. Index the registered managers and their bound
        # attribute names so the shadow can be reported below.
        manager_attributes: dict[str, ManagerDefinition] = {}
        registered_methods: set[int] = set()
        for cls in mro:
            for definition in cls.__dict__.get(MANAGER_REGISTRY_ATTR, ()):
                if definition.method is not None:
                    registered_methods.add(id(definition.method))
                attribute_name = definition.attribute_name
                if attribute_name is not None:
                    manager_attributes.setdefault(attribute_name, definition)

        for cls in mro:
            for attribute_name, value in cls.__dict__.items():
                base_manager = manager_attributes.get(attribute_name)
                if base_manager is None:
                    continue
                if base_manager.owner not in cls.__mro__[1:]:
                    continue
                if isinstance(value, Manager):
                    continue
                if id(value) in registered_methods:
                    continue
                self.worker.logger.warning(
                    "Manager attribute %r on %s shadows the manager %r "
                    "defined on %s without re-decorating; the base manager "
                    "still runs and this attribute is not executed. Decorate "
                    "it with @Manager(name=%r) or register it with "
                    "register_manager() to override.",
                    attribute_name,
                    cls.__name__,
                    base_manager.name,
                    base_manager.owner.__name__ if base_manager.owner is not None else "<unknown>",
                    base_manager.name,
                )

        seen: set[str] = set()
        for cls in mro:
            for definition in cls.__dict__.get(MANAGER_REGISTRY_ATTR, ()):
                manager_name = definition.name
                if manager_name in seen:
                    self.worker.logger.warning(
                        "Manager name %r collides with an already-registered manager "
                        "(found on %s.%s); the first definition in the MRO wins "
                        "and this one is skipped.",
                        manager_name,
                        cls.__name__,
                        definition.attribute_name,
                    )
                    continue
                seen.add(manager_name)
                yield manager_name, definition

    async def run_manager(self, name: str, manager: ManagerDefinition) -> None:
        """
        Execute a manager's lifecycle loop with automatic restart on error.

        Runs the manager's method in a loop. On cancellation the manager
        stops cleanly. On any other exception the error is recorded and the
        manager is retried after a backoff delay, up to
        ``manager_max_retries`` consecutive retries (i.e. it gives up after
        ``manager_max_retries + 1`` consecutive failures). The retry happens
        inside this same task, so the manager never
        cancels itself (which previously deadlocked the restart). The
        finally block runs cleanup, marks the manager STOPPED (or FAILED if
        it gave up), and removes the task from internal tracking.

        Args:
            name: Human-readable name for the manager
            manager: The ManagerDefinition whose method will be executed
        """
        self.worker.logger.info("[START] Manager %s started", name)
        # Mark RUNNING as soon as the loop starts; it stays RUNNING through
        # error-retry backoff because the task is still alive. The enum is the
        # source of truth for manager lifecycle (AR-057).
        self.statuses[name] = ManagerStatus.RUNNING

        consecutive_failures = 0
        # Set when the retry budget is exhausted so the finally block (which
        # normally lands the manager in STOPPED) can instead end it in FAILED.
        # A clean shutdown must NOT be marked FAILED (AR-063).
        gave_up = False
        try:
            while True:
                try:
                    if manager.method:
                        await manager.method(self.worker)
                    else:
                        raise RuntimeError("Manager has no associated executable method.")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.errors[name] = e
                    consecutive_failures += 1
                    if consecutive_failures > self.worker.manager_max_retries:
                        self.worker.logger.error(
                            "[GIVE-UP] Manager %s failed %d consecutive times (%s). Giving up.",
                            name,
                            consecutive_failures,
                            e,
                        )
                        gave_up = True
                        break
                    self.worker.logger.error(
                        "[ERROR] Manager %s error %s. Restarting in %.1fs (attempt %d/%d)",
                        name,
                        e,
                        self.worker.manager_restart_backoff,
                        consecutive_failures,
                        self.worker.manager_max_retries,
                    )
                    await asyncio.sleep(self.worker.manager_restart_backoff)
                    continue
                # A successful iteration resets the failure counter.
                consecutive_failures = 0
        except asyncio.CancelledError:
            pass
        finally:
            self.statuses[name] = ManagerStatus.STOPPING
            self.worker.logger.info("[STOP] Manager %s stopping", name)
            try:
                if manager.cleanup:
                    await manager.cleanup(self.worker)
            finally:
                self.statuses[name] = ManagerStatus.STOPPED
                self.worker.logger.info("[STOP] Manager %s stopped", name)
                # Remove this task from tracking so a later restart is
                # possible, even if cleanup raised.
                if self.tasks.get(name) is asyncio.current_task():
                    self.tasks.pop(name, None)
                # A manager that exhausted its retry budget is not cleanly
                # stopped; report it as FAILED so the watchdog can observe it.
                if gave_up:
                    self.statuses[name] = ManagerStatus.FAILED

    async def start_manager(self, name: str, manager: ManagerDefinition) -> None:
        """
        Start a named manager as an asyncio task.

        Args:
            name: Identifier for the manager
            manager: The ManagerDefinition to execute
        """
        if self.statuses.get(name) in (ManagerStatus.STARTING, ManagerStatus.RUNNING):
            self.worker.logger.log(logging.DEBUG, "%s is already running", name)
            return
        self.statuses[name] = ManagerStatus.STARTING
        self.errors[name] = None

        task = asyncio.create_task(
            self.run_manager(name, manager),
            name=name,
        )

        self.tasks[name] = task

    async def stop_manager(self, name: str) -> None:
        """
        Stop a named manager task with a timeout.

        Cancels the task and waits up to `manager_shutdown_timeout` seconds
        for it to complete. Removes the task from internal tracking on
        success; on timeout the still-running task stays tracked so a later
        start cannot double-spawn the same name.

        Args:
            name: Identifier of the manager to stop
        """
        if name not in self.tasks:
            self.worker.logger.log(logging.DEBUG, "%s is not running", name)
            return
        self.tasks[name].cancel()
        try:
            await asyncio.wait_for(self.tasks[name], self.worker.manager_shutdown_timeout)
        except asyncio.TimeoutError:
            # The task is still running; keep it tracked so a later
            # start_manager cannot double-spawn the same name.
            self.worker.logger.log(logging.DEBUG, "Timeout during %s shut down", name)
            return
        # The task removes itself from tracking in its finally block; pop
        # defensively in case it already did so.
        self.tasks.pop(name, None)

    async def start_managers(self) -> None:
        """Start all registered managers as asyncio tasks.

        Iterates over all ``Manager``-decorated methods found in the
        class MRO (from most-derived to base classes) and starts each
        one as a named ``asyncio.Task``.
        """
        for name, definition in self.iter_manager_definitions():
            await self.start_manager(name, definition)

    async def stop_managers(self, *, reverse: bool = False) -> None:
        """Stop all registered managers in order.

        Args:
            reverse: When ``True``, stop managers in reverse discovery order
                (last-started first). The cancellation unwind in
                ``BasicWorker`` uses this so teardown mirrors startup (AR-106).
        """
        definitions = list(self.iter_manager_definitions())
        if reverse:
            definitions.reverse()
        for name, _ in definitions:
            await self.stop_manager(name)
