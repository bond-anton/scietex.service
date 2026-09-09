"""Manager runtime component for ``scietex.service``.

Provides ``ManagerRuntime``, which owns manager discovery, start/stop
bookkeeping, and the restart-on-error loop used by ``BasicWorker``.
"""

import asyncio
import logging
from collections.abc import Generator
from typing import TYPE_CHECKING

from . import Manager, ManagerStatus

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

    def iter_manager_definitions(self) -> Generator[tuple[str, Manager]]:
        """
        Iterate over all registered managers from the worker's class MRO.

        Managers are yielded most-derived-first so that a subclass override
        of a same-named manager shadows the base definition. Each manager
        name is yielded at most once. When two managers independently pick
        the same ``name=``, a WARNING is logged and the first (most-derived)
        definition wins; the later one is skipped, never silently dropped.

        Yields:
            Tuple of (manager_name, manager) for each Manager decorator found
            in the class hierarchy, processed from most-derived to base classes.
        """
        seen: set[str] = set()
        for cls in type(self.worker).__mro__:
            for attribute_name, attribute in cls.__dict__.items():
                if not isinstance(attribute, Manager):
                    continue
                manager_name = attribute.name or attribute_name
                if manager_name in seen:
                    self.worker.logger.warning(
                        "Manager name %r collides with an already-registered manager "
                        "(found on %s.%s); the first definition in the MRO wins "
                        "and this one is skipped.",
                        manager_name,
                        cls.__name__,
                        attribute_name,
                    )
                    continue
                seen.add(manager_name)
                yield manager_name, attribute

    async def run_manager(self, name: str, manager: Manager) -> None:
        """
        Execute a manager's lifecycle loop with automatic restart on error.

        Runs the manager's method in a loop. On cancellation the manager
        stops cleanly. On any other exception the error is recorded and the
        manager is retried after a backoff delay, up to
        ``manager_max_retries`` consecutive failures, after which it gives
        up. The retry happens inside this same task, so the manager never
        cancels itself (which previously deadlocked the restart). The
        finally block runs cleanup, marks the manager STOPPED (or FAILED if
        it gave up), and removes the task from internal tracking.

        Args:
            name: Human-readable name for the manager
            manager: The Manager instance whose method will be executed
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

    async def start_manager(self, name: str, manager: Manager) -> None:
        """
        Start a named manager as an asyncio task.

        Args:
            name: Identifier for the manager
            manager: The Manager instance to execute
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
        for name, manager in self.iter_manager_definitions():
            await self.start_manager(name, manager)

    async def stop_managers(self) -> None:
        """Stop all registered managers in order.

        Iterates over all ``Manager``-decorated methods found in the
        class MRO and stops each one, waiting up to
        ``manager_shutdown_timeout`` seconds per manager.
        """
        for name, _ in self.iter_manager_definitions():
            await self.stop_manager(name)
