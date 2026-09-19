"""Worker lifecycle component for ``scietex.service``.

Provides ``WorkerLifecycle``, which owns the service state machine,
lifecycle events, and the pending stop-task guard used by ``BasicWorker``.
"""

import asyncio
from collections.abc import Mapping
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .basic_worker import BasicWorker, ServiceStatus


class InvalidStateTransition(Exception):
    """Raised when a lifecycle transition violates the state machine (AR-106).

    Not a ``RuntimeError`` subclass: ``BasicWorker._startup`` catches
    ``RuntimeError`` as "initialization failed" and would otherwise mask a
    programming error as a normal shutdown.
    """


class WorkerLifecycle:
    """Owns the service state machine and lifecycle-event bookkeeping.

    Extracted from ``BasicWorker`` (AR-087) so the worker delegates its
    state/event tracking here while keeping its public API and subclass
    hooks stable. State, start time, and both lifecycle events live here
    as one unit so their transitions stay atomic.
    """

    def __init__(self, worker: "BasicWorker") -> None:
        """Initialize the lifecycle with a back-reference to its worker.

        Args:
            worker: The owning ``BasicWorker`` instance, whose ``exit()`` is
                spawned by ``request_exit()`` when an exit is requested.
        """
        from .basic_worker import ServiceStatus

        self.worker: BasicWorker = worker
        self._state: ServiceStatus = ServiceStatus.STOPPED
        self._start_time: datetime | None = None
        self._events: dict[str, asyncio.Event] = {
            "exit_requested": asyncio.Event(),
            "exit": asyncio.Event(),
        }
        self._stop_task: asyncio.Task | None = None

        # Set exactly while the lifecycle is STOPPED. It is the awaitable
        # replacement for the previous 0.1 s poll and the barrier a start uses
        # to wait out an in-flight shutdown (AR-106).
        self._stopped: asyncio.Event = asyncio.Event()
        self._stopped.set()

        # Allowed (current, next) edges. force_stopped() is the unguarded
        # terminal escape and deliberately has no entry here.
        self._allowed_transitions: frozenset[tuple[ServiceStatus, ServiceStatus]] = frozenset(
            {
                (ServiceStatus.STOPPED, ServiceStatus.STARTING),
                (ServiceStatus.STARTING, ServiceStatus.RUNNING),
                (ServiceStatus.STARTING, ServiceStatus.STOPPING),
                (ServiceStatus.RUNNING, ServiceStatus.STOPPING),
                (ServiceStatus.STOPPING, ServiceStatus.STOPPED),
            }
        )

    @property
    def state(self) -> "ServiceStatus":
        """Current lifecycle state of the service (read-only).

        Returns:
            The current ``ServiceStatus`` enum value indicating whether
            the service is stopped, starting, running, or stopping.
        """
        return self._state

    def transition(self, new_state: "ServiceStatus") -> None:
        """Validate and apply a lifecycle state transition.

        The only way to move the lifecycle forward. An illegal edge raises a
        programming error instead of silently overwriting the current state.

        Args:
            new_state: The state to transition to.

        Raises:
            InvalidStateTransition: If ``(current, new_state)`` is not an
                allowed edge. Use :meth:`force_stopped` for the unguarded
                terminal escape used by cancellation handling.
        """
        from .basic_worker import ServiceStatus

        current = self._state
        if current is ServiceStatus.STOPPED and new_state is ServiceStatus.STOPPED:
            return
        if (current, new_state) not in self._allowed_transitions:
            raise InvalidStateTransition(f"Invalid lifecycle transition {current.name} -> {new_state.name}")
        self._set_state(new_state)

    def _set_state(self, new_state: "ServiceStatus") -> None:
        """Apply ``new_state`` and keep the stopped-wait event consistent.

        The event is set iff the state is STOPPED, so a waiter resumed by it
        always observes STOPPED.
        """
        from .basic_worker import ServiceStatus

        self._state = new_state
        if new_state is ServiceStatus.STOPPED:
            self._stopped.set()
        else:
            self._stopped.clear()

    @property
    def start_time(self) -> datetime | None:
        """Timestamp when the service started running (read-only).

        Returns:
            The UTC ``datetime`` when the service transitioned to
            ``RUNNING`` state, or ``None`` if the service has not
            started or has been stopped.
        """
        return self._start_time

    @start_time.setter
    def start_time(self, value: datetime | None) -> None:
        """Update the service start timestamp.

        Set by ``_startup`` just before managers begin and cleared by
        ``_shutdown``/``force_stopped()`` on stop.
        """
        self._start_time = value

    @property
    def events(self) -> Mapping[str, asyncio.Event]:
        """Dictionary of lifecycle events for external coordination.

        Contains two events:
            - ``exit_requested``: Set when an exit is requested (e.g., via signal).
            - ``exit``: Set when the worker has fully stopped.

        Returns:
            A read-only mapping view of the internal events dictionary. The
            ``asyncio.Event`` values remain mutable and may be awaited or
            inspected, but the mapping itself cannot be modified.
        """
        return MappingProxyType(self._events)

    def request_exit(self) -> None:
        """Spawn a single exit task, guarding against re-entry.

        Repeated signals must not each spawn their own exit() task. A pending
        stop task or an already-requested exit short-circuits so only one
        shutdown runs.
        """
        if self._stop_task is not None and not self._stop_task.done():
            return
        if self.events["exit_requested"].is_set():
            return
        self._stop_task = asyncio.create_task(self.worker.exit(), name="StopTask")

    def force_stopped(self) -> None:
        """Force the worker into a terminal STOPPED state.

        Used by the startup/shutdown cancellation handlers so a cancelled task
        never strands the worker in STARTING or STOPPING, which would block a
        later start() (AR-017). If an exit was requested, surface it as a
        completed exit instead of leaving the exit event dangling.
        """
        from .basic_worker import ServiceStatus

        self._set_state(ServiceStatus.STOPPED)
        self._start_time = None
        if self.events["exit_requested"].is_set():
            self._events["exit_requested"].clear()
            self._events["exit"].set()

    async def _wait_until_stopped(self) -> None:
        """Block until a previous shutdown has fully completed.

        Awaiting the ``_stopped`` event replaces the previous 100 ms poll: the
        event is set iff the state is STOPPED, so this returns exactly when a
        start may proceed, with a single wakeup.
        """
        await self._stopped.wait()
