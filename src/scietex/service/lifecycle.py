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

WAIT_FOR_SERVICE_STOPPED_DELAY: float = 0.1


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

    @property
    def state(self) -> "ServiceStatus":
        """Current lifecycle state of the service (read-only).

        Returns:
            The current ``ServiceStatus`` enum value indicating whether
            the service is stopped, starting, running, or stopping.
        """
        return self._state

    @state.setter
    def state(self, value: "ServiceStatus") -> None:
        """Update the lifecycle state.

        Written by the worker's orchestrators (``_startup``/``_shutdown``) as
        they drive the service through STARTING -> RUNNING -> STOPPING ->
        STOPPED.
        """
        self._state = value

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

        self._state = ServiceStatus.STOPPED
        self._start_time = None
        if self.events["exit_requested"].is_set():
            self._events["exit_requested"].clear()
            self._events["exit"].set()

    async def _wait_until_stopped(self) -> None:
        """Block until a previous shutdown has fully completed.

        A startup must not begin while a prior stop is still in flight, so the
        worker polls the state until it reaches STOPPED before proceeding.
        """
        from .basic_worker import ServiceStatus

        while not self._state == ServiceStatus.STOPPED:
            await asyncio.sleep(WAIT_FOR_SERVICE_STOPPED_DELAY)
