"""Transport contract and in-memory implementation for ``TaskProcessor``.

Defines the extension seam a transport must implement (AR-072): a
:class:`TaskSink` receives enqueued tasks, and a :class:`TaskTransport`
owns every ordering-sensitive hook the processor used to expose as
template-method overrides. :class:`InMemoryTransport` is the default
transport for a bare ``TaskProcessor``; ``ValkeyWorker`` swaps in its own
stream-backed transport at construction.
"""

import logging
from collections import deque
from typing import Protocol
from uuid import UUID

from .task_handler.schemas import CancelReason, TaskData, TaskResult, task_data_id


class TaskSink(Protocol):
    """The receiving side of a transport's ``fetch``.

    ``TaskProcessor`` satisfies this structurally: its bounded internal queue
    reports fullness via :meth:`task_queue_full` and accepts tasks via the
    non-blocking :meth:`enqueue_task` (returning ``False`` when full).
    Control-plane commands are delivered through the separate
    :meth:`enqueue_control_task`, so a transport addresses a task's lane by
    which surface it calls rather than by inspecting the task type.
    """

    def task_queue_full(self) -> bool: ...

    def enqueue_task(self, task_data: TaskData) -> bool: ...

    def enqueue_control_task(self, task_data: TaskData) -> bool: ...


class TaskTransport(Protocol):
    """The async extension contract a task transport must implement.

    Each method corresponds to one lifecycle hook the processor calls. A
    transport is responsible for source-appropriate delivery, acknowledgement,
    and shutdown-drain policy; the processor is transport-agnostic.
    """

    async def fetch(self, sink: TaskSink) -> bool: ...

    async def requeue(self, task_data: TaskData) -> None: ...

    async def on_started(self, task_data: TaskData) -> None: ...

    async def ack(
        self,
        task_data: TaskData,
        task_result: TaskResult | None,
        *,
        cancel_reason: CancelReason | None = None,
    ) -> None: ...

    async def on_progress(self, task_id: UUID, value: float) -> None: ...

    async def refresh_leases(self) -> None:
        """Renew any per-entry ownership claims held for in-flight tasks.

        A transport with no claim/lease mechanism implements this as a no-op.
        """
        ...

    async def recover_pending_tasks(self, sink: TaskSink) -> tuple[bool, bool]:
        """Re-deliver entries left pending by a previous run.

        Returns ``(recovery_complete, enqueued)``: ``recovery_complete`` is
        ``False`` when a full sink interrupted recovery so the next poll
        retries. A transport with no recovery step returns ``(True, False)``.
        """
        ...

    async def on_drain(self, task_data: TaskData) -> None:
        """Release the transport-side claim for a queued-but-undispatched task.

        The release is unconditional. Re-delivery policy is transport-owned: a
        durable transport leaves the entry pending (redelivered on restart or
        recovery) and must not re-publish; a non-durable transport additionally
        re-delivers when ``task_data.canceled_action == "requeue"`` so it does
        not lose the work on shutdown (AR-041).
        """
        ...


class RecoverableTransport:
    """Shared recovery-once orchestration for broker-backed transports (AR-120).

    Owning the ``recovered`` flag and the "mark recovery complete only when the
    pending list was fully drained" guard here defines that behavior once: a
    concrete transport supplies only its transport-specific
    :meth:`recover_pending_tasks` scan and calls :meth:`ensure_recovered` from
    the start of ``fetch``. Not part of the ``TaskTransport`` Protocol; an
    implementation scaffold for transports that have a recovery step.
    """

    #: True once pending-entry recovery has fully drained the pending list.
    recovered: bool = False

    async def recover_pending_tasks(self, sink: TaskSink) -> tuple[bool, bool]:
        """Re-deliver entries left pending by a previous run (transport-owned).

        Returns ``(recovery_complete, enqueued)``; ``recovery_complete`` is
        ``False`` when a full sink interrupted recovery so the next poll retries.
        """
        raise NotImplementedError

    async def ensure_recovered(self, sink: TaskSink) -> bool:
        """Run recovery at most once to completion; return its enqueued signal.

        Idempotent: after a complete recovery the flag is set and later calls
        are no-ops returning ``False``. An incomplete recovery (full sink) is
        retried on the next call, matching AR-051.
        """
        if self.recovered:
            return False
        recovery_complete, enqueued = await self.recover_pending_tasks(sink)
        if recovery_complete:
            self.recovered = True
        return enqueued


class InMemoryTransport:
    """A working in-memory transport backed by two ``deque`` s.

    ``submit`` queues a data task for delivery; ``submit_control`` queues a
    control command. ``fetch`` drains control first (subject to the control
    lane's own backpressure) then data (subject to data backpressure). A
    ``requeue`` re-appends a task to the deque it came from, so the next
    ``fetch`` re-delivers it on the correct lane, matching the base
    ``TaskProcessor`` policy of returning a task to its source queue when it
    cannot be processed.
    """

    def __init__(self, *, logger: logging.Logger) -> None:
        self._logger = logger
        self._pending: deque[TaskData] = deque()
        self._control_pending: deque[TaskData] = deque()
        # Ids delivered from the control deque. requeue/on_drain must route a
        # control task back to the control deque, not the data deque, or a
        # requeued control command would be re-dispatched against the data
        # registry and fail as an unknown task type.
        self._control_ids: set[UUID] = set()

    def submit(self, task_data: TaskData) -> None:
        """Append a data task for delivery on the next :meth:`fetch`."""
        self._pending.append(task_data)

    def submit_control(self, task_data: TaskData) -> None:
        """Append a control command for delivery on the next :meth:`fetch`."""
        self._control_pending.append(task_data)

    async def fetch(self, sink: TaskSink) -> bool:
        """Drain pending tasks into ``sink``, control first.

        A control command is delivered through :meth:`TaskSink.enqueue_control_task`
        so it lands on the control lane; a data task goes through
        :meth:`TaskSink.enqueue_task`. Data delivery stops at the first rejected
        data task (backpressure, order preserved); control delivery is independent
        of data backpressure so a saturated data plane never starves a control
        command (AR-108).
        """
        enqueued = False
        for _ in range(len(self._control_pending)):
            task_data = self._control_pending.popleft()
            if sink.enqueue_control_task(task_data):
                self._control_ids.add(task_data_id(task_data))
                enqueued = True
            else:
                self._control_pending.append(task_data)
        data_blocked = False
        for _ in range(len(self._pending)):
            task_data = self._pending.popleft()
            if data_blocked or sink.task_queue_full():
                self._pending.append(task_data)
                data_blocked = True
                continue
            if sink.enqueue_task(task_data):
                enqueued = True
            else:
                self._pending.append(task_data)
                data_blocked = True
        return enqueued

    async def requeue(self, task_data: TaskData) -> None:
        """Re-append a task to the deque it was delivered from.

        A control task stays on the control deque, so a requeued control command
        is re-dispatched against the control registry rather than the data
        registry (where its task type would be unknown).
        """
        if task_data_id(task_data) in self._control_ids:
            self._control_pending.append(task_data)
        else:
            self._pending.append(task_data)

    async def on_started(self, task_data: TaskData) -> None:
        """No-op: an in-memory transport publishes no tracking records."""

    async def ack(
        self,
        task_data: TaskData,
        task_result: TaskResult | None,
        *,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        """No-op: an in-memory task has no transport entry to acknowledge.

        Drops the delivered-from-control marker so the per-delivery routing set
        stays bounded for a long-running transport.
        """
        self._control_ids.discard(task_data_id(task_data))

    async def on_progress(self, task_id: UUID, value: float) -> None:
        """No-op: an in-memory transport stores no progress records."""

    async def refresh_leases(self) -> None:
        """No-op: an in-memory task holds no lease to renew."""

    async def recover_pending_tasks(self, sink: TaskSink) -> tuple[bool, bool]:
        """Nothing is pending across restarts, so recovery is trivially complete."""
        return True, False

    async def on_drain(self, task_data: TaskData) -> None:
        """Return a drained task to the queue when its action is ``requeue``.

        Reproduces the base ``TaskProcessor`` shutdown-drain policy: a
        non-durable transport would otherwise silently lose the work (AR-041).
        """
        if task_data.canceled_action == "requeue":
            self._logger.log(logging.WARNING, "Task %s will be returned to queue.", task_data_id(task_data))
            await self.requeue(task_data)
