"""Task execution loop: dequeue -> track -> dispatch -> retry -> ack (AR-101)."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from uuid import UUID

import msgspec

from .config_reload import ReloadableSettings
from .task_handler import CancelOutcome, TaskData, TaskResult, TaskTracker
from .task_lifecycle import TaskLifecycle

#: AR-022 v4: the framework grants exactly one error-path retry per task id.
#: Moved from task_processor._MAX_TASK_RETRIES (AR-101).
DEFAULT_MAX_TASK_RETRIES: int = 1


class TaskExecutor:
    """Executes the dequeue -> track -> dispatch -> retry -> ack loop (AR-101).

    Extracted from ``TaskProcessor``. The storage it mutates — the in-process
    ``queue``, the running :class:`TaskLifecycle`, and the per-task
    ``retry_attempts`` budget — is owned by ``TaskProcessor`` and injected by
    reference (mirroring ``ValkeyTransport(entry_ids=...)``), so the executor
    and the processor share the same live state without the executor owning it.
    Every ordering-sensitive processor hook (``on_started``, ``process_task``,
    ``requeue``, ``on_completed``, ``on_drain``) is injected as a callable, and
    the effective reloadable settings are read through the ``settings``
    callable, so this module imports no transport package and no processor
    type.
    """

    def __init__(
        self,
        *,
        queue: asyncio.Queue[tuple[UUID, TaskData]],
        lifecycle: TaskLifecycle,
        retry_attempts: dict[UUID, int],
        process_task: Callable[[UUID, TaskData], Awaitable[TaskResult]],
        on_started: Callable[[UUID, TaskData], Awaitable[None]],
        on_completed: Callable[..., Awaitable[None]],
        requeue: Callable[[UUID, TaskData], Awaitable[None]],
        on_drain: Callable[[UUID, TaskData], Awaitable[None]],
        settings: Callable[[], ReloadableSettings],
        logger: logging.Logger,
        max_retries: int = DEFAULT_MAX_TASK_RETRIES,
    ) -> None:
        self._queue = queue
        self._lifecycle = lifecycle
        self._retry_attempts = retry_attempts
        self._process_task = process_task
        self._on_started = on_started
        self._on_completed = on_completed
        self._requeue = requeue
        self._on_drain = on_drain
        self._settings = settings
        self._logger = logger
        self._max_retries = max_retries

    async def run_once(self) -> None:
        """Run one task-manager iteration: dequeue, dispatch, and track a task."""
        if len(self._lifecycle.trackers()) < self._settings().max_concurrent_tasks:
            try:
                task_id, task_data = await asyncio.wait_for(
                    self._queue.get(), timeout=self._settings().task_queue_fetch_timeout
                )
                task = asyncio.create_task(self._handle_task(task_id, task_data))
                self._lifecycle.register(
                    task_id, TaskTracker(worker_task=task, data=task_data, started=time.monotonic())
                )
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(self._settings().task_manager_sleep_time)

    async def _handle_task(self, task_id: UUID, task_data: TaskData) -> None:
        """Execute a single task, then settle its transport entry exactly once."""
        result: TaskResult | None = None
        try:
            result = await self._execute(task_id, task_data)
        finally:
            await self._settle(task_id, task_data, result)

    async def _execute(self, task_id: UUID, task_data: TaskData) -> TaskResult | None:
        """Run the on_started hook, dispatch to the handler, and log completion.

        Catches ``Exception`` (never ``BaseException``) so ``CancelledError``
        propagates and ``result`` stays ``None`` for the settle step to ack as a
        cancellation.
        """
        try:
            await self._on_started(task_id, task_data)
            result = await self._process_task(task_id, task_data)
            self._logger.log(
                logging.DEBUG,
                "Task %s (%s) finished with status %s",
                task_data.task,
                task_id,
                result.status,
            )
            return result
        except Exception as exc:
            # process_task is expected to return an error TaskResult for every
            # failure, but a defensive catch guarantees no exception escapes
            # into the unawaited task (which would surface as an unretrieved
            # task exception).
            self._logger.log(
                logging.ERROR,
                "Task %s (%s) raised unexpectedly: %s",
                task_data.task,
                task_id,
                exc,
            )
            return None

    async def _settle(self, task_id: UUID, task_data: TaskData, result: TaskResult | None) -> None:
        """Drop the tracker, balance the queue, apply retry policy, then ack."""
        self._lifecycle.remove_tracker(task_id)
        self._queue.task_done()
        ack_result = await self._apply_retry_policy(task_id, task_data, result)
        try:
            # Ack the transport entry exactly when the handler's work on it
            # ends (success, error, or cancellation). On CancelledError, result
            # is None and the hook still runs. The cancel reason (if any) is
            # popped here so the transport can distinguish a deliberate cancel
            # from a timeout.
            await self._on_completed(
                task_id,
                task_data,
                ack_result,
                cancel_reason=self._lifecycle.take_cancel_reason(task_id),
            )
        except Exception as exc:
            # A transport ack failure must never crash handle_task or leak into
            # the unawaited task; the entry stays pending and is redelivered on
            # restart (at-least-once).
            self._logger.log(
                logging.ERROR,
                "Failed to acknowledge task %s (%s): %s",
                task_data.task,
                task_id,
                exc,
            )

    async def _apply_retry_policy(
        self, task_id: UUID, task_data: TaskData, result: TaskResult | None
    ) -> TaskResult | None:
        """Requeue a retryable error once, and return the result to ack.

        Retry-once (AR-022 v4): requeue a retryable error BEFORE acking the
        transport entry (XADD then XACK), so the retry copy is durable before
        the original is dropped. The framework grants exactly one error-path
        retry per task id; the second consecutive retryable failure is
        terminal. Permanent errors and successes are acked and dropped without
        requeue. A requeue failure is logged and the task is still acked (the
        retry copy is lost, but the entry must not stay pending forever).
        """
        if result is None or result.status != "error" or not result.retryable:
            # Terminal for this id: drop the budget so the dict cannot grow
            # without bound.
            self._retry_attempts.pop(task_id, None)
            return result

        attempts = self._retry_attempts.get(task_id, 0)
        if attempts < self._max_retries:
            self._retry_attempts[task_id] = attempts + 1
            try:
                await self._requeue(task_id, task_data)
            except Exception as exc:
                # The retry copy is lost, so the budget must not stay behind to
                # grant a phantom second retry; the task is still acked.
                self._retry_attempts.pop(task_id, None)
                self._logger.log(
                    logging.ERROR,
                    "Failed to requeue retryable task %s (%s): %s",
                    task_data.task,
                    task_id,
                    exc,
                )
            return result

        # The transports deliberately leave an entry pending for a retryable
        # result (AR-077b), so the terminal ack must present retryable=False or
        # the entry would wait for a retry that never comes.
        ack_result = msgspec.structs.replace(result, retryable=False)
        self._retry_attempts.pop(task_id, None)
        self._logger.log(
            logging.WARNING,
            "Task %s (%s) exhausted its single retry; acking as terminal.",
            task_data.task,
            task_id,
        )
        return ack_result

    async def cancel(self, target_id: UUID) -> CancelOutcome:
        """Cancel a running or queued task by id.

        A running target is cancelled with the same pattern as the watchdog
        (``cancel()`` plus ``asyncio.wait``, never ``wait_for``); a queued
        target is removed before it starts. A deliberate cancel is never
        requeued automatically — the external process decides whether to
        resubmit.
        """
        tracker = self._lifecycle.get(target_id)
        if tracker is not None and not tracker.worker_task.done():
            if tracker.worker_task is asyncio.current_task():
                # A task cannot cancel itself: the cancel handler runs inside
                # the target's own worker task.
                return "not_running"
            # Set the reason before cancel() with no intervening await, so
            # handle_task's finally always observes it (no TOCTOU).
            self._lifecycle.mark_cancelled(target_id, "deliberate")
            tracker.worker_task.cancel()
            # asyncio.wait, not wait_for (see watchdog for why): wait_for
            # re-cancels on its timeout and blocks on a handler that swallows
            # cancellation.
            await asyncio.wait(
                [tracker.worker_task],
                timeout=self._settings().task_cancellation_timeout,
            )
            if tracker.worker_task.done():
                return "cancelled"
            # The handler ignored cancellation and is still running. It will
            # acknowledge its entry when it eventually finishes; requeueing now
            # would run the task twice. Leave the entry pending.
            self._logger.log(
                logging.ERROR,
                "Task %s (%s) ignored cancellation; not requeueing to avoid duplicate work.",
                tracker.data.task,
                target_id,
            )
            return "ignored"

        queued_data = await self._remove_queued(target_id)
        if queued_data is not None:
            # The target never started, so no handle_task will run for it:
            # write the terminal status directly.
            await self._on_completed(target_id, queued_data, None, cancel_reason="deliberate")
            return "cancelled"

        return "not_running"

    async def _remove_queued(self, task_id: UUID) -> TaskData | None:
        """Remove a queued-but-undispatched task from the internal queue.

        Drains the queue with the synchronous ``get_nowait``/``put_nowait`` pair
        and re-enqueues everything except the target. There is no ``await``
        between the drain and the re-enqueue, so the operation is atomic with
        respect to the event loop: no task can be dispatched mid-drain.
        """
        removed: TaskData | None = None
        pending: list[tuple[UUID, TaskData]] = []
        while not self._queue.empty():
            item = self._queue.get_nowait()
            if item[0] == task_id and removed is None:
                removed = item[1]
            else:
                pending.append(item)
        for item in pending:
            self._queue.put_nowait(item)
        if removed is not None:
            # The removed item was counted by the queue's unfinished-task
            # counter when it was put; balance it here since handle_task will
            # never run for it.
            self._queue.task_done()
        return removed

    async def watchdog(self) -> None:
        """Detect timed-out running tasks and cancel/requeue them by timeout_action."""
        now = time.monotonic()
        for task_id, task_tracker in self._lifecycle.trackers().items():
            timeout = task_tracker.data.timeout.timeout
            if timeout is None:
                timeout = self._settings().task_timeout
            # A non-positive timeout means "no timeout": the watchdog never
            # cancels the task (timeout <= 0 is treated as unbounded).
            if timeout > 0 and (now - task_tracker.started) > timeout and not task_tracker.worker_task.done():
                self._logger.log(
                    logging.WARNING,
                    "Task %s (%s) exceeded timeout and will be canceled.",
                    task_tracker.data.task,
                    task_id,
                )
                task_tracker.worker_task.cancel()
                # asyncio.wait, not wait_for: wait_for (3.13+) re-cancels the
                # task on its timeout and then blocks until a handler that
                # swallows cancellation eventually finishes, hanging the
                # watchdog. wait() returns after the timeout with the task
                # still pending when the handler ignored the cancellation.
                self._lifecycle.mark_cancelled(task_id, "timeout")
                await asyncio.wait(
                    [task_tracker.worker_task],
                    timeout=self._settings().task_cancellation_timeout,
                )
                if task_tracker.worker_task.done():
                    # The handler actually stopped; handle_task's finally has
                    # already acknowledged the transport entry. Requeue a fresh
                    # delivery only now, so a handler that ignores cancellation
                    # cannot cause the task to run twice.
                    if task_tracker.data.timeout.timeout_action == "requeue":
                        self._logger.log(
                            logging.WARNING,
                            "Task %s (%s) will be returned to queue.",
                            task_tracker.data.task,
                            task_id,
                        )
                        await self._requeue(task_id, task_tracker.data)
                else:
                    # The handler ignored cancellation and is still running. It
                    # will acknowledge its entry when it eventually finishes;
                    # requeueing now would run the task twice. Leave the entry
                    # pending so a restart redelivers it if the handler never
                    # returns.
                    self._logger.log(
                        logging.ERROR,
                        "Task %s (%s) ignored cancellation; not requeueing to avoid duplicate work.",
                        task_tracker.data.task,
                        task_id,
                    )
                # The tracker is removed unconditionally, even when the handler
                # ignored cancellation and the task is still alive: the cancel
                # reason must survive for handle_task's eventual ack, so only
                # the tracker is dropped here (remove_tracker leaves the reason
                # in place for the ack to consume).
                self._lifecycle.remove_tracker(task_id)

    async def shutdown(self) -> None:
        """Drain the queue, cancel/requeue running tasks, and clear the retry budget."""
        # Drain the in-process queue. Each queued task goes through
        # _on_queue_drain_task_processing: the base default requeues it when
        # canceled_action == "requeue" (a non-durable transport would otherwise
        # silently lose it on shutdown). A durable transport (e.g. a Valkey
        # stream) keeps entries pending and redelivers them on restart, so its
        # subclass overrides the hook to a no-op to avoid duplicating them.
        while not self._queue.empty():
            task_id, task_data = self._queue.get_nowait()
            await self._on_drain(task_id, task_data)
            self._queue.task_done()
        self._logger.debug("Task queue is empty")

        # Cancel and requeue running tasks. A task is requeued only after its
        # handler has actually stopped (handle_task's finally acknowledges the
        # transport entry); a handler that ignores cancellation is left pending
        # so a restart redelivers it rather than running it twice.
        for task_id, task_tracker in self._lifecycle.trackers().items():
            if not task_tracker.worker_task.done():
                task_tracker.worker_task.cancel()
                self._lifecycle.mark_cancelled(task_id, "shutdown")
                # asyncio.wait, not wait_for (see watchdog for why): wait_for
                # re-cancels on its timeout and blocks on a handler that
                # swallows cancellation, hanging shutdown.
                await asyncio.wait(
                    [task_tracker.worker_task],
                    timeout=self._settings().task_cancellation_timeout,
                )
                if task_tracker.worker_task.done() and task_tracker.data.canceled_action == "requeue":
                    self._logger.log(logging.WARNING, "Task %s will be returned to queue.", task_id)
                    await self._requeue(task_id, task_tracker.data)
        self._logger.debug("All tasks cancelled")

        # A task requeued but never re-handled before shutdown would otherwise
        # leave its retry budget behind; the budget is per-execution state.
        self._retry_attempts.clear()
