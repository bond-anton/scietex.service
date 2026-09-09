"""
Asynchronous task processing worker for ``scietex.service``.

Provides ``TaskProcessor``, a concurrent task processing framework
built on ``BasicWorker`` with task queue management, timeout
monitoring (watchdog), handler dispatch, and graceful shutdown support.
"""

import asyncio
import logging
import os
import time
from collections.abc import Mapping
from types import MappingProxyType
from typing import cast
from uuid import UUID

from .basic_worker import BasicWorker, ServiceStatus
from .config import (
    DEFAULT_MANAGER_SLEEP_TIME,
    DEFAULT_MAX_CONCURRENT_TASKS,
    DEFAULT_MAX_TASKS_QUEUE_SIZE,
    DEFAULT_TASK_HANDLER_START_TIMEOUT,
    DEFAULT_TASK_HANDLER_STOP_TIMEOUT,
    TaskProcessorConfig,
)
from .manager import Manager
from .task_handler import TaskData, TaskHandler, TaskHandlerContext, TaskResult, TaskTracker

DEFAULT_TASK_TIMEOUT = 3  # Timeout in seconds for task completion
"""Timeout in seconds for task completion before cancellation."""

TASK_QUEUE_FETCH_TIMEOUT: float = 1

WORKER_TASK_CANCELLATION_TIMEOUT: float = 5


class TaskProcessor(BasicWorker):
    """
    Concurrent asynchronous task processor built on ``BasicWorker``.

    Extends the base worker with a task queue, handler dispatch, concurrent
    task execution, timeout monitoring via watchdog, and cleanup on shutdown.

    Subclasses should override:
        - ``fetch_tasks()``: Retrieve tasks from an external source.
        - ``return_task_to_queue()``: Re-queue tasks on cancellation/timeout.
        - ``cleanup()``: Service-specific cleanup logic.
        - ``initialize()``: Service-specific initialization logic.

    Properties:
        service_name (str): Name of the service (read-only).
        instance_id (str): Unique identifier for this worker instance (read-only).
        version (str): Version string of the service (read-only).
        logger (logging.Logger): Logger instance for the worker.
        logging_level (int): Current logging level (configurable).
        task_handlers_map (dict): Registered task type to handler mappings.
        queue_size (int): Maximum size of the internal task queue.
        max_concurrent_tasks (int): Maximum concurrent task count.
    """

    def __init__(self, config: TaskProcessorConfig | None = None):
        """
        Initialize the TaskProcessor.

        Args:
            config: A :class:`~scietex.service.config.TaskProcessorConfig`
                holding the worker's service identity and task-queue /
                handler-lifecycle settings. ``None`` uses the struct defaults.
                A ``None`` timing/count field resolves to its ``DEFAULT_*``
                constant at read time; an out-of-range value is rejected at
                construction.
        """
        super().__init__(config)
        cfg = config if config is not None else TaskProcessorConfig()
        # The base stores WorkerConfig() when config is None; re-store the full
        # TaskProcessorConfig so the read-time getters below have its fields.
        self._config = cfg

        self.__task_handlers_map: dict[str, tuple[type[TaskHandler], dict[str, object]]] = {}
        self.__task_handlers: dict[str, TaskHandler] = {}

        # Initialize queues and tracking structures
        self.__running_tasks: dict[UUID, TaskTracker] = {}  # Track running tasks
        self.__queue_size: int = cfg.queue_size if cfg.queue_size is not None else DEFAULT_MAX_TASKS_QUEUE_SIZE
        if cfg.max_concurrent_tasks is not None:
            self.__max_concurrent_tasks: int = cfg.max_concurrent_tasks
        elif cfg.auto_tune:
            cpu_count = os.cpu_count() or 1
            self.__max_concurrent_tasks: int = max(1, cpu_count)
            self.logger.log(
                logging.INFO,
                "Auto-tuned max_concurrent_tasks to %d (from %d CPUs)",
                self.__max_concurrent_tasks,
                cpu_count,
            )
        else:
            self.__max_concurrent_tasks: int = DEFAULT_MAX_CONCURRENT_TASKS

        self.__task_queue: asyncio.Queue[tuple[UUID, TaskData]] = asyncio.Queue(maxsize=self.queue_size)

    @property
    def task_handlers(self) -> Mapping[str, TaskHandler]:
        """Dictionary of currently active (started) task handlers.

        Keys are handler class names and values are the corresponding
        ``TaskHandler`` instances that have been initialized.

        Returns:
            A read-only mapping view of the active task handlers.
        """
        return MappingProxyType(self.__task_handlers)

    @property
    def running_tasks(self) -> Mapping[UUID, TaskTracker]:
        """Read-only mapping of currently running tasks and their trackers."""
        return MappingProxyType(self.__running_tasks)

    @property
    def queue_size(self) -> int:
        """Maximum size of the internal task queue."""
        return self.__queue_size

    @property
    def max_concurrent_tasks(self) -> int:
        """Maximum number of tasks that can be processed concurrently."""
        return self.__max_concurrent_tasks

    def enqueue_task(self, task_id: UUID, task_data: TaskData) -> bool:
        """Enqueue a task for processing without blocking.

        Non-blocking: if the bounded queue is full the task is not enqueued
        and ``False`` is returned so the caller can retry later (e.g. on the
        next intake poll). Returns ``True`` on success.
        """
        try:
            self.__task_queue.put_nowait((task_id, task_data))
        except asyncio.QueueFull:
            return False
        return True

    def task_queue_empty(self) -> bool:
        """Whether the internal task queue has no pending tasks."""
        return self.__task_queue.empty()

    def task_queue_full(self) -> bool:
        """Whether the internal task queue has reached its maximum size."""
        return self.__task_queue.full()

    def dequeue_task(self) -> tuple[UUID, TaskData] | None:
        """Remove and return the next pending task without blocking.

        Returns:
            The ``(task_id, task_data)`` tuple, or ``None`` if the queue is
            empty.
        """
        try:
            return self.__task_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    @property
    def task_manager_sleep_time(self) -> float:
        """Sleep time in seconds between task manager loop iterations (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_MANAGER_SLEEP_TIME``; a non-``None`` value is validated against
        ``[MIN_MANAGER_SLEEP_TIME, MAX_MANAGER_SLEEP_TIME]`` at construction.
        """
        v = cast(TaskProcessorConfig, self._config).task_manager_sleep_time
        return v if v is not None else DEFAULT_MANAGER_SLEEP_TIME

    @property
    def task_queue_manager_sleep_time(self) -> float:
        """Sleep time in seconds between task queue manager loop iterations (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_MANAGER_SLEEP_TIME``; a non-``None`` value is validated against
        ``[MIN_MANAGER_SLEEP_TIME, MAX_MANAGER_SLEEP_TIME]`` at construction.
        """
        v = cast(TaskProcessorConfig, self._config).task_queue_manager_sleep_time
        return v if v is not None else DEFAULT_MANAGER_SLEEP_TIME

    @property
    def task_handler_start_timeout(self) -> float:
        """Timeout in seconds for starting task handlers (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_TASK_HANDLER_START_TIMEOUT``; a non-``None`` value is validated
        against
        ``[MIN_TASK_HANDLER_START_TIMEOUT, MAX_TASK_HANDLER_START_TIMEOUT]``
        at construction.

        Returns:
            The current task handler start timeout in seconds.
        """
        v = cast(TaskProcessorConfig, self._config).task_handler_start_timeout
        return v if v is not None else DEFAULT_TASK_HANDLER_START_TIMEOUT

    @property
    def task_handler_stop_timeout(self) -> float:
        """Timeout in seconds for stopping task handlers (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_TASK_HANDLER_STOP_TIMEOUT``; a non-``None`` value is validated
        against
        ``[MIN_TASK_HANDLER_STOP_TIMEOUT, MAX_TASK_HANDLER_STOP_TIMEOUT]``
        at construction.

        Returns:
            The current task handler stop timeout in seconds.
        """
        v = cast(TaskProcessorConfig, self._config).task_handler_stop_timeout
        return v if v is not None else DEFAULT_TASK_HANDLER_STOP_TIMEOUT

    def add_task_handler(
        self,
        handler_class: type[TaskHandler],
        *,
        name: str | None = None,
        **handler_kwargs: object,
    ) -> None:
        """Register a task handler class.

        The handler class is stored in the internal map under its lifecycle
        key, which is the resolved handler name: ``name`` if given, otherwise
        ``handler_class.__name__``. Dispatch is driven by
        ``_find_task_handler``, which selects handlers by their
        ``supported_tasks`` membership — the class-level declaration is the
        dispatch contract, not an argument. One instance per registered key is
        created on start; the optional ``name`` lets multiple instances of a
        single class coexist under distinct keys (e.g. to split one class's
        task types across instances via name-derived ``supported_tasks``).
        Duplicate detection is on the resolved key.

        Args:
            handler_class: The ``TaskHandler`` subclass to register.
            name: Optional lifecycle key, defaulting to the handler class name.
                Enables multiple instances of one class under distinct keys.
            **handler_kwargs: Extra keyword arguments forwarded to the handler
                constructor on every instantiation. Enables stateful handlers
                by injecting shared mutable objects (e.g. a shared counter or
                cache) that outlive a single start/stop cycle. A misspelled
                kwarg raises a loud ``TypeError`` at construction, because
                ``TaskHandler`` subclasses do not accept arbitrary kwargs.

        Raises:
            ValueError: If the resolved handler name is already registered.
        """
        handler_name = name or handler_class.__name__
        if handler_name in self.__task_handlers_map:
            raise ValueError(f"Task handler {handler_name!r} is already registered")
        self.__task_handlers_map[handler_name] = (handler_class, handler_kwargs)
        self.logger.log(logging.INFO, "Added Task handler: %s", handler_name)
        if self.state in (ServiceStatus.RUNNING, ServiceStatus.STARTING):
            asyncio.create_task(self._start_task_handler(handler_name))

    async def _start_task_handler(self, handler_name) -> bool:
        """Start a registered task handler and initialize it.

        Creates an instance of the handler class, stores it in the
        active handlers dictionary, and calls its ``start()`` method
        with a timeout.

        Args:
            handler_name: The name of the handler to start.

        Returns:
            ``True`` if the handler started and became ready; ``False``
            if it timed out, raised, or was not registered.
        """
        if handler_name in self.__task_handlers:
            self.logger.log(logging.DEBUG, "Task handler %s is already started", handler_name)
            return True
        if handler_name not in self.__task_handlers_map:
            self.logger.log(logging.DEBUG, "Task handler %s not found", handler_name)
            return False
        handler_class, handler_kwargs = self.__task_handlers_map[handler_name]
        context = TaskHandlerContext(
            service_name=self.service_name,
            instance_id=self.instance_id,
            logger=self.logger,
        )
        handler_instance = handler_class(handler_name, context, **handler_kwargs)
        self.__task_handlers[handler_name] = handler_instance
        try:
            await asyncio.wait_for(self.__task_handlers[handler_name].start(), timeout=self.task_handler_start_timeout)
        except asyncio.TimeoutError:
            self.logger.log(logging.ERROR, "Timeout while starting Task handler %s", handler_name)
            del self.__task_handlers[handler_name]
            return False
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to start Task handler %s: %s", handler_name, exc)
            del self.__task_handlers[handler_name]
            return False
        if not self.__task_handlers[handler_name].is_ready:
            self.logger.log(logging.ERROR, "Task handler %s failed to become ready", handler_name)
            del self.__task_handlers[handler_name]
            return False
        return True

    async def _stop_task_handler(self, handler_name: str) -> None:
        """Stop a running task handler and remove it from active handlers.

        Calls the handler's ``stop()`` method with a timeout and removes it
        from the active handlers dictionary on success. If ``stop()`` times
        out the handler is removed from the active handlers dictionary too
        (with a WARNING), so it is not left in an ambiguous tracked-but-stuck
        state; its ``stop()`` may still be finishing cleanup in the background.

        Args:
            handler_name: The name of the handler to stop.
        """
        if handler_name not in self.__task_handlers:
            self.logger.log(logging.DEBUG, "Task handler %s not found", handler_name)
            return
        # Perform cleanup before removal
        try:
            await asyncio.wait_for(self.__task_handlers[handler_name].stop(), timeout=self.task_handler_stop_timeout)
            self.__task_handlers.pop(handler_name, None)
        except asyncio.TimeoutError:
            # Do not leave the handler tracked-but-stuck: its stop() timed out,
            # so it is no longer reliably active. pop() guards against it having
            # already been removed concurrently.
            self.__task_handlers.pop(handler_name, None)
            self.logger.log(logging.WARNING, "Task handler %s removed after stop timeout", handler_name)

    def remove_task_handler(self, handler_name: str) -> None:
        """Remove a registered task handler.

        Stops the handler asynchronously if it is currently active, then
        removes it from the registration map so it is no longer dispatched
        to. Safe to call for a handler that is not registered.

        Args:
            handler_name: The class name of the handler to remove.
        """
        if handler_name in self.__task_handlers:
            asyncio.create_task(self._stop_task_handler(handler_name))
        if handler_name in self.__task_handlers_map:
            del self.__task_handlers_map[handler_name]
            self.logger.log(logging.INFO, "Removed handler: %s", handler_name)

    def _find_task_handler(self, task: str) -> TaskHandler | None:
        """Find a registered handler that supports the given task type.

        Iterates over all registered task handlers and returns the first
        one whose ``supports(task_type)`` method returns ``True``.

        Args:
            task: The task type string to look up.

        Returns:
            The matching ``TaskHandler`` instance, or ``None`` if no
            handler supports the given task type.
        """
        for _, handler in self.task_handlers.items():
            if handler.supports(task):
                return handler
        return None

    async def return_task_to_queue(self, task_id: UUID, task_data: TaskData) -> None:
        """Return a task to its external source queue.

        Subclasses should override this method to implement the specific
        logic for re-queueing tasks when they cannot be processed or
        need to be retried (e.g., writing back to a message queue). The
        default is a no-op.

        Args:
            task_id: The unique identifier of the task.
            task_data: The task data to return to the external queue.
        """

    async def on_task_completed(
        self,
        task_id: UUID,
        task_data: TaskData,
        task_result: TaskResult | None,
    ) -> None:
        """Notify the transport that a task's processing has terminated.

        Called by ``handle_task`` when a task's work ends — on success, on
        a terminal error, or on cancellation — with the final
        ``TaskResult``, or ``None`` when the task was cancelled before
        producing a result. Subclasses that source tasks from a durable
        transport (e.g. ``ValkeyWorker``) override this to acknowledge the
        transport entry so it is removed only after the handler's work on
        it is done (at-least-once). The default is a no-op.
        """

    async def initialize(self) -> bool:
        """Start all registered task handlers.

        Override point for custom initialization. Starts every handler
        registered via ``add_task_handler()`` before the worker enters
        the RUNNING state.

        Returns:
            ``True`` if every registered handler started successfully;
            ``False`` if any handler failed to start (timeout or error),
            so the worker fails fast instead of running with a handler
            that never became ready.
        """
        for handler_name in self.__task_handlers_map:
            if not await self._start_task_handler(handler_name):
                return False
        return True

    async def _on_queue_drain_task_processing(self, task_id: UUID, task_data: TaskData) -> None:
        """Handle a task still queued when the in-process queue is drained on shutdown.

        Called by ``cleanup()`` for every queued-but-undispatched task. The base
        default returns the task to its external source when ``canceled_action``
        is ``"requeue"``, so a non-durable transport (whose entries are not kept
        pending anywhere) does not silently lose work on shutdown (AR-041).

        Subclasses backed by a durable transport (e.g. ``ValkeyWorker``) override
        this to a no-op: their entries stay pending in the transport and are
        redelivered on restart, so re-enqueueing here would duplicate them.

        Args:
            task_id: Identifier of the queued task.
            task_data: The task data that was still queued at drain time.
        """
        if task_data.canceled_action == "requeue":
            self.logger.log(logging.WARNING, "Task %s will be returned to queue.", task_id)
            await self.return_task_to_queue(task_id, task_data)

    async def cleanup(self) -> None:
        """Release resources and stop processing before exit.

        Drains the in-process task queue (each queued task is handed to
        ``_on_queue_drain_task_processing``), cancels and requeues running
        tasks whose handlers actually stopped, and stops all task handlers.
        Subclasses add transport-specific teardown (e.g. closing a database
        or Valkey connection) by overriding this method and calling
        ``super().cleanup()``.
        """
        await super().cleanup()
        # Drain the in-process queue. Each queued task goes through
        # _on_queue_drain_task_processing: the base default requeues it when
        # canceled_action == "requeue" (a non-durable transport would otherwise
        # silently lose it on shutdown). A durable transport (e.g. a Valkey
        # stream) keeps entries pending and redelivers them on restart, so its
        # subclass overrides the hook to a no-op to avoid duplicating them.
        while not self.__task_queue.empty():
            task_id, task_data = self.__task_queue.get_nowait()
            await self._on_queue_drain_task_processing(task_id, task_data)
            self.__task_queue.task_done()
        self.logger.debug("Task queue is empty")

        # Cancel and requeue running tasks. A task is requeued only after its
        # handler has actually stopped (handle_task's finally acknowledges the
        # transport entry); a handler that ignores cancellation is left pending
        # so a restart redelivers it rather than running it twice.
        for task_id, task_tracker in list(self.running_tasks.items()):
            if not task_tracker.worker_task.done():
                task_tracker.worker_task.cancel()
                # asyncio.wait, not wait_for (see watchdog for why): wait_for
                # re-cancels on its timeout and blocks on a handler that
                # swallows cancellation, hanging shutdown.
                await asyncio.wait(
                    [task_tracker.worker_task],
                    timeout=WORKER_TASK_CANCELLATION_TIMEOUT,
                )
                if task_tracker.worker_task.done() and task_tracker.data.canceled_action == "requeue":
                    self.logger.log(logging.WARNING, "Task %s will be returned to queue.", task_id)
                    await self.return_task_to_queue(task_id, task_tracker.data)
        self.logger.debug("All tasks cancelled")

        # Cleanup task handlers
        for handler_name in self.__task_handlers_map:
            await self._stop_task_handler(handler_name)
        self.logger.debug("All task handlers cleaned up")

    async def process_task(self, task_id: UUID, task_data: TaskData) -> TaskResult:
        """Process a single task by dispatching to the appropriate handler.

        Looks up a handler that supports the task type via
        ``_find_task_handler()`` and calls its ``handle()`` method.
        Returns a ``TaskResult`` with ``status="error"`` if no handler
        is found or an exception occurs.

        A handler that raises produces an error result marked permanent
        (``retryable=False``): an unhandled exception is unclassified, so
        it must not create an infinite requeue loop under retry-once. A
        handler that returns a ``TaskResult`` controls its own fields
        (``retryable=True`` opts into a single retry) and is passed
        through unchanged. Framework-level failures (empty ``task`` field,
        no matching handler) are permanent and leave ``retryable=False``.

        Args:
            task_id: Identifier of the task to process.
            task_data: The data associated with the task.

        Returns:
            A ``TaskResult`` with the processing outcome.
        """
        self.logger.log(logging.DEBUG, "Processing task %s (%s): %s", task_data.task, task_id, task_data)

        task_type = task_data.task
        if not task_type:
            self.logger.log(
                logging.ERROR,
                "Wrong task format for %s (%s): %s",
                task_data.task,
                task_id,
                task_data,
            )
            return TaskResult(status="error", error="Task data must contain 'task' field")

        handler = self._find_task_handler(task_type)
        if handler and handler.is_ready:
            try:
                result = await handler.handle(task_data)
            except Exception as e:
                # A handler raising is unclassified: treat it as permanent
                # (retryable=False) so an unhandled exception cannot create an
                # infinite requeue loop under retry-once. A handler that wants
                # a retry must return a retryable=True result explicitly.
                result = TaskResult(status="error", error=str(e))
        else:
            result = TaskResult(status="error", error=f"No handler found for task type '{task_type}'")

        self.logger.log(logging.DEBUG, "Task %s (%s) completed with result: %s", task_data, task_id, result)
        return result

    @Manager("TaskManager")
    async def task_manager(self):
        """Manage task processing from the internal task queue.

        Continuously fetches tasks from ``task_queue``, processes them
        via ``process_task()``, and tracks running tasks for timeout
        monitoring. Respects ``max_concurrent_tasks`` to limit parallel
        execution.

        This method is decorated with ``@Manager`` and runs as an
        infinite loop managed by ``BasicWorker``.
        """

        async def handle_task(t_id: UUID, t_data: TaskData):
            result: TaskResult | None = None
            try:
                result = await self.process_task(t_id, t_data)
                self.logger.log(
                    logging.DEBUG,
                    "Task %s (%s) finished with status %s",
                    t_data.task,
                    t_id,
                    result.status,
                )
            except Exception as exc:
                # process_task is expected to return an error TaskResult for
                # every failure, but a defensive catch guarantees no exception
                # escapes into the unawaited task (which would surface as an
                # unretrieved task exception).
                self.logger.log(
                    logging.ERROR,
                    "Task %s (%s) raised unexpectedly: %s",
                    t_data.task,
                    t_id,
                    exc,
                )
            finally:
                self.__running_tasks.pop(t_id, None)
                self.__task_queue.task_done()
                # Retry-once (AR-022 v4): requeue a retryable error BEFORE
                # acking the transport entry (XADD then XACK), so the retry
                # copy is durable before the original is dropped. Permanent
                # errors and successes are acked and dropped without requeue.
                # A requeue failure is logged and the task is still acked (the
                # retry copy is lost, but the entry must not stay pending
                # forever).
                if result is not None and result.status == "error" and result.retryable:
                    try:
                        await self.return_task_to_queue(t_id, t_data)
                    except Exception as exc:
                        self.logger.log(
                            logging.ERROR,
                            "Failed to requeue retryable task %s (%s): %s",
                            t_data.task,
                            t_id,
                            exc,
                        )
                try:
                    # Ack the transport entry exactly when the handler's work
                    # on it ends (success, error, or cancellation). On
                    # CancelledError, result is None and the hook still runs.
                    await self.on_task_completed(t_id, t_data, result)
                except Exception as exc:
                    # A transport ack failure must never crash handle_task or
                    # leak into the unawaited task; the entry stays pending
                    # and is redelivered on restart (at-least-once).
                    self.logger.log(
                        logging.ERROR,
                        "Failed to acknowledge task %s (%s): %s",
                        t_data.task,
                        t_id,
                        exc,
                    )

        if len(self.running_tasks) < self.max_concurrent_tasks:
            try:
                task_id, task_data = await asyncio.wait_for(self.__task_queue.get(), timeout=TASK_QUEUE_FETCH_TIMEOUT)
                task = asyncio.create_task(handle_task(task_id, task_data))
                self.__running_tasks[task_id] = TaskTracker(worker_task=task, data=task_data, started=time.monotonic())
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(self.task_manager_sleep_time)

    async def fetch_tasks(self) -> bool:
        """Fetch tasks from external sources and enqueue them.

        Override this method in subclasses to implement the specific
        logic for retrieving tasks from external sources such as message
        queues, databases, or APIs, and enqueuing them via
        ``enqueue_task()`` as ``(UUID, TaskData)`` tuples. The default is
        a no-op.

        Returns:
            ``True`` if at least one task was enqueued, ``False`` otherwise.
            The caller (``task_queue_manager``) skips its idle backoff after a
            productive fetch so a backlog drains back-to-back. A subclass that
            does not report this (returns ``None``) is treated as ``False`` and
            always backs off, preserving the previous behavior.
        """
        return False

    @Manager("TaskQueueManager")
    async def task_queue_manager(self):
        """Periodically fetch tasks from external sources into the task queue.

        Calls ``fetch_tasks()`` only when the queue is not full. After a
        productive fetch (``fetch_tasks()`` returned ``True``) the loop
        continues immediately so a backlog drains back-to-back; after an empty
        fetch it sleeps for ``task_queue_manager_sleep_time`` to prevent busy
        waiting. A full queue also backs off, providing backpressure.

        This method is decorated with ``@Manager`` and runs as an
        infinite loop managed by ``BasicWorker``.
        """
        if not self.__task_queue.full():
            fetched = await self.fetch_tasks()
            if not fetched:
                await asyncio.sleep(self.task_queue_manager_sleep_time)
        else:
            await asyncio.sleep(self.task_queue_manager_sleep_time)

    async def watchdog(self):
        """Monitor running tasks for timeouts and handle stalled tasks.

        Periodically checks all running tasks and cancels any that have
        exceeded their configured ``timeout`` (or ``DEFAULT_TASK_TIMEOUT``
        if no timeout is set). Tasks with ``timeout_action="requeue"`` are
        returned to the external queue for potential retry.

        Requeue boundary: error-path requeue is handled in ``handle_task``
        (retry-once via ``TaskResult.retryable``); the watchdog handles only
        timeout-driven requeue via the task's ``timeout_action`` literal.
        The handler's final ``TaskResult`` lives in the ``handle_task``
        closure and is not accessible here.

        Override this method in subclasses to add additional watchdog
        logic. The default implementation handles task timeout detection
        and cancellation.
        """
        now = time.monotonic()
        for task_id, task_tracker in list(self.running_tasks.items()):
            timeout = task_tracker.data.timeout.timeout
            if timeout is None:
                timeout = DEFAULT_TASK_TIMEOUT
            # A non-positive timeout means "no timeout": the watchdog never
            # cancels the task (timeout <= 0 is treated as unbounded).
            if timeout > 0 and (now - task_tracker.started) > timeout and not task_tracker.worker_task.done():
                self.logger.log(
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
                await asyncio.wait(
                    [task_tracker.worker_task],
                    timeout=WORKER_TASK_CANCELLATION_TIMEOUT,
                )
                if task_tracker.worker_task.done():
                    # The handler actually stopped; handle_task's finally has
                    # already acknowledged the transport entry. Requeue a fresh
                    # delivery only now, so a handler that ignores cancellation
                    # cannot cause the task to run twice.
                    if task_tracker.data.timeout.timeout_action == "requeue":
                        self.logger.log(
                            logging.WARNING,
                            "Task %s (%s) will be returned to queue.",
                            task_tracker.data.task,
                            task_id,
                        )
                        await self.return_task_to_queue(task_id, task_tracker.data)
                else:
                    # The handler ignored cancellation and is still running. It
                    # will acknowledge its entry when it eventually finishes;
                    # requeueing now would run the task twice. Leave the entry
                    # pending so a restart redelivers it if the handler never
                    # returns.
                    self.logger.log(
                        logging.ERROR,
                        "Task %s (%s) ignored cancellation; not requeueing to avoid duplicate work.",
                        task_tracker.data.task,
                        task_id,
                    )
                self.__running_tasks.pop(task_id, None)
