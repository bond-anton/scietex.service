"""
Asynchronous task processing worker for ``scietex.service``.

Provides ``AsyncTaskProcessor``, a concurrent task processing framework
built on ``BasicAsyncWorker`` with task queue management, timeout
monitoring (watchdog), handler dispatch, and graceful shutdown support.
"""

import asyncio
import logging
import time
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from uuid import UUID

from .basic_async_worker import BasicAsyncWorker, ServiceStatus
from .manager import Manager
from .task_handler import TaskData, TaskHandler, TaskHandlerContext, TaskResult, TaskTracker

DEFAULT_MAX_TASKS_QUEUE_SIZE = 2
"""Default maximum number of tasks queue size."""
DEFAULT_MAX_CONCURRENT_TASKS = 2
"""Default maximum number of concurrent tasks that can be processed."""

DEFAULT_TASK_TIMEOUT = 3  # Timeout in seconds for task completion
"""Timeout in seconds for task completion before cancellation."""

TASK_QUEUE_FETCH_TIMEOUT: float = 1

DEFAULT_MANAGER_SLEEP_TIME: float = 0.01
MIN_MANAGER_SLEEP_TIME: float = 0.001
MAX_MANAGER_SLEEP_TIME: float = 1

WORKER_TASK_CANCELLATION_TIMEOUT: float = 5

MIN_TASK_HANDLER_START_TIMEOUT: float = 1
MAX_TASK_HANDLER_START_TIMEOUT: float = 60
DEFAULT_TASK_HANDLER_START_TIMEOUT: float = 5

MIN_TASK_HANDLER_STOP_TIMEOUT: float = 1
MAX_TASK_HANDLER_STOP_TIMEOUT: float = 60
DEFAULT_TASK_HANDLER_STOP_TIMEOUT: float = 5


class AsyncTaskProcessor(BasicAsyncWorker):
    """
    Concurrent asynchronous task processor built on ``BasicAsyncWorker``.

    Extends the base worker with a task queue, handler dispatch, concurrent
    task execution, timeout monitoring via watchdog, and cleanup on shutdown.

    Subclasses should override:
        - ``fetch_tasks()``: Retrieve tasks from an external source.
        - ``return_task_to_queue()``: Re-queue tasks on cancellation/timeout.
        - ``cleanup()``: Service-specific cleanup logic.
        - ``initialize()``: Service-specific initialization logic.

    Properties:
        service_name (str): Name of the service (read-only).
        worker_id (int): Unique identifier for this worker (read-only).
        version (str): Version string of the service (read-only).
        logger (logging.Logger): Logger instance for the worker.
        logging_level (int): Current logging level (configurable).
        task_handlers_map (dict): Registered task type to handler mappings.
        queue_size (int): Maximum size of the internal task queue.
        max_concurrent_tasks (int): Maximum concurrent task count.
    """

    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        worker_id: int = 1,
        conf_dir: str | Path | None = None,
        logging_level: int | str = logging.DEBUG,
        heartbeat_interval: float | None = None,
        watchdog_interval: float | None = None,
        queue_size: int | None = None,
        max_concurrent_tasks: int | None = None,
        **kwargs,
    ):
        """
        Initialize the AsyncTaskProcessor.

        Args:
            service_name: Name of the service, used for logging and identification.
            version: Version string of the service.
            worker_id: Unique identifier for this worker instance.
            conf_dir: Directory to use for configuration files.
            logging_level: Logging level as string or integer.
            heartbeat_interval: Heartbeat interval in seconds.
            watchdog_interval: Watchdog check interval in seconds.
            queue_size: Maximum size of the internal task queue.
            max_concurrent_tasks: Maximum number of tasks processed concurrently.
            **kwargs: Additional keyword arguments passed to the parent
                ``BasicAsyncWorker`` constructor. May include
                ``task_manager_sleep_time``, ``task_queue_manager_sleep_time``,
                ``logger_handler_timeout``, and ``manager_shutdown_timeout``.
        """
        super().__init__(
            service_name=service_name,
            version=version,
            worker_id=worker_id,
            conf_dir=conf_dir,
            logging_level=logging_level,
            heartbeat_interval=heartbeat_interval,
            watchdog_interval=watchdog_interval,
            **kwargs,
        )

        self.__task_handlers_map: dict[str, type[TaskHandler]] = {}
        self.__task_handlers: dict[str, TaskHandler] = {}

        # Initialize queues and tracking structures
        self.__running_tasks: dict[UUID, TaskTracker] = {}  # Track running tasks
        self.__queue_size: int = queue_size if queue_size is not None else DEFAULT_MAX_TASKS_QUEUE_SIZE
        self.__max_concurrent_tasks: int = max(1, max_concurrent_tasks or DEFAULT_MAX_CONCURRENT_TASKS)

        self.__task_queue: asyncio.Queue[tuple[UUID, TaskData]] = asyncio.Queue(maxsize=self.queue_size)

        self.__task_manager_sleep_time: float = min(
            MAX_MANAGER_SLEEP_TIME,
            max(
                MIN_MANAGER_SLEEP_TIME,
                kwargs.get("task_manager_sleep_time", DEFAULT_MANAGER_SLEEP_TIME),
            ),
        )

        self.__task_queue_manager_sleep_time: float = min(
            MAX_MANAGER_SLEEP_TIME,
            max(
                MIN_MANAGER_SLEEP_TIME,
                kwargs.get("task_queue_manager_sleep_time", DEFAULT_MANAGER_SLEEP_TIME),
            ),
        )

        self.__task_handler_start_timeout: float = min(
            MAX_TASK_HANDLER_START_TIMEOUT,
            max(
                MIN_TASK_HANDLER_START_TIMEOUT,
                kwargs.get("task_handler_start_timeout", DEFAULT_TASK_HANDLER_START_TIMEOUT),
            ),
        )

        self.__task_handler_stop_timeout: float = min(
            MAX_TASK_HANDLER_STOP_TIMEOUT,
            max(
                MIN_TASK_HANDLER_STOP_TIMEOUT,
                kwargs.get("task_handler_stop_timeout", DEFAULT_TASK_HANDLER_STOP_TIMEOUT),
            ),
        )

    @property
    def task_handlers(self) -> Mapping[str, TaskHandler]:
        """Dictionary of currently active (started) task handlers.

        Keys are handler names and values are the corresponding
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
        """Sleep time in seconds between task manager loop iterations."""
        return self.__task_manager_sleep_time

    @task_manager_sleep_time.setter
    def task_manager_sleep_time(self, delay: float | None) -> None:
        """
        Set the sleep time for the task manager loop.

        Args:
            delay: Sleep time in seconds, clamped between MIN_MANAGER_SLEEP_TIME
                and MAX_MANAGER_SLEEP_TIME, or None to use DEFAULT_MANAGER_SLEEP_TIME
        """
        self.__task_manager_sleep_time: float = min(
            MAX_MANAGER_SLEEP_TIME,
            max(MIN_MANAGER_SLEEP_TIME, delay or DEFAULT_MANAGER_SLEEP_TIME),
        )

    @property
    def task_queue_manager_sleep_time(self) -> float:
        """Sleep time in seconds between task queue manager loop iterations."""
        return self.__task_queue_manager_sleep_time

    @task_queue_manager_sleep_time.setter
    def task_queue_manager_sleep_time(self, delay: float | None) -> None:
        """
        Set the sleep time for the task queue manager loop.

        Args:
            delay: Sleep time in seconds, clamped between MIN_MANAGER_SLEEP_TIME
                and MAX_MANAGER_SLEEP_TIME, or None to use DEFAULT_MANAGER_SLEEP_TIME
        """
        self.__task_queue_manager_sleep_time: float = min(
            MAX_MANAGER_SLEEP_TIME,
            max(MIN_MANAGER_SLEEP_TIME, delay or DEFAULT_MANAGER_SLEEP_TIME),
        )

    @property
    def task_handler_start_timeout(self) -> float:
        """Timeout in seconds for starting task handlers (read-only).

        Clamped between ``MIN_TASK_HANDLER_START_TIMEOUT`` and
        ``MAX_TASK_HANDLER_START_TIMEOUT``.

        Returns:
            The current task handler start timeout in seconds.
        """
        return self.__task_handler_start_timeout

    @task_handler_start_timeout.setter
    def task_handler_start_timeout(self, timeout: float | None) -> None:
        """
        Set the timeout for starting task handlers.

        Args:
            timeout: Timeout in seconds, clamped between
                ``MIN_TASK_HANDLER_START_TIMEOUT`` and
                ``MAX_TASK_HANDLER_START_TIMEOUT``, or None to use
                ``DEFAULT_TASK_HANDLER_START_TIMEOUT``.
        """
        self.__task_handler_start_timeout = min(
            MAX_TASK_HANDLER_START_TIMEOUT,
            max(
                MIN_TASK_HANDLER_START_TIMEOUT,
                timeout or DEFAULT_TASK_HANDLER_START_TIMEOUT,
            ),
        )

    @property
    def task_handler_stop_timeout(self) -> float:
        """Timeout in seconds for stopping task handlers (read-only).

        Clamped between ``MIN_TASK_HANDLER_STOP_TIMEOUT`` and
        ``MAX_TASK_HANDLER_STOP_TIMEOUT``.

        Returns:
            The current task handler stop timeout in seconds.
        """
        return self.__task_handler_stop_timeout

    @task_handler_stop_timeout.setter
    def task_handler_stop_timeout(self, timeout: float | None) -> None:
        """
        Set the timeout for stopping task handlers.

        Args:
            timeout: Timeout in seconds, clamped between
                ``MIN_TASK_HANDLER_STOP_TIMEOUT`` and
                ``MAX_TASK_HANDLER_STOP_TIMEOUT``, or None to use
                ``DEFAULT_TASK_HANDLER_STOP_TIMEOUT``.
        """
        self.__task_handler_stop_timeout = min(
            MAX_TASK_HANDLER_STOP_TIMEOUT,
            max(
                MIN_TASK_HANDLER_STOP_TIMEOUT,
                timeout or DEFAULT_TASK_HANDLER_STOP_TIMEOUT,
            ),
        )

    def add_task_handler(self, handler_name: str, handler_class: type[TaskHandler]) -> None:
        """Register a task handler class for a given handler name.

        The handler class is stored in the internal map. If the worker
        is already running or starting, the handler is started
        asynchronously.

        Args:
            handler_name: Unique identifier for the handler.
            handler_class: The ``TaskHandler`` subclass to register.
        """
        self.__task_handlers_map[handler_name] = handler_class
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
        handler_class = self.__task_handlers_map[handler_name]
        context = TaskHandlerContext(
            service_name=self.service_name,
            worker_id=self.worker_id,
            logger=self.logger,
        )
        handler_instance = handler_class(handler_name, context)
        self.__task_handlers[handler_name] = handler_instance
        try:
            await asyncio.wait_for(self.__task_handlers[handler_name].start(), timeout=self.task_handler_start_timeout)
        except asyncio.TimeoutError:
            self.logger.log(logging.ERROR, "Timeout while starting Task handler %s", handler_name)
            return False
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to start Task handler %s: %s", handler_name, exc)
            return False
        if not self.__task_handlers[handler_name].is_ready:
            self.logger.log(logging.ERROR, "Task handler %s failed to become ready", handler_name)
            return False
        return True

    async def _stop_task_handler(self, handler_name) -> None:
        """Stop a running task handler and remove it from active handlers.

        Calls the handler's ``stop()`` method with a timeout, then
        removes it from the internal handlers dictionary.

        Args:
            handler_name: The name of the handler to stop.
        """
        if handler_name not in self.__task_handlers:
            self.logger.log(logging.DEBUG, "Task handler %s not found", handler_name)
            return
        # Perform cleanup before removal
        try:
            await asyncio.wait_for(self.__task_handlers[handler_name].stop(), timeout=self.task_handler_stop_timeout)
            del self.__task_handlers[handler_name]
        except asyncio.TimeoutError:
            self.logger.log(logging.ERROR, "Timeout while stopping Task handler %s", handler_name)

    def remove_task_handler(self, handler_name: str) -> None:
        """
        Remove a task handler.
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
        need to be retried (e.g., writing back to a message queue).

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

    async def cleanup(self):
        """
        Cleanup everything before exit.

        This method is intended to be overridden by subclasses to perform
        service-specific cleanup such as closing database connections,
        releasing resources, or sending final status updates.
        """
        await super().cleanup()
        # Drain the in-process queue. Items fetched from a durable transport
        # (e.g. a Valkey stream) are still pending there and will be
        # redelivered on restart, so they must NOT be re-enqueued here (that
        # would duplicate them). Subclasses whose transport does not keep
        # items pending after enqueue must override cleanup to requeue drained
        # items.
        while not self.__task_queue.empty():
            self.__task_queue.get_nowait()
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
                result = TaskResult(status="error", error=str(e))
        else:
            result = TaskResult(status="error", error=f"No handler found for task type '{task_type}'")

        self.logger.log(logging.DEBUG, "Task %s (%s) completed with result: {result}", task_data, task_id)
        return result

    @Manager("TaskManager")
    async def task_manager(self):
        """Manage task processing from the internal task queue.

        Continuously fetches tasks from ``task_queue``, processes them
        via ``process_task()``, and tracks running tasks for timeout
        monitoring. Respects ``max_concurrent_tasks`` to limit parallel
        execution.

        This method is decorated with ``@Manager`` and runs as an
        infinite loop managed by ``BasicAsyncWorker``.
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
                self.__running_tasks[task_id] = TaskTracker(worker_task=task, data=task_data, started=time.time())
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(self.task_manager_sleep_time)

    async def fetch_tasks(self):
        """Fetch tasks from external sources and enqueue them.

        Override this method in subclasses to implement the specific
        logic for retrieving tasks from external sources such as message
        queues, databases, or APIs, and enqueuing them via
        ``enqueue_task()`` as ``(UUID, TaskData)`` tuples.
        """

    @Manager("TaskQueueManager")
    async def task_queue_manager(self):
        """Periodically fetch tasks from external sources into the task queue.

        Calls ``fetch_tasks()`` only when the queue is not full, then
        sleeps for ``task_queue_manager_sleep_time`` to prevent busy
        waiting.

        This method is decorated with ``@Manager`` and runs as an
        infinite loop managed by ``BasicAsyncWorker``.
        """
        if not self.__task_queue.full():
            await self.fetch_tasks()
        await asyncio.sleep(self.task_queue_manager_sleep_time)

    async def watchdog(self):
        """Monitor running tasks for timeouts and handle stalled tasks.

        Periodically checks all running tasks and cancels any that have
        exceeded their configured ``timeout`` (or ``DEFAULT_TASK_TIMEOUT``
        if no timeout is set). Tasks with ``timeout_action="requeue"`` are
        returned to the external queue for potential retry.

        Override this method in subclasses to add additional watchdog
        logic. The default implementation handles task timeout detection
        and cancellation.
        """
        now = time.time()
        for task_id, task_tracker in list(self.running_tasks.items()):
            timeout = task_tracker.data.timeout.timeout
            if timeout is None:
                timeout = DEFAULT_TASK_TIMEOUT
            if 0 < timeout < (now - task_tracker.started) and not task_tracker.worker_task.done():
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
