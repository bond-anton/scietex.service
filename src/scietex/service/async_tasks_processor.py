"""
Asynchronous task processing worker for ``scietex.service``.

Provides ``AsyncTaskProcessor``, a concurrent task processing framework
built on ``BasicAsyncWorker`` with task queue management, timeout
monitoring (watchdog), handler dispatch, and graceful shutdown support.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Generic
from uuid import UUID

from .basic_async_worker import BasicAsyncWorker
from .manager import Manager
from .task_handler import TaskData, TaskHandler, TaskResult, TaskTracker, task_type

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


class AsyncTaskProcessor(BasicAsyncWorker, Generic[task_type]):
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

        self.__task_handlers_map: dict[task_type, TaskHandler] = {}

        # Initialize queues and tracking structures
        self.__running_tasks: dict[UUID, TaskTracker] = {}  # Track running tasks
        self.__queue_size: int = (
            queue_size if queue_size is not None else DEFAULT_MAX_TASKS_QUEUE_SIZE
        )
        self.__max_concurrent_tasks: int = max(
            1, max_concurrent_tasks or DEFAULT_MAX_CONCURRENT_TASKS
        )

        self.__task_queue: asyncio.Queue[tuple[UUID, TaskData]] = asyncio.Queue(
            maxsize=self.queue_size
        )

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

    @property
    def task_handlers_map(self) -> dict[task_type, TaskHandler]:
        """Mapping of registered task types to their handler instances."""
        return self.__task_handlers_map

    @property
    def running_tasks(self) -> dict[UUID, TaskTracker]:
        """Dictionary of currently running tasks and their trackers."""
        return self.__running_tasks

    @property
    def queue_size(self) -> int:
        """Maximum size of the internal task queue."""
        return self.__queue_size

    @property
    def max_concurrent_tasks(self) -> int:
        """Maximum number of tasks that can be processed concurrently."""
        return self.__max_concurrent_tasks

    @property
    def task_queue(self) -> asyncio.Queue[tuple[UUID, TaskData]]:
        """The internal async queue holding pending tasks."""
        return self.__task_queue

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

    def add_task_handler(self, task: task_type, handler_class: type[TaskHandler]) -> None:
        """Add a task handler for a specific task type."""
        handler_instance = handler_class(self)
        self.__task_handlers_map[task] = handler_instance

    def remove_task_handler(self, task: task_type) -> None:
        """
        Remove a task handler for a specific task type.

        Args:
            task_type: The type of task for which to unregister the handler
        """
        if task in self.__task_handlers_map:
            # Perform cleanup before removal
            asyncio.create_task(self.__task_handlers_map[task].cleanup())
            del self.__task_handlers_map[task]

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
        for _, handler in self.task_handlers_map.items():
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

    async def cleanup(self):
        """
        Cleanup everything before exit.

        This method is intended to be overridden by subclasses to perform
        service-specific cleanup such as closing database connections,
        releasing resources, or sending final status updates.
        """
        await super().cleanup()
        # Return tasks in worker queue to external queue
        while not self.task_queue.empty():
            task_id, task_data = await self.task_queue.get()
            await self.return_task_to_queue(task_id, task_data)
            self.task_queue.task_done()
        self.logger.debug("Task queue is empty")

        # Cancel and requeue running tasks
        for task_id, task_tracker in list(self.running_tasks.items()):
            if not task_tracker.worker_task.done():
                task_tracker.worker_task.cancel()
                if task_tracker.data.canceled_action == "requeue":
                    self.logger.log(logging.WARNING, "Task %s will be returned to queue.", task_id)
                    await self.return_task_to_queue(task_id, task_tracker.data)
                try:
                    await asyncio.wait_for(
                        task_tracker.worker_task, timeout=WORKER_TASK_CANCELLATION_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    self.logger.log(logging.ERROR, "Timeout canceling Task %s.", task_id)
                except asyncio.CancelledError:
                    pass
        self.logger.debug("All tasks cancelled")

        # Cleanup task handlers
        task_handlers_cleanup_tasks = [
            handler.cleanup() for handler in self.task_handlers_map.values()
        ]
        if task_handlers_cleanup_tasks:
            await asyncio.gather(*task_handlers_cleanup_tasks, return_exceptions=True)
        self.task_handlers_map.clear()
        self.logger.debug("All task handlers cleaned up")

    async def process_task(self, task_id: UUID, task_data: TaskData) -> TaskResult:
        """
        Process a single task.

        Args:
            task_id: Identifier of the task to process
            task_data: The data associated with the task
        """
        self.logger.log(
            logging.DEBUG, "Processing task %s (%s): %s", task_data.task, task_id, task_data
        )

        task_type = task_data.task
        if not task_type:
            self.logger.log(
                logging.ERROR,
                "Wrong task format for %s (%s): %s",
                task_data.task,
                task_id,
                task_data,
            )
            raise ValueError("Task data must contain 'task' field")

        handler = self._find_task_handler(task_type)
        if handler and handler.is_ready:
            try:
                result = await handler.handle(task_data)
            except Exception as e:
                result = TaskResult(status="error", error=str(e))
        else:
            result = TaskResult(
                status="error", error=f"No handler found for task type '{task_type}'"
            )

        self.logger.log(
            logging.DEBUG, "Task %s (%s) completed with result: {result}", task_data, task_id
        )
        return result

    @Manager("TaskManager")
    async def task_manager(self):
        """
        Manage task processing from the task queue.

        Continuously takes tasks from the task queue, processes them,
        and puts results in the results queue. Tracks running tasks
        and their start times for timeout monitoring.
        """

        async def handle_task(t_id: UUID, t_data: TaskData):
            try:
                await self.process_task(t_id, t_data)
            finally:
                self.running_tasks.pop(t_id, None)
                self.task_queue.task_done()

        if len(self.running_tasks) < self.max_concurrent_tasks:
            try:
                task_id, task_data = await asyncio.wait_for(
                    self.task_queue.get(), timeout=TASK_QUEUE_FETCH_TIMEOUT
                )
                task = asyncio.create_task(handle_task(task_id, task_data))
                self.__running_tasks[task_id] = TaskTracker(
                    worker_task=task, data=task_data, started=time.time()
                )
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(self.task_manager_sleep_time)

    async def fetch_tasks(self):
        """Fetch tasks from external sources and enqueue them.

        Subclasses should override this method to implement the specific
        logic for retrieving tasks from external sources such as message
        queues, databases, or APIs, and putting them into
        ``self.task_queue``.
        """

    @Manager("TaskQueueManager")
    async def task_queue_manager(self):
        """
        Task queue manager fetches tasks periodically.

        Continuously calls fetch_tasks() with a small delay between
        calls to prevent busy waiting.
        """
        if not self.task_queue.full():
            await self.fetch_tasks()
        await asyncio.sleep(self.task_queue_manager_sleep_time)

    async def watchdog(self):
        """
        Monitor running tasks for timeouts and handle stalled tasks.

        Periodically checks all running tasks and cancels any that have
        exceeded the DEFAULT_TASK_TIMEOUT. Returns cancelled tasks to the external
        queue for potential retry.
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
                if task_tracker.data.timeout.timeout_action == "requeue":
                    self.logger.log(
                        logging.WARNING,
                        "Task %s (%s) will be returned to queue.",
                        task_tracker.data.task,
                        task_id,
                    )
                    await self.return_task_to_queue(task_id, task_tracker.data)
                    self.running_tasks.pop(task_id, None)
                try:
                    await asyncio.wait_for(
                        task_tracker.worker_task, timeout=WORKER_TASK_CANCELLATION_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    self.logger.log(logging.ERROR, "Timeout canceling Task %s.", task_id)
                except asyncio.CancelledError:
                    pass
