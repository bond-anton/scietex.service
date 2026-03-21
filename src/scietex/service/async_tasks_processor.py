"""
Module providing basic asynchronous task processing worker, which can be used to construct
more advanced services. Worker provides task queue management, watchdog, and console logging.
"""

import asyncio
import logging
import time
from typing import Generic
from uuid import UUID

from .basic_async_worker import BasicAsyncWorker
from .task_handlers import TaskData, TaskHandler, TaskResult, task_type

DEFAULT_MAX_TASKS_QUEUE_SIZE = 2
"""Default maximum number of tasks queue size."""
DEFAULT_MAX_CONCURRENT_TASKS = 2
"""Default maximum number of concurrent tasks that can be processed."""

DEFAULT_TASK_TIMEOUT = 3  # Timeout in seconds for task completion
"""Timeout in seconds for task completion before cancellation."""


# pylint: disable=too-many-instance-attributes, too-many-public-methods
class AsyncTaskProcessor(BasicAsyncWorker, Generic[task_type]):
    """
    A basic asynchronous worker framework for processing tasks concurrently.

    This class provides a foundation for building async workers that can:
    - Process tasks from a queue with configurable concurrency
    - Handle graceful shutdown on signals
    - Manage logging with custom handlers
    - Monitor task timeouts and handle failures
    - Process results asynchronously

    Properties:
        service_name (str): Name of the service (read-only)
        worker_id (int): Unique identifier for this worker (read-only)
        version (str): Version string of the service (read-only)
        logger (logging.Logger): Logger instance for the worker
        logging_level (int): Current logging level (configurable)
    """

    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        queue_size: int | None = None,
        max_concurrent_tasks: int | None = None,
        **kwargs,
    ):
        """
        Initialize the AsyncTaskProcessor.

        Args:
            service_name: Name of the service, used for logging and identification
            version: Version string of the service
            queue_size: Queue size as integer
            max_concurrent_tasks: Maximum number of concurrent tasks
            **kwargs: Additional keyword arguments including:
                conf_dir: Directory to use for configuration files
                worker_id: Unique identifier for this worker instance
                logging_level: Logging level as string or integer
        """
        super().__init__(service_name, version, **kwargs)

        self._task_handlers_map: dict[task_type, TaskHandler] = {}

        # Initialize queues and tracking structures
        self.running_tasks: dict[
            UUID, tuple[asyncio.Task, TaskData, int | float]
        ] = {}  # Track running tasks and their start times
        self.queue_size: int = (
            queue_size if queue_size is not None else DEFAULT_MAX_TASKS_QUEUE_SIZE
        )
        self.max_concurrent_tasks: int = (
            max_concurrent_tasks
            if max_concurrent_tasks is not None
            else DEFAULT_MAX_CONCURRENT_TASKS
        )
        self.task_queue: asyncio.Queue[tuple[UUID, TaskData]] = asyncio.Queue(
            maxsize=self.queue_size
        )

    def register_task_handler(self, task: task_type, handler_class: type[TaskHandler]) -> None:
        """Register a task handler for a specific task type."""
        handler_instance = handler_class(self)
        self._task_handlers_map[task] = handler_instance

    def unregister_task_handler(self, task: task_type) -> None:
        """
        Unregister a task handler for a specific task type.

        Args:
            task_type: The type of task for which to unregister the handler
        """
        if task in self._task_handlers_map:
            # Perform cleanup before removal
            asyncio.create_task(self._task_handlers_map[task].cleanup())
            del self._task_handlers_map[task]

    def _find_task_handler(self, task: str) -> TaskHandler | None:
        """
        Finds a handler for the specified task type.

        Args:
            task_type: The type of task

        Returns:
            An instance of the handler or None if not found
        """
        for _, handler in self._task_handlers_map.items():
            if handler.supports(task):
                return handler
        return None

    async def initialize(self) -> bool:
        """
        Perform any initialization before starting the main loop.

        This method is intended to be overridden by subclasses to perform
        service-specific initialization such as database connections,
        API client setup, or other preparatory work.
        """
        if self.initialized:
            await self.log("Already initialized", level=logging.DEBUG)
            return True

        if not await super().initialize():
            return False

        # Start managers
        self.managers_tasks += [
            asyncio.create_task(self.task_manager()),
            asyncio.create_task(self.task_queue_manager()),
            asyncio.create_task(self.watchdog()),
        ]

        await self.log("Task manager started", level=logging.DEBUG)
        await self.log("Task queue manager started", level=logging.DEBUG)
        await self.log("Watchdog started", level=logging.DEBUG)

        return True

    async def return_task_to_queue(self, task_id: UUID, task_data: TaskData) -> None:
        """
        Return a task to the external queue.

        This method should be overridden by subclasses to implement
        the specific logic for returning tasks to their source queue
        when they cannot be processed or need to be retried.

        Args:
            task_id (UUID): The task id
            task_data (dict[str, Any]): The task data to return to the external queue
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
        for task_id, (task, task_data, _) in list(self.running_tasks.items()):
            if not task.done():
                task.cancel()
                if task_data.canceled_action == "requeue":
                    await self.log(
                        f"Task {task_id} will be returned to queue.",
                        logging.WARNING,
                    )
                    await self.return_task_to_queue(task_id, task_data)
                try:
                    await asyncio.wait_for(task, timeout=5)  # Wait for cancellation to complete
                except asyncio.CancelledError:
                    pass
        self.logger.debug("All tasks cancelled")

        # Cleanup task handlers
        task_handlers_cleanup_tasks = [
            handler.cleanup() for handler in self._task_handlers_map.values()
        ]
        if task_handlers_cleanup_tasks:
            await asyncio.gather(*task_handlers_cleanup_tasks, return_exceptions=True)
        self._task_handlers_map.clear()
        self.logger.debug("All task handlers cleaned up")

    async def process_task(self, task_id: UUID, task_data: TaskData) -> TaskResult:
        """
        Process a single task.

        Args:
            task_id: Identifier of the task to process
            task_data: The data associated with the task
        """
        await self.log(f"Processing task {task_id}: {task_data}", level=logging.DEBUG)

        task_type = task_data.task
        if not task_type:
            await self.log(f"Wrong task format for {task_id}: {task_data}", level=logging.ERROR)
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

        await self.log(f"Task {task_id} completed with result: {result}", level=logging.DEBUG)
        return result

    async def task_manager(self):
        """
        Manage task processing from the task queue.

        Continuously takes tasks from the task queue, processes them,
        and puts results in the results queue. Tracks running tasks
        and their start times for timeout monitoring.
        """

        async def handle_task(t_id: UUID, t_data: TaskData):
            try:
                await self.log(f"Sending to handler. Task {t_id}: {t_data}", level=logging.INFO)
                await self.process_task(t_id, t_data)
            finally:
                self.task_queue.task_done()

        while not self._stop_event.is_set():
            if len(self.running_tasks) < self.max_concurrent_tasks:
                try:
                    task_id, task_data = await asyncio.wait_for(self.task_queue.get(), timeout=1)
                    task = asyncio.create_task(handle_task(task_id, task_data))
                    self.running_tasks[task_id] = (
                        task,
                        task_data,
                        time.time(),
                    )  # Track start time
                    task.add_done_callback(lambda t, _id=task_id: self.running_tasks.pop(_id, None))
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(1)

    async def fetch_tasks(self):
        """
        Fetch tasks from external sources and add them to the task queue.

        This method should be overridden by subclasses to implement
        the specific logic for retrieving tasks from external sources
        such as message queues, databases, or APIs.
        """

    async def task_queue_manager(self):
        """
        Main loop that fetches tasks periodically.

        Continuously calls fetch_tasks() with a small delay between
        calls to prevent busy waiting. Runs until the stop event is set.
        """
        while not self._stop_event.is_set():
            if not self.task_queue.full():
                await self.fetch_tasks()
            await asyncio.sleep(0.05)

    async def watchdog(self):
        """
        Monitor running tasks for timeouts and handle stalled tasks.

        Periodically checks all running tasks and cancels any that have
        exceeded the DEFAULT_TASK_TIMEOUT. Returns cancelled tasks to the external
        queue for potential retry.
        """
        while not self._stop_event.is_set():
            now = time.time()
            for task_id, (task, task_data, start_time) in list(self.running_tasks.items()):
                timeout = task_data.timeout.timeout
                if timeout is None:
                    timeout = DEFAULT_TASK_TIMEOUT
                if 0 < timeout < (now - start_time) and not task.done():
                    await self.log(
                        f"Task {task_id} exceeded timeout and will be canceled.",
                        logging.WARNING,
                    )
                    task.cancel()
                    if task_data.timeout.timeout_action == "requeue":
                        await self.log(
                            f"Task {task_id} will be returned to queue.",
                            logging.WARNING,
                        )
                        await self.return_task_to_queue(task_id, task_data)
                        self.running_tasks.pop(task_id, None)
                    try:
                        await task  # Wait for cancellation to complete
                    except asyncio.CancelledError:
                        pass
            await asyncio.sleep(1)  # Check for timeouts every 1 second
