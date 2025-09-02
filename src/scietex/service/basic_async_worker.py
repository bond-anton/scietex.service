"""
Module providing basic asynchronous worker, which can be used to construct more advanced services.
Worker provides task queue management, watchdog, and console logging.
"""

from typing import Union, Optional, Tuple, Mapping, Any
import asyncio
import signal
import time
import logging
from pathlib import Path

from scietex.logging import AsyncBaseHandler

from .version import __version__
from .logo import LOGO

DEFAULT_LOGGING_LEVEL: int = logging.DEBUG
"""Default logging level for the worker if no valid level is provided."""

DEFAULT_MAX_TASKS_QUEUE_SIZE = 2
"""Default maximum number of tasks queue size."""
DEFAULT_MAX_CONCURRENT_TASKS = 2
"""Default maximum number of concurrent tasks that can be processed."""

TASK_TIMEOUT = 3  # Timeout in seconds for task completion
"""Timeout in seconds for task completion before cancellation."""

CONF_PATHS = [
    Path.home() / ".config" / "scietex",
    Path("/etc") / "scietex",
    Path("/usr/local/etc") / "scietex",
    Path().cwd() / "config",
]


# pylint: disable=too-many-instance-attributes, too-many-public-methods
class BasicAsyncWorker:
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
        **kwargs,
    ):
        """
        Initialize the BasicAsyncWorker.

        Args:
            service_name: Name of the service, used for logging and identification
            version: Version string of the service
            logging_level: Logging level as string or integer
            **kwargs: Additional keyword arguments including:
                conf_dir: Directory to use for configuration files
                worker_id: Unique identifier for this worker instance
                queue_size: Queue size as integer
                max_concurrent_tasks: Maximum number of concurrent tasks

        Note:
            If logging_level is invalid, defaults to DEFAULT_LOGGING_LEVEL
        """
        self.__service_name: str = service_name
        self.__worker_id: int = kwargs.get("worker_id", 1)
        self.__version: str = version
        self.__logging_level: int = DEFAULT_LOGGING_LEVEL

        # Configure logging level from kwargs if provided
        if "logging_level" in kwargs:
            try:
                if not isinstance(logging.getLevelName(kwargs["logging_level"]), int):
                    self.__logging_level = DEFAULT_LOGGING_LEVEL
                else:
                    self.__logging_level = logging.getLevelName(kwargs["logging_level"])
            except (TypeError, ValueError):
                pass

        # Config dir setup
        self.conf_dir: Optional[Path] = None

        if "conf_dir" in kwargs and isinstance(kwargs["conf_dir"], (str, Path)):
            conf_dir_path = Path(kwargs["conf_dir"])
            if conf_dir_path.is_dir():
                self.conf_dir = conf_dir_path
        if self.conf_dir is None:
            for conf_dir_path in CONF_PATHS:
                if conf_dir_path.is_dir():
                    self.conf_dir = conf_dir_path
                    break
        if self.conf_dir is None:
            self.conf_dir = CONF_PATHS[0]
            self.conf_dir.mkdir(exist_ok=True)

        # Set up logger with async handler
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._logger.setLevel(self.logging_level)
        stdout_handler = AsyncBaseHandler(
            service_name=self.__service_name, worker_id=self.__worker_id
        )
        stdout_handler.setLevel(self.logging_level)
        self._logger.addHandler(stdout_handler)

        # Initialize queues and tracking structures
        self.log_queue: asyncio.Queue[Tuple[int, str]] = asyncio.Queue()
        self.running_tasks: dict = {}  # Track running tasks and their start times
        self.running_result_processors: dict = (
            {}
        )  # Track running result processors and their start times
        self.queue_size: int = kwargs.get("queue_size", DEFAULT_MAX_TASKS_QUEUE_SIZE)
        self.max_concurrent_tasks: int = kwargs.get(
            "max_concurrent_tasks", DEFAULT_MAX_CONCURRENT_TASKS
        )
        self.task_queue: asyncio.Queue[Tuple[Union[int, str], Mapping[str, Any]]] = (
            asyncio.Queue(maxsize=self.queue_size)
        )
        self.results_queue: asyncio.Queue[Tuple[Union[int, str], Mapping[str, Any]]] = (
            asyncio.Queue()
        )
        self._stop_event: asyncio.Event = asyncio.Event()
        self._completion_event: asyncio.Event = asyncio.Event()
        self.message_handler_task = None
        self.managers_tasks: list = []

    @property
    def service_name(self) -> str:
        """Service name string (read-only)."""
        return self.__service_name

    @property
    def worker_id(self) -> int:
        """Worker id number (read-only)."""
        return self.__worker_id

    @property
    def version(self) -> str:
        """Service version string (read-only)."""
        return self.__version

    @property
    def logger(self) -> logging.Logger:
        """Service logger instance."""
        return self._logger

    @property
    def logging_level(self) -> int:
        """Current logging level for the service."""
        return self.__logging_level

    @logging_level.setter
    def logging_level(self, level: Union[int, str]) -> None:
        """
        Set the logging level for the worker.

        Args:
            level: Logging level as string or integer. Supported string values:
                - DEBUG: 'D', 'DBG', 'DEBUG', logging.DEBUG
                - INFO: 'I', 'INF', 'INFO', 'INFORMATION', logging.INFO
                - WARNING: 'W', 'WRN', 'WARN', 'WARNING', logging.WARNING
                - ERROR: 'E', 'ERR', 'ERROR', logging.ERROR
                - CRITICAL: 'C', 'CRT', 'CRIT', 'CRITICAL', logging.CRITICAL
                - FATAL: 'F', 'FTL', 'FAT', 'FATAL', logging.FATAL

        Note:
            If level is not recognized, defaults to DEFAULT_LOGGING_LEVEL
        """
        if level in ("D", "DBG", "DEBUG", logging.DEBUG):
            self.__logging_level = logging.DEBUG
        elif level in ("I", "INF", "INFO", "INFORMATION", logging.INFO):
            self.__logging_level = logging.INFO
        elif level in ("W", "WRN", "WARN", "WARNING", logging.WARNING):
            self.__logging_level = logging.WARNING
        elif level in ("E", "ERR", "ERROR", logging.ERROR):
            self.__logging_level = logging.ERROR
        elif level in ("C", "CRT", "CRIT", "CRITICAL", logging.CRITICAL):
            self.__logging_level = logging.CRITICAL
        elif level in ("F", "FTL", "FAT", "FATAL", logging.FATAL):
            self.__logging_level = logging.FATAL
        else:
            self.__logging_level = DEFAULT_LOGGING_LEVEL

        # Update logger and all handlers
        self.logger.setLevel(self.__logging_level)
        for handler in self.logger.handlers:
            handler.setLevel(self.__logging_level)
        self.logger.debug(
            "Logging level set to %s", logging.getLevelName(self.logging_level)
        )

    async def logger_add_custom_handlers(self) -> None:
        """
        Override this method to add custom handlers to logger.

        This method is intended to be overridden by subclasses to add
        additional logging handlers beyond the default AsyncBaseHandler.
        """

    async def _logger_init_handlers(self) -> None:
        """Initialize all async logging handlers."""
        await self.logger_add_custom_handlers()
        for handler in self.logger.handlers:
            if isinstance(handler, AsyncBaseHandler):
                await handler.start_logging()

    async def _logger_shut_down_handlers(self) -> None:
        """Cleanly shut down all async logging handlers."""
        for handler in self.logger.handlers:
            if isinstance(handler, AsyncBaseHandler):
                try:
                    await handler.stop_logging()
                except Exception as e:  # pylint: disable=broad-exception-caught
                    print("Failed to shut down logging handler", handler)
                    print(e)

    async def initialize(self) -> bool:
        """
        Perform any initialization before starting the main loop.

        This method is intended to be overridden by subclasses to perform
        service-specific initialization such as database connections,
        API client setup, or other preparatory work.
        """
        await self._logger_init_handlers()
        return True

    async def start(self):
        """
        Start the worker and all its components.

        This method:
        1. Sets up signal handlers for graceful shutdown
        2. Initializes logging handlers
        3. Performs custom initialization
        4. Starts all manager tasks
        5. Begins listening for control messages

        Raises:
            RuntimeError: If the worker fails to start properly
        """
        print(
            LOGO.format(
                service_name=self.service_name,
                version=self.version,
                scietex_version=__version__,
            )
        )
        self.setup_signal_handlers()
        if not await self.initialize():
            raise RuntimeError("Initialization failed")

        # Start control messages listener
        self.message_handler_task = asyncio.create_task(
            self.listen_for_control_messages()
        )

        # Main tasks
        self.managers_tasks = [
            asyncio.create_task(self.logging_manager()),
            asyncio.create_task(self.task_manager()),
            asyncio.create_task(self.task_fetcher()),
            asyncio.create_task(self.results_manager()),
            asyncio.create_task(self.watchdog()),
        ]
        await self.log("LOG Message from start", level=logging.DEBUG)

    async def return_task_to_queue(
        self, task_id: Union[int, str], task_data: Mapping[str, Any]
    ) -> None:
        """
        Return a task to the external queue.

        This method should be overridden by subclasses to implement
        the specific logic for returning tasks to their source queue
        when they cannot be processed or need to be retried.

        Args:
            task_id (Union[int, str]): The task id
            task_data (Mapping[str, Any]): The task data to return to the external queue
        """

    async def process_result(
        self, task_id: Union[int, str], result: Mapping[str, Any]
    ) -> None:
        """
        Process a completed task result.

        This method should be overridden by subclasses to implement
        result processing logic such as storing results, sending
        notifications, or updating status.

        Args:
            task_id: Identifier of the completed task
            result: The result data from the completed task
        """

    async def stop(self):
        """
        Stop the worker gracefully.

        This method:
        1. Sets the stop event to signal all tasks to stop
        2. Returns all queued tasks to the external queue
        3. Cancels and returns to queue running tasks
        4. Processes remaining results
        5. Waits for all tasks to complete
        6. Processes remaining log messages
        7. Shuts down logging handlers
        8. Performs cleanup

        Note:
            This method is automatically called on SIGINT or SIGTERM
        """
        self.logger.debug("Stopping worker gracefully...")
        self._stop_event.set()

        if self.message_handler_task is not None:
            self.message_handler_task.cancel()
            self.message_handler_task = None

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
                await self.return_task_to_queue(task_id, task_data)
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self.logger.debug("All tasks cancelled")

        # Process remaining results
        while not self.results_queue.empty():
            task_id, result = await self.results_queue.get()
            await self.process_result(task_id, result)
            self.results_queue.task_done()
        self.logger.debug("Results queue is empty")

        await asyncio.gather(*self.managers_tasks, return_exceptions=True)
        self.logger.debug("Managers tasks finished")

        # Process remaining logs
        while not self.log_queue.empty():
            level, message = await self.log_queue.get()
            self.logger.log(level, message)
            self.log_queue.task_done()
        self.logger.debug("Log queue is empty")

        # await self.log("Worker stopped.", logging.DEBUG)
        await self.cleanup()
        await self._logger_shut_down_handlers()
        self._completion_event.set()

    async def cleanup(self):
        """
        Cleanup everything before exit.

        This method is intended to be overridden by subclasses to perform
        service-specific cleanup such as closing database connections,
        releasing resources, or sending final status updates.
        """

    def setup_signal_handlers(self):
        """
        Set up signal handlers for graceful shutdown.

        Registers handlers for SIGINT and SIGTERM signals that will
        trigger a graceful shutdown of the worker.
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda _: asyncio.create_task(self.stop()), (sig,)
            )

    async def logging_manager(self):
        """
        Manage log message processing from the log queue.

        Continuously processes log messages from the log queue and
        passes them to the logger. Runs until the stop event is set.
        """
        while not self._stop_event.is_set():
            # Wait for a message to arrive in the queue
            try:
                level, message = await asyncio.wait_for(self.log_queue.get(), timeout=1)
                self.logger.log(level, message)
                self.log_queue.task_done()
            except TimeoutError:
                pass

    async def log(self, message: str, level: int = logging.INFO):
        """
        Add a log message to the log queue for processing.

        Args:
            message: The log message text
            level: The logging level (default: INFO)
        """
        await self.log_queue.put((level, message))

    async def process_task(
        self, task_id: Union[int, str], task_data: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """
        Process a single task.

        This method should be overridden by subclasses to implement
        the actual task processing logic.

        Args:
            task_id: Identifier of the task to process
            task_data: The data associated with the task

        Returns:
            The result of processing the task

        Note:
            This is a placeholder implementation that sleeps for 1 second
            and returns a mock result. Subclasses should override this.
        """
        await self.log(f"Processing task {task_id}: {task_data}", level=logging.DEBUG)
        await asyncio.sleep(1)
        result: Mapping[str, Any] = {"data": f"Result of {task_id}"}
        await self.log(
            f"Task {task_id} completed with result: {result}", level=logging.DEBUG
        )
        return result

    async def task_manager(self):
        """
        Manage task processing from the task queue.

        Continuously takes tasks from the task queue, processes them,
        and puts results in the results queue. Tracks running tasks
        and their start times for timeout monitoring.
        """

        async def handle_task(t_id: Union[int, str], t_data: Mapping[str, Any]):
            try:
                await self.log(
                    f"Sending to handler. Task {t_id}: {t_data}", level=logging.INFO
                )
                result = await self.process_task(t_id, t_data)
                await self.results_queue.put((t_id, result))
            finally:
                self.task_queue.task_done()

        while not self._stop_event.is_set():
            if len(self.running_tasks) < self.max_concurrent_tasks:
                try:
                    task_id, task_data = await asyncio.wait_for(
                        self.task_queue.get(), timeout=1
                    )
                    task = asyncio.create_task(handle_task(task_id, task_data))
                    self.running_tasks[task_id] = (
                        task,
                        task_data,
                        time.time(),
                    )  # Track start time
                    task.add_done_callback(
                        lambda t: self.running_tasks.pop(task_id, None)
                    )
                except TimeoutError:
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

    async def task_fetcher(self):
        """
        Main loop that fetches tasks periodically.

        Continuously calls fetch_tasks() with a small delay between
        calls to prevent busy waiting. Runs until the stop event is set.
        """
        while not self._stop_event.is_set():
            await self.fetch_tasks()
            await asyncio.sleep(0.1)

    async def results_manager(self):
        """
        Manage result processing from the results queue.

        Continuously takes results from the results queue and processes
        them. Runs until the stop event is set.
        """

        async def handle_result(t_id: Union[int, str], r_data: Mapping[str, Any]):
            try:
                await self.log(
                    f"Processing results of task {t_id}: {r_data}", level=logging.INFO
                )
                await self.process_result(t_id, r_data)
            finally:
                self.results_queue.task_done()

        while not self._stop_event.is_set():
            try:
                task_id, result = await asyncio.wait_for(
                    self.results_queue.get(), timeout=1
                )
                r_task = asyncio.create_task(handle_result(task_id, result))
                self.running_result_processors[task_id] = (
                    r_task,
                    result,
                    time.time(),
                )  # Track start time
                r_task.add_done_callback(
                    lambda t: self.running_result_processors.pop(task_id, None)
                )
            except TimeoutError:
                pass

    async def listen_for_control_messages(self):
        """
        Listen for control messages to manage worker behavior.

        This method should be overridden by subclasses to implement
        control message handling for dynamic configuration changes,
        priority adjustments, or other runtime controls.
        """
        while not self._stop_event.is_set():
            await asyncio.sleep(5)

    async def watchdog(self):
        """
        Monitor running tasks for timeouts and handle stalled tasks.

        Periodically checks all running tasks and cancels any that have
        exceeded the TASK_TIMEOUT. Returns cancelled tasks to the external
        queue for potential retry.
        """
        while not self._stop_event.is_set():
            now = time.time()
            for task_id, (task, task_data, start_time) in list(
                self.running_tasks.items()
            ):
                timeout = TASK_TIMEOUT
                if "timeout" in task_data:
                    timeout = task_data["timeout"]
                if 0 < timeout < (now - start_time) and not task.done():
                    await self.log(
                        f"Task {task_id} exceeded timeout. Cancelling and returning to queue.",
                        logging.WARNING,
                    )
                    task.cancel()
                    await self.return_task_to_queue(task_id, task_data)
                    self.running_tasks.pop(task_id, None)
                    try:
                        await task  # Wait for cancellation to complete
                    except asyncio.CancelledError:
                        pass
                    # self.task_queue.task_done()
            await asyncio.sleep(5)  # Check for timeouts every 5 seconds

    async def run(self):
        """
        Run the worker indefinitely.

        Starts the worker and waits indefinitely until stopped by
        a signal or external event. This is the main entry point
        for running the worker.
        """
        await self.start()
        await self._completion_event.wait()
