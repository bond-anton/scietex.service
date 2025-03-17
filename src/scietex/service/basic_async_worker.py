from typing import Union
import asyncio
import signal
import time
import logging

from nts.service.logging.console import CustomConsoleFormatter

DEFAULT_LOGGING_LEVEL: int = logging.DEBUG

DEFAULT_MAX_CONCURRENT_TASKS = 5
TASK_TIMEOUT = 10  # Timeout in seconds for task completion


class BasicAsyncWorker:
    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        delay: float = 5,
        **kwargs,
    ):

        self.__service_name: str = service_name
        self.__worker_id: int = kwargs.get("worker_id", 1)
        self.__version: str = version
        self.__delay: float = delay
        self.__logging_level: int = DEFAULT_LOGGING_LEVEL
        if "logging_level" in kwargs:
            try:
                if not isinstance(logging.getLevelName(kwargs["logging_level"]), int):
                    self.__logging_level = DEFAULT_LOGGING_LEVEL
                else:
                    self.__logging_level = logging.getLevelName(kwargs["logging_level"])
            except (TypeError, ValueError):
                pass
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._logger.setLevel(self.logging_level)
        stdout_handler = logging.StreamHandler()
        stdout_handler.setLevel(self.logging_level)
        formatter = CustomConsoleFormatter(
            service_name=self.service_name, worker_id=self.worker_id
        )
        stdout_handler.setFormatter(formatter)
        self._logger.addHandler(stdout_handler)

        self.log_queue = asyncio.Queue()

        self.running_tasks = {}  # Track running tasks and their start times
        self.max_queue_size: int = DEFAULT_MAX_CONCURRENT_TASKS
        self.task_queue = asyncio.Queue(maxsize=self.max_queue_size)
        self.results_queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self.tasks = []

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
        """Service logger."""
        return self._logger

    @property
    def logging_level(self) -> int:
        """Service log level."""
        return self.__logging_level

    @logging_level.setter
    def logging_level(self, level: Union[int, str]) -> None:
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
        self.logger.setLevel(self.__logging_level)
        for handler in self.logger.handlers:
            handler.setLevel(self.__logging_level)
        self.logger.debug(
            "Logging level set to %s", logging.getLevelName(self.logging_level)
        )

    async def _logger_add_custom_handlers(self) -> None:
        """Override this method to add custom handlers to logger."""

    async def initialize(self):
        """Any initialization before starting the main loop is done here."""

    async def start(self):
        self.setup_signal_handlers()
        await self._logger_add_custom_handlers()
        await self.initialize()

        # Start control messages listener
        asyncio.create_task(self.listen_for_control_messages())

        # Main tasks
        self.tasks = [
            asyncio.create_task(self.task_manager()),
            asyncio.create_task(self.worker()),
            asyncio.create_task(self.results_manager()),
            asyncio.create_task(self.watchdog()),
        ]

    async def return_task_to_queue(self, task_dta):
        pass

    async def process_result(self, task_id, result):
        pass

    async def stop(self):
        self.logger.debug("Stopping worker gracefully...")
        self._stop_event.set()

        # Return tasks in worker queue to external queue
        while not self.task_queue.empty():
            task_id, task_data = await self.task_queue.get()
            await self.return_task_to_queue(task_data)
            self.task_queue.task_done()

        # Cancel and requeue running tasks
        for task_id, (task, task_data, _) in list(self.running_tasks.items()):
            if not task.done():
                task.cancel()
                await self.return_task_to_queue(task_data)
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Process remaining results
        while not self.results_queue.empty():
            task_id, result = await self.results_queue.get()
            await self.process_result(task_id, result)
            self.results_queue.task_done()

        await asyncio.gather(*self.tasks, return_exceptions=True)

        # Process remaining logs
        while not self.log_queue.empty():
            level, message = await self.log_queue.get()
            self.logger.log(level, message)
            self.log_queue.task_done()

        await self.log("Worker stopped.", logging.DEBUG)
        await self.cleanup()

    async def cleanup(self):
        """Cleanup everything before exit."""

    def setup_signal_handlers(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

    async def logging_manager(self):
        while not self._stop_event.is_set():
            # Wait for a message to arrive in the queue
            level, message = await self.log_queue.get()
            self.logger.log(level, message)
            self.log_queue.task_done()

    async def log(self, message: str, level: int = logging.INFO):
        await self.log_queue.put((level, message))

    async def process_task(self, task_id, task_data):
        self.logger.debug(f"Processing task {task_id}: {task_data}")
        await asyncio.sleep(1)
        result = f"Result of {task_id}"
        self.logger.debug(f"Task {task_id} completed with result: {result}")
        return result

    async def task_manager(self):
        while not self._stop_event.is_set():
            task_id, task_data = await self.task_queue.get()

            async def handle_task(task_id, task_data):
                try:
                    result = await self.process_task(task_id, task_data)
                    await self.results_queue.put((task_id, result))
                finally:
                    self.task_queue.task_done()

            task = asyncio.create_task(handle_task(task_id, task_data))
            self.running_tasks[task_id] = (
                task,
                task_data,
                time.time(),
            )  # Track start time
            task.add_done_callback(lambda t: self.running_tasks.pop(task_id, None))

    async def fetch_tasks(self):
        pass

    async def worker(self):
        while not self._stop_event.is_set():
            await self.fetch_tasks()
            await asyncio.sleep(0.1)

    async def results_manager(self):
        while not self._stop_event.is_set():
            task_id, result = await self.results_queue.get()
            if "Result" in result:
                await self.process_result(task_id, result)
            self.results_queue.task_done()

    async def listen_for_control_messages(self):
        while not self._stop_event.is_set():
            await asyncio.sleep(5)

    async def watchdog(self):
        """Periodically check for tasks that exceed the timeout."""
        while not self._stop_event.is_set():
            now = time.time()
            for task_id, (task, task_data, start_time) in list(
                self.running_tasks.items()
            ):
                if not task.done() and (now - start_time) > TASK_TIMEOUT:
                    print(f"Task {task_id} exceeded timeout. Cancelling and requeuing.")
                    task.cancel()
                    await self.return_task_to_queue(task_data)
                    self.running_tasks.pop(task_id, None)
                    try:
                        await task  # Wait for cancellation to complete
                    except asyncio.CancelledError:
                        pass
            await asyncio.sleep(5)  # Check for timeouts every 5 seconds

    async def run(self):
        await self.start()
        await asyncio.Event().wait()
