"""
Module providing basic asynchronous worker, which can be used to construct more advanced services.
Worker provides console logging, signal handlers, etc.
"""

import asyncio
import logging
import signal
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from pathlib import Path

from scietex.logging import AsyncBaseHandler

from .utils import parse_logging_level, prepare_conf_dir, print_scietex_logo

DEFAULT_HEARTBEAT_INTERVAL: int = 10
MIN_HEARTBEAT_INTERVAL: int = 1
MAX_HEARTBEAT_INTERVAL: int = 600


class BasicAsyncWorker:
    """
    A basic asynchronous worker framework.

    This class provides a foundation for building async workers that can:
    - Handle graceful shutdown on signals
    - Manage logging with custom handlers

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
        worker_id: int = 1,
        conf_dir: str | Path | None = None,
        logging_level: int | str = logging.DEBUG,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    ):
        """
        Initialize the BasicAsyncWorker.

        Args:
            service_name: Name of the service, used for logging and identification
            version: Version string of the service
            worker_id: Unique identifier for this worker instance
            conf_dir: Directory to use for configuration files
            logging_level: Logging level as string or integer.
                If logging_level is invalid, defaults to DEFAULT_LOGGING_LEVEL
            heartbeat_interval: Heartbeat interval in seconds as integer

        Note:
            In a single process service_name, worker_id combination should be unique.
            This ensures separate logger names for different workers.
        """
        self.__service_name: str = service_name
        self.__worker_id: int = worker_id
        self.__version: str = version
        self.__logging_level: int = parse_logging_level(logging_level)
        self.__initialized: bool = False
        self.__start_time: datetime | None = None

        self.__heartbeat_interval: int = max(
            MIN_HEARTBEAT_INTERVAL,
            min(MAX_HEARTBEAT_INTERVAL, heartbeat_interval),
        )

        # Config dir setup
        self.__conf_dir: Path = prepare_conf_dir(conf_dir)

        # Set up logger with async handler
        self._logger: logging.Logger = logging.getLogger(
            f"{self.__service_name}.{self.__worker_id}"
        )
        self._logger.setLevel(self.logging_level)
        stdout_handler = AsyncBaseHandler(
            service_name=self.__service_name, worker_id=self.__worker_id
        )
        stdout_handler.setLevel(self.logging_level)
        self._logger.addHandler(stdout_handler)

        self._start_event: asyncio.Event = asyncio.Event()
        self._stop_event: asyncio.Event = asyncio.Event()
        self._completion_event: asyncio.Event = asyncio.Event()

        self.__managers: dict[str, Callable[[], Coroutine[None, None, None]]] = {}
        self.__managers_tasks: dict[str, asyncio.Task[None]] = {}
        self.register_manager("Heartbeat", self.heartbeat)

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
    def conf_dir(self) -> Path:
        return self.__conf_dir

    @property
    def heartbeat_interval(self) -> int:
        return self.__heartbeat_interval

    @heartbeat_interval.setter
    def heartbeat_interval(self, interval: int) -> None:
        self.__heartbeat_interval = max(
            MIN_HEARTBEAT_INTERVAL,
            min(MAX_HEARTBEAT_INTERVAL, interval),
        )

    @property
    def start_time(self) -> datetime | None:
        """Service start time."""
        return self.__start_time

    @property
    def initialized(self) -> bool:
        """Indicates whether the worker has completed initialization."""
        return self.__initialized

    @property
    def managers_tasks(self) -> list[asyncio.Task[None]]:
        return [self.__managers_tasks[name] for name in self.__managers_tasks]

    @property
    def logger(self) -> logging.Logger:
        """Service logger instance."""
        return self._logger

    @property
    def logging_level(self) -> int:
        """Current logging level for the service."""
        return self.__logging_level

    @logging_level.setter
    def logging_level(self, level: int | str | None) -> None:
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
            If level is None or not recognized, defaults to DEFAULT_LOGGING_LEVEL
        """

        self.__logging_level = parse_logging_level(level)

        # Update logger and all handlers
        self.logger.setLevel(self.__logging_level)
        for handler in self.logger.handlers:
            handler.setLevel(self.__logging_level)
        self.logger.debug("Logging level set to %s", logging.getLevelName(self.logging_level))

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
        """Cleanly shut down all async logging handlers.

        This will attempt to stop each `AsyncBaseHandler` with a per-handler
        timeout to avoid hanging shutdowns if a handler blocks.
        """
        per_handler_timeout: float = 2.0
        for handler in self.logger.handlers:
            if isinstance(handler, AsyncBaseHandler):
                try:
                    await asyncio.wait_for(handler.stop_logging(), timeout=per_handler_timeout)
                except asyncio.TimeoutError:
                    try:
                        self.logger.warning("Timeout stopping logging handler %s", handler)
                    except Exception:
                        # logger itself may be in a bad state; fallback to print
                        print("Timeout stopping logging handler", handler)
                except Exception as e:
                    try:
                        self.logger.exception(
                            "Failed to shut down logging handler %s: %s", handler, e
                        )
                    except Exception:
                        print("Failed to shut down logging handler", handler)
                        print(e)

    def register_manager(self, name: str, manager: Callable[[], Coroutine[None, None, None]]):
        """This method allows to add custom managers to the service."""
        self.__managers[name] = manager

    async def initialize(self) -> bool:
        """
        Perform any initialization before starting the main loop.

        This method is intended to be overridden by subclasses to perform
        service-specific initialization such as database connections,
        API client setup, or other preparatory work.
        """
        return True

    async def heartbeat_manager(self) -> None:
        """Continuously repeat Heartbeat procedure."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.heartbeat_interval,
                )
            except asyncio.TimeoutError:
                await self.heartbeat()

    async def heartbeat(self) -> None:
        """Heartbeat function to be overwritten."""
        self.logger.debug("💓 Heartbeat")

    async def _start_managers(self) -> None:
        """Start managers tasks."""
        for name in self.__managers:
            if name in self.__managers_tasks:
                self.logger.log(logging.DEBUG, "%s is already running", name)
                continue
            self.__managers_tasks[name] = asyncio.create_task(self.__managers[name](), name=name)
            self.logger.log(logging.DEBUG, "%s has started")

    async def start(self):
        """
        Start the worker and all its components.

        This method:
        1. Starts all manager tasks
        2. Sets up signal handlers for graceful shutdown
        3. Performs custom initialization

        Raises:
            RuntimeError: If the worker fails to start properly
        """
        if self._stop_event.is_set():
            self.logger.log(
                logging.INFO,
                "Worker %s:{%d} is stopping, please wait.",
                self.service_name,
                self.worker_id,
            )
            return
        if not self.managers_tasks:
            if self._start_event.is_set():
                # start has already been called wait for completion.
                return

            self._start_event.set()

            self._stop_event.clear()
            self._completion_event.clear()

            print_scietex_logo(service_name=self.service_name, version=self.version)

            # Init Logging Handlers
            if not self.initialized:
                await self._logger_init_handlers()

            # Start main managers
            await self._start_managers()

            self.setup_signal_handlers()

            # Perform any custom initialization and check if successful
            self.__initialized = await self.initialize()
            if not self.initialized:
                raise RuntimeError("Initialization failed")

            self.__start_time = datetime.now(timezone.utc)
            self.logger.log(
                logging.DEBUG, "Worker %s:%d started", self.service_name, self.worker_id
            )
            self._start_event.clear()
        else:
            self.logger.log(
                logging.WARNING,
                "Worker %s:%d is already running",
                self.service_name,
                self.worker_id,
            )

    async def stop(self) -> None:
        """
        Stop the worker gracefully.

        This method:
        1. Shuts down managers tasks
        2. Processes remaining log messages
        3. Performs cleanup

        Note:
            This method is automatically called on SIGINT or SIGTERM
        """
        if self._start_event.is_set():
            self.logger.log(
                logging.INFO,
                "Worker %s:{%d} is starting, please wait.",
                self.service_name,
                self.worker_id,
            )
            return
        if self.managers_tasks:
            if self._stop_event.is_set():
                # stop has already been called
                return

            self.logger.debug("Stopping worker gracefully...")

            self._stop_event.set()

            await asyncio.gather(*self.managers_tasks, return_exceptions=True)
            self.logger.debug("Managers tasks finished")

            self.logger.log(logging.DEBUG, "Worker stopped.")

            await self.cleanup()

            # Shut down logging handlers with an overall timeout
            try:
                await asyncio.wait_for(self._logger_shut_down_handlers(), timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning("Timeout while shutting down logging handlers")
            except Exception as e:
                try:
                    self.logger.exception("Error shutting down logging handlers: %s", e)
                except Exception:
                    print("Error shutting down logging handlers:", e)
            self.__managers_tasks = {}
            self.__start_time = None
            self.__initialized = False
            self._completion_event.set()
        else:
            self.logger.log(
                logging.WARNING, "Worker %s:{%d} is not running", self.service_name, self.worker_id
            )

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
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop(), name="StopTask"))
        self.logger.log(logging.DEBUG, "Signal handlers are all setup")

    async def run(self):
        """
        Run the worker indefinitely.

        Starts the worker and waits indefinitely until stopped by
        a signal or external event. This is the main entry point
        for running the worker.
        """
        await self.start()
        await self._completion_event.wait()
