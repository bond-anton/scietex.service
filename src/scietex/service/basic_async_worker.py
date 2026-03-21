"""
Module providing basic asynchronous worker, which can be used to construct more advanced services.
Worker provides console logging, signal handlers, etc.
"""

import asyncio
import logging
import signal
from datetime import datetime, timezone
from pathlib import Path

from scietex.logging import AsyncBaseHandler

from .logo import LOGO
from .version import __version__

DEFAULT_LOGGING_LEVEL: int = logging.DEBUG
"""Default logging level for the worker if no valid level is provided."""

CONF_PATHS = [
    Path.home() / ".config" / "scietex",
    Path("/etc") / "scietex",
    Path("/usr/local/etc") / "scietex",
    Path().cwd() / "config",
]


# pylint: disable=too-many-instance-attributes, too-many-public-methods
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

        Note:
            If logging_level is invalid, defaults to DEFAULT_LOGGING_LEVEL
        """
        self.__service_name: str = service_name
        self.__worker_id: int = kwargs.get("worker_id", 1)
        self.__version: str = version
        self.__logging_level: int = DEFAULT_LOGGING_LEVEL
        self.__start_time: datetime | None = None

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
        self.conf_dir: Path | None = None

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
        self.log_queue: asyncio.Queue[tuple[int, str]] = asyncio.Queue()

        self._stop_event: asyncio.Event = asyncio.Event()
        self._completion_event: asyncio.Event = asyncio.Event()
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
    def start_time(self) -> datetime | None:
        """Service start time."""
        return self.__start_time

    @property
    def initialized(self) -> bool:
        """Indicates whether the worker has completed initialization."""
        return self.__start_time is not None

    @property
    def logger(self) -> logging.Logger:
        """Service logger instance."""
        return self._logger

    @property
    def logging_level(self) -> int:
        """Current logging level for the service."""
        return self.__logging_level

    @logging_level.setter
    def logging_level(self, level: int | str) -> None:
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
                except Exception as e:  # pylint: disable=broad-exception-caught
                    try:
                        self.logger.exception(
                            "Failed to shut down logging handler %s: %s", handler, e
                        )
                    except Exception:
                        print("Failed to shut down logging handler", handler)
                        print(e)

    async def _drain_log_queue(self) -> None:
        """Drain `log_queue` and deliver remaining messages to the logger.

        Implemented as a separate coroutine so it can be awaited with a timeout
        from `stop()` to avoid hanging forever.
        """
        while not self.log_queue.empty():
            level, message = await self.log_queue.get()
            try:
                self.logger.log(level, message)
            except Exception:
                # Best-effort: don't fail draining because of logging errors
                print("Failed to emit log message:", level, message)
            finally:
                try:
                    self.log_queue.task_done()
                except Exception:
                    pass

    async def initialize(self) -> bool:
        """
        Perform any initialization before starting the main loop.

        This method is intended to be overridden by subclasses to perform
        service-specific initialization such as database connections,
        API client setup, or other preparatory work.
        """
        if not self.initialized:
            await self._logger_init_handlers()
        return True

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
        if not self.managers_tasks:
            print(
                LOGO.format(
                    service_name=self.service_name,
                    version=self.version,
                    scietex_version=__version__,
                )
            )

            # Start main managers
            self.managers_tasks = [
                asyncio.create_task(self.logging_manager()),
            ]

            await self.log("Log manager started", level=logging.DEBUG)

            self.setup_signal_handlers()
            await self.log("Signal handlers are all setup", level=logging.DEBUG)

            # Perform any custom initialization and check if successful
            if not await self.initialize():
                raise RuntimeError("Initialization failed")

            self.__start_time = datetime.now(timezone.utc)
            await self.log(
                f"Worker {self.service_name}:{self.worker_id} started",
                level=logging.DEBUG,
            )
        else:
            await self.log(
                f"Worker {self.service_name}:{self.worker_id} is already running",
                level=logging.WARNING,
            )

    async def stop(self):
        """
        Stop the worker gracefully.

        This method:
        1. Shuts down managers tasks
        2. Processes remaining log messages
        3. Performs cleanup

        Note:
            This method is automatically called on SIGINT or SIGTERM
        """
        if self.managers_tasks:
            self.logger.debug("Stopping worker gracefully...")
            self._stop_event.set()

            await asyncio.gather(*self.managers_tasks, return_exceptions=True)
            self.logger.debug("Managers tasks finished")

            # Process remaining logs with a timeout to avoid hanging forever
            try:
                await asyncio.wait_for(self._drain_log_queue(), timeout=2)
            except asyncio.TimeoutError:
                self.logger.warning("Timeout while draining log_queue")
            except Exception as e:  # pylint: disable=broad-exception-caught
                try:
                    self.logger.exception("Error while draining log_queue: %s", e)
                except Exception:
                    print("Error while draining log_queue:", e)

            self.logger.debug("Log queue is empty")

            # await self.log("Worker stopped.", logging.DEBUG)
            await self.cleanup()

            # Shut down logging handlers with an overall timeout
            try:
                await asyncio.wait_for(self._logger_shut_down_handlers(), timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning("Timeout while shutting down logging handlers")
            except Exception as e:  # pylint: disable=broad-exception-caught
                try:
                    self.logger.exception("Error shutting down logging handlers: %s", e)
                except Exception:
                    print("Error shutting down logging handlers:", e)
            self.__start_time = None
            self._completion_event.set()
        else:
            await self.log(
                f"Worker {self.service_name}:{self.worker_id} is not running",
                level=logging.WARNING,
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
            loop.add_signal_handler(sig, lambda _: asyncio.create_task(self.stop()), (sig,))

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
            except asyncio.TimeoutError:
                pass

    async def log(self, message: str, level: int = logging.INFO):
        """
        Add a log message to the log queue for processing.

        Args:
            message: The log message text
            level: The logging level (default: INFO)
        """
        await self.log_queue.put((level, message))

    async def run(self):
        """
        Run the worker indefinitely.

        Starts the worker and waits indefinitely until stopped by
        a signal or external event. This is the main entry point
        for running the worker.
        """
        await self.start()
        await self._completion_event.wait()
