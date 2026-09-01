"""
Basic asynchronous worker for ``scietex.service``.

Provides ``BasicAsyncWorker``, a foundation class for building async
daemon services with signal handling, async logging, heartbeat and
watchdog managers, and graceful shutdown support.
"""

import asyncio
import logging
import signal
from collections.abc import Generator
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from scietex.logging import AsyncBaseHandler

from .logging import LoggerStatus, parse_logging_level
from .manager import Manager, ManagerStatus
from .utils import prepare_conf_dir, print_scietex_logo

DEFAULT_HEARTBEAT_INTERVAL: float = 10
MIN_HEARTBEAT_INTERVAL: float = 0.1
MAX_HEARTBEAT_INTERVAL: float = 600

DEFAULT_WATCHDOG_INTERVAL: float = 1
MIN_WATCHDOG_INTERVAL: float = 0.01
MAX_WATCHDOG_INTERVAL: float = 600

DEFAULT_LOGGER_HANDLER_TIMEOUT: float = 2
MIN_LOGGER_HANDLER_TIMEOUT: float = 1
MAX_LOGGER_HANDLER_TIMEOUT: float = 10

DEFAULT_MANAGER_SHUTDOWN_TIMEOUT: float = 2
MIN_MANAGER_SHUTDOWN_TIMEOUT: float = 1
MAX_MANAGER_SHUTDOWN_TIMEOUT: float = 10

WAIT_FOR_SERVICE_STOPPED_DELAY: float = 0.1


class ServiceStatus(Enum):
    """Service Status."""

    STOPPED = "Stopped"
    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"


class BasicAsyncWorker:
    """
    Base async worker framework for daemon services.

    Provides signal handling, async logging with custom handlers,
    heartbeat and watchdog managers (decorated with ``@Manager``),
    automatic manager restart on error, and graceful shutdown.

    Subclasses should override:
        - ``initialize()``: Service-specific initialization logic.
        - ``heartbeat()``: Periodic heartbeat behavior.
        - ``watchdog()``: Periodic watchdog checks.
        - ``cleanup()``: Service-specific cleanup on shutdown.

    Properties:
        service_name (str): Name of the service (read-only).
        worker_id (int): Unique identifier for this worker (read-only).
        version (str): Version string of the service (read-only).
        logger (logging.Logger): Logger instance for the worker.
        logging_level (int): Current logging level (configurable).
        state (ServiceStatus): Current service lifecycle state.
        start_time (datetime | None): Service start timestamp.
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
        **kwargs,
    ):
        """
        Initialize the BasicAsyncWorker.

        Args:
            service_name: Name of the service, used for logging and identification.
            version: Version string of the service.
            worker_id: Unique identifier for this worker instance.
            conf_dir: Directory to use for configuration files.
            logging_level: Logging level as string or integer.
                If invalid, defaults to ``DEFAULT_LOGGING_LEVEL`` (DEBUG).
            heartbeat_interval: Heartbeat interval in seconds.
            watchdog_interval: Watchdog check interval in seconds.
            **kwargs: Additional keyword arguments including:
                ``logger_handler_timeout``: Timeout for logger handler ops.
                ``manager_shutdown_timeout``: Timeout for manager shutdown.

        Note:
            Within a single process, the ``(service_name, worker_id)``
            combination should be unique to ensure separate logger names.
        """
        self.__service_name: str = service_name
        self.__worker_id: int = worker_id
        self.__version: str = version
        self.__logging_level: int = parse_logging_level(logging_level)

        self.__heartbeat_interval: float = max(
            MIN_HEARTBEAT_INTERVAL,
            min(MAX_HEARTBEAT_INTERVAL, heartbeat_interval or DEFAULT_HEARTBEAT_INTERVAL),
        )
        self.__watchdog_interval: float = max(
            MIN_WATCHDOG_INTERVAL,
            min(MAX_WATCHDOG_INTERVAL, watchdog_interval or DEFAULT_WATCHDOG_INTERVAL),
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

        self.__logger_handler_timeout = max(
            MIN_LOGGER_HANDLER_TIMEOUT,
            min(
                MAX_LOGGER_HANDLER_TIMEOUT,
                kwargs.get("logger_handler_timeout", DEFAULT_LOGGER_HANDLER_TIMEOUT),
            ),
        )

        self.__manager_shutdown_timeout = max(
            MIN_MANAGER_SHUTDOWN_TIMEOUT,
            min(
                MAX_MANAGER_SHUTDOWN_TIMEOUT,
                kwargs.get("manager_shutdown_timeout", DEFAULT_MANAGER_SHUTDOWN_TIMEOUT),
            ),
        )

        # State tracking

        self.__loggers_statuses: dict[str, LoggerStatus] = {}
        self.__state: ServiceStatus = ServiceStatus.STOPPED
        self.__start_time: datetime | None = None

        self.__manager_statuses: dict[str, ManagerStatus] = {}
        self.__manager_tasks: dict[str, asyncio.Task[None]] = {}
        self.__manager_errors: dict[str, Exception | None] = {}

        self.__events: dict[str, asyncio.Event] = {
            "exit_requested": asyncio.Event(),
            "exit": asyncio.Event(),
        }

        self._setup_signal_handlers()

    @property
    def state(self) -> ServiceStatus:
        """Current service status (read-only)."""
        return self.__state

    @property
    def events(self) -> dict[str, asyncio.Event]:
        """Dictionary of lifecycle events (exit_requested, exit)."""
        return self.__events

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
        """Configuration directory path (read-only)."""
        return self.__conf_dir

    @property
    def logger_handler_timeout(self) -> float:
        """Timeout in seconds for logger handler operations."""
        return self.__logger_handler_timeout

    @logger_handler_timeout.setter
    def logger_handler_timeout(self, timeout: float | None) -> None:
        """
        Set the timeout for logger handler operations.

        Args:
            timeout: Timeout in seconds, clamped between MIN_LOGGER_HANDLER_TIMEOUT
                and MAX_LOGGER_HANDLER_TIMEOUT, or None to use DEFAULT_LOGGER_HANDLER_TIMEOUT
        """
        self.__logger_handler_timeout = max(
            MIN_LOGGER_HANDLER_TIMEOUT,
            min(
                MAX_LOGGER_HANDLER_TIMEOUT,
                timeout or DEFAULT_LOGGER_HANDLER_TIMEOUT,
            ),
        )

    @property
    def manager_shutdown_timeout(self) -> float:
        """Timeout in seconds for manager shutdown operations."""
        return self.__manager_shutdown_timeout

    @manager_shutdown_timeout.setter
    def manager_shutdown_timeout(self, timeout: float | None) -> None:
        """
        Set the timeout for manager shutdown operations.

        Args:
            timeout: Timeout in seconds, clamped between MIN_MANAGER_SHUTDOWN_TIMEOUT
                and MAX_MANAGER_SHUTDOWN_TIMEOUT, or None to use DEFAULT_MANAGER_SHUTDOWN_TIMEOUT
        """
        self.__manager_shutdown_timeout = max(
            MIN_MANAGER_SHUTDOWN_TIMEOUT,
            min(
                MAX_MANAGER_SHUTDOWN_TIMEOUT,
                timeout or DEFAULT_MANAGER_SHUTDOWN_TIMEOUT,
            ),
        )

    @property
    def heartbeat_interval(self) -> float:
        """Interval in seconds between heartbeat calls."""
        return self.__heartbeat_interval

    @heartbeat_interval.setter
    def heartbeat_interval(self, interval: float) -> None:
        """
        Set the heartbeat interval.

        Args:
            interval: Heartbeat interval in seconds, clamped between
                MIN_HEARTBEAT_INTERVAL and MAX_HEARTBEAT_INTERVAL
        """
        self.__heartbeat_interval = max(
            MIN_HEARTBEAT_INTERVAL,
            min(MAX_HEARTBEAT_INTERVAL, interval),
        )

    @property
    def watchdog_interval(self) -> float:
        """Interval in seconds between watchdog checks."""
        return self.__watchdog_interval

    @watchdog_interval.setter
    def watchdog_interval(self, interval: float) -> None:
        """
        Set the watchdog interval.

        Args:
            interval: Watchdog interval in seconds, clamped between
                MIN_WATCHDOG_INTERVAL and MAX_WATCHDOG_INTERVAL
        """
        self.__watchdog_interval = max(
            MIN_WATCHDOG_INTERVAL,
            min(MAX_WATCHDOG_INTERVAL, interval),
        )

    @property
    def start_time(self) -> datetime | None:
        """Service start time."""
        return self.__start_time

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

    def _iter_manager_definitions(self) -> Generator[tuple[str, Manager]]:
        """
        Iterate over all registered managers from the class MRO.

        Yields:
            Tuple of (manager_name, manager) for each Manager decorator found
            in the class hierarchy, processed from most-derived to base classes.
        """
        for cls in reversed(type(self).__mro__):
            for attribute_name, attribute in cls.__dict__.items():
                if not isinstance(attribute, Manager):
                    continue
                manager_name = attribute.name or attribute_name
                yield manager_name, attribute

    def _setup_signal_handlers(self) -> None:
        """
        Set up signal handlers for graceful shutdown.

        Registers handlers for SIGINT and SIGTERM signals that will
        trigger a graceful shutdown of the worker.
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.exit(), name="StopTask"))
        self.logger.log(logging.DEBUG, "Signal handlers are all setup")

    async def _logger_start_handlers(self) -> None:
        """
        Start all async logging handlers that are not already running.

        Iterates over the logger's handlers and calls start_logging() on each
        AsyncBaseHandler that is stopped or not yet tracked. Handles timeouts
        and errors gracefully, falling back to print statements if the logger
        is in an unrecoverable state.
        """
        for handler in self.logger.handlers:
            handler_name = handler.name or handler.__class__.__name__
            if (
                handler_name not in self.__loggers_statuses
                or self.__loggers_statuses[handler_name] == LoggerStatus.STOPPED
            ):
                if isinstance(handler, AsyncBaseHandler):
                    try:
                        await asyncio.wait_for(
                            handler.start_logging(), timeout=self.logger_handler_timeout
                        )
                    except asyncio.TimeoutError:
                        try:
                            self.logger.warning(
                                "Timeout starting logging handler %s (%s)", handler_name, handler
                            )
                        except Exception:
                            # logger itself may be in a bad state; fallback to print
                            print(f"Timeout starting logging handler {handler_name} ({handler})")
                    except Exception as e:
                        try:
                            self.logger.error(
                                "Failed to start logging handler %s (%s): %s",
                                handler_name,
                                handler,
                                e,
                            )
                        except Exception:
                            print(
                                f"Failed to start logging handler {handler_name} ({handler}): {e}"
                            )
                self.__loggers_statuses[handler_name] = LoggerStatus.RUNNING

    async def _logger_shut_down_handlers(self) -> None:
        """Cleanly shut down all async logging handlers.

        This will attempt to stop each `AsyncBaseHandler` with a per-handler
        timeout to avoid hanging shutdowns if a handler blocks.
        """
        for handler in self.logger.handlers:
            handler_name = handler.name or handler.__class__.__name__
            if isinstance(handler, AsyncBaseHandler):
                try:
                    await asyncio.wait_for(
                        handler.stop_logging(), timeout=self.logger_handler_timeout
                    )
                except asyncio.TimeoutError:
                    try:
                        self.logger.warning(
                            "Timeout stopping logging handler %s (%s)", handler_name, handler
                        )
                    except Exception:
                        # logger itself may be in a bad state; fallback to print
                        print(f"Timeout stopping logging handler {handler_name} ({handler})")
                except Exception as e:
                    try:
                        self.logger.error(
                            "Failed to shut down logging handler %s (%s): %s",
                            handler_name,
                            handler,
                            e,
                        )
                    except Exception:
                        print(
                            f"Failed to shut down logging handler {handler_name} ({handler}): {e}"
                        )
            self.__loggers_statuses[handler_name] = LoggerStatus.RUNNING

    async def initialize(self) -> bool:
        """
        Perform any additional initialization before starting the managers.

        This method is intended to be overridden by subclasses to perform
        service-specific initialization such as database connections,
        API client setup, or other preparatory work.
        """
        return True

    async def _run_manager(self, name: str, manager: Manager) -> None:
        """
        Execute a manager's lifecycle loop with automatic restart on error.

        Runs the manager's method in an infinite loop. On cancellation, the
        manager stops cleanly. On any other exception, the error is recorded
        and the manager is automatically restarted. The finally block handles
        cleanup and status updates.

        Args:
            name: Human-readable name for the manager
            manager: The Manager instance whose method will be executed
        """
        self.logger.info("🟢 Manager %s started", name)

        try:
            while True:
                if manager.method:
                    await manager.method(self)
                else:
                    raise RuntimeError("Manager has no associated executable method.")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.__manager_errors[name] = e
            self.logger.error("❌ Manager %s error %s. Restarting", name, e)
            await self._restart_manager(name, manager)
        finally:
            self.__manager_statuses[name] = ManagerStatus.STOPPING
            self.logger.info("🟡 Manager %s stopping", name)
            if manager.cleanup:
                await manager.cleanup(self)
            self.__manager_statuses[name] = ManagerStatus.STOPPED
            self.logger.info("🔴 Manager %s stopped", name)

    async def _start_manager(self, name: str, manager: Manager) -> None:
        """
        Start a named manager as an asyncio task.

        Args:
            name: Identifier for the manager
            manager: The Manager instance to execute
        """
        if name in self.__manager_tasks:
            self.logger.log(logging.DEBUG, "%s is already running", name)
            return
        self.__manager_statuses[name] = ManagerStatus.STARTING
        self.__manager_errors[name] = None

        task = asyncio.create_task(
            self._run_manager(name, manager),
            name=name,
        )

        self.__manager_tasks[name] = task

    async def _stop_manager(self, name: str) -> None:
        """
        Stop a named manager task with a timeout.

        Cancels the task and waits up to `manager_shutdown_timeout` seconds
        for it to complete. Removes the task from internal tracking.

        Args:
            name: Identifier of the manager to stop
        """
        if name not in self.__manager_tasks:
            self.logger.log(logging.DEBUG, "%s is not running", name)
            return
        self.__manager_tasks[name].cancel()
        try:
            await asyncio.wait_for(self.__manager_tasks[name], self.manager_shutdown_timeout)
        except asyncio.TimeoutError:
            self.logger.log(logging.DEBUG, "Timeout during %s shut down", name)
        del self.__manager_tasks[name]

    async def _restart_manager(self, name: str, manager: Manager) -> None:
        """
        Stop and then restart a manager.

        Args:
            name: Identifier of the manager to restart
            manager: The Manager instance to run
        """
        await self._stop_manager(name)
        await self._start_manager(name, manager)

    async def _start_managers(self) -> None:
        """Start all registered managers as asyncio tasks."""
        for name, manager in self._iter_manager_definitions():
            await self._start_manager(name, manager)

    async def _stop_managers(self) -> None:
        """Stop all registered managers in order."""
        for name, _ in self._iter_manager_definitions():
            await self._stop_manager(name)

    async def _startup(self):
        """
        Execute the full startup sequence for the worker.

        Waits for any previous shutdown to complete, prints the service logo,
        starts logging handlers, starts all managers, runs custom initialization
        via initialize(), sets the start time, and transitions to RUNNING state.

        Raises:
            asyncio.CancelledError: If the startup process is cancelled
            RuntimeError: If initialization fails
        """
        try:
            if self.__state != ServiceStatus.STOPPED:
                self.logger.log(logging.INFO, "Waiting for service shutdown complete.")
            self.logger.log(logging.INFO, "Service is starting up.")
            while not self.__state == ServiceStatus.STOPPED:
                await asyncio.sleep(WAIT_FOR_SERVICE_STOPPED_DELAY)
            self.logger.log(logging.INFO, "Service is starting up.")
            self.__state = ServiceStatus.STARTING
            print_scietex_logo(service_name=self.service_name, version=self.version)
            # Init Logging Handlers
            await self._logger_start_handlers()

            # Start managers
            await self._start_managers()

            # Perform any custom initialization and check if successful
            if not await self.initialize():
                raise RuntimeError("Initialization failed")

            self.__start_time = datetime.now(timezone.utc)
            self.logger.log(
                logging.DEBUG, "Worker %s:%d started", self.service_name, self.worker_id
            )
            self.__state = ServiceStatus.RUNNING
        except asyncio.CancelledError:
            self.logger.log(logging.INFO, "Startup task canceled.")
            raise
        except RuntimeError as e:
            self.logger.log(logging.ERROR, "Initialization failed, shutting down. Error: %s", e)
            await self.stop()

    async def start(self) -> None:
        """
        Transition the worker to RUNNING state by spawning the startup task.

        If the worker is already running or starting, logs a warning and returns.
        If the worker is stopping or stopped, creates a task to execute the
        full startup sequence.
        """
        if self.__state == ServiceStatus.RUNNING:
            self.logger.log(
                logging.WARNING,
                "Worker %s:%d is already running",
                self.service_name,
                self.worker_id,
            )
            return
        if self.__state == ServiceStatus.STARTING:
            self.logger.log(
                logging.WARNING,
                "Worker %s:%d is already starting up",
                self.service_name,
                self.worker_id,
            )
            return
        if self.__state in (ServiceStatus.STOPPING, ServiceStatus.STOPPED):
            asyncio.create_task(self._startup(), name="Start")

    async def _shutdown(self) -> None:
        """
        Stop the worker gracefully.

        This method:
        1. Shuts down managers tasks
        2. Processes remaining log messages
        3. Performs cleanup

        Note:
            This method is automatically called on SIGINT or SIGTERM
        """
        try:
            self.logger.debug("Stopping worker gracefully...")
            self.__state = ServiceStatus.STOPPING
            self.logger.log(logging.DEBUG, "Worker stopped.")
            await self._stop_managers()
            self.logger.debug("Cleaning up...")
            await self.cleanup()

            self.logger.debug("Stopping loggers...")
            # Shut down logging handlers with an overall timeout
            try:
                loggers_timeout = len(self.logger.handlers) * self.logger_handler_timeout + 1
                await asyncio.wait_for(self._logger_shut_down_handlers(), timeout=loggers_timeout)
            except asyncio.TimeoutError:
                self.logger.warning("Timeout while shutting down logging handlers")
            except Exception as e:
                try:
                    self.logger.exception("Error shutting down logging handlers: %s", e)
                except Exception:
                    print("Error shutting down logging handlers:", e)
            self.__start_time = None

            self.__state = ServiceStatus.STOPPED

            if self.events["exit_requested"].is_set():
                self.events["exit_requested"].clear()
                self.events["exit"].set()
        except asyncio.CancelledError:
            self.logger.log(logging.ERROR, "Shutdown task cancelled")
            pass

    async def stop(self) -> None:
        """
        Request a graceful shutdown of the worker.

        If the worker is stopped or already stopping, clears the exit events
        and returns. Otherwise, creates a task to execute the full shutdown
        sequence (stop managers, cleanup, shut down loggers).

        Note:
            This method is automatically called when SIGINT or SIGTERM is received.
        """
        if self.__state == ServiceStatus.STOPPED:
            self.logger.log(
                logging.DEBUG,
                "Worker %s:%d is not running",
                self.service_name,
                self.worker_id,
            )
            if self.events["exit_requested"].is_set() and not self.events["exit"].is_set():
                self.events["exit_requested"].clear()
                self.events["exit"].set()
            return
        if self.__state == ServiceStatus.STOPPING:
            self.logger.log(
                logging.DEBUG,
                "Worker %s:%d is already shutting down",
                self.service_name,
                self.worker_id,
            )
            if self.events["exit_requested"].is_set() and not self.events["exit"].is_set():
                self.events["exit_requested"].clear()
                self.events["exit"].set()
            return
        if self.__state in (ServiceStatus.RUNNING, ServiceStatus.STARTING):
            self.logger.log(
                logging.DEBUG,
                "Worker %s:%d is going to SHUT DOWN",
                self.service_name,
                self.worker_id,
            )
            asyncio.create_task(self._shutdown(), name="Stop")

    async def exit(self):
        """
        Request exit and wait for the worker to stop.

        Sets the exit_requested event and triggers a graceful shutdown via stop().
        """
        self.events["exit_requested"].set()
        await self.stop()

    @Manager(name="Heartbeat")
    async def _heartbeat_manager(self) -> None:
        """
        Manager that periodically invokes the heartbeat() method.

        Sleeps for heartbeat_interval seconds, then calls heartbeat().
        Repeats indefinitely until cancelled.
        """
        await asyncio.sleep(self.heartbeat_interval)
        await self.heartbeat()

    @Manager(name="Watchdog")
    async def _watchdog_manager(self) -> None:
        """
        Manager that periodically invokes the watchdog() method.

        Sleeps for watchdog_interval seconds, then calls watchdog().
        Repeats indefinitely until cancelled.
        """
        await asyncio.sleep(self.watchdog_interval)
        await self.watchdog()

    async def heartbeat(self) -> None:
        """Heartbeat function to be overwritten."""
        self.logger.debug("💓 Heartbeat")

    async def watchdog(self) -> None:
        """Watchdog function to be overwritten."""
        self.logger.debug("🐕 Watchdog")

    async def cleanup(self):
        """
        Cleanup everything before exit.

        This method is intended to be overridden by subclasses to perform
        service-specific cleanup such as closing database connections,
        releasing resources, or sending final status updates.
        """
