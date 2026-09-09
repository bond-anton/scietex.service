"""
Basic asynchronous worker for ``scietex.service``.

Provides ``BasicWorker``, a foundation class for building async
daemon services with signal handling, async logging, heartbeat and
watchdog managers, and graceful shutdown support.
"""

import asyncio
import logging
import signal
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import ClassVar

from scietex.logging import ConsoleHandler

from .config import (
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_LOGGER_HANDLER_TIMEOUT,
    DEFAULT_MANAGER_MAX_RETRIES,
    DEFAULT_MANAGER_RESTART_BACKOFF,
    DEFAULT_MANAGER_SHUTDOWN_TIMEOUT,
    DEFAULT_WATCHDOG_INTERVAL,
    WorkerConfig,
)
from .log_handlers import parse_logging_level
from .log_handlers.lifecycle import LoggingLifecycle
from .manager import Manager
from .manager.runtime import ManagerRuntime
from .utils import prepare_conf_dir, print_scietex_logo

WAIT_FOR_SERVICE_STOPPED_DELAY: float = 0.1


class ServiceStatus(Enum):
    """Lifecycle states of a ``BasicWorker`` instance.

    Attributes:
        STOPPED: The service is not running.
        STARTING: The service is in the process of starting up.
        RUNNING: The service is actively running and processing.
        STOPPING: The service is in the process of shutting down.
    """

    STOPPED = "Stopped"
    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"


class BasicWorker:
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
        instance_id (str): Unique identifier for this worker instance (read-only).
        version (str): Version string of the service (read-only).
        logger (logging.Logger): Logger instance for the worker.
        logging_level (int): Current logging level (read-only).
        state (ServiceStatus): Current service lifecycle state.
        start_time (datetime | None): Service start timestamp.
    """

    # Concrete config struct type for this worker. The base stores the config
    # instance into ``self._config``; subclasses override this to their own
    # config struct so the base instantiates the concrete type when ``config``
    # is ``None`` (AR-069). Subclasses no longer re-store / double-instantiate.
    _config_type: ClassVar[type[WorkerConfig]] = WorkerConfig

    def __init__(self, config: WorkerConfig | None = None):
        """
        Initialize the BasicWorker.

        Args:
            config: A :class:`~scietex.service.config.WorkerConfig` holding the
                worker's service identity, config directory, logging level, and
                timing/retry settings. ``None`` uses the struct defaults. A
                ``None`` timing/retry field resolves to its ``DEFAULT_*``
                constant at read time; an out-of-range value is rejected at
                construction.

        Note:
            Each instance auto-generates a unique ``instance_id`` used for
            logger names and (in ``ValkeyWorker``) consumer/status keys, so
            multiple instances of the same service can coexist in one process.
        """
        cfg = config if config is not None else self._config_type()
        self._config: WorkerConfig = cfg
        self.__service_name: str = cfg.service_name
        self.__instance_id: str = uuid.uuid4().hex
        self.__version: str = cfg.version
        self.__logging_level: int = parse_logging_level(cfg.logging_level)
        self.__conf_dir: Path = prepare_conf_dir(cfg.conf_dir)

        # Extracted components own their respective bookkeeping; the worker
        # keeps only identity/config and the lifecycle state machine. They are
        # constructed before the logger handler registration below.
        self._manager_runtime = ManagerRuntime(self)
        self._logging_lifecycle = LoggingLifecycle(self)

        # Set up logger with async handler
        self._logger: logging.Logger = logging.getLogger(f"{self.__service_name}:{self.__instance_id}")
        self._logger.setLevel(self.logging_level)
        # Async handlers are restartable in place (scietex.logging >= 1.0), so a
        # single instance is registered once and restarted on each start cycle.
        # The console handler derives its identity from the logger name above.
        self._logging_lifecycle.register_logger_handler(ConsoleHandler())

        # State tracking

        self.__state: ServiceStatus = ServiceStatus.STOPPED
        self.__start_time: datetime | None = None

        self.__events: dict[str, asyncio.Event] = {
            "exit_requested": asyncio.Event(),
            "exit": asyncio.Event(),
        }

        # Reference to the pending signal-triggered stop task, used to guard
        # against spawning a new exit() per signal (AR-033).
        self.__stop_task: asyncio.Task | None = None

    @property
    def state(self) -> ServiceStatus:
        """Current lifecycle state of the service (read-only).

        Returns:
            The current ``ServiceStatus`` enum value indicating whether
            the service is stopped, starting, running, or stopping.
        """
        return self.__state

    @property
    def events(self) -> Mapping[str, asyncio.Event]:
        """Dictionary of lifecycle events for external coordination.

        Contains two events:
            - ``exit_requested``: Set when an exit is requested (e.g., via signal).
            - ``exit``: Set when the worker has fully stopped.

        Returns:
            A read-only mapping view of the internal events dictionary. The
            ``asyncio.Event`` values remain mutable and may be awaited or
            inspected, but the mapping itself cannot be modified.
        """
        return MappingProxyType(self.__events)

    @property
    def service_name(self) -> str:
        """Name of the service, used for logging and identification (read-only).

        Returns:
            The service name string provided during initialization.
        """
        return self.__service_name

    @property
    def instance_id(self) -> str:
        """Unique identifier for this worker instance (read-only).

        Returns:
            The auto-generated instance ID string (``uuid4().hex``).
        """
        return self.__instance_id

    @property
    def version(self) -> str:
        """Version string of the service (read-only).

        Returns:
            The version string provided during initialization.
        """
        return self.__version

    @property
    def conf_dir(self) -> Path:
        """Resolved configuration directory path (read-only).

        The directory is determined by the precedence rules:
        ``conf_dir`` argument, ``~/.config/scietex/``, ``/etc/scietex/``,
        ``/usr/local/etc/scietex/``, or ``./config/`` (CWD).

        Returns:
            The ``Path`` object pointing to the configuration directory.
        """
        return self.__conf_dir

    @property
    def logger_handler_timeout(self) -> float:
        """Timeout in seconds for logger handler start/stop operations (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_LOGGER_HANDLER_TIMEOUT``; a non-``None`` value is validated
        against ``[MIN_LOGGER_HANDLER_TIMEOUT, MAX_LOGGER_HANDLER_TIMEOUT]`` at
        construction.

        Returns:
            The current timeout value in seconds.
        """
        v = self._config.logger_handler_timeout
        return v if v is not None else DEFAULT_LOGGER_HANDLER_TIMEOUT

    @property
    def manager_shutdown_timeout(self) -> float:
        """Timeout in seconds for manager task shutdown operations (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_MANAGER_SHUTDOWN_TIMEOUT``; a non-``None`` value is validated
        against ``[MIN_MANAGER_SHUTDOWN_TIMEOUT, MAX_MANAGER_SHUTDOWN_TIMEOUT]``
        at construction.

        Returns:
            The current timeout value in seconds.
        """
        v = self._config.manager_shutdown_timeout
        return v if v is not None else DEFAULT_MANAGER_SHUTDOWN_TIMEOUT

    @property
    def manager_max_retries(self) -> int:
        """Maximum consecutive failures before a manager gives up (read-only).

        A ``None`` configuration value resolves to ``DEFAULT_MANAGER_MAX_RETRIES``;
        a non-``None`` value is validated against
        ``[MIN_MANAGER_MAX_RETRIES, MAX_MANAGER_MAX_RETRIES]`` at construction.

        Returns:
            The current maximum retry count.
        """
        v = self._config.manager_max_retries
        return v if v is not None else DEFAULT_MANAGER_MAX_RETRIES

    @property
    def manager_restart_backoff(self) -> float:
        """Backoff delay in seconds between manager restart attempts (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_MANAGER_RESTART_BACKOFF``; a non-``None`` value is validated
        against ``[MIN_MANAGER_RESTART_BACKOFF, MAX_MANAGER_RESTART_BACKOFF]`` at
        construction.

        Returns:
            The current backoff delay in seconds.
        """
        v = self._config.manager_restart_backoff
        return v if v is not None else DEFAULT_MANAGER_RESTART_BACKOFF

    @property
    def failed_managers(self) -> list[str]:
        """Names of managers that exhausted their retry budget and gave up (read-only).

        Returns:
            A list of manager names whose runtime status is
            ``ManagerStatus.FAILED`` (``[]`` when no manager has failed).
        """
        return self._manager_runtime.failed_managers

    @property
    def heartbeat_interval(self) -> float:
        """Interval in seconds between heartbeat calls (read-only).

        A ``None`` configuration value resolves to ``DEFAULT_HEARTBEAT_INTERVAL``;
        a non-``None`` value is validated against
        ``[MIN_HEARTBEAT_INTERVAL, MAX_HEARTBEAT_INTERVAL]`` at construction.

        Returns:
            The current heartbeat interval in seconds.
        """
        v = self._config.heartbeat_interval
        return v if v is not None else DEFAULT_HEARTBEAT_INTERVAL

    @property
    def watchdog_interval(self) -> float:
        """Interval in seconds between watchdog checks (read-only).

        A ``None`` configuration value resolves to ``DEFAULT_WATCHDOG_INTERVAL``;
        a non-``None`` value is validated against
        ``[MIN_WATCHDOG_INTERVAL, MAX_WATCHDOG_INTERVAL]`` at construction.

        Returns:
            The current watchdog interval in seconds.
        """
        v = self._config.watchdog_interval
        return v if v is not None else DEFAULT_WATCHDOG_INTERVAL

    @property
    def start_time(self) -> datetime | None:
        """Timestamp when the service started running (read-only).

        Returns:
            The UTC ``datetime`` when the service transitioned to
            ``RUNNING`` state, or ``None`` if the service has not
            started or has been stopped.
        """
        return self.__start_time

    @property
    def logger(self) -> logging.Logger:
        """Logger instance for the worker.

        The logger is named using the pattern ``{service_name}:{instance_id}``
        and is configured with a ``ConsoleHandler`` for async logging.

        Returns:
            The ``logging.Logger`` instance associated with this worker.
        """
        return self._logger

    @property
    def logging_level(self) -> int:
        """Current logging level for the worker (read-only).

        Parsed once from the worker's configuration at construction.

        Returns:
            The logging level as an integer constant from the
            ``logging`` module (e.g., ``logging.DEBUG``, ``logging.INFO``).
        """
        return self.__logging_level

    def _setup_signal_handlers(self) -> None:
        """
        Set up signal handlers for graceful shutdown.

        Registers handlers for SIGINT and SIGTERM signals that will
        trigger a graceful shutdown of the worker.

        No-ops on platforms without ``loop.add_signal_handler`` support
        (e.g. Windows).
        """
        loop = asyncio.get_running_loop()
        if not hasattr(loop, "add_signal_handler"):
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._request_exit)
        self.logger.log(logging.DEBUG, "Signal handlers are all setup")

    def _request_exit(self) -> None:
        """Spawn a single exit task, guarding against re-entry.

        Repeated signals must not each spawn their own exit() task. A pending
        stop task or an already-requested exit short-circuits so only one
        shutdown runs.
        """
        if self.__stop_task is not None and not self.__stop_task.done():
            return
        if self.events["exit_requested"].is_set():
            return
        self.__stop_task = asyncio.create_task(self.exit(), name="StopTask")

    def _remove_signal_handlers(self) -> None:
        """
        Remove the signal handlers registered for graceful shutdown.

        Mirrors ``_setup_signal_handlers`` and no-ops on platforms without
        ``loop.remove_signal_handler`` support (e.g. Windows).
        """
        loop = asyncio.get_running_loop()
        if not hasattr(loop, "remove_signal_handler"):
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        self.logger.log(logging.DEBUG, "Signal handlers removed")

    async def initialize(self) -> bool:
        """
        Perform any additional initialization before starting the managers.

        This method is intended to be overridden by subclasses to perform
        service-specific initialization such as database connections,
        API client setup, or other preparatory work.
        """
        return True

    async def _startup(self):
        """
        Execute the full startup sequence for the worker.

        Waits for any previous shutdown to complete, prints the service logo,
        starts logging handlers, runs custom initialization via initialize(),
        sets the start time, then starts all managers, and transitions to
        RUNNING state. The start time is set before the managers start so the
        first immediate heartbeat is not skipped (AR-049).

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
            await self._logging_lifecycle.start_handlers()

            # Perform any custom initialization and check if successful.
            # Must run before managers start: managers/handlers may depend on
            # resources created by initialize() (e.g. a Valkey client) and
            # would otherwise race a not-yet-ready resource.
            if not await self.initialize():
                raise RuntimeError("Initialization failed")

            # Register with any external registry only after initialize()
            # succeeded (transport/client exists) and before managers start.
            await self._register_instance()

            # Set the start time before managers start: the heartbeat manager
            # fires its first beat immediately, and the heartbeat is guarded by
            # start_time, so a late set would skip the first beat (AR-049).
            self.__start_time = datetime.now(timezone.utc)

            # Start managers
            await self._manager_runtime.start_managers()

            self.logger.log(logging.DEBUG, "Worker %s:%s started", self.service_name, self.instance_id)
            self.__state = ServiceStatus.RUNNING
        except asyncio.CancelledError:
            self.logger.log(logging.INFO, "Startup task canceled.")
            self._force_stopped()
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
                "Worker %s:%s is already running",
                self.service_name,
                self.instance_id,
            )
            return
        if self.__state == ServiceStatus.STARTING:
            self.logger.log(
                logging.WARNING,
                "Worker %s:%s is already starting up",
                self.service_name,
                self.instance_id,
            )
            return
        if self.__state in (ServiceStatus.STOPPING, ServiceStatus.STOPPED):
            self._setup_signal_handlers()
            asyncio.create_task(self._startup(), name="Start")

    def _force_stopped(self) -> None:
        """Force the worker into a terminal STOPPED state.

        Used by the startup/shutdown cancellation handlers so a cancelled task
        never strands the worker in STARTING or STOPPING, which would block a
        later start() (AR-017). If an exit was requested, surface it as a
        completed exit instead of leaving the exit event dangling.
        """
        self.__state = ServiceStatus.STOPPED
        self.__start_time = None
        if self.events["exit_requested"].is_set():
            self.__events["exit_requested"].clear()
            self.__events["exit"].set()

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
            await self._manager_runtime.stop_managers()
            # Unregister while the transport is still open (cleanup() may
            # disconnect it). Framework-owned, not part of user cleanup().
            await self._unregister_instance()
            self.logger.debug("Cleaning up...")
            await self.cleanup()

            self.logger.debug("Stopping loggers...")
            # Shut down logging handlers with an overall timeout
            try:
                loggers_timeout = len(self.logger.handlers) * self.logger_handler_timeout + 1
                await asyncio.wait_for(self._logging_lifecycle.shut_down_handlers(), timeout=loggers_timeout)
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
                self.__events["exit_requested"].clear()
                self.__events["exit"].set()
        except asyncio.CancelledError:
            self.logger.log(logging.ERROR, "Shutdown task cancelled")
            self._force_stopped()
            raise

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
                "Worker %s:%s is not running",
                self.service_name,
                self.instance_id,
            )
            if self.events["exit_requested"].is_set() and not self.events["exit"].is_set():
                self.__events["exit_requested"].clear()
                self.__events["exit"].set()
            self._remove_signal_handlers()
            return
        if self.__state == ServiceStatus.STOPPING:
            self.logger.log(
                logging.DEBUG,
                "Worker %s:%s is already shutting down",
                self.service_name,
                self.instance_id,
            )
            if self.events["exit_requested"].is_set() and not self.events["exit"].is_set():
                self.__events["exit_requested"].clear()
                self.__events["exit"].set()
            return
        if self.__state in (ServiceStatus.RUNNING, ServiceStatus.STARTING):
            self.logger.log(
                logging.DEBUG,
                "Worker %s:%s is going to SHUT DOWN",
                self.service_name,
                self.instance_id,
            )
            asyncio.create_task(self._shutdown(), name="Stop")

    async def exit(self):
        """Request exit and wait for the worker to fully stop.

        Sets the ``exit_requested`` event and triggers a graceful shutdown
        via ``stop()``. The caller should await ``events["exit"].wait()``
        to confirm the worker has fully stopped.
        """
        self.__events["exit_requested"].set()
        await self.stop()

    @Manager(name="Heartbeat")
    async def _heartbeat_manager(self) -> None:
        """
        Manager that periodically invokes the heartbeat() method.

        Calls heartbeat() immediately, then sleeps for heartbeat_interval
        seconds. Repeats indefinitely until cancelled.
        """
        await self.heartbeat()
        await asyncio.sleep(self.heartbeat_interval)

    @Manager(name="Watchdog")
    async def _watchdog_manager(self) -> None:
        """
        Manager that periodically invokes the watchdog() method.

        Calls watchdog() immediately, then sleeps for watchdog_interval
        seconds. Repeats indefinitely until cancelled.
        """
        await self.watchdog()
        await asyncio.sleep(self.watchdog_interval)

    async def heartbeat(self) -> None:
        """Periodic heartbeat callback invoked by the Heartbeat manager.

        Override this method in subclasses to define custom heartbeat
        behavior, such as health checks or status reporting. The default
        implementation logs a debug message.

        The Heartbeat manager calls this method every ``heartbeat_interval``
        seconds.
        """
        self.logger.debug("[HEARTBEAT] Heartbeat")

    async def watchdog(self) -> None:
        """Periodic watchdog callback invoked by the Watchdog manager.

        Override this method in subclasses to define custom watchdog
        behavior, such as monitoring resource usage or checking
        dependencies. The default implementation logs a debug message.

        The Watchdog manager calls this method every ``watchdog_interval``
        seconds. By default it also surfaces any manager that exhausted its
        retry budget and gave up (AR-063), logging CRITICAL but never
        auto-shutting down so the degradation is observable.
        """
        self.logger.debug("[WATCHDOG] Watchdog")
        failed = self.failed_managers
        if failed:
            self.logger.critical(
                "Manager(s) %s gave up after exhausting retry budget; worker is degraded",
                ", ".join(failed),
            )

    async def cleanup(self):
        """
        Cleanup everything before exit.

        This method is intended to be overridden by subclasses to perform
        service-specific cleanup such as closing database connections,
        releasing resources, or sending final status updates.
        """

    async def _register_instance(self) -> None:
        """Register this worker instance with any external registry.

        Called once by ``_startup()`` after ``initialize()`` succeeds and
        before managers start. The base implementation is a no-op (a single
        worker has no external registry); subclasses override to add their
        instance id to a transport-scoped registry. Best-effort: a failure
        must not fail startup (log and continue).
        """

    async def _unregister_instance(self) -> None:
        """Unregister this worker instance from any external registry.

        Called once by ``_shutdown()`` after managers stop and before
        ``cleanup()`` tears down the transport, so the transport is still
        open for the removal. The base implementation is a no-op; subclasses
        override to remove their instance id. Best-effort: a failure must
        not fail shutdown (log and continue).
        """
