"""
Basic asynchronous worker for ``scietex.service``.

Provides ``BasicWorker``, a foundation class for building async
daemon services with signal handling, async logging, heartbeat and
watchdog managers, and graceful shutdown support.
"""

import asyncio
import logging
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import ClassVar

from scietex.logging import ConsoleHandler

from .config import (
    DEFAULT_ACTIVE_TTL_MULTIPLIER,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_INACTIVE_TTL_MULTIPLIER,
    DEFAULT_LOGGER_HANDLER_TIMEOUT,
    DEFAULT_MANAGER_MAX_RETRIES,
    DEFAULT_MANAGER_RESTART_BACKOFF,
    DEFAULT_MANAGER_SHUTDOWN_TIMEOUT,
    DEFAULT_WATCHDOG_INTERVAL,
    WorkerConfig,
    prepare_conf_dir,
)
from .lifecycle import WorkerLifecycle
from .log_handlers import parse_logging_level
from .log_handlers.lifecycle import LoggingLifecycle
from .manager import Manager
from .manager.runtime import ManagerRuntime
from .signal_handler import SignalHandler
from .theme import ScietexMonochrome, Theme, print_banner


class ServiceStatus(Enum):
    """Lifecycle states of a ``BasicWorker`` instance."""

    #: The service is not running.
    STOPPED = "Stopped"
    #: The service is in the process of starting up.
    STARTING = "Starting"
    #: The service is actively running and processing.
    RUNNING = "Running"
    #: The service is in the process of shutting down.
    STOPPING = "Stopping"


class BasicWorker:
    """
    Base async worker framework for daemon services.

    Provides signal handling, async logging with custom handlers,
    heartbeat and watchdog managers (registered through the manager registry),
    automatic manager restart on error, and graceful shutdown.

    Subclasses should override:
        - ``initialize()``: Service-specific initialization logic.
        - ``heartbeat()``: Periodic heartbeat behavior.
        - ``watchdog()``: Periodic watchdog checks.
        - ``cleanup()``: Service-specific cleanup on shutdown.
    """

    # Concrete config struct type for this worker. The base stores the config
    # instance into ``self._config``; subclasses override this to their own
    # config struct so the base instantiates the concrete type when ``config``
    # is ``None`` (AR-069). Subclasses no longer re-store / double-instantiate.
    _config_type: ClassVar[type[WorkerConfig]] = WorkerConfig

    def __init__(self, config: WorkerConfig | None = None, *, theme: Theme | None = None):
        """
        Initialize the worker.

        Each instance auto-generates a unique ``instance_id`` used for
        logger names and (in ``ValkeyWorker``) consumer/status keys, so
        multiple instances of the same service can coexist in one process.

        Args:
            config: A :class:`~scietex.service.config.WorkerConfig` holding the
                worker's service identity, config directory, logging level, and
                timing/retry settings. ``None`` uses the struct defaults. A
                ``None`` timing/retry field resolves to its ``DEFAULT_*``
                constant at read time; an out-of-range value is rejected at
                construction.
            theme: The rendering theme for the startup banner and console log
                formatter. Defaults to :class:`ScietexMonochrome` when ``None``.
                It is a live object, not a config field, so it is injected via
                the constructor.
        """
        cfg = config if config is not None else self._config_type()
        self._theme: Theme = theme if theme is not None else ScietexMonochrome()
        self._config: WorkerConfig = cfg
        self.__service_name: str = cfg.service_name
        self.__instance_id: str = uuid.uuid4().hex
        self.__version: str = cfg.version
        self.__logging_level: int = parse_logging_level(cfg.logging_level)
        self.__conf_dir: Path = prepare_conf_dir(cfg.conf_dir)

        # Resolve the None-means-default timing fields eagerly, matching
        # TaskProcessor's hot-loop fields (AR-080). ``is not None`` (never
        # ``or``) preserves the ``<= 0`` unbounded sentinel.
        self.__logger_handler_timeout: float = (
            cfg.logger_handler_timeout if cfg.logger_handler_timeout is not None else DEFAULT_LOGGER_HANDLER_TIMEOUT
        )
        self.__manager_shutdown_timeout: float = (
            cfg.manager_shutdown_timeout
            if cfg.manager_shutdown_timeout is not None
            else DEFAULT_MANAGER_SHUTDOWN_TIMEOUT
        )
        self.__manager_max_retries: int = (
            cfg.manager_max_retries if cfg.manager_max_retries is not None else DEFAULT_MANAGER_MAX_RETRIES
        )
        self.__manager_restart_backoff: float = (
            cfg.manager_restart_backoff if cfg.manager_restart_backoff is not None else DEFAULT_MANAGER_RESTART_BACKOFF
        )
        self.__heartbeat_interval: float = (
            cfg.heartbeat_interval if cfg.heartbeat_interval is not None else DEFAULT_HEARTBEAT_INTERVAL
        )
        self.__active_ttl: float = (
            cfg.active_ttl if cfg.active_ttl is not None else self.__heartbeat_interval * DEFAULT_ACTIVE_TTL_MULTIPLIER
        )
        self.__inactive_ttl: float = (
            cfg.inactive_ttl
            if cfg.inactive_ttl is not None
            else self.__heartbeat_interval * DEFAULT_INACTIVE_TTL_MULTIPLIER
        )
        self.__watchdog_interval: float = (
            cfg.watchdog_interval if cfg.watchdog_interval is not None else DEFAULT_WATCHDOG_INTERVAL
        )

        # Extracted components own their respective bookkeeping; the worker
        # keeps only identity/config. They are constructed before the logger
        # handler registration below.
        self._manager_runtime = ManagerRuntime(self)
        self._logging_lifecycle = LoggingLifecycle(self)
        self._lifecycle = WorkerLifecycle(self)
        self._signal_handler = SignalHandler(self)

        # Set up logger with async handler
        self._logger: logging.Logger = logging.getLogger(f"{self.__service_name}:{self.__instance_id}")
        self._logger.setLevel(self.logging_level)
        # Async handlers are restartable in place (scietex.logging >= 1.0), so a
        # single instance is registered once and restarted on each start cycle.
        # The console handler derives its identity from the logger name above.
        # The formatter is theme-owned; scietex.logging defaults to
        # ScietexFormatter() when formatter=None, so this preserves today's output.
        self._logging_lifecycle.register_logger_handler(ConsoleHandler(formatter=self._theme.console_formatter()))

    @property
    def state(self) -> ServiceStatus:
        """Current lifecycle state of the service (read-only)."""
        return self._lifecycle.state

    @property
    def events(self) -> Mapping[str, asyncio.Event]:
        """Dictionary of lifecycle events for external coordination.

        Contains two events:
            - ``exit_requested``: Set when an exit is requested (e.g., via signal).
            - ``exit``: Set when the worker has fully stopped.
        """
        return self._lifecycle.events

    @property
    def service_name(self) -> str:
        """Name of the service, used for logging and identification (read-only)."""
        return self.__service_name

    @property
    def instance_id(self) -> str:
        """Unique identifier for this worker instance (read-only).

        Auto-generated from ``uuid4().hex`` at construction.
        """
        return self.__instance_id

    @property
    def version(self) -> str:
        """Version string of the service (read-only)."""
        return self.__version

    @property
    def conf_dir(self) -> Path:
        """Resolved configuration directory path (read-only).

        The directory is determined by the precedence rules:
        ``conf_dir`` argument, ``SCIETEX_CONFIG_DIR``, ``$XDG_CONFIG_HOME/scietex``,
        ``~/.config/scietex/``, ``/etc/scietex/``, ``/usr/local/etc/scietex/``,
        or ``./config/`` (CWD).
        """
        return self.__conf_dir

    @property
    def logger_handler_timeout(self) -> float:
        """Timeout in seconds for logger handler start/stop operations (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_LOGGER_HANDLER_TIMEOUT``; a non-``None`` value is validated
        against ``[MIN_LOGGER_HANDLER_TIMEOUT, MAX_LOGGER_HANDLER_TIMEOUT]`` at
        construction. Resolution happens eagerly in ``__init__`` (AR-080).
        """
        return self.__logger_handler_timeout

    @property
    def manager_shutdown_timeout(self) -> float:
        """Timeout in seconds for manager task shutdown operations (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_MANAGER_SHUTDOWN_TIMEOUT``; a non-``None`` value is validated
        against ``[MIN_MANAGER_SHUTDOWN_TIMEOUT, MAX_MANAGER_SHUTDOWN_TIMEOUT]``
        at construction. Resolution happens eagerly in ``__init__`` (AR-080).
        """
        return self.__manager_shutdown_timeout

    @property
    def manager_max_retries(self) -> int:
        """Maximum consecutive failures before a manager gives up (read-only).

        A ``None`` configuration value resolves to ``DEFAULT_MANAGER_MAX_RETRIES``;
        a non-``None`` value is validated against
        ``[MIN_MANAGER_MAX_RETRIES, MAX_MANAGER_MAX_RETRIES]`` at construction.
        Resolution happens eagerly in ``__init__`` (AR-080).
        """
        return self.__manager_max_retries

    @property
    def manager_restart_backoff(self) -> float:
        """Backoff delay in seconds between manager restart attempts (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_MANAGER_RESTART_BACKOFF``; a non-``None`` value is validated
        against ``[MIN_MANAGER_RESTART_BACKOFF, MAX_MANAGER_RESTART_BACKOFF]``
        at construction. Resolution happens eagerly in ``__init__`` (AR-080).
        """
        return self.__manager_restart_backoff

    @property
    def failed_managers(self) -> list[str]:
        """Names of managers that exhausted their retry budget and gave up (read-only)."""
        return self._manager_runtime.failed_managers

    @property
    def manager_runtime(self) -> ManagerRuntime:
        """The worker's manager runtime (read-only view).

        Exposes the ``ManagerRuntime`` so callers can inspect manager tasks
        and control individual managers (``start_manager``/``stop_manager``),
        statuses, and errors. The runtime's own methods are the documented
        API; this property only provides access to the runtime instance.
        """
        return self._manager_runtime

    @property
    def heartbeat_interval(self) -> float:
        """Interval in seconds between heartbeat calls (read-only).

        A ``None`` configuration value resolves to ``DEFAULT_HEARTBEAT_INTERVAL``;
        a non-``None`` value is validated against
        ``[MIN_HEARTBEAT_INTERVAL, MAX_HEARTBEAT_INTERVAL]`` at construction.
        Resolution happens eagerly in ``__init__`` (AR-080).
        """
        return self.__heartbeat_interval

    @property
    def active_ttl(self) -> float:
        """Lifetime in seconds of an ``active`` heartbeat entry (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_ACTIVE_TTL_MULTIPLIER × heartbeat_interval``; a non-``None``
        value is validated against ``[MIN_HEARTBEAT_TTL, MAX_HEARTBEAT_TTL]``
        and must exceed ``heartbeat_interval`` at construction. Resolution
        happens eagerly in ``__init__`` (AR-080).
        """
        return self.__active_ttl

    @property
    def inactive_ttl(self) -> float:
        """Lifetime in seconds of an ``inactive`` heartbeat entry (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_INACTIVE_TTL_MULTIPLIER × heartbeat_interval``; a non-``None``
        value is validated against ``[MIN_HEARTBEAT_TTL, MAX_HEARTBEAT_TTL]``
        at construction. Resolution happens eagerly in ``__init__`` (AR-080).
        """
        return self.__inactive_ttl

    @property
    def watchdog_interval(self) -> float:
        """Interval in seconds between watchdog checks (read-only).

        A ``None`` configuration value resolves to ``DEFAULT_WATCHDOG_INTERVAL``;
        a non-``None`` value is validated against
        ``[MIN_WATCHDOG_INTERVAL, MAX_WATCHDOG_INTERVAL]`` at construction.
        Resolution happens eagerly in ``__init__`` (AR-080).
        """
        return self.__watchdog_interval

    @property
    def start_time(self) -> datetime | None:
        """Timestamp when the service started running (read-only).

        UTC; ``None`` until the service has started and after it has been stopped.
        """
        return self._lifecycle.start_time

    @property
    def logger(self) -> logging.Logger:
        """Logger instance for the worker.

        The logger is named using the pattern ``{service_name}:{instance_id}``
        and is configured with a ``ConsoleHandler`` for async logging.
        """
        return self._logger

    @property
    def theme(self) -> Theme:
        """The rendering theme for the startup banner and console formatter (read-only)."""
        return self._theme

    @property
    def logging_level(self) -> int:
        """Current logging level for the worker (read-only).

        Parsed once from the worker's configuration at construction.
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
        self._signal_handler.setup()

    def _request_exit(self) -> None:
        """Spawn a single exit task, guarding against re-entry.

        Repeated signals must not each spawn their own exit() task. A pending
        stop task or an already-requested exit short-circuits so only one
        shutdown runs.
        """
        self._lifecycle.request_exit()

    def _remove_signal_handlers(self) -> None:
        """
        Remove the signal handlers registered for graceful shutdown.

        Mirrors ``_setup_signal_handlers`` and no-ops on platforms without
        ``loop.remove_signal_handler`` support (e.g. Windows).
        """
        self._signal_handler.remove()

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

        Waits for any previous shutdown to complete, prints the service banner,
        starts logging handlers, runs custom initialization via initialize(),
        sets the start time, then starts all managers, and transitions to
        RUNNING state. The start time is set before the managers start so the
        first immediate heartbeat is not skipped (AR-049).

        Raises:
            asyncio.CancelledError: If the startup process is cancelled. A
                failed ``initialize()`` is handled internally (the worker is
                stopped) rather than propagated.
        """
        try:
            if self._lifecycle.state != ServiceStatus.STOPPED:
                self.logger.log(logging.INFO, "Waiting for service shutdown complete.")
            self.logger.log(logging.INFO, "Service is starting up.")
            await self._lifecycle._wait_until_stopped()
            self.logger.log(logging.INFO, "Service is starting up.")
            self._lifecycle.transition(ServiceStatus.STARTING)
            print_banner(service_name=self.service_name, version=self.version, theme=self._theme)
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
            self._lifecycle.start_time = datetime.now(timezone.utc)

            # Start managers
            await self._manager_runtime.start_managers()

            # A concurrent stop() may have moved the lifecycle to STOPPING
            # while managers were starting (AR-106). The shutdown path now owns
            # teardown; do not overwrite its state with RUNNING.
            if self._lifecycle.state is not ServiceStatus.STARTING:
                self.logger.log(
                    logging.INFO,
                    "Startup superseded by a concurrent shutdown; yielding teardown to it.",
                )
                return

            self.logger.log(logging.DEBUG, "Worker %s:%s started", self.service_name, self.instance_id)
            self._lifecycle.transition(ServiceStatus.RUNNING)
        except asyncio.CancelledError:
            self.logger.log(logging.INFO, "Startup task canceled.")
            await self._stop_managers_best_effort()
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
        if self._lifecycle.state == ServiceStatus.RUNNING:
            self.logger.log(
                logging.WARNING,
                "Worker %s:%s is already running",
                self.service_name,
                self.instance_id,
            )
            return
        if self._lifecycle.state == ServiceStatus.STARTING:
            self.logger.log(
                logging.WARNING,
                "Worker %s:%s is already starting up",
                self.service_name,
                self.instance_id,
            )
            return
        if self._lifecycle.state in (ServiceStatus.STOPPING, ServiceStatus.STOPPED):
            self._setup_signal_handlers()
            asyncio.create_task(self._startup(), name="Start")

    def _force_stopped(self) -> None:
        """Force the worker into a terminal STOPPED state.

        Used by the startup/shutdown cancellation handlers so a cancelled task
        never strands the worker in STARTING or STOPPING, which would block a
        later start() (AR-017). If an exit was requested, surface it as a
        completed exit instead of leaving the exit event dangling.
        """
        self._lifecycle.force_stopped()

    async def _stop_managers_best_effort(self) -> None:
        """Stop running managers in reverse start order, tolerating cancellation.

        Called by the cancellation handlers in ``_startup``/``_shutdown``
        (AR-106): a cancelled orchestrator must not strand running managers
        under a terminal STOPPED state. A second cancellation raised while
        unwinding is swallowed so the caller's own re-raise still wins and the
        terminal transition still runs.
        """
        try:
            await self._manager_runtime.stop_managers(reverse=True)
        except asyncio.CancelledError:
            self.logger.log(logging.DEBUG, "Manager teardown interrupted by a second cancellation.")
        except Exception as exc:
            self.logger.log(logging.ERROR, "Error stopping managers during cancellation: %s", exc)

    async def _shutdown(self) -> None:
        """
        Stop the worker gracefully.

        Stops managers, unregisters the instance, runs cleanup, and shuts down
        the logging handlers. Invoked automatically on SIGINT or SIGTERM.
        """
        try:
            self.logger.debug("Stopping worker gracefully...")
            self._lifecycle.transition(ServiceStatus.STOPPING)
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
            self._lifecycle.start_time = None

            self._lifecycle.transition(ServiceStatus.STOPPED)

            if self._lifecycle.events["exit_requested"].is_set():
                self._lifecycle.events["exit_requested"].clear()
                self._lifecycle.events["exit"].set()
        except asyncio.CancelledError:
            self.logger.log(logging.ERROR, "Shutdown task cancelled")
            await self._stop_managers_best_effort()
            self._force_stopped()
            raise

    async def stop(self) -> None:
        """
        Request a graceful shutdown of the worker.

        If the worker is stopped or already stopping, returns after setting the
        ``exit`` event when an exit was requested (and clearing
        ``exit_requested``). Otherwise, creates a task to execute the full
        shutdown sequence (stop managers, cleanup, shut down loggers). Invoked
        automatically when SIGINT or SIGTERM is received.
        """
        if self._lifecycle.state == ServiceStatus.STOPPED:
            self.logger.log(
                logging.DEBUG,
                "Worker %s:%s is not running",
                self.service_name,
                self.instance_id,
            )
            if self._lifecycle.events["exit_requested"].is_set() and not self._lifecycle.events["exit"].is_set():
                self._lifecycle.events["exit_requested"].clear()
                self._lifecycle.events["exit"].set()
            self._remove_signal_handlers()
            return
        if self._lifecycle.state == ServiceStatus.STOPPING:
            self.logger.log(
                logging.DEBUG,
                "Worker %s:%s is already shutting down",
                self.service_name,
                self.instance_id,
            )
            if self._lifecycle.events["exit_requested"].is_set() and not self._lifecycle.events["exit"].is_set():
                self._lifecycle.events["exit_requested"].clear()
                self._lifecycle.events["exit"].set()
            return
        if self._lifecycle.state in (ServiceStatus.RUNNING, ServiceStatus.STARTING):
            self.logger.log(
                logging.DEBUG,
                "Worker %s:%s is going to SHUT DOWN",
                self.service_name,
                self.instance_id,
            )
            asyncio.create_task(self._shutdown(), name="Stop")

    async def exit(self):
        """Request exit, set ``exit_requested``, and trigger shutdown via ``stop()``.

        Returns immediately: ``stop()`` only spawns the shutdown task, so this
        method does not wait for the worker to stop. Await
        ``events["exit"].wait()`` to confirm the worker has fully stopped.
        """
        self._lifecycle.events["exit_requested"].set()
        await self.stop()

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

    @Manager(name="Heartbeat")
    async def _heartbeat_manager(self) -> None:
        """Manager body that invokes ``heartbeat()`` once per iteration.

        Calls ``heartbeat()`` immediately, then sleeps for ``heartbeat_interval``
        seconds. ``ManagerRuntime.run_manager`` repeats this body until cancelled.
        """
        await self.heartbeat()
        await asyncio.sleep(self.heartbeat_interval)

    @Manager(name="Watchdog")
    async def _watchdog_manager(self) -> None:
        """Manager body that invokes ``watchdog()`` once per iteration.

        Calls ``watchdog()`` immediately, then sleeps for ``watchdog_interval``
        seconds. ``ManagerRuntime.run_manager`` repeats this body until cancelled.
        """
        await self.watchdog()
        await asyncio.sleep(self.watchdog_interval)

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
