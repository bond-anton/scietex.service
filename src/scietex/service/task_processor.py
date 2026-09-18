"""
Asynchronous task processing worker for ``scietex.service``.

Provides ``TaskProcessor``, a concurrent task processing framework
built on ``BasicWorker`` with task queue management, timeout
monitoring (watchdog), handler dispatch, and graceful shutdown support.
"""

import asyncio
import logging
import os
import time
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, ClassVar, cast
from uuid import UUID

import msgspec

from .basic_worker import BasicWorker, ServiceStatus
from .config import (
    DEFAULT_MANAGER_SLEEP_TIME,
    DEFAULT_MAX_CONCURRENT_TASKS,
    DEFAULT_MAX_TASKS_QUEUE_SIZE,
    DEFAULT_TASK_CANCELLATION_TIMEOUT,
    DEFAULT_TASK_HANDLER_START_TIMEOUT,
    DEFAULT_TASK_HANDLER_STOP_TIMEOUT,
    DEFAULT_TASK_QUEUE_FETCH_TIMEOUT,
    DEFAULT_TASK_TIMEOUT,
    TaskProcessorConfig,
)
from .config_reload import (
    CONFIG_SOURCE_UNAVAILABLE,
    CONFIG_STORE_FAILED,
    RELOADABLE_FIELDS,
    REMOTE_CONFIG_DISABLED,
    ConfigApplyOutcome,
    ConfigReloader,
    ConfigSource,
    ConfigStoreOutcome,
    ReloadableSettings,
    write_local_config,
)
from .manager import Manager
from .task_handler import (
    CancelOutcome,
    CancelReason,
    CancelTaskHandler,
    ConfigApplyHandler,
    ConfigShowHandler,
    ConfigShowResponse,
    ConfigSourceLabel,
    ConfigStoreHandler,
    TaskCapabilities,
    TaskData,
    TaskHandler,
    TaskHandlerContext,
    TaskResult,
    TaskTracker,
)
from .task_lifecycle import TaskLifecycle
from .transport import InMemoryTransport, TaskTransport

#: AR-022 v4: the framework grants exactly one error-path retry per task id.
#: A second consecutive retryable failure is acked as terminal.
_MAX_TASK_RETRIES: int = 1


class TaskProcessor(BasicWorker):
    """
    Concurrent asynchronous task processor built on ``BasicWorker``.

    Extends the base worker with a task queue, handler dispatch, concurrent
    task execution, timeout monitoring via watchdog, and cleanup on shutdown.

    Subclasses should override:
        - ``fetch_tasks()``: Retrieve tasks from an external source.
        - ``return_task_to_queue()``: Re-queue tasks on cancellation/timeout.
        - ``cleanup()``: Service-specific cleanup logic.
        - ``initialize()``: Service-specific initialization logic.

    Properties:
        service_name (str): Name of the service (read-only).
        instance_id (str): Unique identifier for this worker instance (read-only).
        version (str): Version string of the service (read-only).
        logger (logging.Logger): Logger instance for the worker.
        logging_level (int): Current logging level (read-only).
        task_handlers (dict): Active handler key to handler instance mappings.
        queue_size (int): Maximum size of the internal task queue.
        max_concurrent_tasks (int): Maximum concurrent task count.
    """

    # Concrete config struct for this processor. The base stores it into
    # ``self._config``, so ``config=None`` instantiates the concrete type here
    # (AR-069) and no re-store / double-instantiation is needed.
    _config_type: ClassVar[type[TaskProcessorConfig]] = TaskProcessorConfig

    def __init__(self, config: TaskProcessorConfig | None = None, *, transport: TaskTransport | None = None):
        """
        Initialize the TaskProcessor.

        Args:
            config: A :class:`~scietex.service.config.TaskProcessorConfig`
                holding the worker's service identity and task-queue /
                handler-lifecycle settings. ``None`` uses the struct defaults.
                A ``None`` timing/count field resolves to its ``DEFAULT_*``
                constant at read time; an out-of-range value is rejected at
                construction.
            transport: The :class:`~scietex.service.transport.TaskTransport`
                owning the delivery/acknowledgement/drain hooks. ``None`` uses
                a working :class:`~scietex.service.transport.InMemoryTransport`
                that re-delivers requeued tasks on the next fetch. Subclasses
                (e.g. ``ValkeyWorker``) inject their own transport.
        """
        super().__init__(config)
        self._task_lifecycle = TaskLifecycle()
        # Error-path retry budget per task id (AR-022 v4: exactly one retry).
        # Keyed by the stable task id so it survives the requeue -> dequeue ->
        # re-handle cycle, unlike a per-attempt tracker.
        self._retry_attempts: dict[UUID, int] = {}
        # Transport extension seam (AR-072): the ordering-sensitive hooks below
        # delegate here. A bare processor gets a working in-memory transport;
        # subclasses swap it for their own at construction.
        self._transport: TaskTransport = transport if transport is not None else InMemoryTransport(logger=self.logger)
        # The base already stored the concrete config into ``self._config``
        # (AR-069); keep a typed local reference for the synchronous setup reads
        # below.
        cfg = cast(TaskProcessorConfig, self._config)

        self.__task_handlers_map: dict[str, tuple[type[TaskHandler], dict[str, object]]] = {}
        self.__task_handlers: dict[str, TaskHandler] = {}

        # Initialize queues and tracking structures
        self.__queue_size: int = cfg.queue_size if cfg.queue_size is not None else DEFAULT_MAX_TASKS_QUEUE_SIZE
        if cfg.max_concurrent_tasks is not None:
            self.__max_concurrent_tasks: int = cfg.max_concurrent_tasks
        elif cfg.auto_tune:
            cpu_count = os.cpu_count() or 1
            self.__max_concurrent_tasks: int = max(1, cpu_count)
            self.logger.log(
                logging.INFO,
                "Auto-tuned max_concurrent_tasks to %d (from %d CPUs)",
                self.__max_concurrent_tasks,
                cpu_count,
            )
        else:
            self.__max_concurrent_tasks: int = DEFAULT_MAX_CONCURRENT_TASKS

        # Resolve the task-level timing fields once: they are read in the
        # watchdog/task_manager hot loops, so per-iteration property indirection
        # is avoided. `is not None` (not `or`) preserves an explicit 0/negative
        # task_timeout as the "unbounded" sentinel instead of falling back to
        # the default.
        self.__task_timeout: float = cfg.task_timeout if cfg.task_timeout is not None else DEFAULT_TASK_TIMEOUT
        self.__task_queue_fetch_timeout: float = (
            cfg.task_queue_fetch_timeout
            if cfg.task_queue_fetch_timeout is not None
            else DEFAULT_TASK_QUEUE_FETCH_TIMEOUT
        )
        self.__task_cancellation_timeout: float = (
            cfg.task_cancellation_timeout
            if cfg.task_cancellation_timeout is not None
            else DEFAULT_TASK_CANCELLATION_TIMEOUT
        )

        self.__task_queue: asyncio.Queue[tuple[UUID, TaskData]] = asyncio.Queue(maxsize=self.queue_size)

        # Built-in cancellation handler. Registered here so every processor can
        # cancel its own tasks; the callback is a bound method, so the handler
        # stays transport-agnostic and never reaches into processor internals.
        self.add_task_handler(CancelTaskHandler, cancel=self._cancel_task)

        # Remote configuration channel. The reloader is transport-agnostic and
        # calls back into this processor through injected callables, so the
        # private shadows stay private. `_config_source` is attached by a
        # transport subclass (steps 6/7); `None` means "no source of truth".
        self._config_source: ConfigSource | None = None
        self._config_reloader = ConfigReloader(
            apply=self._apply_reloadable_config,
            current=self._current_reloadable_settings,
            restart_required=self._restart_required_fields,
            logger=self.logger,
            signing_key=cfg.config_signing_key,
            enabled=cfg.remote_config_enabled,
        )
        self.add_task_handler(ConfigApplyHandler, apply=self._config_apply)
        self.add_task_handler(ConfigStoreHandler, store=self._config_store)
        self.add_task_handler(ConfigShowHandler, show=self._config_show)

    @property
    def task_handlers(self) -> Mapping[str, TaskHandler]:
        """Dictionary of currently active (started) task handlers.

        Keys are the resolved handler keys — the ``name`` passed to
        ``add_task_handler`` when given, otherwise the handler class name —
        and values are the corresponding ``TaskHandler`` instances that have
        been initialized.

        Returns:
            A read-only mapping view of the active task handlers.
        """
        return MappingProxyType(self.__task_handlers)

    @property
    def running_tasks(self) -> Mapping[UUID, TaskTracker]:
        """Snapshot mapping of currently running tasks and their trackers.

        Returns a copy (not a live view) of the running trackers, delegated to
        ``TaskLifecycle``. Callers may safely iterate the snapshot while tasks
        are being removed or cancelled.
        """
        return self._task_lifecycle.trackers()

    @property
    def queue_size(self) -> int:
        """Maximum size of the internal task queue."""
        return self.__queue_size

    @property
    def max_concurrent_tasks(self) -> int:
        """Maximum number of tasks that can be processed concurrently."""
        return self.__max_concurrent_tasks

    @property
    def config_revision(self) -> int:
        """Revision of the last successfully applied remote config (read-only)."""
        return self._config_reloader.revision

    @property
    def config_hash(self) -> str:
        """Hash of the last successfully applied remote config (read-only)."""
        return self._config_reloader.hash

    @property
    def config_source(self) -> str:
        """Source label of the last successfully applied remote config (read-only)."""
        return self._config_reloader.source

    def register_config_settings(
        self,
        name: str,
        struct_type: type[msgspec.Struct],
        *,
        apply: Callable[[Any], None],
    ) -> None:
        """Register a custom service settings struct and its apply hook.

        Extension point for custom services: the reloadable surface grows with
        service-specific fields without the core knowing them. The registered
        struct is decoded against ``forbid_unknown_fields`` and its ``apply``
        hook is called (in registration order) after the core swap succeeds.
        Delegates to the reloader.

        Args:
            name: Section name used as the key in ``ConfigSections.services``.
            struct_type: The ``msgspec.Struct`` type to decode the section
                bytes against.
            apply: Hook called with the decoded struct during an apply.
        """
        self._config_reloader.register_section(name, struct_type, apply)

    def _current_reloadable_settings(self) -> ReloadableSettings:
        """Snapshot the current effective reloadable core settings.

        Reads the already-resolved sources (the re-shadowed fields and the
        live-read properties) rather than the raw config, so the snapshot
        reflects the effective values including auto-tuned concurrency and
        ``None``-resolved defaults.
        """
        return ReloadableSettings(
            max_concurrent_tasks=self.__max_concurrent_tasks,
            task_manager_sleep_time=self.task_manager_sleep_time,
            task_queue_manager_sleep_time=self.task_queue_manager_sleep_time,
            task_handler_start_timeout=self.task_handler_start_timeout,
            task_handler_stop_timeout=self.task_handler_stop_timeout,
            task_timeout=self.__task_timeout,
            task_queue_fetch_timeout=self.__task_queue_fetch_timeout,
            task_cancellation_timeout=self.__task_cancellation_timeout,
        )

    def _restart_required_fields(self) -> list[str]:
        """Return the concrete config's field names that are not reloadable."""
        return sorted(f.name for f in msgspec.structs.fields(type(self._config)) if f.name not in RELOADABLE_FIELDS)

    def _apply_reloadable_config(self, settings: ReloadableSettings) -> list[str]:
        """Validate-then-swap the reloadable core settings into the config.

        Builds a fresh concrete config by overlaying the eight reloadable
        values onto a shallow copy of the current config's fields, then
        constructs ``type(current)(**merged)`` so
        ``__post_init__``/``validate_range`` reject an out-of-range value
        before any mutation. Nested structs (e.g. ``valkey_config``) are
        preserved by reference, so ``msgspec.structs.asdict`` (which recurses
        into them) must not be used. Only after a valid candidate exists are
        the four shadows updated and the reference swapped.

        Args:
            settings: The complete snapshot of reloadable core values.

        Returns:
            The names of the reloadable fields whose value changed.
        """
        current = cast(TaskProcessorConfig, self._config)
        merged = {f.name: getattr(current, f.name) for f in msgspec.structs.fields(type(current))}
        merged.update(
            {
                "max_concurrent_tasks": settings.max_concurrent_tasks,
                "task_manager_sleep_time": settings.task_manager_sleep_time,
                "task_queue_manager_sleep_time": settings.task_queue_manager_sleep_time,
                "task_handler_start_timeout": settings.task_handler_start_timeout,
                "task_handler_stop_timeout": settings.task_handler_stop_timeout,
                "task_timeout": settings.task_timeout,
                "task_queue_fetch_timeout": settings.task_queue_fetch_timeout,
                "task_cancellation_timeout": settings.task_cancellation_timeout,
            }
        )
        candidate = type(current)(**merged)
        changed = [
            f.name
            for f in msgspec.structs.fields(type(current))
            if f.name in RELOADABLE_FIELDS and getattr(candidate, f.name) != getattr(current, f.name)
        ]

        # Update the shadows with the candidate's None-resolved values so the
        # hot loops (task_manager/watchdog) see the new settings immediately.
        # The reloadable values are always explicit, so the resolution here
        # degenerates to the value itself; it mirrors __init__ for safety.
        self.__max_concurrent_tasks = (
            candidate.max_concurrent_tasks
            if candidate.max_concurrent_tasks is not None
            else (max(1, os.cpu_count() or 1) if candidate.auto_tune else DEFAULT_MAX_CONCURRENT_TASKS)
        )
        self.__task_timeout = candidate.task_timeout if candidate.task_timeout is not None else DEFAULT_TASK_TIMEOUT
        self.__task_queue_fetch_timeout = (
            candidate.task_queue_fetch_timeout
            if candidate.task_queue_fetch_timeout is not None
            else DEFAULT_TASK_QUEUE_FETCH_TIMEOUT
        )
        self.__task_cancellation_timeout = (
            candidate.task_cancellation_timeout
            if candidate.task_cancellation_timeout is not None
            else DEFAULT_TASK_CANCELLATION_TIMEOUT
        )
        self._config = candidate
        return changed

    def _write_local_config(self) -> ConfigStoreOutcome:
        """Write the effective config to ``<conf_dir>/<config_file>``.

        Uses the reloader's atomic ``write_local_config``; a failure returns
        ``CONFIG_STORE_FAILED`` and leaves any previous file intact.
        """
        path = self.conf_dir / cast(TaskProcessorConfig, self._config).config_file
        try:
            write_local_config(path, self._config_reloader.show())
        except Exception as exc:
            self.logger.error("Failed to write local config %s: %s", path, exc)
            return ConfigStoreOutcome(
                stored=False,
                target="disk",
                path=str(path),
                revision=self._config_reloader.revision,
                hash=self._config_reloader.hash,
                error=str(exc),
                error_code=CONFIG_STORE_FAILED,
            )
        return ConfigStoreOutcome(
            stored=True,
            target="disk",
            path=str(path),
            revision=self._config_reloader.revision,
            hash=self._config_reloader.hash,
        )

    async def _config_apply(self, payload: bytes | None, persist: bool) -> ConfigApplyOutcome:
        """Apply a config envelope (inline or from the source of truth).

        Injected into ``ConfigApplyHandler``. A present ``payload`` is applied
        inline; ``payload=None`` re-reads the transport source, which requires
        a source to be attached (``CONFIG_SOURCE_UNAVAILABLE`` otherwise).
        ``persist`` additionally writes the local snapshot after a successful
        apply.
        """
        if payload is not None:
            outcome = await self._config_reloader.apply_envelope(payload, source="inline")
        else:
            source = self._config_source
            if source is None:
                return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_UNAVAILABLE)
            outcome = await self._config_reloader.reload(source)
        if persist and outcome.applied:
            self._write_local_config()
        return outcome

    async def _config_store(self, target: str) -> ConfigStoreOutcome:
        """Persist the effective config to the requested target.

        Injected into ``ConfigStoreHandler``. ``disk`` writes the local
        snapshot; ``remote`` publishes back to the transport source; ``both``
        does both. A remote target without an attached source is
        ``CONFIG_SOURCE_UNAVAILABLE``.
        """
        if target == "disk":
            return self._write_local_config()
        source = self._config_source
        if source is None:
            return ConfigStoreOutcome(stored=False, target=target, error_code=CONFIG_SOURCE_UNAVAILABLE)
        if target == "both":
            disk_outcome = self._write_local_config()
            if not disk_outcome.stored:
                return disk_outcome
        return await self._config_reloader.store(source, target=target)

    def _config_show(self, include_restart_required: bool) -> ConfigShowResponse:
        """Build the effective-config inspection response.

        Injected into ``ConfigShowHandler``. ``settings`` is the msgpack
        encoding of the reloader's ``ConfigSections`` (never secrets);
        ``restart_required_fields`` is only populated when requested. When the
        master switch is off, the response carries ``REMOTE_CONFIG_DISABLED``
        instead of the effective settings.
        """
        if not self._config_reloader.enabled:
            return ConfigShowResponse(
                error_code=REMOTE_CONFIG_DISABLED,
                error="remote config is disabled",
            )
        return ConfigShowResponse(
            settings=msgspec.msgpack.encode(self._config_reloader.show()),
            revision=self._config_reloader.revision,
            hash=self._config_reloader.hash,
            source=cast(ConfigSourceLabel, self._config_reloader.source),
            restart_required_fields=self._restart_required_fields() if include_restart_required else [],
        )

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
        """Sleep time in seconds between task manager loop iterations (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_MANAGER_SLEEP_TIME``; a non-``None`` value is validated against
        ``[MIN_MANAGER_SLEEP_TIME, MAX_MANAGER_SLEEP_TIME]`` at construction.
        """
        v = cast(TaskProcessorConfig, self._config).task_manager_sleep_time
        return v if v is not None else DEFAULT_MANAGER_SLEEP_TIME

    @property
    def task_queue_manager_sleep_time(self) -> float:
        """Sleep time in seconds between task queue manager loop iterations (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_MANAGER_SLEEP_TIME``; a non-``None`` value is validated against
        ``[MIN_MANAGER_SLEEP_TIME, MAX_MANAGER_SLEEP_TIME]`` at construction.
        """
        v = cast(TaskProcessorConfig, self._config).task_queue_manager_sleep_time
        return v if v is not None else DEFAULT_MANAGER_SLEEP_TIME

    @property
    def task_handler_start_timeout(self) -> float:
        """Timeout in seconds for starting task handlers (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_TASK_HANDLER_START_TIMEOUT``; a non-``None`` value is validated
        against
        ``[MIN_TASK_HANDLER_START_TIMEOUT, MAX_TASK_HANDLER_START_TIMEOUT]``
        at construction.

        Returns:
            The current task handler start timeout in seconds.
        """
        v = cast(TaskProcessorConfig, self._config).task_handler_start_timeout
        return v if v is not None else DEFAULT_TASK_HANDLER_START_TIMEOUT

    @property
    def task_handler_stop_timeout(self) -> float:
        """Timeout in seconds for stopping task handlers (read-only).

        A ``None`` configuration value resolves to
        ``DEFAULT_TASK_HANDLER_STOP_TIMEOUT``; a non-``None`` value is validated
        against
        ``[MIN_TASK_HANDLER_STOP_TIMEOUT, MAX_TASK_HANDLER_STOP_TIMEOUT]``
        at construction.

        Returns:
            The current task handler stop timeout in seconds.
        """
        v = cast(TaskProcessorConfig, self._config).task_handler_stop_timeout
        return v if v is not None else DEFAULT_TASK_HANDLER_STOP_TIMEOUT

    def add_task_handler(
        self,
        handler_class: type[TaskHandler],
        *,
        name: str | None = None,
        **handler_kwargs: object,
    ) -> None:
        """Register a task handler class.

        The handler class is stored in the internal map under its lifecycle
        key, which is the resolved handler name: ``name`` if given, otherwise
        ``handler_class.__name__``. Dispatch is driven by
        ``_find_task_handler``, which selects handlers by their
        ``supported_tasks`` membership — the class-level declaration is the
        dispatch contract, not an argument. One instance per registered key is
        created on start; the optional ``name`` lets multiple instances of a
        single class coexist under distinct keys (e.g. to split one class's
        task types across instances via name-derived ``supported_tasks``).
        Duplicate detection is on the resolved key.

        Args:
            handler_class: The ``TaskHandler`` subclass to register.
            name: Optional lifecycle key, defaulting to the handler class name.
                Enables multiple instances of one class under distinct keys.
            **handler_kwargs: Extra keyword arguments forwarded to the handler
                constructor on every instantiation. This is the per-instance
                state-injection channel: shared mutable objects (e.g. a shared
                counter or cache) outlive a single start/stop cycle, and
                constructor kwargs such as ``cancel=`` inject a per-instance
                callback. A misspelled kwarg raises a loud ``TypeError`` at
                construction, because ``TaskHandler`` subclasses do not accept
                arbitrary kwargs.

                Per-call capabilities are delivered separately: the processor
                constructs a ``TaskCapabilities`` object per task and passes it
                to ``handle``. Progress reporting goes through
                ``capabilities.report_progress(value)`` rather than through a
                constructor-injected callable.

        Raises:
            ValueError: If the resolved handler name is already registered.
        """
        handler_name = name or handler_class.__name__
        if handler_name in self.__task_handlers_map:
            raise ValueError(f"Task handler {handler_name!r} is already registered")
        self.__task_handlers_map[handler_name] = (handler_class, handler_kwargs)
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
        handler_class, handler_kwargs = self.__task_handlers_map[handler_name]
        context = TaskHandlerContext(
            service_name=self.service_name,
            instance_id=self.instance_id,
            logger=self.logger,
        )
        handler_instance = handler_class(handler_name, context, **handler_kwargs)
        self.__task_handlers[handler_name] = handler_instance
        try:
            await asyncio.wait_for(self.__task_handlers[handler_name].start(), timeout=self.task_handler_start_timeout)
        except asyncio.TimeoutError:
            self.logger.log(logging.ERROR, "Timeout while starting Task handler %s", handler_name)
            del self.__task_handlers[handler_name]
            return False
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to start Task handler %s: %s", handler_name, exc)
            del self.__task_handlers[handler_name]
            return False
        if not self.__task_handlers[handler_name].is_ready:
            self.logger.log(logging.ERROR, "Task handler %s failed to become ready", handler_name)
            del self.__task_handlers[handler_name]
            return False
        return True

    async def _stop_task_handler(self, handler_name: str) -> None:
        """Stop a running task handler and remove it from active handlers.

        Calls the handler's ``stop()`` method with a timeout and removes it
        from the active handlers dictionary on success. If ``stop()`` times
        out the handler is removed from the active handlers dictionary too
        (with a WARNING), so it is not left in an ambiguous tracked-but-stuck
        state; its ``stop()`` may still be finishing cleanup in the background.

        Args:
            handler_name: The name of the handler to stop.
        """
        if handler_name not in self.__task_handlers:
            self.logger.log(logging.DEBUG, "Task handler %s not found", handler_name)
            return
        # Perform cleanup before removal
        try:
            await asyncio.wait_for(self.__task_handlers[handler_name].stop(), timeout=self.task_handler_stop_timeout)
            self.__task_handlers.pop(handler_name, None)
        except asyncio.TimeoutError:
            # Do not leave the handler tracked-but-stuck: its stop() timed out,
            # so it is no longer reliably active. pop() guards against it having
            # already been removed concurrently.
            self.__task_handlers.pop(handler_name, None)
            self.logger.log(logging.WARNING, "Task handler %s removed after stop timeout", handler_name)

    def remove_task_handler(self, handler_name: str) -> None:
        """Remove a registered task handler.

        Stops the handler asynchronously if it is currently active, then
        removes it from the registration map so it is no longer dispatched
        to. Safe to call for a handler that is not registered.

        Args:
            handler_name: The class name of the handler to remove.
        """
        if handler_name in self.__task_handlers:
            asyncio.create_task(self._stop_task_handler(handler_name))
        if handler_name in self.__task_handlers_map:
            del self.__task_handlers_map[handler_name]
            self.logger.log(logging.INFO, "Removed handler: %s", handler_name)

    def _find_task_handler(self, task: str) -> TaskHandler | None:
        """Find an active handler that supports the given task type.

        Iterates over the active (started) task handlers and returns the first
        one whose ``supports(task_type)`` method returns ``True``. A handler
        that is registered but not yet started is not searched.

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
        need to be retried (e.g., writing back to a message queue). The
        default delegates to the transport's ``requeue`` hook.

        Args:
            task_id: The unique identifier of the task.
            task_data: The task data to return to the external queue.
        """
        await self._transport.requeue(task_id, task_data)

    def _remove_queued_task(self, task_id: UUID) -> TaskData | None:
        """Remove a queued-but-undispatched task from the internal queue.

        Drains the queue with the synchronous ``dequeue_task``/``enqueue_task``
        pair and re-enqueues everything except the target. There is no ``await``
        between the drain and the re-enqueue, so the operation is atomic with
        respect to the event loop: no task can be dispatched mid-drain.

        Args:
            task_id: Identifier of the queued task to remove.

        Returns:
            The removed task's data, or ``None`` if it was not queued.
        """
        removed: TaskData | None = None
        pending: list[tuple[UUID, TaskData]] = []
        while (item := self.dequeue_task()) is not None:
            if item[0] == task_id and removed is None:
                removed = item[1]
            else:
                pending.append(item)
        for item in pending:
            self.enqueue_task(*item)
        if removed is not None:
            # The removed item was counted by the queue's unfinished-task
            # counter when it was put; balance it here since handle_task will
            # never run for it.
            self.__task_queue.task_done()
        return removed

    async def _cancel_task(self, target_id: UUID) -> CancelOutcome:
        """Cancel a running or queued task by id.

        Injected into the built-in ``CancelTaskHandler``. A running target is
        cancelled with the same pattern as the watchdog (``cancel()`` plus
        ``asyncio.wait``, never ``wait_for``); a queued target is removed
        before it starts. A deliberate cancel is never requeued automatically —
        the external process decides whether to resubmit.

        Args:
            target_id: Identifier of the task to cancel.

        Returns:
            ``"cancelled"`` if the target stopped or was removed from the
            queue, ``"ignored"`` if a running target did not stop within the
            cancellation timeout, or ``"not_running"`` if the target is not
            running or queued (including a self-cancel request).
        """
        tracker = self._task_lifecycle.get(target_id)
        if tracker is not None and not tracker.worker_task.done():
            if tracker.worker_task is asyncio.current_task():
                # A task cannot cancel itself: the cancel handler runs inside
                # the target's own worker task.
                return "not_running"
            # Set the reason before cancel() with no intervening await, so
            # handle_task's finally always observes it (no TOCTOU).
            self._task_lifecycle.mark_cancelled(target_id, "deliberate")
            tracker.worker_task.cancel()
            # asyncio.wait, not wait_for (see watchdog for why): wait_for
            # re-cancels on its timeout and blocks on a handler that swallows
            # cancellation.
            await asyncio.wait(
                [tracker.worker_task],
                timeout=self.__task_cancellation_timeout,
            )
            if tracker.worker_task.done():
                return "cancelled"
            # The handler ignored cancellation and is still running. It will
            # acknowledge its entry when it eventually finishes; requeueing now
            # would run the task twice. Leave the entry pending.
            self.logger.log(
                logging.ERROR,
                "Task %s (%s) ignored cancellation; not requeueing to avoid duplicate work.",
                tracker.data.task,
                target_id,
            )
            return "ignored"

        queued_data = self._remove_queued_task(target_id)
        if queued_data is not None:
            # The target never started, so no handle_task will run for it:
            # write the terminal status directly.
            await self.on_task_completed(target_id, queued_data, None, cancel_reason="deliberate")
            return "cancelled"

        return "not_running"

    async def on_task_completed(
        self,
        task_id: UUID,
        task_data: TaskData,
        task_result: TaskResult | None,
        *,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        """Notify the transport that a task's processing has terminated.

        Called by ``handle_task`` when a task's work ends — on success, on
        a terminal error, or on cancellation — with the final
        ``TaskResult``, or ``None`` when the task was cancelled before
        producing a result. The default delegates to the transport's ``ack``
        hook, which acknowledges the transport entry so it is removed only
        after the handler's work on it is done (at-least-once).

        Args:
            task_id: Identifier of the task.
            task_data: The task data that was processed.
            task_result: The handler's result, or ``None`` on cancellation.
            cancel_reason: Why the task was cancelled, when it was. ``None``
                for a normal completion. ``"deliberate"`` marks an explicit
                ``cancel_task`` request; ``"timeout"``/``"shutdown"`` mark
                framework-driven cancellation.
        """
        await self._transport.ack(task_id, task_data, task_result, cancel_reason=cancel_reason)

    async def on_task_started(self, task_id: UUID, task_data: TaskData) -> None:
        """Hook invoked when a task begins processing.

        The default delegates to the transport's ``on_started`` hook, which
        publishes a ``running`` tracking record.
        """
        await self._transport.on_started(task_id, task_data)

    async def _write_task_progress(self, task_id: UUID, value: float) -> None:
        """Hook invoked when a handler reports granular progress.

        The default delegates to the transport's ``on_progress`` hook, which
        updates the tracking record.
        """
        await self._transport.on_progress(task_id, value)

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

    async def _on_queue_drain_task_processing(self, task_id: UUID, task_data: TaskData) -> None:
        """Handle a task still queued when the in-process queue is drained on shutdown.

        Called by ``cleanup()`` for every queued-but-undispatched task. The base
        default returns the task to its external source when ``canceled_action``
        is ``"requeue"``, so a non-durable transport (whose entries are not kept
        pending anywhere) does not silently lose work on shutdown (AR-041).

        Subclasses backed by a durable transport (e.g. ``ValkeyWorker``) override
        this to a no-op: their entries stay pending in the transport and are
        redelivered on restart, so re-enqueueing here would duplicate them.

        Args:
            task_id: Identifier of the queued task.
            task_data: The task data that was still queued at drain time.
        """
        await self._transport.on_drain(task_id, task_data)

    async def cleanup(self) -> None:
        """Release resources and stop processing before exit.

        Drains the in-process task queue (each queued task is handed to
        ``_on_queue_drain_task_processing``), cancels and requeues running
        tasks whose handlers actually stopped, and stops all task handlers.
        Subclasses add transport-specific teardown (e.g. closing a database
        or Valkey connection) by overriding this method and calling
        ``super().cleanup()``.
        """
        await super().cleanup()
        # Drain the in-process queue. Each queued task goes through
        # _on_queue_drain_task_processing: the base default requeues it when
        # canceled_action == "requeue" (a non-durable transport would otherwise
        # silently lose it on shutdown). A durable transport (e.g. a Valkey
        # stream) keeps entries pending and redelivers them on restart, so its
        # subclass overrides the hook to a no-op to avoid duplicating them.
        while not self.__task_queue.empty():
            task_id, task_data = self.__task_queue.get_nowait()
            await self._on_queue_drain_task_processing(task_id, task_data)
            self.__task_queue.task_done()
        self.logger.debug("Task queue is empty")

        # Cancel and requeue running tasks. A task is requeued only after its
        # handler has actually stopped (handle_task's finally acknowledges the
        # transport entry); a handler that ignores cancellation is left pending
        # so a restart redelivers it rather than running it twice.
        for task_id, task_tracker in self._task_lifecycle.trackers().items():
            if not task_tracker.worker_task.done():
                task_tracker.worker_task.cancel()
                self._task_lifecycle.mark_cancelled(task_id, "shutdown")
                # asyncio.wait, not wait_for (see watchdog for why): wait_for
                # re-cancels on its timeout and blocks on a handler that
                # swallows cancellation, hanging shutdown.
                await asyncio.wait(
                    [task_tracker.worker_task],
                    timeout=self.__task_cancellation_timeout,
                )
                if task_tracker.worker_task.done() and task_tracker.data.canceled_action == "requeue":
                    self.logger.log(logging.WARNING, "Task %s will be returned to queue.", task_id)
                    await self.return_task_to_queue(task_id, task_tracker.data)
        self.logger.debug("All tasks cancelled")

        # Cleanup task handlers
        for handler_name in self.__task_handlers_map:
            await self._stop_task_handler(handler_name)
        self.logger.debug("All task handlers cleaned up")

        # A task requeued but never re-handled before shutdown would otherwise
        # leave its retry budget behind; the budget is per-execution state.
        self._retry_attempts.clear()

    async def process_task(self, task_id: UUID, task_data: TaskData) -> TaskResult:
        """Process a single task by dispatching to the appropriate handler.

        Looks up a handler that supports the task type via
        ``_find_task_handler()`` and calls its ``handle()`` method.
        Returns a ``TaskResult`` with ``status="error"`` if no handler
        is found or an exception occurs.

        A handler that raises produces an error result marked permanent
        (``retryable=False``): an unhandled exception is unclassified, so
        it must not create an infinite requeue loop under retry-once. A
        handler that returns a ``TaskResult`` controls its own fields
        (``retryable=True`` opts into a single retry) and is passed
        through unchanged. Framework-level failures (empty ``task`` field,
        no matching handler) are permanent and leave ``retryable=False``.

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
                capabilities = TaskCapabilities(task_id=task_id, _write_progress=self._write_task_progress)
                result = await handler.handle(task_data, capabilities=capabilities)
            except Exception as e:
                # A handler raising is unclassified: treat it as permanent
                # (retryable=False) so an unhandled exception cannot create an
                # infinite requeue loop under retry-once. A handler that wants
                # a retry must return a retryable=True result explicitly.
                result = TaskResult(status="error", error=str(e))
        else:
            result = TaskResult(status="error", error=f"No handler found for task type '{task_type}'")

        self.logger.log(logging.DEBUG, "Task %s (%s) completed with result: %s", task_data, task_id, result)
        return result

    @Manager("TaskManager")
    async def task_manager(self):
        """Manage task processing from the internal task queue.

        Continuously fetches tasks from ``task_queue``, processes them
        via ``process_task()``, and tracks running tasks for timeout
        monitoring. Respects ``max_concurrent_tasks`` to limit parallel
        execution.

        This method is decorated with ``@Manager`` and runs as an
        infinite loop managed by ``BasicWorker``.
        """

        async def handle_task(t_id: UUID, t_data: TaskData):
            result: TaskResult | None = None
            try:
                await self.on_task_started(t_id, t_data)
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
                self._task_lifecycle.remove_tracker(t_id)
                self.__task_queue.task_done()
                # Retry-once (AR-022 v4): requeue a retryable error BEFORE
                # acking the transport entry (XADD then XACK), so the retry
                # copy is durable before the original is dropped. The framework
                # grants exactly one error-path retry per task id; the second
                # consecutive retryable failure is terminal. Permanent errors
                # and successes are acked and dropped without requeue. A
                # requeue failure is logged and the task is still acked (the
                # retry copy is lost, but the entry must not stay pending
                # forever).
                retry_scheduled = False
                ack_result = result
                if result is not None and result.status == "error" and result.retryable:
                    attempts = self._retry_attempts.get(t_id, 0)
                    if attempts < _MAX_TASK_RETRIES:
                        retry_scheduled = True
                        self._retry_attempts[t_id] = attempts + 1
                        try:
                            await self.return_task_to_queue(t_id, t_data)
                        except Exception as exc:
                            retry_scheduled = False
                            self.logger.log(
                                logging.ERROR,
                                "Failed to requeue retryable task %s (%s): %s",
                                t_data.task,
                                t_id,
                                exc,
                            )
                    else:
                        # The transports deliberately leave an entry pending for
                        # a retryable result (AR-077b), so the terminal ack must
                        # present retryable=False or the entry would wait for a
                        # retry that never comes.
                        ack_result = msgspec.structs.replace(result, retryable=False)
                        self.logger.log(
                            logging.WARNING,
                            "Task %s (%s) exhausted its single retry; acking as terminal.",
                            t_data.task,
                            t_id,
                        )
                if not retry_scheduled:
                    # Terminal for this id: drop the budget so the dict cannot
                    # grow without bound.
                    self._retry_attempts.pop(t_id, None)
                try:
                    # Ack the transport entry exactly when the handler's work
                    # on it ends (success, error, or cancellation). On
                    # CancelledError, result is None and the hook still runs.
                    # The cancel reason (if any) is popped here so the transport
                    # can distinguish a deliberate cancel from a timeout.
                    await self.on_task_completed(
                        t_id,
                        t_data,
                        ack_result,
                        cancel_reason=self._task_lifecycle.take_cancel_reason(t_id),
                    )
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

        if len(self._task_lifecycle.trackers()) < self.max_concurrent_tasks:
            try:
                task_id, task_data = await asyncio.wait_for(
                    self.__task_queue.get(), timeout=self.__task_queue_fetch_timeout
                )
                task = asyncio.create_task(handle_task(task_id, task_data))
                self._task_lifecycle.register(
                    task_id, TaskTracker(worker_task=task, data=task_data, started=time.monotonic())
                )
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(self.task_manager_sleep_time)

    async def fetch_tasks(self) -> bool:
        """Fetch tasks from external sources and enqueue them.

        Override this method in subclasses to implement the specific
        logic for retrieving tasks from external sources such as message
        queues, databases, or APIs, and enqueuing them via
        ``enqueue_task()`` as ``(UUID, TaskData)`` tuples. The default
        delegates to the transport's ``fetch`` hook.

        Returns:
            ``True`` if at least one task was enqueued, ``False`` otherwise.
            The caller (``task_queue_manager``) skips its idle backoff after a
            productive fetch so a backlog drains back-to-back. A subclass that
            does not report this (returns ``None``) is treated as ``False`` and
            always backs off, preserving the previous behavior.
        """
        return await self._transport.fetch(self)

    @Manager("TaskQueueManager")
    async def task_queue_manager(self):
        """Periodically fetch tasks from external sources into the task queue.

        Calls ``fetch_tasks()`` only when the queue is not full. After a
        productive fetch (``fetch_tasks()`` returned ``True``) the loop
        continues immediately so a backlog drains back-to-back; after an empty
        fetch it sleeps for ``task_queue_manager_sleep_time`` to prevent busy
        waiting. A full queue also backs off, providing backpressure.

        This method is decorated with ``@Manager`` and runs as an
        infinite loop managed by ``BasicWorker``.
        """
        if not self.__task_queue.full():
            fetched = await self.fetch_tasks()
            if not fetched:
                await asyncio.sleep(self.task_queue_manager_sleep_time)
        else:
            await asyncio.sleep(self.task_queue_manager_sleep_time)

    async def watchdog(self):
        """Monitor running tasks for timeouts and handle stalled tasks.

        Periodically checks all running tasks and cancels any that have
        exceeded their configured ``timeout`` (or the configured
        ``task_timeout`` if no timeout is set). Tasks with
        ``timeout_action="requeue"`` are returned to the external queue for
        potential retry.

        Requeue boundary: error-path requeue is handled in ``handle_task``
        (retry-once via ``TaskResult.retryable``); the watchdog handles only
        timeout-driven requeue via the task's ``timeout_action`` literal.
        The handler's final ``TaskResult`` lives in the ``handle_task``
        closure and is not accessible here.

        Override this method in subclasses to add additional watchdog
        logic. The default implementation handles task timeout detection
        and cancellation.
        """
        now = time.monotonic()
        for task_id, task_tracker in self._task_lifecycle.trackers().items():
            timeout = task_tracker.data.timeout.timeout
            if timeout is None:
                timeout = self.__task_timeout
            # A non-positive timeout means "no timeout": the watchdog never
            # cancels the task (timeout <= 0 is treated as unbounded).
            if timeout > 0 and (now - task_tracker.started) > timeout and not task_tracker.worker_task.done():
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
                self._task_lifecycle.mark_cancelled(task_id, "timeout")
                await asyncio.wait(
                    [task_tracker.worker_task],
                    timeout=self.__task_cancellation_timeout,
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
                # The tracker is removed unconditionally, even when the handler
                # ignored cancellation and the task is still alive: the cancel
                # reason must survive for handle_task's eventual ack, so only
                # the tracker is dropped here (remove_tracker leaves the reason
                # in place for the ack to consume).
                self._task_lifecycle.remove_tracker(task_id)
