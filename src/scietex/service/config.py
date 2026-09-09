"""Typed worker configuration objects for ``scietex.service``.

Provides ``WorkerConfig`` and ``TaskProcessorConfig`` — immutable
``msgspec.Struct`` classes that replace the constructor keyword arguments of
``BasicWorker`` and ``TaskProcessor``. The module-level
MIN/MAX/DEFAULT constants are the single source of truth for the timing and
retry bounds; the structs enforce them at construction (raising
``msgspec.ValidationError`` on an out-of-range value instead of silently
clamping).

A field with a ``None`` default means "use the DEFAULT constant at read time":
the config struct stays declarative while the worker resolves ``None`` to the
corresponding ``DEFAULT_*`` value when it reads the field.

The ``ValkeyWorkerConfig`` struct lives in :mod:`scietex.service.valkey.config`
(alongside ``ValkeyConfig``), not here, because its ``valkey_config`` field
references the optional ``glide.GlideClientConfiguration`` type and must not
force a glide dependency on the always-imported core package.
"""

import logging
from pathlib import Path

import msgspec

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

DEFAULT_MANAGER_MAX_RETRIES: int = 5
MIN_MANAGER_MAX_RETRIES: int = 0
MAX_MANAGER_MAX_RETRIES: int = 100

DEFAULT_MANAGER_RESTART_BACKOFF: float = 1
MIN_MANAGER_RESTART_BACKOFF: float = 0
MAX_MANAGER_RESTART_BACKOFF: float = 60

DEFAULT_MAX_TASKS_QUEUE_SIZE: int = 100
DEFAULT_MAX_CONCURRENT_TASKS: int = 10

DEFAULT_MANAGER_SLEEP_TIME: float = 0.01
MIN_MANAGER_SLEEP_TIME: float = 0.001
MAX_MANAGER_SLEEP_TIME: float = 1

MIN_TASK_HANDLER_START_TIMEOUT: float = 1
MAX_TASK_HANDLER_START_TIMEOUT: float = 60
DEFAULT_TASK_HANDLER_START_TIMEOUT: float = 5

MIN_TASK_HANDLER_STOP_TIMEOUT: float = 1
MAX_TASK_HANDLER_STOP_TIMEOUT: float = 60
DEFAULT_TASK_HANDLER_STOP_TIMEOUT: float = 5

MIN_TASK_TIMEOUT: float = 0.1
MAX_TASK_TIMEOUT: float = 3600
DEFAULT_TASK_TIMEOUT: float = 3

MIN_TASK_QUEUE_FETCH_TIMEOUT: float = 0.01
MAX_TASK_QUEUE_FETCH_TIMEOUT: float = 60
DEFAULT_TASK_QUEUE_FETCH_TIMEOUT: float = 1

MIN_TASK_CANCELLATION_TIMEOUT: float = 0.1
MAX_TASK_CANCELLATION_TIMEOUT: float = 60
DEFAULT_TASK_CANCELLATION_TIMEOUT: float = 5


def _validate_range(
    value: float | int | None,
    name: str,
    *,
    minimum: float | int,
    maximum: float | int | None = None,
    unbounded_ok: bool = False,
) -> None:
    """Raise ``msgspec.ValidationError`` if ``value`` is outside the bounds.

    ``None`` is always allowed (it means "use the default"). ``maximum`` may be
    ``None`` to enforce only a lower bound. When ``unbounded_ok`` is ``True``, a
    non-positive value is also allowed: it is the "unbounded" sentinel (e.g. the
    task timeout watchdog treats ``<= 0`` as "no timeout").

    Args:
        value: The value to validate.
        name: Field name used in the error message.
        minimum: Inclusive lower bound.
        maximum: Inclusive upper bound, or ``None`` for no upper bound.
        unbounded_ok: If ``True``, allow ``value <= 0`` as the unbounded
            sentinel, bypassing the lower-bound check.

    Raises:
        msgspec.ValidationError: If ``value`` is below ``minimum`` or above
            ``maximum``.
    """
    if value is None:
        return
    if unbounded_ok and value <= 0:
        return
    if value < minimum:
        raise msgspec.ValidationError(f"{name} must be >= {minimum}, got {value!r}")
    if maximum is not None and value > maximum:
        raise msgspec.ValidationError(f"{name} must be <= {maximum}, got {value!r}")


class WorkerConfig(msgspec.Struct, frozen=True):
    """Immutable configuration for a :class:`~scietex.service.basic_worker.BasicWorker`.

    All timing/retry fields are optional; ``None`` means "use the module
    ``DEFAULT_*`` constant" and is resolved by the worker at read time. A
    non-``None`` value outside the documented bounds raises
    ``msgspec.ValidationError`` at construction.

    Args:
        service_name: Name of the service, used for logging and identification.
        version: Version string of the service.
        conf_dir: Directory to use for configuration files. Accepts a ``str``
            or :class:`pathlib.Path`; the worker resolves it via
            ``prepare_conf_dir``.
        logging_level: Logging level as a string or integer constant (e.g.
            ``"DEBUG"`` or ``logging.DEBUG``). Parsed by the worker via
            ``parse_logging_level``; no bounds are enforced here.
        heartbeat_interval: Heartbeat interval in seconds (``[0.1, 600]``).
        watchdog_interval: Watchdog check interval in seconds (``[0.01, 600]``).
        logger_handler_timeout: Timeout for logger handler operations in
            seconds (``[1, 10]``).
        manager_shutdown_timeout: Timeout for manager shutdown in seconds
            (``[1, 10]``).
        manager_max_retries: Maximum consecutive manager failures before giving
            up (``[0, 100]``).
        manager_restart_backoff: Backoff between manager restarts in seconds
            (``[0, 60]``).
    """

    service_name: str = "service"
    version: str = "0.0.1"
    conf_dir: str | Path | None = None
    logging_level: int | str = logging.DEBUG
    heartbeat_interval: float | None = None
    watchdog_interval: float | None = None
    logger_handler_timeout: float | None = None
    manager_shutdown_timeout: float | None = None
    manager_max_retries: int | None = None
    manager_restart_backoff: float | None = None

    def __post_init__(self) -> None:
        _validate_range(
            self.heartbeat_interval,
            "heartbeat_interval",
            minimum=MIN_HEARTBEAT_INTERVAL,
            maximum=MAX_HEARTBEAT_INTERVAL,
        )
        _validate_range(
            self.watchdog_interval,
            "watchdog_interval",
            minimum=MIN_WATCHDOG_INTERVAL,
            maximum=MAX_WATCHDOG_INTERVAL,
        )
        _validate_range(
            self.logger_handler_timeout,
            "logger_handler_timeout",
            minimum=MIN_LOGGER_HANDLER_TIMEOUT,
            maximum=MAX_LOGGER_HANDLER_TIMEOUT,
        )
        _validate_range(
            self.manager_shutdown_timeout,
            "manager_shutdown_timeout",
            minimum=MIN_MANAGER_SHUTDOWN_TIMEOUT,
            maximum=MAX_MANAGER_SHUTDOWN_TIMEOUT,
        )
        _validate_range(
            self.manager_max_retries,
            "manager_max_retries",
            minimum=MIN_MANAGER_MAX_RETRIES,
            maximum=MAX_MANAGER_MAX_RETRIES,
        )
        _validate_range(
            self.manager_restart_backoff,
            "manager_restart_backoff",
            minimum=MIN_MANAGER_RESTART_BACKOFF,
            maximum=MAX_MANAGER_RESTART_BACKOFF,
        )


class TaskProcessorConfig(WorkerConfig, frozen=True):
    """Immutable configuration for an :class:`~scietex.service.task_processor.TaskProcessor`.

    Extends :class:`WorkerConfig` with the task-queue and handler-lifecycle
    fields. A field with a ``None`` default means "use the module ``DEFAULT_*``
    constant"; a non-``None`` out-of-range value raises
    ``msgspec.ValidationError`` at construction.

    Args:
        queue_size: Maximum size of the internal task queue (no bound).
        max_concurrent_tasks: Maximum concurrent task count (``>= 1``).
        auto_tune: If ``True`` and ``max_concurrent_tasks`` is ``None``, the
            worker derives the concurrency from the CPU count
            (``os.cpu_count()``) at startup instead of the static default.
            Explicit ``max_concurrent_tasks`` always wins. Default ``False``.
        task_manager_sleep_time: Sleep time between task-manager iterations in
            seconds (``[0.001, 1]``).
        task_queue_manager_sleep_time: Sleep time between task-queue-manager
            iterations in seconds (``[0.001, 1]``).
        task_handler_start_timeout: Timeout for starting a task handler in
            seconds (``[1, 60]``).
        task_handler_stop_timeout: Timeout for stopping a task handler in
            seconds (``[1, 60]``).
        task_timeout: Global per-task timeout in seconds used when a task's own
            ``TaskTimeout.timeout`` is ``None``. Bounds ``[0.1, 3600]`` for a
            positive deadline; ``<= 0`` means "no timeout" (unbounded — the
            watchdog never cancels the task).
        task_queue_fetch_timeout: Timeout in seconds for waiting to dequeue the
            next task (``[0.01, 60]``).
        task_cancellation_timeout: Timeout in seconds for waiting on a
            cancelled task to actually stop during cleanup/watchdog
            (``[0.1, 60]``).
    """

    queue_size: int | None = None
    max_concurrent_tasks: int | None = None
    auto_tune: bool = False
    task_manager_sleep_time: float | None = None
    task_queue_manager_sleep_time: float | None = None
    task_handler_start_timeout: float | None = None
    task_handler_stop_timeout: float | None = None
    task_timeout: float | None = None
    task_queue_fetch_timeout: float | None = None
    task_cancellation_timeout: float | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        _validate_range(self.max_concurrent_tasks, "max_concurrent_tasks", minimum=1)
        _validate_range(
            self.task_manager_sleep_time,
            "task_manager_sleep_time",
            minimum=MIN_MANAGER_SLEEP_TIME,
            maximum=MAX_MANAGER_SLEEP_TIME,
        )
        _validate_range(
            self.task_queue_manager_sleep_time,
            "task_queue_manager_sleep_time",
            minimum=MIN_MANAGER_SLEEP_TIME,
            maximum=MAX_MANAGER_SLEEP_TIME,
        )
        _validate_range(
            self.task_handler_start_timeout,
            "task_handler_start_timeout",
            minimum=MIN_TASK_HANDLER_START_TIMEOUT,
            maximum=MAX_TASK_HANDLER_START_TIMEOUT,
        )
        _validate_range(
            self.task_handler_stop_timeout,
            "task_handler_stop_timeout",
            minimum=MIN_TASK_HANDLER_STOP_TIMEOUT,
            maximum=MAX_TASK_HANDLER_STOP_TIMEOUT,
        )
        _validate_range(
            self.task_timeout,
            "task_timeout",
            minimum=MIN_TASK_TIMEOUT,
            maximum=MAX_TASK_TIMEOUT,
            unbounded_ok=True,
        )
        _validate_range(
            self.task_queue_fetch_timeout,
            "task_queue_fetch_timeout",
            minimum=MIN_TASK_QUEUE_FETCH_TIMEOUT,
            maximum=MAX_TASK_QUEUE_FETCH_TIMEOUT,
        )
        _validate_range(
            self.task_cancellation_timeout,
            "task_cancellation_timeout",
            minimum=MIN_TASK_CANCELLATION_TIMEOUT,
            maximum=MAX_TASK_CANCELLATION_TIMEOUT,
        )
