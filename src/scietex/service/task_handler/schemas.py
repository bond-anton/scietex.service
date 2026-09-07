"""Typed schemas for the task handler subsystem.

Provides frozen :class:`msgspec.Struct` definitions for task data,
results, timeout configuration, and task tracking so handlers and
processors can use consistent types for ``task_data`` and returned
results.
"""

from asyncio import Task
from datetime import datetime, timezone
from typing import Literal

import msgspec


class TaskTimeout(msgspec.Struct, frozen=True):
    """Configuration for task timeout behavior.

    Args:
        timeout: Maximum seconds allowed for task completion. ``None``
            means use ``DEFAULT_TASK_TIMEOUT`` (3s).
        timeout_action: Action when timeout is exceeded: ``"requeue"``
            returns the task to the queue; ``"discard"`` drops it.
    """

    timeout: float | None = None
    timeout_action: Literal["requeue", "discard"] = "requeue"


class TaskData(msgspec.Struct, frozen=True):
    """Immutable task payload passed to task handlers.

    Args:
        task: Task type string used to select a handler.
        timeout: Timeout configuration for this task.
        canceled_action: Action when task is canceled: ``"requeue"``
            or ``"discard"``.
        payload: Raw bytes payload associated with the task.
    """

    # The task identifier/type string used to select a handler.
    task: str
    timeout: TaskTimeout = TaskTimeout(timeout=None, timeout_action="requeue")
    canceled_action: Literal["requeue", "discard"] = "requeue"
    payload: bytes = b""


class TaskResult(msgspec.Struct, frozen=True):
    """Standardized result structure returned from task handlers.

    Args:
        status: ``"success"`` or ``"error"``.
        error: Error message string; empty on success.
        processed_at: UTC timestamp when the result was created.
        payload: Optional raw bytes payload from the handler.
        error_code: Structured error taxonomy code (e.g. ``"PERMANENT"``
            or ``"TRANSIENT"``, or a domain-specific code). Empty string
            means unset.
        retryable: The single retry signal: whether a failure is
            transient and may succeed on retry. ``True`` triggers the
            framework's one retry. A handler that raises is treated as
            permanent (``False``).
        partial: Whether partial progress was made before the error.

    All error-taxonomy fields are optional and default to "no extra
    information", so handlers that only set ``status`` and ``error``
    keep working unchanged.
    """

    status: Literal["success", "error"]
    error: str = ""
    processed_at: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
    payload: bytes = b""
    error_code: str = ""
    retryable: bool = False
    partial: bool = False


class TaskTracker(msgspec.Struct, frozen=True):
    """Tracks a running task's asyncio.Task, data, and start time.

    Used by ``AsyncTaskProcessor`` to monitor task progress, enforce
    timeouts, and manage cleanup on shutdown.

    Args:
        worker_task: The ``asyncio.Task`` executing this task.
        data: The ``TaskData`` associated with the task.
        started: Monotonic timestamp when the task was created.
    """

    worker_task: Task
    data: TaskData
    started: int | float
