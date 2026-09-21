"""Typed schemas for the task handler subsystem.

Provides frozen :class:`msgspec.Struct` definitions for task data,
results, timeout configuration, and task status so handlers and
processors can use consistent types for ``task_data`` and returned
results.
"""

from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

import msgspec

#: Task name that selects the built-in cancellation handler.
CANCEL_TASK_NAME: str = "task:cancel"

#: Task name that selects the built-in remote-config apply handler.
CONFIG_APPLY_TASK_NAME: str = "config:apply"

#: Task name that selects the built-in remote-config store handler.
CONFIG_STORE_TASK_NAME: str = "config:store"

#: Task name that selects the built-in remote-config show handler.
CONFIG_SHOW_TASK_NAME: str = "config:show"

#: Task name that selects the built-in worker start handler.
WORKER_START_TASK_NAME: str = "worker:start"

#: Task name that selects the built-in worker stop handler.
WORKER_STOP_TASK_NAME: str = "worker:stop"

#: Task name that selects the built-in worker restart handler.
WORKER_RESTART_TASK_NAME: str = "worker:restart"

#: Task name that selects the built-in worker exit handler.
WORKER_EXIT_TASK_NAME: str = "worker:exit"

#: The canonical enumeration of built-in control-plane task names. This is a
#: catalogue, not a routing table: a task reaches the control lane because it
#: was delivered on a control channel, and it is served because its handler
#: declares ``control = True``. Nothing consults this set to decide routing.
#: ``task:cancel`` and the ``worker:*`` handlers are always registered; the
#: ``config:*`` handlers exist only when remote config is enabled.
CONTROL_TASK_NAMES: frozenset[str] = frozenset(
    {
        CANCEL_TASK_NAME,
        CONFIG_APPLY_TASK_NAME,
        CONFIG_STORE_TASK_NAME,
        CONFIG_SHOW_TASK_NAME,
        WORKER_START_TASK_NAME,
        WORKER_STOP_TASK_NAME,
        WORKER_RESTART_TASK_NAME,
        WORKER_EXIT_TASK_NAME,
    }
)

#: Why a running task was cancelled. Only ``"deliberate"`` (an explicit
#: ``task:cancel`` request) produces a ``cancelled`` status; ``"timeout"`` and
#: ``"shutdown"`` keep the existing ``failed`` status.
CancelReason = Literal["deliberate", "timeout", "shutdown"]


class TaskTimeout(msgspec.Struct, frozen=True):
    """Configuration for task timeout behavior.

    Args:
        timeout: Maximum seconds allowed for task completion. ``None``
            means use the processor's configured ``task_timeout`` (default 3 s).
        timeout_action: Action when timeout is exceeded: ``"requeue"``
            returns the task to the queue; ``"discard"`` drops it.
    """

    timeout: float | None = None
    timeout_action: Literal["requeue", "discard"] = "requeue"


class TaskData(msgspec.Struct, frozen=True):
    """Immutable task payload passed to task handlers.

    Args:
        task_id: Unique task identifier (string form of a UUID). Required —
            a payload without it is rejected at decode, which is how pre-v5
            wire payloads are refused.
        task: Task type string used to select a handler.
        timeout: Timeout configuration for this task.
        canceled_action: Action when task is canceled: ``"requeue"``
            or ``"discard"``. Applies to shutdown drain and running-task
            cleanup only; a deliberate ``task:cancel`` is never requeued
            automatically — the external process decides.
        payload: Raw bytes payload associated with the task.
    """

    # Required, no default: a pre-v5 payload (no task_id) must fail decode
    # rather than silently decode with a bogus id.
    task_id: str
    task: str
    timeout: TaskTimeout = TaskTimeout(timeout=None, timeout_action="requeue")
    canceled_action: Literal["requeue", "discard"] = "requeue"
    payload: bytes = b""


def task_data_id(task_data: TaskData) -> UUID:
    """In-process UUID identity of a task.

    The wire id is a string (``TaskData.task_id``); the in-process machinery
    (lifecycle, leases, tracking keys) is UUID-keyed. This is the single
    conversion point between the two.
    """
    return UUID(task_data.task_id)


class TaskEnvelope(msgspec.Struct, frozen=True):
    """Versioned transport envelope wrapping a serialized task payload.

    The durable wire format is this envelope, not ``TaskData`` directly, so
    the transport format can evolve independently of the in-process handler
    contract (AR-064). ``data`` holds the serialized ``TaskData`` (or a
    future version's payload) for the given ``version``.

    ``version`` stays ``1``: the envelope structure is unchanged. The payload
    schema changed in v5 — ``data`` is ``msgpack(TaskData)`` where ``TaskData``
    now requires ``task_id``. Pre-v5 payloads (without ``task_id``) fail the
    inner decode and are rejected.

    Args:
        version: Wire-format version. ``1`` wraps a msgpack-encoded
            ``TaskData``.
        data: The serialized task payload bytes for ``version``.
    """

    version: int = 1
    data: bytes = b""


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
            framework's one retry; a second consecutive retryable
            failure is acked as terminal. A handler that raises is
            treated as permanent (``False``).
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


class TaskProgress(msgspec.Struct, frozen=True):
    """Granular progress reported by a task handler.

    ``progress`` is False when the handler does not report granular progress;
    ``value`` is only meaningful when ``progress`` is True.
    """

    progress: bool = False
    value: float = 0.0


class TaskStatus(msgspec.Struct, frozen=True):
    """Per-task tracking record published to the transport.

    Written as ``queued`` when the task is accepted (by the submitter under the
    Valkey split, or by the worker itself for MQTT), overwritten by the worker
    as ``running`` when the task starts and as ``completed``/``failed`` when it
    finishes. A deliberate ``task:cancel`` request produces ``cancelled`` and
    embeds the original :class:`TaskData` in ``data`` so an external process
    can read it, modify it, and resubmit under a new task id.

    ``instance_id`` is the owning worker's instance id, empty when unknown
    (e.g. a record built without an owner).
    """

    task_id: str
    service: str
    task: str
    status: Literal["queued", "running", "completed", "failed", "cancelled"]
    progress: TaskProgress = TaskProgress()
    result: bytes | None = None
    data: TaskData | None = None
    error: str = ""
    error_code: str = ""
    created_at: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
    instance_id: str = ""
