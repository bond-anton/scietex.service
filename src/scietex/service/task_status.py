"""Pure builders for per-task ``TaskStatus`` records (AR-114).

The field-population matrix for a task's lifecycle record — which fields carry
a payload, an error, or the embedded ``TaskData`` — is correctness-relevant and
shared by the Valkey tracking store and the MQTT status publisher. It lives
here once instead of being re-derived per transport.

The builders are pure: no I/O, no persistence, no publishing. Each caller owns
its own durability path (Valkey ``SET`` vs retained MQTT publish).
"""

from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from .task_handler.schemas import CancelReason, TaskData, TaskProgress, TaskResult, TaskStatus

__all__ = ["build_running_status", "build_terminal_status"]

_NonTerminalStatus = Literal["queued", "running"]


def build_running_status(
    task_id: UUID,
    service: str,
    task_data: TaskData,
    *,
    status: _NonTerminalStatus = "running",
    now: datetime | None = None,
    instance_id: str = "",
) -> TaskStatus:
    """Build the non-terminal record for a task that is queued or running.

    ``queued`` (accepted by the transport) and ``running`` (handler started)
    differ only in ``status``. A single timestamp stamps both ``created_at``
    and ``updated_at`` so they are equal. ``instance_id`` records the owning
    worker's instance id, empty when unknown.
    """
    now = now if now is not None else datetime.now(timezone.utc)
    return TaskStatus(
        task_id=str(task_id),
        service=service,
        task=task_data.task,
        status=status,
        progress=TaskProgress(),
        created_at=now,
        updated_at=now,
        instance_id=instance_id,
    )


def build_terminal_status(
    task_id: UUID,
    service: str,
    task_data: TaskData | None,
    task_result: TaskResult | None,
    cancel_reason: CancelReason | None = None,
    *,
    now: datetime | None = None,
    instance_id: str = "",
) -> TaskStatus:
    """Build the terminal record for a task that finished or was cancelled.

    - ``task_result is None`` (cancelled before a result): ``cancelled`` with
      the original ``TaskData`` embedded when ``cancel_reason == "deliberate"``;
      otherwise ``failed`` with no data. Both carry ``error="canceled"``.
    - a success result: ``completed`` with the result payload and the result's
      ``error``/``error_code``.
    - an error result: ``failed`` with no result payload and the result's
      ``error``/``error_code``.

    ``task_data`` is ``None`` only when a caller exercises the ack path in
    isolation; the task name then falls back to ``""``. ``instance_id`` records
    the owning worker's instance id, empty when unknown.
    """
    now = now if now is not None else datetime.now(timezone.utc)
    task_name = task_data.task if task_data is not None else ""
    if task_result is None:
        deliberate = cancel_reason == "deliberate"
        return TaskStatus(
            task_id=str(task_id),
            service=service,
            task=task_name,
            status="cancelled" if deliberate else "failed",
            progress=TaskProgress(),
            data=task_data if deliberate else None,
            error="canceled",
            created_at=now,
            updated_at=now,
            instance_id=instance_id,
        )
    success = task_result.status == "success"
    return TaskStatus(
        task_id=str(task_id),
        service=service,
        task=task_name,
        status="completed" if success else "failed",
        progress=TaskProgress(),
        result=task_result.payload if success else None,
        error=task_result.error,
        error_code=task_result.error_code,
        created_at=now,
        updated_at=now,
        instance_id=instance_id,
    )
