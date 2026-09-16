"""Task tracking-record store for the Valkey transport (AR-002).

Extracts the tracking/status subsystem that ``ValkeyWorker`` previously
inlined: writing ``running``/terminal ``TaskStatus`` records and updating
progress, each under a server-side-TTL key. Tracking is observability, not
correctness: every failure is logged and swallowed so it can never fail or
requeue the task itself.
"""

import logging
from datetime import datetime, timezone
from uuid import UUID

import msgspec
import msgspec.structs

from ..task_handler import CancelReason, TaskData, TaskProgress, TaskResult, TaskStatus
from ._glide import (
    ClientProvider,
    ExpirySet,
    ExpiryType,
    GlideConnectionError,
    GlideTimeoutError,
    RequestError,
)


class TaskStatusStore:
    """Writes per-task tracking records to Valkey.

    Each record lives under ``scietex:{service}:task:{task_id}`` with a TTL, so
    stale records expire rather than accumulating. The ``running`` record is
    written when a task starts; a terminal record overwrites it when the task
    completes (success, error, or cancellation); progress updates patch the
    ``running`` record in place.
    """

    def __init__(
        self,
        *,
        service_name: str,
        tracking_ttl: int,
        client_provider: ClientProvider,
        logger: logging.Logger,
    ) -> None:
        self._service_name = service_name
        self._tracking_ttl = tracking_ttl
        self._client_provider = client_provider
        self._logger = logger
        self._encoder = msgspec.msgpack.Encoder()

    def key(self, task_id: UUID) -> str:
        """Valkey key holding a task's tracking record."""
        return f"scietex:{self._service_name}:task:{task_id}"

    async def _write(self, tracking: TaskStatus) -> None:
        """Write a task tracking record, swallowing transport errors.

        Tracking is observability, not correctness: a failed write must never
        fail or requeue the task itself.
        """
        client = self._client_provider()
        if client is None:
            return
        try:
            await client.set(
                self.key(UUID(tracking.task_id)),
                value=self._encoder.encode(tracking),
                expiry=ExpirySet(ExpiryType.SEC, self._tracking_ttl),
            )
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self._logger.log(logging.WARNING, "Failed to write tracking for task %s: %s", tracking.task_id, exc)

    async def record_running(self, task_id: UUID, task_data: TaskData) -> None:
        """Publish a ``running`` tracking record when a task begins."""
        now = datetime.now(timezone.utc)
        await self._write(
            TaskStatus(
                task_id=str(task_id),
                service=self._service_name,
                task=task_data.task,
                status="running",
                progress=TaskProgress(),
                created_at=now,
                updated_at=now,
            )
        )

    async def record_terminal(
        self,
        task_id: UUID,
        task_data: TaskData | None,
        task_result: TaskResult | None,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        """Publish a terminal tracking record for a completed task.

        ``task_data`` is ``None`` only in unit tests that exercise the ack path
        in isolation, so fall back to an empty task name. ``task_result`` is
        ``None`` when the task was cancelled before producing a result. A
        deliberate ``cancel_task`` (``cancel_reason == "deliberate"``) writes
        ``status="cancelled"`` and embeds the original ``TaskData`` in the
        record; timeout/shutdown cancellations keep the ``failed`` status.
        """
        now = datetime.now(timezone.utc)
        task_name = task_data.task if task_data is not None else ""
        if task_result is None:
            deliberate = cancel_reason == "deliberate"
            await self._write(
                TaskStatus(
                    task_id=str(task_id),
                    service=self._service_name,
                    task=task_name,
                    status="cancelled" if deliberate else "failed",
                    data=task_data if deliberate else None,
                    error="canceled",
                    created_at=now,
                    updated_at=now,
                )
            )
        else:
            await self._write(
                TaskStatus(
                    task_id=str(task_id),
                    service=self._service_name,
                    task=task_name,
                    status="completed" if task_result.status == "success" else "failed",
                    result=task_result.payload if task_result.status == "success" else None,
                    error=task_result.error,
                    error_code=task_result.error_code,
                    created_at=now,
                    updated_at=now,
                )
            )

    async def update_progress(self, task_id: UUID, value: float) -> None:
        """Update the tracking record's progress for a running task."""
        client = self._client_provider()
        if client is None:
            return
        key = self.key(task_id)
        try:
            raw = await client.get(key)
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self._logger.log(logging.WARNING, "Failed to read tracking for task %s: %s", task_id, exc)
            return
        now = datetime.now(timezone.utc)
        if raw is None:
            current = TaskStatus(
                task_id=str(task_id),
                service=self._service_name,
                task="",
                status="running",
                created_at=now,
                updated_at=now,
            )
        else:
            try:
                current = msgspec.msgpack.decode(raw, type=TaskStatus)
            except msgspec.DecodeError:
                return
        updated = msgspec.structs.replace(
            current,
            progress=TaskProgress(progress=True, value=value),
            updated_at=now,
        )
        await self._write(updated)
