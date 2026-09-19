"""Task tracking-record store for the Valkey transport (AR-073).

Extracts the tracking/status subsystem that ``ValkeyWorker`` previously
inlined: writing ``running``/terminal ``TaskStatus`` records and updating
progress, each under a server-side-TTL key. Tracking is observability, not
correctness: every failure is logged and swallowed so it can never fail or
requeue the task itself.
"""

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import UUID

import msgspec
import msgspec.structs

from ..task_handler import CancelReason, TaskData, TaskProgress, TaskResult, TaskStatus
from ..task_status import build_running_status, build_terminal_status
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
        report_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._service_name = service_name
        self._tracking_ttl = tracking_ttl
        self._client_provider = client_provider
        self._logger = logger
        self._report_failure = report_failure
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
            if self._report_failure is not None:
                self._report_failure(exc)

    async def record_running(self, task_id: UUID, task_data: TaskData) -> None:
        """Publish a ``running`` tracking record when a task begins."""
        await self._write(build_running_status(task_id, self._service_name, task_data))

    async def record_terminal(
        self,
        task_id: UUID,
        task_data: TaskData | None,
        task_result: TaskResult | None,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        """Publish a terminal tracking record for a completed task.

        The field-population matrix is shared with the MQTT status publisher;
        see :func:`scietex.service.task_status.build_terminal_status` (AR-114).
        """
        await self._write(build_terminal_status(task_id, self._service_name, task_data, task_result, cancel_reason))

    async def update_progress(self, task_id: UUID, value: float) -> None:
        """Update the tracking record's progress for a running task.

        A progress update for a task with no tracking record is dropped: the
        store never fabricates a record, so a missing record stays missing
        rather than becoming a plausible-but-wrong ``running`` entry. The miss
        is logged at DEBUG. Transport errors are logged at WARNING and never
        fail or requeue the task.
        """
        client = self._client_provider()
        if client is None:
            return
        key = self.key(task_id)
        try:
            raw = await client.get(key)
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self._logger.log(logging.WARNING, "Failed to read tracking for task %s: %s", task_id, exc)
            if self._report_failure is not None:
                self._report_failure(exc)
            return
        now = datetime.now(timezone.utc)
        if raw is None:
            self._logger.log(
                logging.DEBUG,
                "No tracking record for task %s; dropping progress update",
                task_id,
            )
            return
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
