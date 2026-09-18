"""MQTT transport for ``MqttWorker`` (v4.4.0).

Implements the core :class:`~scietex.service.transport.TaskTransport` contract
over MQTT, where at-least-once delivery is restored by the durable
:class:`~scietex.service.mqtt.inbox.MqttInbox` rather than by broker-level
acknowledgement, which aiomqtt v2.5.1 does not expose (design §3).

Beyond delivery, the transport publishes each task's lifecycle — retained
``TaskStatus`` messages and throttled ``TaskProgress`` messages to per-task
topics — as fire-and-forget observability (design §13). A publish failure is
logged and reported to the connection-health supervisor, never raised into the
task path.

Every collaborator is received by injection, so the transport holds no
ownership over the inbox, health supervisor, or connection: those remain the
worker's and are only reached through here.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol
from uuid import UUID

import msgspec

from ..health import TransportHealth
from ..task_handler.schemas import CancelReason, TaskData, TaskProgress, TaskResult, TaskStatus
from ..task_handler.wire import encode_task_envelope
from ..transport import TaskSink
from ._aiomqtt import PacketTypes, Properties
from .config import MqttWorkerConfig
from .inbox import MqttInbox

__all__ = ["MqttPublish", "MqttTransport"]


# The worker owns the aiomqtt connection, so the publish seam is this injected
# callable rather than the client. The signature mirrors ``Client.publish``'s
# ``(topic, payload, qos)``, extended with keyword-only ``retain`` and
# ``properties`` flags so retained status can carry an MQTT 5 message-expiry
# property while the envelope requeue (never retained, no expiry) stays a
# plain ``(topic, payload, qos)`` call.
class MqttPublish(Protocol):
    async def __call__(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        *,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> None: ...


#: ``TaskStatus.status`` values this transport emits.
_StatusValue = Literal["queued", "running", "completed", "failed", "cancelled"]


@dataclass
class _ProgressThrottle:
    """Per-task progress-coalescing state (design §13.5).

    ``last_value``/``last_at`` record the most recent published tick so
    ``on_progress`` can drop high-frequency reports below the publish seam;
    ``pending`` coalesces the newest un-published value (the newest wins, no
    stale backlog). ``ever_published`` forces a task's first tick to always go
    out, so a subscriber sees progress begin.
    """

    last_value: float = 0.0
    last_at: float = 0.0
    ever_published: bool = False
    pending: float | None = None


class MqttTransport:
    """MQTT implementation of the core ``TaskTransport`` contract.

    Drains the durable inbox into the processor's queue, re-publishes tasks
    that need redelivery, and marks inbox entries terminal only after a handler
    finishes, so delivery is at-least-once even though the broker acks a message
    before the handler runs (design §3.1).

    Each lifecycle hook also publishes the task's retained ``TaskStatus`` and
    throttled ``TaskProgress`` to per-task topics when status publishing is
    enabled (design §13). Status and progress are observability: a publish
    failure is logged and reported to the health supervisor, never raised.
    Each retained status carries an MQTT 5 message-expiry property (``status_ttl``)
    so the broker ages out stale per-task markers instead of keeping one forever.
    """

    def __init__(
        self,
        *,
        config: MqttWorkerConfig,
        service_name: str,
        topic: str,
        status_topic_prefix: str | None = None,
        inbox: MqttInbox,
        health: TransportHealth,
        publish: MqttPublish,
        logger: logging.Logger,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._service_name = service_name
        self._topic = topic
        # The worker resolves the configured prefix (substituting ``{service}``)
        # and passes it in; falling back to the configured prefix keeps the
        # transport usable when constructed directly (tests, embedders).
        self._status_topic_prefix = (
            status_topic_prefix
            if status_topic_prefix is not None
            else config.status_topic_prefix.format(service=service_name)
        )
        self._inbox = inbox
        # Connection-health supervisor (design §13.7): the status/progress
        # publish helpers report failures here, so the dependency is
        # load-bearing rather than API parity with ``ValkeyTransport``.
        self._health = health
        self._publish = publish
        self._logger = logger
        self._clock = clock
        self._encoder = msgspec.msgpack.Encoder()

        # True once pending-entry recovery has run (start of the first fetch),
        # so a crash's unacked entries are redelivered exactly once.
        self.recovered: bool = False
        # Task ids handed to the sink but not yet terminal. The inbox snapshot
        # returns every non-terminal entry, so without this the drain would
        # re-enqueue an already-queued task on every poll. An id is added on
        # enqueue accept and discarded on ack/release/on_drain.
        self._enqueued: set[UUID] = set()
        # Per-task progress-coalescing state (design §13.5), keyed by task id.
        # In-process only: a restart loses it, which is correct because a
        # restart also resets the in-flight task set.
        self._progress: dict[UUID, _ProgressThrottle] = {}

    async def _publish_status(
        self,
        task_id: UUID,
        task_data: TaskData,
        status: _StatusValue,
        *,
        result: bytes | None = None,
        data: TaskData | None = None,
        error: str = "",
        error_code: str = "",
    ) -> None:
        """Publish a retained ``TaskStatus`` to the task's status topic.

        Builds the record with the same field semantics as
        ``TaskStatusStore.record_running``/``record_terminal`` (design §13.3),
        then publishes it at ``status_qos`` with ``retain=True`` so the broker
        keeps the latest status per task. A failure is logged at WARNING and
        reported to the health supervisor, never raised.
        """
        if not self._config.status_publish_enabled:
            return
        now = datetime.now(timezone.utc)
        record = TaskStatus(
            task_id=str(task_id),
            service=self._service_name,
            task=task_data.task,
            status=status,
            progress=TaskProgress(),
            result=result,
            data=data,
            error=error,
            error_code=error_code,
            created_at=now,
            updated_at=now,
        )
        # Build the expiry property per publish: a retained status with no TTL
        # (status_ttl=None) publishes no properties, exactly as before this
        # feature. The property is a fresh instance each publish because paho
        # Properties objects are not reusable across sends.
        properties = None
        if self._config.status_ttl is not None:
            properties = Properties(PacketTypes.PUBLISH)
            properties.MessageExpiryInterval = self._config.status_ttl
        try:
            await self._publish(
                f"{self._status_topic_prefix}/{task_id}/status",
                self._encoder.encode(record),
                self._config.status_qos,
                retain=True,
                properties=properties,
            )
        except Exception as exc:
            self._logger.log(
                logging.WARNING,
                "Failed to publish %s status for task %s: %s",
                status,
                task_id,
                exc,
            )
            self._health.report_failure(exc)

    async def _publish_progress(self, task_id: UUID, value: float) -> None:
        """Publish a non-retained ``TaskProgress`` tick to the progress topic.

        Progress is high-frequency and never retained (design §13.2), so a
        failure is logged at DEBUG — WARNING would be noise — but is still
        reported to the health supervisor so the connection failure is seen.
        """
        try:
            await self._publish(
                f"{self._status_topic_prefix}/{task_id}/progress",
                self._encoder.encode(TaskProgress(progress=True, value=value)),
                self._config.progress_qos,
            )
        except Exception as exc:
            self._logger.log(
                logging.DEBUG,
                "Failed to publish progress for task %s: %s",
                task_id,
                exc,
            )
            self._health.report_failure(exc)

    async def fetch(self, sink: TaskSink) -> bool:
        """Fetch inbox entries and enqueue them into ``sink``.

        On the first call, replays every non-terminal inbox entry
        (:meth:`recover_pending_tasks`) before draining, so tasks persisted by a
        previous run are redelivered exactly once. The drain then walks the
        inbox's non-terminal snapshot, skipping task ids already handed over
        this run and stopping on backpressure: a rejected task is left pending
        in the inbox (not recorded as enqueued), so it is redelivered, never
        lost. Each accepted task is advertised as ``queued`` (design §13.4).

        Returns:
            ``True`` if at least one task was enqueued (from recovery or the
            drain), ``False`` otherwise.
        """
        enqueued = False
        if not self.recovered:
            # Only mark recovery done when the pending list was fully drained;
            # a queue-full interruption is retried on the next poll (AR-051).
            recovery_complete, recovered_enqueued = await self.recover_pending_tasks(sink)
            if recovery_complete:
                self.recovered = True
            enqueued = recovered_enqueued
        for task_id, task_data in await self._inbox.pending():
            if task_id in self._enqueued:
                continue
            if sink.task_queue_full():
                break
            if not sink.enqueue_task(task_id, task_data):
                # Queue is full; leave the inbox entry pending (not recorded as
                # enqueued) so the next poll redelivers it. Never block intake.
                self._logger.log(logging.DEBUG, "Task queue full; deferring task %s", task_id)
                break
            self._enqueued.add(task_id)
            await self._publish_status(task_id, task_data, "queued")
            enqueued = True
        return enqueued

    async def recover_pending_tasks(self, sink: TaskSink) -> tuple[bool, bool]:
        """Re-enqueue inbox entries left non-terminal by a previous run.

        Replays every entry :meth:`~MqttInbox.recover` returns (oldest first)
        into ``sink``, so tasks that were persisted but never acknowledged
        before a crash are redelivered (at-least-once). Called once from the
        first :meth:`fetch`, before any new drain, when no tasks are in flight.

        A task id already enqueued this run is skipped, so an interrupted
        recovery (a queue-full stop leaves earlier accepted entries in the
        enqueued set and later ones still pending) retries only the remainder.
        When the queue is full the stop is immediate and recovery reports
        incomplete, so the next poll retries. Each accepted task is advertised
        as ``queued`` (design §13.4), so a task redelivered after a restart
        re-advertises itself.

        Returns:
            A ``(recovery_complete, enqueued)`` tuple.
        """
        enqueued = False
        for task_id, task_data in await self._inbox.recover():
            if task_id in self._enqueued:
                continue
            if sink.task_queue_full():
                return False, enqueued
            if not sink.enqueue_task(task_id, task_data):
                self._logger.log(
                    logging.DEBUG,
                    "Task queue full during recovery; deferring task %s",
                    task_id,
                )
                return False, enqueued
            self._enqueued.add(task_id)
            await self._publish_status(task_id, task_data, "queued")
            enqueued = True
        return True, enqueued

    async def requeue(self, task_id: UUID, task_data: TaskData) -> None:
        """Re-queue a task by re-publishing it to the task topic.

        Encodes ``task_data`` into a versioned transport envelope (msgpack) and
        publishes it to the resolved task topic at ``task_qos``, so the broker
        redelivers it. The inbox entry is left non-terminal, so it is also
        redelivered by recovery after a crash.

        The task is then re-advertised as ``queued`` (design §13.4), and its
        progress throttle is dropped without flushing: the fresh run that
        starts on redelivery should not inherit a stale progress value.
        """
        await self._publish(self._topic, encode_task_envelope(task_data), self._config.task_qos)
        self._progress.pop(task_id, None)
        await self._publish_status(task_id, task_data, "queued")

    async def release(self, task_id: UUID) -> None:
        """Release this task's in-process claim without re-publishing it.

        The inbox entry stays non-terminal (the broker still holds the message
        and a restart replays it), so only the in-process enqueued marker is
        dropped. No status is published; the throttle state is dropped because
        the task returns to a not-yet-started state (design §13.4).
        """
        self._enqueued.discard(task_id)
        self._progress.pop(task_id, None)

    async def on_started(self, task_id: UUID, task_data: TaskData) -> None:
        """Record that a task began processing (the inbox entry is in-flight).

        The task is advertised as ``running`` and its progress throttle is reset
        so a re-delivered task starts clean (design §13.4).
        """
        await self._inbox.mark_in_flight(task_id)
        self._progress.pop(task_id, None)
        await self._publish_status(task_id, task_data, "running")

    async def ack(
        self,
        task_id: UUID,
        task_data: TaskData,
        task_result: TaskResult | None,
        *,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        """Mark the inbox entry terminal once the handler's work on it is done.

        ``task_result`` is ``None`` when the task was cancelled before producing
        a result. Marking terminal writes a tombstone that dedupes any
        re-delivered copy of this task id, and releases the in-process enqueued
        marker.

        On the non-retryable path, any coalesced progress value is flushed and a
        terminal ``TaskStatus`` is published first (design §13.4). The retryable
        path publishes nothing: the task is not terminal, and ``requeue`` has
        already advertised it as ``queued``.
        """
        # A retryable error was already re-published by ``requeue`` with the
        # SAME task id; writing a tombstone here would suppress that retry copy
        # when it arrives (inbox.put skips tombstoned ids), silently losing the
        # retry (AR-077b mirror). Leave the entry non-terminal so the retry is
        # accepted and redelivered.
        if task_result is not None and task_result.status == "error" and task_result.retryable:
            self._enqueued.discard(task_id)
            self._progress.pop(task_id, None)
            return
        # Flush any coalesced progress tick before the terminal status, so a
        # completion is preceded by the final reported value even when that
        # value fell inside the throttle window (design §13.5).
        throttle = self._progress.get(task_id)
        if throttle is not None and throttle.pending is not None:
            await self._publish_progress(task_id, throttle.pending)
        if task_result is None:
            # Mirrors TaskStatusStore.record_terminal: a None result means the
            # task was cancelled; a deliberate cancel embeds the original
            # TaskData, while timeout/shutdown stay ``failed`` (design §13.3).
            deliberate = cancel_reason == "deliberate"
            await self._publish_status(
                task_id,
                task_data,
                "cancelled" if deliberate else "failed",
                data=task_data if deliberate else None,
                error="canceled",
            )
        else:
            await self._publish_status(
                task_id,
                task_data,
                "completed" if task_result.status == "success" else "failed",
                result=task_result.payload if task_result.status == "success" else None,
                error=task_result.error,
                error_code=task_result.error_code,
            )
        self._progress.pop(task_id, None)
        # Mark terminal (persist the tombstone) before releasing the in-process
        # claim, so a crash mid-ack redelivers rather than loses the task.
        await self._inbox.mark_terminal(task_id)
        self._enqueued.discard(task_id)

    async def on_progress(self, task_id: UUID, value: float) -> None:
        """Publish a throttled ``TaskProgress`` tick (design §13.5).

        Progress is coalesced below the publish seam: a tick publishes on the
        first report for a task, when the interval or delta threshold is met, or
        when both thresholds are disabled; otherwise the newest value is kept as
        ``pending`` and published on the next eligible tick or flushed on a
        terminal ``ack``. A no-op when status publishing is disabled.
        """
        if not self._config.status_publish_enabled:
            return
        now = self._clock()
        throttle = self._progress.get(task_id)
        if throttle is None:
            throttle = _ProgressThrottle()
            self._progress[task_id] = throttle
        min_interval = self._config.progress_min_interval
        min_delta = self._config.progress_min_delta
        should_publish = (
            not throttle.ever_published
            or (min_interval > 0 and now - throttle.last_at >= min_interval)
            or (min_delta > 0 and abs(value - throttle.last_value) >= min_delta)
            or (min_interval <= 0 and min_delta <= 0)
        )
        if should_publish:
            await self._publish_progress(task_id, value)
            throttle.last_value = value
            throttle.last_at = now
            throttle.ever_published = True
            throttle.pending = None
        else:
            throttle.pending = value

    async def on_drain(self, task_id: UUID, task_data: TaskData) -> None:
        """Release the in-process claim for a drained task without re-enqueueing it.

        The inbox entry is left non-terminal (the broker still holds the
        message), so a restart redelivers it via recovery; re-publishing here
        would duplicate it (the MQTT analogue of AR-041). No status is
        published and the throttle state is dropped: the task is neither
        terminal nor restarted (design §13.4).
        """
        self._enqueued.discard(task_id)
        self._progress.pop(task_id, None)

    async def refresh_leases(self) -> None:
        """No-op: the file-backed inbox has no per-entry leases to renew.

        Exists for parity with ``ValkeyTransport.refresh_leases``, which the
        worker's watchdog calls unconditionally. MQTT entries are protected by
        the durable inbox (persist-before-enqueue), not by expiring leases.
        """
