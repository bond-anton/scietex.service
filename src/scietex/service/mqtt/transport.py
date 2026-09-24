"""MQTT transport for ``MqttWorker`` (v4.4.0).

Implements the core :class:`~scietex.service.transport.TaskTransport` contract
over MQTT, where at-least-once delivery is restored by the durable
:class:`~scietex.service.mqtt.inbox.MqttInbox` rather than by broker-level
acknowledgement, which aiomqtt v2.5.1 does not expose (design §3).

Beyond delivery, the transport publishes each task's lifecycle — retained
``TaskStatus`` messages and throttled ``TaskProgress`` messages to per-task
topics — as fire-and-forget observability (design §13). It also publishes a
retained per-task owner marker (design §10.1) so a submitter can resolve which
worker owns a task via
:class:`~scietex.service.mqtt.control.MqttControlPublisher.resolve_owner`. A
publish failure is logged and reported to the connection-health supervisor,
never raised into the task path.

Every collaborator is received by injection, so the transport holds no
ownership over the inbox, health supervisor, or connection: those remain the
worker's and are only reached through here.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

import msgspec

from ..health import TransportHealth
from ..task_handler.schemas import (
    CancelReason,
    TaskData,
    TaskProgress,
    TaskResult,
    TaskStatus,
    task_data_id,
)
from ..task_handler.wire import encode_task_envelope
from ..task_status import build_running_status, build_terminal_status
from ..transport import RecoverableTransport, TaskSink
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


class MqttTransport(RecoverableTransport):
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

    When the ``queued`` status is published — the first ownership-establishing
    write — the transport also publishes a retained owner marker to
    ``{status_topic_prefix}/{task_id}/owner`` carrying the worker's
    ``instance_id`` (design §10.1). The owner marker is what a submitter reads
    back through ``MqttControlPublisher.resolve_owner`` to find where to direct a
    ``task:cancel``.
    """

    def __init__(
        self,
        *,
        config: MqttWorkerConfig,
        service_name: str,
        topic: str,
        status_topic_prefix: str | None = None,
        inbox: MqttInbox,
        control_inbox: MqttInbox,
        health: TransportHealth,
        publish: MqttPublish,
        logger: logging.Logger,
        instance_id: str,
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
        # The owner topic (design §10.1) nests under the status prefix — it is
        # the same resolved prefix, named separately so the ownership marker's
        # address is explicit in ``_publish_owner`` rather than implicit.
        self._owner_topic_prefix = self._status_topic_prefix
        self._inbox = inbox
        # Control lane (design §5.1): an in-memory store, drained independently
        # of the data inbox so control delivery never consults data backpressure.
        self._control_inbox = control_inbox
        # Connection-health supervisor (design §13.7): the status/progress
        # publish helpers report failures here, so the dependency is
        # load-bearing rather than API parity with ``ValkeyTransport``.
        self._health = health
        self._publish = publish
        self._logger = logger
        self._instance_id = instance_id
        self._clock = clock
        self._encoder = msgspec.msgpack.Encoder()

        # Task ids handed to the sink but not yet terminal. The inbox snapshot
        # returns every non-terminal entry, so without this the drain would
        # re-enqueue an already-queued task on every poll. An id is added on
        # enqueue accept and discarded on ack/on_drain.
        self._enqueued: set[UUID] = set()
        # Control ids handed to the sink but not yet terminal, mirroring
        # ``_enqueued`` for the control inbox. Kept separate so a control entry
        # owned by the control inbox routes its ack/on_started to that inbox.
        self._control_enqueued: set[UUID] = set()
        # Per-task progress-coalescing state (design §13.5), keyed by task id.
        # In-process only: a restart loses it, which is correct because a
        # restart also resets the in-flight task set.
        self._progress: dict[UUID, _ProgressThrottle] = {}

    async def _publish_status(self, record: TaskStatus) -> None:
        """Publish a retained ``TaskStatus`` to the task's status topic.

        The record is built by the shared core builders
        (:mod:`scietex.service.task_status`), so both transports populate exactly
        the same fields (AR-114); this method only owns the publish. Publishes at
        ``status_qos`` with ``retain=True``. A failure is logged at WARNING and
        reported to the health supervisor, never raised.
        """
        if not self._config.status_publish_enabled:
            return
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
                f"{self._status_topic_prefix}/{record.task_id}/status",
                self._encoder.encode(record),
                self._config.status_qos,
                retain=True,
                properties=properties,
            )
        except Exception as exc:
            self._logger.log(
                logging.WARNING,
                "Failed to publish %s status for task %s: %s",
                record.status,
                record.task_id,
                exc,
            )
            self._health.report_failure(exc)
        if record.status == "queued":
            await self._publish_owner(record.task_id)

    async def _publish_owner(self, task_id: str) -> None:
        """Publish the retained owner marker for a task (design §10.1).

        The owner topic maps a task id to the worker that owns it, so a submitter
        can resolve where to direct a ``task:cancel``
        (:meth:`scietex.service.mqtt.control.MqttControlPublisher.resolve_owner`).
        Published only at the ``queued`` transition — the first ownership-
        establishing write — so the marker is written once per task delivery, not
        on the later ``running``/terminal writes or on every progress tick. Every
        ``running`` write is preceded by a ``queued`` write from the same worker
        (fetch, recovery, control enqueue, and requeue all publish ``queued``
        before ``on_started``), so the marker is always established before a task
        can be observed as running. The payload is the worker's ``instance_id`` as
        raw UTF-8 bytes, published retained at ``status_qos`` with the same
        ``status_ttl`` expiry as the status, so a stale marker ages out. A failure
        is logged at WARNING and reported to the health supervisor, never raised,
        exactly like the status it follows.
        """
        properties = None
        if self._config.status_ttl is not None:
            properties = Properties(PacketTypes.PUBLISH)
            properties.MessageExpiryInterval = self._config.status_ttl
        try:
            await self._publish(
                f"{self._owner_topic_prefix}/{task_id}/owner",
                self._instance_id.encode(),
                self._config.status_qos,
                retain=True,
                properties=properties,
            )
        except Exception as exc:
            self._logger.log(
                logging.WARNING,
                "Failed to publish owner marker for task %s: %s",
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

    async def _enqueue_control(self, task_id: UUID, task_data: TaskData, sink: TaskSink) -> bool:
        """Enqueue one control task and advertise it, bypassing data backpressure.

        Control never consults ``task_queue_full`` (design §5.1): the in-process
        control lane has its own concurrency ceiling, so the only rejection
        signal is ``enqueue_control_task`` returning ``False`` (control lane
        full). On accept the id is recorded in ``_control_enqueued`` so ``ack``/
        ``on_started`` route to the control inbox, and a ``queued`` status is
        published.

        Returns:
            ``True`` when the sink accepted the task (and it was newly
            enqueued); ``False`` when the control lane is full and the entry
            stays pending for the next poll.
        """
        if not sink.enqueue_control_task(task_data):
            return False
        self._control_enqueued.add(task_id)
        await self._publish_status(
            build_running_status(task_id, self._service_name, task_data, status="queued", instance_id=self._instance_id)
        )
        return True

    async def fetch(self, sink: TaskSink) -> bool:
        """Fetch inbox entries and enqueue them into ``sink``.

        On the first call, replays every non-terminal inbox entry
        (:meth:`recover_pending_tasks`) before draining, so tasks persisted by a
        previous run are redelivered exactly once.

        The drain is split (design §5.1): the control inbox is drained first,
        independently of data backpressure, then the data inbox is drained with
        existing data backpressure. Data tasks stop at backpressure: a rejected
        data task is left pending in the inbox (not recorded as enqueued), so it
        is redelivered, never lost. Each data entry is claimed before enqueue
        (cross-process mutual exclusion on a shared store); a lost claim is
        skipped without setting backpressure. Each accepted task is advertised
        as ``queued`` (design §13.4).

        Returns:
            ``True`` if at least one task was enqueued (from recovery or either
            drain), ``False`` otherwise.
        """
        enqueued = await self.ensure_recovered(sink)
        # Control first: a saturated data lane must not delay a control command
        # at the transport layer (design §5.1). A rejected control entry (its
        # lane is full) stays pending for the next poll.
        for task_data in await self._control_inbox.pending():
            task_id = task_data_id(task_data)
            if task_id in self._control_enqueued:
                continue
            if await self._enqueue_control(task_id, task_data, sink):
                enqueued = True
        data_blocked = False
        for task_data in await self._inbox.pending():
            task_id = task_data_id(task_data)
            if task_id in self._enqueued:
                continue
            if data_blocked or sink.task_queue_full():
                data_blocked = True
                continue
            # Cross-process claim: a peer draining the same shared store may own
            # this entry. A lost claim is not local backpressure, so it must not
            # set data_blocked -- leave the entry for the next poll.
            if not await self._inbox.claim(task_id):
                continue
            if not sink.enqueue_task(task_data):
                await self._inbox.release(task_id)  # a full queue leaves no claim
                data_blocked = True
                continue
            self._enqueued.add(task_id)
            await self._publish_status(
                build_running_status(
                    task_id, self._service_name, task_data, status="queued", instance_id=self._instance_id
                )
            )
            enqueued = True
        return enqueued

    async def recover_pending_tasks(self, sink: TaskSink) -> tuple[bool, bool]:
        """Re-enqueue inbox entries left non-terminal by a previous run.

        Replays every entry :meth:`~MqttInbox.recover` returns (oldest first)
        into ``sink``, so tasks that were persisted but never acknowledged
        before a crash are redelivered (at-least-once). Called once from the
        first :meth:`fetch`, before any new drain, when no tasks are in flight.

        Both inboxes are recovered (design §5.1): the control inbox first, with
        no backpressure, then the data inbox. A task id already enqueued this
        run is skipped, so an interrupted recovery retries only the remainder.
        When the data lane is full the stop is immediate for data tasks and
        recovery reports incomplete, so the next poll retries. Each accepted
        task is advertised as ``queued`` (design §13.4), so a task redelivered
        after a restart re-advertises itself.

        Returns:
            A ``(recovery_complete, enqueued)`` tuple. ``recovery_complete`` is
            ``False`` when a full sink interrupted *data* recovery; control
            recovery never sets it false because control bypasses backpressure.
        """
        enqueued = False
        # Control recovery first: never blocked by data backpressure, so a
        # control entry persisted before a crash is replayed unconditionally.
        for task_data in await self._control_inbox.recover():
            task_id = task_data_id(task_data)
            if task_id in self._control_enqueued:
                continue
            if await self._enqueue_control(task_id, task_data, sink):
                enqueued = True
        data_blocked = False
        for task_data in await self._inbox.recover():
            task_id = task_data_id(task_data)
            if task_id in self._enqueued:
                continue
            if data_blocked or sink.task_queue_full():
                data_blocked = True
                continue
            # Cross-process claim: a peer may already own this recovered entry.
            # A lost claim is not local backpressure (see ``fetch``).
            if not await self._inbox.claim(task_id):
                continue
            if not sink.enqueue_task(task_data):
                await self._inbox.release(task_id)
                data_blocked = True
                continue
            self._enqueued.add(task_id)
            await self._publish_status(
                build_running_status(
                    task_id, self._service_name, task_data, status="queued", instance_id=self._instance_id
                )
            )
            enqueued = True
        return (not data_blocked), enqueued

    async def requeue(self, task_data: TaskData) -> None:
        """Re-queue a task by re-publishing it to the task topic.

        Encodes ``task_data`` into a versioned transport envelope (msgpack) and
        publishes it to the resolved task topic at ``task_qos``, so the broker
        redelivers it. The task id travels inside the encoded ``TaskData``, so
        no user property is needed on the re-published copy. The inbox entry is
        left non-terminal, so it is also redelivered by recovery after a crash.

        The task is then re-advertised as ``queued`` (design §13.4), and its
        progress throttle is dropped without flushing: the fresh run that
        starts on redelivery should not inherit a stale progress value. The
        cross-process claim is released so the re-published copy is immediately
        claimable (mirrors Valkey's lease delete on requeue).
        """
        task_id = task_data_id(task_data)
        await self._publish(
            self._topic,
            encode_task_envelope(task_data),
            self._config.task_qos,
        )
        if task_id not in self._control_enqueued:
            await self._inbox.release(task_id)
        self._progress.pop(task_id, None)
        await self._publish_status(
            build_running_status(task_id, self._service_name, task_data, status="queued", instance_id=self._instance_id)
        )

    async def on_started(self, task_data: TaskData) -> None:
        """Record that a task began processing (the owning inbox entry is in-flight).

        Routes to the control inbox when the id was enqueued from the control
        inbox, otherwise to the data inbox (design §5.1). The task is advertised
        as ``running`` and its progress throttle is reset so a re-delivered task
        starts clean (design §13.4).
        """
        task_id = task_data_id(task_data)
        if task_id in self._control_enqueued:
            await self._control_inbox.mark_in_flight(task_id)
        else:
            await self._inbox.mark_in_flight(task_id)
        self._progress.pop(task_id, None)
        await self._publish_status(
            build_running_status(task_id, self._service_name, task_data, instance_id=self._instance_id)
        )

    async def ack(
        self,
        task_data: TaskData,
        task_result: TaskResult | None,
        *,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        """Mark the owning inbox entry terminal once the handler's work is done.

        ``task_result`` is ``None`` when the task was cancelled before producing
        a result. Marking terminal writes a tombstone that dedupes any
        re-delivered copy of this task id, and releases the in-process enqueued
        marker. The owning inbox is chosen by which enqueued set holds the id
        (design §5.1).

        On the non-retryable path, any coalesced progress value is flushed and a
        terminal ``TaskStatus`` is published first (design §13.4). The retryable
        path publishes nothing: the task is not terminal, and ``requeue`` has
        already advertised it as ``queued``.
        """
        task_id = task_data_id(task_data)
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
        await self._publish_status(
            build_terminal_status(
                task_id, self._service_name, task_data, task_result, cancel_reason, instance_id=self._instance_id
            )
        )
        self._progress.pop(task_id, None)
        # Mark terminal (persist the tombstone) before releasing the in-process
        # claim, so a crash mid-ack redelivers rather than loses the task. The
        # owning inbox is chosen by which enqueued set holds the id (design §5.1).
        if task_id in self._control_enqueued:
            await self._control_inbox.mark_terminal(task_id)
            self._control_enqueued.discard(task_id)
        else:
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

    async def on_drain(self, task_data: TaskData) -> None:
        """Release the in-process claim for a drained task without re-enqueueing it.

        The owning inbox entry is left non-terminal (the broker still holds the
        message), so a restart redelivers it via recovery; re-publishing here
        would duplicate it (the MQTT analogue of AR-041). The in-process claim
        is released from the owning enqueued set (design §5.1), and the
        cross-process claim is released so a peer can pick the entry up. No
        status is published and the throttle state is dropped: the task is
        neither terminal nor restarted (design §13.4).
        """
        task_id = task_data_id(task_data)
        if task_id in self._control_enqueued:
            self._control_enqueued.discard(task_id)
        else:
            self._enqueued.discard(task_id)
            await self._inbox.release(task_id)
        self._progress.pop(task_id, None)

    async def refresh_leases(self) -> None:
        """Renew the cross-process claims on this worker's enqueued data tasks.

        Exists for parity with ``ValkeyTransport.refresh_leases``, which the
        worker's watchdog calls unconditionally. For the ``"sqlite"`` backend
        this extends each live claim's lease so a slow-but-alive worker keeps
        its entries; for ``"memory"``/``"none"`` it is a no-op. Control ids are
        not refreshed: the control inbox is per-instance and unclaimed.
        """
        await self._inbox.refresh(self._enqueued)
