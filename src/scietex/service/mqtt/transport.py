"""MQTT transport for ``MqttWorker`` (v4.4.0).

Implements the core :class:`~scietex.service.transport.TaskTransport` contract
over MQTT, where at-least-once delivery is restored by the durable
:class:`~scietex.service.mqtt.inbox.MqttInbox` rather than by broker-level
acknowledgement, which aiomqtt v2.5.1 does not expose (design §3).

Every collaborator is received by injection, so the transport holds no
ownership over the inbox, health supervisor, or connection: those remain the
worker's and are only reached through here.
"""

import logging
from collections.abc import Awaitable, Callable
from uuid import UUID

from ..health import TransportHealth
from ..task_handler.schemas import CancelReason, TaskData, TaskResult
from ..task_handler.wire import encode_task_envelope
from ..transport import TaskSink
from .config import MqttWorkerConfig
from .inbox import MqttInbox

__all__ = ["MqttPublish", "MqttTransport"]

# The worker owns the aiomqtt connection, so the publish seam is this injected
# callable rather than the client. The signature mirrors ``Client.publish``'s
# ``(topic, payload, qos)``.
MqttPublish = Callable[[str, bytes, int], Awaitable[None]]


class MqttTransport:
    """MQTT implementation of the core ``TaskTransport`` contract.

    Drains the durable inbox into the processor's queue, re-publishes tasks
    that need redelivery, and marks inbox entries terminal only after a handler
    finishes, so delivery is at-least-once even though the broker acks a message
    before the handler runs (design §3.1).
    """

    def __init__(
        self,
        *,
        config: MqttWorkerConfig,
        service_name: str,
        topic: str,
        inbox: MqttInbox,
        health: TransportHealth,
        publish: MqttPublish,
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._service_name = service_name
        self._topic = topic
        self._inbox = inbox
        # Held for API parity with ``ValkeyTransport`` and for the worker's
        # message loop, which reports connection failures; the inbox-draining
        # hooks below perform no network I/O of their own.
        self._health = health
        self._publish = publish
        self._logger = logger

        # True once pending-entry recovery has run (start of the first fetch),
        # so a crash's unacked entries are redelivered exactly once.
        self.recovered: bool = False
        # Task ids handed to the sink but not yet terminal. The inbox snapshot
        # returns every non-terminal entry, so without this the drain would
        # re-enqueue an already-queued task on every poll. An id is added on
        # enqueue accept and discarded on ack/release/on_drain.
        self._enqueued: set[UUID] = set()

    async def fetch(self, sink: TaskSink) -> bool:
        """Fetch inbox entries and enqueue them into ``sink``.

        On the first call, replays every non-terminal inbox entry
        (:meth:`recover_pending_tasks`) before draining, so tasks persisted by a
        previous run are redelivered exactly once. The drain then walks the
        inbox's non-terminal snapshot, skipping task ids already handed over
        this run and stopping on backpressure: a rejected task is left pending
        in the inbox (not recorded as enqueued), so it is redelivered, never
        lost.

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
        incomplete, so the next poll retries.

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
            enqueued = True
        return True, enqueued

    async def requeue(self, task_id: UUID, task_data: TaskData) -> None:
        """Re-queue a task by re-publishing it to the task topic.

        Encodes ``task_data`` into a versioned transport envelope (msgpack) and
        publishes it to the resolved task topic at ``task_qos``, so the broker
        redelivers it. The inbox entry is left non-terminal, so it is also
        redelivered by recovery after a crash.
        """
        await self._publish(self._topic, encode_task_envelope(task_data), self._config.task_qos)

    async def release(self, task_id: UUID) -> None:
        """Release this task's in-process claim without re-publishing it.

        The inbox entry stays non-terminal (the broker still holds the message
        and a restart replays it), so only the in-process enqueued marker is
        dropped.
        """
        self._enqueued.discard(task_id)

    async def on_started(self, task_id: UUID, task_data: TaskData) -> None:
        """Record that a task began processing (the inbox entry is in-flight)."""
        await self._inbox.mark_in_flight(task_id)

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
        """
        # A retryable error was already re-published by ``requeue`` with the
        # SAME task id; writing a tombstone here would suppress that retry copy
        # when it arrives (inbox.put skips tombstoned ids), silently losing the
        # retry (AR-077b mirror). Leave the entry non-terminal so the retry is
        # accepted and redelivered.
        if task_result is not None and task_result.status == "error" and task_result.retryable:
            self._enqueued.discard(task_id)
            return
        # Mark terminal (persist the tombstone) before releasing the in-process
        # claim, so a crash mid-ack redelivers rather than loses the task.
        await self._inbox.mark_terminal(task_id)
        self._enqueued.discard(task_id)

    async def on_progress(self, task_id: UUID, value: float) -> None:
        """No-op (design §10 #5): MQTT has no server-side key space for records.

        Progress reporting stays functional in-process via ``TaskCapabilities``;
        it is simply not persisted to the broker.
        """

    async def on_drain(self, task_id: UUID, task_data: TaskData) -> None:
        """Release the in-process claim for a drained task without re-enqueueing it.

        The inbox entry is left non-terminal (the broker still holds the
        message), so a restart redelivers it via recovery; re-publishing here
        would duplicate it (the MQTT analogue of AR-041).
        """
        self._enqueued.discard(task_id)

    async def refresh_leases(self) -> None:
        """No-op: the file-backed inbox has no per-entry leases to renew.

        Exists for parity with ``ValkeyTransport.refresh_leases``, which the
        worker's watchdog calls unconditionally. MQTT entries are protected by
        the durable inbox (persist-before-enqueue), not by expiring leases.
        """
