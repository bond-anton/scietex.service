"""Stream-backed transport for ``ValkeyWorker`` (AR-072).

Extracts the ordering-sensitive Valkey stream operations that ``ValkeyWorker``
previously inlined as ``TaskProcessor`` hook overrides — intake, pending-entry
recovery, acknowledgement, progress, and shutdown-drain policy — into a
:class:`ValkeyTransport` implementing the core ``TaskTransport`` contract.

Every collaborator is received by injection, so the transport holds no
ownership over the lease manager, tracking store, or entry-id map: those
remain the worker's and are only reached through here.
"""

import logging
from collections import deque
from collections.abc import Mapping
from uuid import UUID

from ..health import TransportHealth
from ..task_handler.schemas import CancelReason, TaskData, TaskResult, is_control_task, task_data_id
from ..task_handler.wire import decode_task_envelope, decode_task_envelope_version, encode_task_envelope
from ..transport import RecoverableTransport, TaskSink
from ._glide import (
    ClientProvider,
    GlideConnectionError,
    GlideTimeoutError,
    RequestError,
    StreamReadGroupOptions,
    StreamReadOptions,
)
from .config import DEFAULT_CLAIM_MIN_IDLE_MS, ValkeyWorkerConfig
from .lease import TaskLeaseManager
from .tracking import TaskStatusStore

#: Fixed stream field name under which an entry carries its encoded
#: ``TaskData``. The task id moved inside the payload (``TaskData.task_id`` is
#: now required), so the field no longer doubles as the id key and stays
#: constant across entries.
TASK_FIELD = b"task"


class ValkeyTransport(RecoverableTransport):
    """Valkey-stream implementation of the core ``TaskTransport`` contract.

    Reads new entries with ``XREADGROUP``, recovers a crashed run's pending
    entries with ``XAUTOCLAIM``, and acks entries (``XACK`` + ``XDEL``) only
    after a handler finishes, so delivery is at-least-once.
    """

    def __init__(
        self,
        *,
        config: ValkeyWorkerConfig,
        service_name: str,
        consumer_name: str,
        stream_name: str,
        group_name: str,
        client_provider: ClientProvider,
        health: TransportHealth,
        lease: TaskLeaseManager,
        status: TaskStatusStore,
        entry_ids: dict[UUID, str | bytes],
        control_stream_name: str,
        control_broadcast_stream_name: str,
        control_entry_ids: dict[UUID, tuple[str, str | bytes]],
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._service_name = service_name
        self._consumer_name = consumer_name
        self._stream_name = stream_name
        self._group_name = group_name
        self._client_provider = client_provider
        self._health = health
        self._lease = lease
        self._status = status
        self._entry_ids = entry_ids
        self._control_stream_name = control_stream_name
        self._control_broadcast_stream_name = control_broadcast_stream_name
        self._control_entry_ids = control_entry_ids
        self._logger = logger

        # Idle floor (ms) before XAUTOCLAIM reclaims a pending entry. With 0, a
        # replica's startup recovery can claim an entry a slow-but-alive handler
        # on another replica is still processing, causing double-processing.
        self._claim_min_idle_ms: int = (
            config.claim_min_idle_ms if config.claim_min_idle_ms is not None else DEFAULT_CLAIM_MIN_IDLE_MS
        )

        # Bounded buffer for entries claimed by XREADGROUP but not yet enqueued
        # because the data lane was full. ``XREADGROUP ">"`` never redelivers a
        # claimed-but-unenqueued entry and ``ensure_recovered`` runs only once,
        # so without this the entry would be stranded. Flushed before each read
        # and capped at ``task_fetch_batch_size`` so a saturated data plane
        # cannot grow it without bound.
        self._deferred: deque[tuple[UUID, TaskData, str | bytes]] = deque()

        # Bounded buffer for directed control entries read via XREAD but not yet
        # enqueued because the control lane was full. Plain XREAD never re-reads
        # a consumed entry (the in-memory cursor has moved past it), so a
        # rejected entry must be held here or it is dropped. The third element
        # is the entry id; the stream is always ``_control_stream_name`` for the
        # directed path. Flushed before each control read and capped by the same
        # stop-on-reject discipline as ``_deferred``.
        self._control_deferred: deque[tuple[UUID, TaskData, str | bytes]] = deque()

        # In-memory last-seen id for the directed control stream (AR-123 §4.2).
        # ``None`` means "not yet seeded": the first read passes ``$`` (the
        # stream tail), so a command published before startup is skipped. After
        # each read it advances to the last entry id returned; it is never
        # persisted, matching the event-only, no-recovery control contract.
        self._control_cursor: str | bytes | None = None

        # The broadcast stream is service-scoped and read by every worker, so it
        # needs its own cursor and deferred buffer independent of the directed
        # stream: a worker's position on one must never affect the other, and a
        # full directed lane must not block broadcast delivery (AR-123 §4.4).
        self._broadcast_deferred: deque[tuple[UUID, TaskData, str | bytes]] = deque()
        self._broadcast_cursor: str | bytes | None = None

    async def fetch(self, sink: TaskSink) -> bool:
        """Fetch new tasks from the Valkey task stream and enqueue them.

        Reads up to ``task_fetch_batch_size`` entries with ``XREADGROUP``,
        decodes each envelope, and enqueues it via ``sink``. Entries are not
        acked here: they stay in the group's pending list until each handler
        completes (see :meth:`ack`), so a crash after enqueue redelivers the
        task. Each entry id is recorded in the shared entry-id map and its
        lease is written at enqueue-accept, so a queued task is protected from
        a peer's recovery for its whole queue wait. On read errors, disconnects
        and reconnects.

        Before reading, entries deferred by a previously full data lane are
        flushed first: ``XREADGROUP ">"`` never redelivers a claimed-but-
        unenqueued entry and recovery runs only once, so those entries would
        otherwise be stranded. An entry that cannot be enqueued (data lane, or
        rarely control lane, full) is held in the bounded deferred buffer and
        retried on the next poll instead of being dropped; its entry id and
        lease are recorded only on successful enqueue.

        After the data read, the directed and broadcast control streams are
        polled with plain ``XREAD`` (non-blocking) and any deferred control
        entries are flushed first (AR-123 §4.4). Control is read every poll even
        when the data lane is full or backpressured, so a saturated data plane
        cannot delay control delivery.

        Returns:
            ``True`` if at least one task was enqueued (from recovery, the
            deferred buffers, or any read), ``False`` otherwise.
        """
        client = self._client_provider()
        if client is None:
            return False
        enqueued = await self.ensure_recovered(sink)
        enqueued = await self._flush_deferred(sink) or enqueued
        # Backpressure: do not claim more entries than the deferred buffer holds,
        # or a full data plane would let unprocessable entries accumulate without
        # bound. Only the data read is gated here; the control read below runs
        # regardless.
        if len(self._deferred) < self._config.task_fetch_batch_size:
            try:
                res = await client.xreadgroup(
                    {self._stream_name: ">"},
                    self._group_name,
                    self._consumer_name,
                    StreamReadGroupOptions(count=self._config.task_fetch_batch_size, block_ms=1000),
                )
                if res:
                    for stream, entries in res.items():
                        for entry_id, pairs in entries.items():
                            if pairs is None:
                                continue
                            for field, payload_bytes in pairs:
                                if payload_bytes is None:
                                    continue
                                task_data = decode_task_envelope(payload_bytes)
                                if task_data is None:
                                    version = decode_task_envelope_version(payload_bytes)
                                    self._logger.error(
                                        "Failed to decode task envelope for entry %s (version=%s)",
                                        entry_id,
                                        version if version is not None else "malformed",
                                    )
                                    continue
                                uuid = task_data_id(task_data)
                                if sink.enqueue_task(task_data):
                                    self._entry_ids[uuid] = entry_id
                                    # The entry id is recorded before the lease write so
                                    # the local ownership guard is active from the same
                                    # synchronous moment and this worker's own recovery
                                    # can never re-enqueue the entry during the await.
                                    await self._lease.write(uuid)
                                    enqueued = True
                                else:
                                    # Data lane (or, rarely, control lane) full: hold the
                                    # claimed entry and retry next poll. Entry id/lease are
                                    # recorded only on successful enqueue, preserving the
                                    # existing "full queue leaves no lease" policy.
                                    self._deferred.append((uuid, task_data, entry_id))
                                    self._logger.log(logging.DEBUG, "Task queue full; deferring task %s", uuid)
            except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
                self._logger.debug("Failed to fetch/parse task from Valkey stream: %s", exc)
                self._health.report_failure(exc)
                await self._health.recover()
        # Control reads run after the data read so data keeps its 1000 ms block
        # and control is polled every iteration; they are not gated by the data
        # backpressure guard above (AR-123 §4.4).
        enqueued = await self._flush_control_deferred(sink) or enqueued
        enqueued = await self._read_control(sink) or enqueued
        enqueued = await self._flush_broadcast_deferred(sink) or enqueued
        enqueued = await self._read_broadcast(sink) or enqueued
        return enqueued

    async def _flush_deferred(self, sink: TaskSink) -> bool:
        """Re-attempt enqueueing of entries held in the deferred buffer.

        Preserves the "full queue leaves no lease" policy: an entry is only
        recorded in the shared entry-id map and given a lease once the sink
        accepts it. The first entry that is still rejected stops the flush, so
        ordering is preserved and a saturated lane never reorders the backlog.
        """
        enqueued = False
        while self._deferred:
            task_id, task_data, entry_id = self._deferred[0]
            if not sink.enqueue_task(task_data):
                break
            self._deferred.popleft()
            self._entry_ids[task_id] = entry_id
            await self._lease.write(task_id)
            enqueued = True
        return enqueued

    async def _flush_control_deferred(self, sink: TaskSink) -> bool:
        """Flush the directed control deferred buffer (AR-123 §4.4)."""
        return await self._flush_control_deferred_stream(
            sink, stream_name=self._control_stream_name, deferred=self._control_deferred
        )

    async def _flush_broadcast_deferred(self, sink: TaskSink) -> bool:
        """Flush the broadcast control deferred buffer (AR-123 §4.4)."""
        return await self._flush_control_deferred_stream(
            sink, stream_name=self._control_broadcast_stream_name, deferred=self._broadcast_deferred
        )

    async def _flush_control_deferred_stream(
        self,
        sink: TaskSink,
        *,
        stream_name: str,
        deferred: deque[tuple[UUID, TaskData, str | bytes]],
    ) -> bool:
        """Re-attempt enqueueing of entries held in a control deferred buffer.

        Mirrors :meth:`_flush_deferred` for a control stream: the first entry
        that is still rejected stops the flush so ordering is preserved, and the
        (stream, entry id) pair is recorded only once the sink accepts the
        entry. No lease is written — control entries are never leased
        (AR-123 §4.6).
        """
        enqueued = False
        while deferred:
            task_id, task_data, entry_id = deferred[0]
            if not sink.enqueue_task(task_data):
                break
            deferred.popleft()
            self._control_entry_ids[task_id] = (stream_name, entry_id)
            enqueued = True
        return enqueued

    async def _read_control(self, sink: TaskSink) -> bool:
        """Read the directed control stream (AR-123 §4.4)."""
        enqueued, self._control_cursor = await self._read_control_stream(
            sink,
            stream_name=self._control_stream_name,
            cursor=self._control_cursor,
            deferred=self._control_deferred,
        )
        return enqueued

    async def _read_broadcast(self, sink: TaskSink) -> bool:
        """Read the service-scoped broadcast control stream (AR-123 §4.4)."""
        enqueued, self._broadcast_cursor = await self._read_control_stream(
            sink,
            stream_name=self._control_broadcast_stream_name,
            cursor=self._broadcast_cursor,
            deferred=self._broadcast_deferred,
        )
        return enqueued

    async def _read_control_stream(
        self,
        sink: TaskSink,
        *,
        stream_name: str,
        cursor: str | bytes | None,
        deferred: deque[tuple[UUID, TaskData, str | bytes]],
    ) -> tuple[bool, str | bytes | None]:
        """Read one control stream with plain ``XREAD`` (AR-123 §4.4).

        Control uses no consumer group, so the read is ``XREAD`` with an
        in-memory cursor rather than ``XREADGROUP``. The cursor is seeded to
        ``$`` (the stream tail) on the first read, so any command published
        before startup is skipped (§4.2); each subsequent read advances it to
        the last entry id returned. The read is non-blocking (``block=0``): it
        runs every poll after the data read, never delaying data latency.

        Each decoded ``TaskData`` is enqueued via ``sink``. A non-control entry
        on a control stream is a misroute and is skipped (logged, never
        enqueued). A rejected entry (control lane full) is held in the stream's
        bounded ``deferred`` buffer and retried next poll; the cursor still
        advances past it so it is not re-read. No lease is written (§4.6).

        The explicit ``cursor`` parameter and ``(enqueued, cursor)`` return keep
        the directed and broadcast streams independent without attribute-name
        strings, so one stream's position can never advance the other's.

        Returns:
            ``(enqueued, new_cursor)``: whether a control command was enqueued,
            and the cursor to resume from on the next read.
        """
        client = self._client_provider()
        if client is None:
            return False, cursor
        enqueued = False
        if cursor is None:
            cursor = "$"
        try:
            res = await client.xread({stream_name: cursor}, StreamReadOptions(block_ms=0))
            if res:
                for _stream, entries in res.items():
                    for entry_id, pairs in entries.items():
                        # Advance the cursor before deciding the entry's fate: a
                        # deferred entry must never be re-read from the stream,
                        # because the deferred buffer owns its retry.
                        cursor = entry_id
                        if pairs is None:
                            continue
                        for field, payload_bytes in pairs:
                            if payload_bytes is None:
                                continue
                            task_data = decode_task_envelope(payload_bytes)
                            if task_data is None:
                                version = decode_task_envelope_version(payload_bytes)
                                self._logger.error(
                                    "Failed to decode control envelope for entry %s (version=%s)",
                                    entry_id,
                                    version if version is not None else "malformed",
                                )
                                continue
                            if not is_control_task(task_data):
                                self._logger.error(
                                    "Non-control task %s misrouted to control stream %s",
                                    task_data_id(task_data),
                                    stream_name,
                                )
                                continue
                            uuid = task_data_id(task_data)
                            if sink.enqueue_task(task_data):
                                self._control_entry_ids[uuid] = (stream_name, entry_id)
                                enqueued = True
                            else:
                                # Control lane full: hold the entry and retry next
                                # poll, preserving order by stopping this poll's read.
                                deferred.append((uuid, task_data, entry_id))
                                self._logger.log(logging.DEBUG, "Control queue full; deferring task %s", uuid)
                                return enqueued, cursor
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self._logger.debug("Failed to fetch/parse control task from Valkey stream: %s", exc)
            self._health.report_failure(exc)
            await self._health.recover()
            return False, cursor
        return enqueued, cursor

    async def recover_pending_tasks(self, sink: TaskSink) -> tuple[bool, bool]:
        """Re-enqueue stream entries left pending by a previous run.

        Uses ``XAUTOCLAIM`` to claim every entry in the consumer group's
        pending list that is idle for at least ``claim_min_idle_ms`` and
        enqueue it, so tasks that were read but never acknowledged before a
        crash are redelivered (at-least-once). Called once from the first
        :meth:`fetch`, before any ``'>'`` read, when no tasks are in flight.

        Each claimed entry is checked against its per-entry lease before
        enqueueing (AR-060). The lease is claimed atomically via ``SET ... NX``
        before enqueue, so when two replicas run startup recovery concurrently
        over the same pending entry exactly one wins the claim and the other
        defers. An entry whose claim is lost (a live holder owns it) is skipped
        and marks recovery incomplete so a later poll reclaims it if that
        holder dies. A locally-owned entry is skipped without marking recovery
        incomplete. When the queue is full the lease claimed above is rolled
        back before the early return.

        Returns:
            A ``(recovery_complete, enqueued)`` tuple.
        """
        client = self._client_provider()
        if client is None:
            return True, False
        enqueued = False
        lease_skipped = False
        try:
            start: str | bytes = "0-0"
            while True:
                res = await client.xautoclaim(
                    self._stream_name,
                    self._group_name,
                    self._consumer_name,
                    self._claim_min_idle_ms,
                    start,
                    count=10,
                )
                # glide types xautoclaim's return as a heterogeneous list;
                # narrow the positions we read (next_start, entries) first.
                next_start = res[0]
                entries = res[1]
                if not isinstance(next_start, (str, bytes)) or not isinstance(entries, Mapping):
                    break
                for entry_id, pairs in entries.items():
                    for field, payload_bytes in pairs:
                        task_data = decode_task_envelope(payload_bytes)
                        if task_data is None:
                            version = decode_task_envelope_version(payload_bytes)
                            self._logger.error(
                                "Failed to decode recovered task envelope for entry %s (version=%s)",
                                entry_id,
                                version if version is not None else "malformed",
                            )
                            continue
                        uuid = task_data_id(task_data)
                        if uuid in self._entry_ids:
                            continue
                        if not await self._lease.acquire(uuid):
                            lease_skipped = True
                            self._logger.log(
                                logging.DEBUG,
                                "Task %s is leased by a live holder; deferring recovery",
                                uuid,
                            )
                            continue
                        if not sink.enqueue_task(task_data):
                            # Queue full mid-recovery: roll back the lease we just
                            # acquired (the entry was never accepted) and stop
                            # claiming so the remaining pending entries stay
                            # pending and are redelivered on a later poll.
                            await self._lease.delete(uuid)
                            self._logger.log(
                                logging.DEBUG,
                                "Task queue full during recovery; deferring task %s",
                                uuid,
                            )
                            return False, enqueued
                        self._entry_ids[uuid] = entry_id
                        enqueued = True
                if next_start == b"0-0" or next_start == "0-0":
                    break
                start = next_start
        except Exception as exc:
            self._logger.log(logging.ERROR, "Failed to recover pending tasks: %s", exc)
            return False, enqueued
        return (not lease_skipped), enqueued

    async def requeue(self, task_data: TaskData) -> None:
        """Re-queue a task by appending it to the Valkey task stream.

        Encodes ``task_data`` into a versioned transport envelope (msgpack)
        and appends a new entry carrying it under the fixed ``TASK_FIELD``
        field name.

        The lease is deleted as part of the requeue: the requeued copy reuses
        the same ``task_id``, so leaving this worker's lease in place would
        either block a peer from claiming the copy or be clobbered by the
        peer's fresh lease (AR-077b). Releasing it here means the copy is
        immediately claimable by any worker.

        Control commands are never retried (AR-123 §4.7): re-publishing a
        control command on retry would re-deliver a command that is
        event-only and, for broadcast, re-fan-out to the whole fleet. A control
        task id in the control ownership map therefore returns without an
        ``XADD``.
        """
        task_id = task_data_id(task_data)
        if task_id in self._control_entry_ids:
            return
        client = self._client_provider()
        if client:
            packed = encode_task_envelope(task_data)
            await client.xadd(self._stream_name, [(TASK_FIELD, packed)])
        await self._lease.delete(task_id)

    async def on_started(self, task_data: TaskData) -> None:
        """Publish a ``running`` tracking record when a task begins.

        The lease is written for data entries only: a control entry has no
        lease to write (AR-123 §4.6).
        """
        task_id = task_data_id(task_data)
        await self._status.record_running(task_id, task_data)
        if task_id not in self._control_entry_ids:
            # The lease marks this entry as owned by a live worker, so recovery on
            # another replica skips it while processing is still in flight.
            await self._lease.write(task_id)

    async def ack(
        self,
        task_data: TaskData,
        task_result: TaskResult | None,
        *,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        """Acknowledge and delete the stream entry for a completed task.

        Looks up the stream entry id recorded at fetch time and ``XACK``s +
        ``XDEL``s it, so the entry leaves the consumer group's pending list only
        after the handler's work on it is done (at-least-once). A retryable error
        is not terminal: ``requeue`` has already advertised its new copy as
        ``queued``, so publishing a terminal ``failed`` record here would
        misreport a task that is still in flight. ``task_result`` is ``None``
        when the task was cancelled before producing a result.

        A control entry (in the control ownership map) takes a separate path:
        it is ``XDEL``'d from its group-less control stream and a terminal
        record is always written — no ``XACK`` (no group), no lease delete, and
        no retryable-error early return, because control commands are never
        retried (AR-123 §4.5/§4.7).
        """
        task_id = task_data_id(task_data)
        control = self._control_entry_ids.pop(task_id, None)
        if control is not None:
            stream_name, entry_id = control
            client = self._client_provider()
            if client is not None:
                try:
                    await client.xdel(stream_name, [entry_id])
                except Exception as exc:
                    self._logger.log(logging.ERROR, "Failed to delete control entry for task %s: %s", task_id, exc)
            await self._status.record_terminal(task_id, task_data, task_result, cancel_reason)
            return
        # The retry copy is a NEW stream entry with the same task id; the old
        # entry must still be acked/deleted before returning. ``task_data`` is
        # None only in unit tests that exercise the ack path in isolation.
        entry_id = self._entry_ids.pop(task_id, None)
        client = self._client_provider()
        if entry_id is not None and client is not None:
            try:
                await client.xack(self._stream_name, self._group_name, [entry_id])
                await client.xdel(self._stream_name, [entry_id])
            except Exception as exc:
                self._logger.log(logging.ERROR, "Failed to acknowledge task %s: %s", task_id, exc)
        # A retryable error was already requeued (and its lease released) by
        # ``requeue`` before this ack, so deleting again here would clobber a
        # peer's fresh lease for the requeued copy (AR-077b).
        if task_result is not None and task_result.status == "error" and task_result.retryable:
            return
        # Terminal record goes first, then the lease is cleared, so a crash
        # between the two leaves a recoverable pending entry rather than a lost
        # terminal record (at-least-once).
        await self._status.record_terminal(task_id, task_data, task_result, cancel_reason)
        await self._lease.delete(task_id)

    async def on_progress(self, task_id: UUID, value: float) -> None:
        """Update the tracking record's progress for a running task."""
        await self._status.update_progress(task_id, value)

    async def on_drain(self, task_data: TaskData) -> None:
        """Release the lease for a drained task without re-enqueueing it.

        For a durable transport the stream entry is still pending and is
        redelivered on restart, so re-enqueueing here would duplicate it
        (AR-041). The lease is deleted so a restart or peer can reclaim the
        entry immediately instead of waiting for it to expire.

        A control entry has no lease to release (AR-123 §4.6), so it is a no-op.
        """
        task_id = task_data_id(task_data)
        if task_id in self._control_entry_ids:
            return
        await self._lease.delete(task_id)

    async def refresh_leases(self) -> None:
        """Renew the lease for every task this worker owns.

        The shared entry-id map is the authoritative ownership map: an entry is
        recorded the moment this worker accepts it and is only popped in
        :meth:`ack`. Iterating it refreshes both queued and running tasks.
        Control entries are deliberately absent from ``_entry_ids`` — they live
        in ``_control_entry_ids`` and are never leased (AR-123 §4.6).
        """
        await self._lease.refresh(self._entry_ids)
