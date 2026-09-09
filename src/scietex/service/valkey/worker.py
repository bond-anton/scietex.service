"""Valkey-backed async task processor for ``scietex.service``.

Provides ``ValkeyWorker`` — an async worker that extends ``TaskProcessor``
with Valkey stream-based task distribution, heartbeat publishing, and async
logging. Uses the ``glide`` client for all Valkey operations.

Requires the optional ``valkey-glide`` dependency.
"""

import asyncio
import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import ClassVar, cast
from uuid import UUID

import msgspec
from scietex.logging import AsyncValkeyHandler

from ..task_handler import TaskData, TaskResult
from ..task_processor import TaskProcessor
from ._glide import (
    ExpirySet,
    ExpiryType,
    GlideClient,
    GlideClientConfiguration,
    GlideConnectionError,
    GlideTimeoutError,
    RequestError,
    StreamGroupOptions,
    StreamReadGroupOptions,
)
from .config import (
    DEFAULT_CLAIM_MIN_IDLE_MS,
    ValkeyConfig,
    ValkeyWorkerConfig,
    generate_glide_config,
    read_valkey_config,
)
from .schemas import Heartbeat


def _logging_handler_config(valkey_config: ValkeyConfig) -> dict:
    """Translate a typed ``ValkeyConfig`` into the logging handler's config dict.

    The external :class:`~scietex.logging.AsyncValkeyHandler` builds its own
    connection from a plain ``dict`` of scalar ``GlideClientConfiguration``
    options passed via ``valkey_config=``. That dict schema does not model
    ``read_from``, ``protocol``, or ``backoff_strategy`` (the handler applies
    its own autonomous reconnect/backoff), so only the scalar address,
    credential, TLS, and timeout fields are translated.
    """
    base = valkey_config.base_config
    credentials = base.user_credentials
    return {
        "addresses": [(node.host, node.port) for node in base.nodes],
        "username": credentials.username if credentials is not None else None,
        "password": credentials.password if credentials is not None else None,
        "use_tls": base.use_tls,
        "request_timeout": base.request_timeout,
        "database_id": base.database_id,
        "client_name": base.client_name,
        "inflight_requests_limit": base.inflight_requests_limit,
        "client_az": base.client_az,
        "lazy_connect": base.lazy_connect,
    }


class ValkeyWorker(TaskProcessor):
    """
    Async worker backed by a Valkey (Redis) stream for task distribution.

    Extends ``TaskProcessor`` with Valkey-specific operations including
    connection management, stream-based task fetching, heartbeat publishing,
    and async logging to a Valkey stream via the ``glide`` client.

    Requires the optional ``valkey-glide`` dependency.

    Connection lifecycle (AR-059): this worker runs one operational
    ``GlideClient`` for heartbeats, registry, intake, and task completion.
    ``connect()``/``disconnect()`` are serialized behind an ``asyncio.Lock`` so
    only one task mutates ``_client`` at a time, and intake's reconnect trigger
    is narrowed to glide errors only. The logging handler
    (:class:`~scietex.logging.AsyncValkeyHandler`) owns its own independent
    connection when a typed ``ValkeyConfig`` is available (``valkey_config=``
    mode), so the worker neither shares nor tears down the logging client; it
    falls back to ``client=`` injection only when the worker was given a raw
    ``GlideClientConfiguration``.

    Attributes:
        client (GlideClient | None): Valkey client instance, initialized
            during ``initialize()``.
    """

    # Concrete config struct for this worker. The base stores it into
    # ``self._config``, so ``config=None`` instantiates the concrete type here
    # (AR-069) and no re-store / double-instantiation is needed.
    _config_type: ClassVar[type[ValkeyWorkerConfig]] = ValkeyWorkerConfig

    def __init__(self, config: ValkeyWorkerConfig | None = None):
        """Initialize the ``ValkeyWorker``.

        Configures the Valkey client from ``config.valkey_config`` or, when
        that is ``None``, by reading ``valkey.yml`` from the config directory.
        Sets up stream names for tasks, heartbeat status, and logging. The
        :class:`~scietex.logging.AsyncValkeyHandler` for async log entries owns
        its own connection (``valkey_config=`` mode) when a typed
        ``ValkeyConfig`` is available, and is built and registered on the first
        successful :meth:`connect`.

        Args:
            config: A :class:`~scietex.service.valkey.config.ValkeyWorkerConfig`
                holding the worker's service identity, task-queue settings, and
                Valkey-specific fields. ``None`` uses the struct defaults.

        Attributes:
            _client (GlideClient | None): Valkey client, initialized during
                :meth:`initialize`.
            _valkey_logger_handler (AsyncValkeyHandler | None): The logging
                handler, built lazily on the first successful :meth:`connect`
                and reused across restarts. Owns its own connection in
                ``valkey_config=`` mode; only the raw-config fallback shares
                the worker's client.
            _heartbeat_key (str): Key for the worker status heartbeat entry.
            _task_stream_name (str): Valkey stream name for task entries.
            _task_group_name (str): Consumer group name for task fetching.
            _consumer_name (str): Consumer identifier within the task group.
            _registry_key (str): Service-scoped worker registry set key.
        """
        super().__init__(config)
        # The base already stored the concrete config into ``self._config``
        # (AR-069); keep a typed local reference for the synchronous setup
        # reads below.
        cfg = cast(ValkeyWorkerConfig, self._config)

        self._log_stream_name = cfg.log_stream_name
        # Deliberate write-capable bootstrap path: default create_default=True creates config + defaults.
        valkey_config = read_valkey_config(self.conf_dir) if cfg.valkey_config is None else cfg.valkey_config
        self._valkey_config = valkey_config
        if isinstance(valkey_config, GlideClientConfiguration):
            self._client_config = valkey_config
        else:
            self._client_config: GlideClientConfiguration = generate_glide_config(
                valkey_config,
                service_name=self.service_name,
                worker_id=self.instance_id,
                listening=False,
            )
        # The logging handler owns its own connection when a typed ValkeyConfig
        # is available (AR-059/061); it is built lazily on the first successful
        # connect() and reused across restarts (see _ensure_logging_handler).
        self._valkey_logger_handler: AsyncValkeyHandler | None = None

        self._client: GlideClient | None = None
        # Serializes connect()/disconnect() so only one task mutates _client at
        # a time (AR-059): intake reconnect and shutdown cannot race each other.
        self._client_lock: asyncio.Lock = asyncio.Lock()
        self._heartbeat_key = f"scietex:{self.service_name}:{self.instance_id}:status"
        self._task_stream_name = f"scietex:{self.service_name}:tasks"
        self._task_group_name = f"scietex:{self.service_name}:task_group"
        self._consumer_name = f"scietex:{self.service_name}:{self.instance_id}"
        self._registry_key = f"scietex:{self.service_name}:workers"
        self.__encoder = msgspec.msgpack.Encoder()

        # Maps a task UUID to the stream entry id it was read from, so the
        # entry can be acknowledged when the handler completes (at-least-once).
        self._task_entry_ids: dict[UUID, str | bytes] = {}

        # True once pending-entry recovery has run (start of the first
        # fetch_tasks), so a crash's unacked entries are redelivered once.
        self._recovered: bool = False

        # Idle floor (ms) before XAUTOCLAIM reclaims a pending entry. With 0, a
        # replica's startup recovery can claim an entry a slow-but-alive handler
        # on another replica is still processing, causing double-processing. A
        # positive floor means only entries idle >= the floor (genuinely
        # abandoned) are claimed. Must be well under the status-key TTL
        # (2 x heartbeat_interval) so a dead replica's entries are reclaimed
        # promptly.
        self.__claim_min_idle_ms: int = (
            cfg.claim_min_idle_ms if cfg.claim_min_idle_ms is not None else DEFAULT_CLAIM_MIN_IDLE_MS
        )

    @property
    def valkey_config(self) -> ValkeyConfig | GlideClientConfiguration:
        """The Valkey configuration used by this worker.

        Returns either a :class:`ValkeyConfig` schema or a raw
        :class:`~glide.GlideClientConfiguration`, depending on how the
        worker was constructed.

        Returns:
            The Valkey configuration instance.
        """
        return self._valkey_config

    @property
    def client(self) -> GlideClient | None:
        """The Valkey :class:`~glide.GlideClient` instance.

        ``None`` until :meth:`initialize` completes successfully.

        Returns:
            The active Valkey client, or ``None`` if not connected.
        """
        return self._client

    def _ensure_logging_handler(self) -> AsyncValkeyHandler | None:
        """Build and register the logging handler once, then reuse it.

        When a typed ``ValkeyConfig`` is available the handler is constructed
        with ``valkey_config=`` so it builds, closes, and reconnects its own
        ``GlideClient`` autonomously (``_owns_client`` is True); the worker
        never touches ``handler.client`` (AR-059/061). When the worker was
        given a raw ``GlideClientConfiguration`` there is no typed config to
        hand the handler, so it falls back to ``client=`` injection and the
        worker keeps the shared-client re-point seam.
        """
        if self._valkey_logger_handler is not None:
            # Only the raw-config fallback shares the worker's client, so only
            # it needs re-pointing across reconnects/restarts.
            if isinstance(self._valkey_config, GlideClientConfiguration) and self._client is not None:
                self._valkey_logger_handler.client = self._client
            return self._valkey_logger_handler

        if isinstance(self._valkey_config, GlideClientConfiguration):
            if self._client is None:
                return None
            self._valkey_logger_handler = AsyncValkeyHandler(
                stream_name=self._log_stream_name,
                client=self._client,
            )
        else:
            self._valkey_logger_handler = AsyncValkeyHandler(
                stream_name=self._log_stream_name,
                valkey_config=_logging_handler_config(self._valkey_config),
            )
        self._logging_lifecycle.register_logger_handler(self._valkey_logger_handler, name="AsyncValkeyHandler")
        return self._valkey_logger_handler

    async def connect(self) -> bool:
        """Establish an asynchronous connection to the Valkey server.

        Serialized behind ``_client_lock`` so a concurrent ``disconnect()``
        (intake reconnect, shutdown) cannot race the create → ping → assign
        sequence (AR-059). Delegates to :meth:`_connect_locked`.

        Returns:
            ``True`` if the connection is established and ``PING``
            succeeds; ``False`` on connection failure or timeout.
        """
        async with self._client_lock:
            return await self._connect_locked()

    async def _connect_locked(self) -> bool:
        """Establish the connection; assumes ``_client_lock`` is held.

        Creates a new :class:`~glide.GlideClient` using the configured
        ``_client_config`` and verifies connectivity with ``PING``.

        ``_client`` is assigned only after ``PING`` succeeds, so a failed
        create or ping leaves ``_client`` as ``None`` and ``connect()``
        returns ``False``. This keeps the return value a reliable
        connectivity signal: callers that guard on ``self.client`` (e.g.
        ``initialize``) never see a half-connected worker.

        On success the logging handler is ensured (constructed lazily here) and
        its worker loop is started if not already running.

        Returns:
            ``True`` if the connection is established and ``PING``
            succeeds; ``False`` on connection failure or timeout.
        """
        if self._client is not None:
            return True
        try:
            client = await GlideClient.create(self._client_config)
        except (GlideConnectionError, GlideTimeoutError):
            self.logger.error("Error connecting to Valkey")
            return False
        try:
            if await client.ping():
                self._client = client
                self.logger.log(logging.INFO, "Connected to Valkey")
                handler = self._ensure_logging_handler()
                if handler is not None and not handler.logging_running_event.is_set():
                    await handler.start_logging()
                return True
            self.logger.error("Error pinging Valkey")
        except (GlideConnectionError, GlideTimeoutError):
            self.logger.error("Error connecting to Valkey")
        # Ping failed or raised: never leave a half-connected client behind.
        try:
            await client.close()
        except Exception:
            pass  # best-effort close; the client is unusable either way
        return False

    async def disconnect(self):
        """Gracefully close the connection to the Valkey server.

        Serialized behind ``_client_lock`` so a concurrent ``connect()`` cannot
        race the close → null sequence (AR-059). Delegates to
        :meth:`_disconnect_locked`.
        """
        async with self._client_lock:
            await self._disconnect_locked()

    async def _disconnect_locked(self):
        """Close the connection; assumes ``_client_lock`` is held.

        For the raw-config fallback the logging handler shares the worker's
        client, so its reference is cleared before the close; the
        ``valkey_config=`` handler owns its own client and is left untouched.
        Then invokes :meth:`~glide.GlideClient.close` on the active client, logs
        the disconnection, and sets ``_client`` to ``None``.
        """
        if self._client is not None:
            if self._valkey_logger_handler is not None and isinstance(self._valkey_config, GlideClientConfiguration):
                self._valkey_logger_handler.client = None
            await self._client.close()
            self.logger.info("Valkey client disconnected")
            self._client = None

    async def heartbeat(self) -> None:
        """Publish a heartbeat entry to the Valkey status key.

        Encodes a ``Heartbeat`` struct with service metadata and writes it
        to ``self._heartbeat_key`` with a TTL set to twice the heartbeat
        interval. Logs the duration at DEBUG and any failure at WARNING.

        The write is guarded by ``self.client and self.start_time``. The start
        time is set in ``_startup`` before the managers start, so the first
        heartbeat fires promptly and the status key's TTL is refreshed from the
        first beat (AR-049).
        """

        if self.client and self.start_time:
            heartbeat_data = Heartbeat(
                service=self.service_name,
                instance_id=self.instance_id,
                status="active",
                heartbeat_interval=self.heartbeat_interval,
                start_time=self.start_time,
                timestamp=datetime.now(timezone.utc),
            )
            self.logger.log(logging.DEBUG, "Sending heartbeat to Valkey: %s", heartbeat_data)
            start_time = time.monotonic()
            try:
                await self.client.set(
                    self._heartbeat_key,
                    value=self.__encoder.encode(heartbeat_data),
                    expiry=ExpirySet(ExpiryType.SEC, int(self.heartbeat_interval * 2)),
                )
                duration = (time.monotonic() - start_time) * 1000
                self.logger.log(logging.DEBUG, "Heartbeat set in Valkey, duration: %.2f ms", duration)
            except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
                duration = (time.monotonic() - start_time) * 1000
                self.logger.log(
                    logging.WARNING,
                    "Failed to set heartbeat in Valkey: %s. Duration: %.2f ms",
                    exc,
                    duration,
                )

    async def initialize(self) -> bool:
        """Initialize the worker and prepare the Valkey task stream.

        Calls the parent ``TaskProcessor.initialize()`` to start
        registered task handlers, then connects to Valkey and creates
        the consumer group for the task stream (with ``make_stream=True``).
        A pre-existing group (``BUSYGROUP``) is ignored; any other group
        creation error fails initialization.

        Returns:
            ``True`` if the parent initialization and Valkey connection
            succeed and the consumer group is ready. ``False`` if the
            parent initialization fails, the client is unavailable, or
            the consumer group could not be created.
        """

        if not await super().initialize():
            return False
        await self.connect()
        if not self.client:
            return False

        try:
            await self.client.xgroup_create(
                self._task_stream_name,
                self._task_group_name,
                "0-0",  # Use "$" to start from new messages, "0-0" to process existing ones
                StreamGroupOptions(make_stream=True),
            )
        except RequestError as exc:
            if "BUSYGROUP" not in str(exc):
                self.logger.error("Failed to create consumer group %s: %s", self._task_group_name, exc)
                return False
        return True

    async def _on_queue_drain_task_processing(self, task_id: UUID, task_data: TaskData) -> None:
        """No-op override: drained tasks must not be re-enqueued.

        The base ``TaskProcessor`` default requeues a drained task when
        ``canceled_action == "requeue"``. For a durable transport the stream
        entry is still pending and is redelivered on restart, so re-enqueueing
        here would duplicate it (AR-041). Overriding to a no-op lets the
        pending entry redeliver instead.

        Args:
            task_id: Identifier of the queued task.
            task_data: The task data that was still queued at drain time.
        """
        pass

    async def cleanup(self):
        """Perform cleanup on shutdown.

        Drains the internal task queue and cancels running tasks via the
        parent ``TaskProcessor.cleanup()``, then clears the pending
        ``_task_entry_ids`` tracking, stops the Valkey logging handler so its
        worker drains remaining records, and finally closes the Valkey
        connection through :meth:`disconnect`.
        """
        await super().cleanup()
        # Tasks whose handlers ignored cancellation are no longer tracked by the
        # parent cleanup (which drains/cancels running tasks), so clear the entry
        # ids here to avoid leaking them across repeated stop/start cycles (AR-050).
        self._task_entry_ids.clear()
        # Stop the valkey logging handler before disconnect() closes the worker's
        # operational client, so its worker drains remaining records instead of
        # reconnecting (shutdown error flood). In valkey_config= mode the handler
        # owns its own client and stop_logging() closes it independently.
        if self._valkey_logger_handler is not None:
            await self._valkey_logger_handler.stop_logging()
        await self.disconnect()

    async def _register_instance(self) -> None:
        """Add this instance id to the service-scoped worker registry set.

        Best-effort: a failed SADD must not fail startup (log WARNING and
        continue). The registry set is the enumeration index; liveness is the
        status-key TTL refreshed by heartbeat(), so a stale member left by a
        crashed replica is tolerated (the operator probes each member's
        status key). Only glide connection errors are swallowed; other
        exceptions propagate.
        """
        if self.client is None:
            return
        try:
            await self.client.sadd(self._registry_key, [self.instance_id])
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self.logger.log(
                logging.WARNING,
                "Failed to register instance %s in %s: %s",
                self.instance_id,
                self._registry_key,
                exc,
            )

    async def _unregister_instance(self) -> None:
        """Remove this instance id from the service-scoped worker registry set.

        Best-effort: a failed SREM must not fail shutdown (log WARNING and
        continue). Called by _shutdown() before cleanup() disconnects the
        client, so the client is still open here. Only glide connection
        errors are swallowed; other exceptions propagate.
        """
        if self.client is None:
            return
        try:
            await self.client.srem(self._registry_key, [self.instance_id])
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self.logger.log(
                logging.WARNING,
                "Failed to unregister instance %s from %s: %s",
                self.instance_id,
                self._registry_key,
                exc,
            )

    async def return_task_to_queue(self, task_id: UUID, task_data: TaskData) -> None:
        """Re-queue a task by appending it to the Valkey task stream.

        Encodes ``task_data`` with msgpack and appends a new entry to
        the stream identified by ``self._task_stream_name``. The entry
        key is the string representation of ``task_id``.

        Args:
            task_id: The unique identifier of the task.
            task_data: The :class:`TaskData` to return to the Valkey stream.

        Returns:
            None. No-op if the Valkey client is ``None``.
        """
        if self.client:
            t_id: bytes = str(task_id).encode("utf-8")
            packed = msgspec.msgpack.encode(task_data)  # bytes
            await self.client.xadd(self._task_stream_name, [(t_id, packed)])

    async def _recover_pending_tasks(self) -> tuple[bool, bool]:
        """Re-enqueue stream entries left pending by a previous run.

        Uses ``XAUTOCLAIM`` to claim every entry in the consumer group's
        pending list that is idle for at least ``claim_min_idle_ms``
        and enqueue it, so tasks that were read but never acknowledged before
        a crash are redelivered (at-least-once). Called once from the first
        ``fetch_tasks``, before any ``'>'`` read, when no tasks are in flight.

        Returns:
            A ``(recovery_complete, enqueued)`` tuple. ``recovery_complete`` is
            ``True`` only when the pending list was fully drained (or there was
            nothing to recover); ``enqueued`` is ``True`` if at least one
            pending entry was enqueued.
        """
        if self.client is None:
            return True, False
        enqueued = False
        try:
            start: str | bytes = "0-0"
            while True:
                res = await self.client.xautoclaim(
                    self._task_stream_name,
                    self._task_group_name,
                    self._consumer_name,
                    self.__claim_min_idle_ms,
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
                        task_id = field.decode("utf-8") if isinstance(field, bytes) else field
                        try:
                            task_data = msgspec.msgpack.decode(payload_bytes, type=TaskData)
                        except Exception as exc:
                            self.logger.error("Failed to decode recovered task data: %s", exc)
                            continue
                        if not self.enqueue_task(UUID(task_id), task_data):
                            # Queue full mid-recovery; stop claiming so the
                            # remaining pending entries stay pending and are
                            # redelivered on a later poll.
                            self.logger.log(
                                logging.DEBUG,
                                "Task queue full during recovery; deferring task %s",
                                task_id,
                            )
                            return False, enqueued
                        self._task_entry_ids[UUID(task_id)] = entry_id
                        enqueued = True
                if next_start == b"0-0" or next_start == "0-0":
                    break
                start = next_start
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to recover pending tasks: %s", exc)
            return False, enqueued
        return True, enqueued

    async def fetch_tasks(self) -> bool:
        """Fetch new tasks from the Valkey task stream and enqueue them.

        Reads up to ``task_fetch_batch_size`` entries from the task stream
        using ``XREADGROUP`` with ``block_ms=1000`` and the configured
        consumer group. Decodes each msgpack payload into a
        :class:`TaskData` struct and enqueues it via ``enqueue_task()`` as a
        ``(UUID, TaskData)`` tuple. The stream entries are NOT acknowledged
        here: they stay in the consumer group's pending list until each
        handler completes (see :meth:`on_task_completed`), so a crash after
        enqueue redelivers the task (at-least-once). Each entry id is
        recorded in ``_task_entry_ids`` for the later acknowledgement.

        Batching (AR-042): reading several entries per call lets the internal
        queue fill up to ``max_concurrent_tasks`` instead of being starved to
        one task per round-trip.

        On read errors, disconnects and attempts to reconnect to Valkey.

        Returns:
            ``True`` if at least one task was enqueued (from pending-entry
            recovery or this read), ``False`` otherwise. The caller uses this
            to skip its idle backoff after a productive fetch so a backlog
            drains back-to-back.
        """
        if self.client is None:
            return False
        enqueued = False
        if not self._recovered:
            # Only mark recovery done when the pending list was fully drained;
            # a queue-full/error interruption is retried on the next poll (AR-051).
            recovery_complete, recovered_enqueued = await self._recover_pending_tasks()
            if recovery_complete:
                self._recovered = True
            enqueued = recovered_enqueued
        try:
            res = await self.client.xreadgroup(
                {self._task_stream_name: ">"},
                self._task_group_name,
                self._consumer_name,
                StreamReadGroupOptions(
                    count=cast(ValkeyWorkerConfig, self._config).task_fetch_batch_size, block_ms=1000
                ),
            )
            if res:
                for stream, entries in res.items():
                    for entry_id, pairs in entries.items():
                        if pairs is None:
                            continue
                        for field, payload_bytes in pairs:
                            task_id = field.decode("utf-8") if isinstance(field, bytes) else field
                            if payload_bytes is None:
                                continue
                            try:
                                task_data = msgspec.msgpack.decode(payload_bytes, type=TaskData)
                            except Exception as exc:
                                self.logger.error("Failed to decode task data: %s", exc)
                                continue
                            if not self.enqueue_task(UUID(task_id), task_data):
                                # Queue is full; leave the stream entry pending
                                # (do not record its id) so the next poll
                                # redelivers it. Never block the intake manager.
                                self.logger.log(
                                    logging.DEBUG,
                                    "Task queue full; deferring task %s",
                                    task_id,
                                )
                                continue
                            self._task_entry_ids[UUID(task_id)] = entry_id
                            enqueued = True
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self.logger.debug("Failed to fetch/parse task from Valkey stream: %s", exc)
            await self.disconnect()
            await self.connect()
        return enqueued

    async def on_task_completed(
        self,
        task_id: UUID,
        task_data: TaskData,
        task_result: TaskResult | None,
    ) -> None:
        """Acknowledge and delete the stream entry for a completed task.

        Called by the base ``TaskProcessor.handle_task`` when a task's
        processing terminates (success, error, or cancellation). Looks up the
        stream entry id recorded at fetch time and ``XACK``s + ``XDEL``s it, so
        the entry leaves the consumer group's pending list only after the
        handler's work on it is done (at-least-once). ``task_result`` is
        ``None`` when the task was cancelled before producing a result.

        Args:
            task_id: The unique identifier of the task.
            task_data: The task data that was processed.
            task_result: The final ``TaskResult``, or ``None`` on cancellation.
        """
        entry_id = self._task_entry_ids.pop(task_id, None)
        if entry_id is None or self.client is None:
            return
        try:
            await self.client.xack(self._task_stream_name, self._task_group_name, [entry_id])
            await self.client.xdel(self._task_stream_name, [entry_id])
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to acknowledge task %s: %s", task_id, exc)
