"""Valkey-backed async task processor for ``scietex.service``.

Provides ``ValkeyWorker`` — an async worker that extends ``AsyncTaskProcessor``
with Valkey stream-based task distribution, heartbeat publishing, and async
logging. Uses the ``glide`` client for all Valkey operations.

Requires the optional ``valkey-glide`` dependency.
"""

import inspect
import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import msgspec

try:
    from glide import (
        ConnectionError as GlideConnectionError,
    )
    from glide import (
        ExpirySet,
        ExpiryType,
        GlideClient,
        GlideClientConfiguration,
        RequestError,
        StreamGroupOptions,
        StreamReadGroupOptions,
    )
    from glide import (
        TimeoutError as GlideTimeoutError,
    )
except ImportError as e:
    raise ImportError(
        "The 'valkey-glide' module is required to use this feature. "
        "Please install it by running:\n\n    pip install scietex.service[valkey]\n"
    ) from e

from scietex.logging import AsyncValkeyHandler

from ..async_tasks_processor import AsyncTaskProcessor
from ..task_handler import TaskData, TaskResult
from .schemas import Heartbeat
from .valkey_config import (
    ValkeyConfig,
    generate_glide_config,
    read_valkey_config,
)


def _handler_supports_client_injection() -> bool:
    """Whether the installed ``AsyncValkeyHandler`` accepts an injected client.

    AR-018 seam: the external ``scietex.logging`` handler currently owns its own
    ``GlideClient`` and accepts only ``valkey_config``. When it gains a
    client-injection parameter this check flips, letting ``share_glide_client``
    pass the worker's client through for a single shared connection
    (see docs/ROADMAP.md).
    """
    return "client" in inspect.signature(AsyncValkeyHandler.__init__).parameters


class ValkeyWorker(AsyncTaskProcessor):
    """
    Async worker backed by a Valkey (Redis) stream for task distribution.

    Extends ``AsyncTaskProcessor`` with Valkey-specific operations including
    connection management, stream-based task fetching, heartbeat publishing,
    and async logging to a Valkey stream via the ``glide`` client.

    Requires the optional ``valkey-glide`` dependency.

    Connection lifecycle (AR-018): this worker owns two independent
    ``GlideClient`` connections — its own task/heartbeat client (``client``)
    and a second client inside the external
    :class:`~scietex.logging.AsyncValkeyHandler` registered for async logging.
    Each has its own teardown owner: ``disconnect()`` closes the task client,
    while the logging handler closes its own client via ``stop_logging()``.
    ``connect()`` reports the health of both and warns when they diverge (see
    :attr:`logging_connected`). True single-connection unification is gated on
    the external handler accepting an injected client; ``share_glide_client``
    is the reserved seam for that (see docs/ROADMAP.md).

    Attributes:
        client (GlideClient | None): Valkey client instance, initialized
            during ``initialize()``.
        logging_connected (bool): Whether the logging handler's client is
            connected (``True`` only when the handler is present and its
            ``client`` is not ``None``).
    """

    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        worker_id: int = 1,
        conf_dir: str | Path | None = None,
        logging_level: int | str = logging.DEBUG,
        heartbeat_interval: float | None = None,
        watchdog_interval: float | None = None,
        queue_size: int | None = None,
        max_concurrent_tasks: int | None = None,
        valkey_config: ValkeyConfig | GlideClientConfiguration | None = None,
        log_stream_name: str = "scietex:log",
        share_glide_client: bool = False,
        **kwargs,
    ):
        """Initialize the ``ValkeyWorker``.

        Configures the Valkey client from ``valkey_config`` or by reading
        ``valkey.yml`` from the config directory. Sets up stream names for
        tasks, heartbeat status, and logging. Registers an
        :class:`~scietex.logging.AsyncValkeyHandler` for async log entries.

        Args:
            service_name: Name of the service, used for logging and identification.
            version: Version string of the service.
            worker_id: Unique identifier for this worker instance.
            conf_dir: Directory to use for configuration files.
            logging_level: Logging level as string or integer.
            heartbeat_interval: Heartbeat interval in seconds.
            watchdog_interval: Watchdog check interval in seconds.
            queue_size: Maximum size of the internal task queue.
            max_concurrent_tasks: Maximum number of tasks processed concurrently.
            valkey_config: Custom Valkey configuration. If ``None``, reads
                ``valkey.yml`` from the config directory; on failure falls back
                to minimal defaults. Accepts either a :class:`ValkeyConfig`
                or a raw :class:`~glide.GlideClientConfiguration`.
            log_stream_name: Name of the Valkey stream used for log entries.
            share_glide_client: Reserved feature flag for a single shared
                ``GlideClient`` across the task client and the logging handler
                (AR-018). The external ``scietex.logging`` handler does not yet
                accept an injected client, so requesting ``True`` logs a warning
                and falls back to the handler owning its own client. Defaults to
                ``False`` (current two-lifecycle behavior).
            **kwargs: Additional keyword arguments passed to the parent
                ``AsyncTaskProcessor`` constructor.

        Attributes:
            _client (GlideClient | None): Valkey client, initialized during
                :meth:`initialize`.
            _heartbeat_key (str): Key for the worker status heartbeat entry.
            _task_stream_name (str): Valkey stream name for task entries.
            _task_group_name (str): Consumer group name for task fetching.
            _consumer_name (str): Consumer identifier within the task group.
        """
        super().__init__(
            service_name=service_name,
            version=version,
            worker_id=worker_id,
            conf_dir=conf_dir,
            logging_level=logging_level,
            heartbeat_interval=heartbeat_interval,
            watchdog_interval=watchdog_interval,
            queue_size=queue_size,
            max_concurrent_tasks=max_concurrent_tasks,
            **kwargs,
        )
        self._log_stream_name = log_stream_name
        if valkey_config is None:
            valkey_config = read_valkey_config(self.conf_dir)
        self._valkey_config = valkey_config
        if isinstance(valkey_config, GlideClientConfiguration):
            self._client_config = valkey_config
        else:
            self._client_config: GlideClientConfiguration = generate_glide_config(
                valkey_config,
                service_name=self.service_name,
                worker_id=self.worker_id,
                listening=False,
            )
        credentials = self._client_config.credentials
        log_valkey_config = {
            "addresses": [(node.host, node.port) for node in self._client_config.addresses],
            "username": credentials.username if credentials else None,
            "password": credentials.password if credentials else None,
            "use_tls": self._client_config.use_tls,
            "request_timeout": self._client_config.request_timeout,
            "database_id": self._client_config.database_id,
            "client_name": self._client_config.client_name,
            "inflight_requests_limit": self._client_config.inflight_requests_limit,
            "client_az": self._client_config.client_az,
            "lazy_connect": self._client_config.lazy_connect,
            "read_only": self._client_config.read_only,
        }
        # Feature-flag seam for AR-018. Reserved until scietex.logging's
        # AsyncValkeyHandler accepts an injected client; requesting a shared
        # client today only warns and falls back to the handler owning its own
        # connection (docs/ROADMAP.md).
        if share_glide_client and not _handler_supports_client_injection():
            self.logger.warning(
                "share_glide_client=True requested, but the installed scietex.logging "
                "AsyncValkeyHandler does not accept an injected GlideClient; the "
                "logging handler will open its own connection. Client sharing is "
                "reserved for a future scietex.logging release (see docs/ROADMAP.md)."
            )
        self._register_logger_handler(
            AsyncValkeyHandler(
                stream_name=self._log_stream_name,
                service_name=self.service_name,
                worker_id=self.worker_id,
                valkey_config=log_valkey_config,
                stdout_enable=False,
            ),
            name="AsyncValkeyHandler",
        )

        self._client: GlideClient | None = None
        self._heartbeat_key = f"scietex:{self.service_name}:{self.worker_id}:status"
        self._task_stream_name = f"scietex:{self.service_name}:{self.worker_id}:tasks"
        self._task_group_name = f"scietex:{self.service_name}:{self.worker_id}:task_group"
        self._consumer_name = f"scietex:{self.service_name}:{self.worker_id}"
        self.__encoder = msgspec.msgpack.Encoder()

        # Maps a task UUID to the stream entry id it was read from, so the
        # entry can be acknowledged when the handler completes (at-least-once).
        self._task_entry_ids: dict[UUID, str | bytes] = {}

        # True once pending-entry recovery has run (start of the first
        # fetch_tasks), so a crash's unacked entries are redelivered once.
        self._recovered: bool = False

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

    def _valkey_logging_handler(self) -> AsyncValkeyHandler | None:
        """Return the registered ``AsyncValkeyHandler``, or ``None`` if absent."""
        for handler in self.logger.handlers:
            if isinstance(handler, AsyncValkeyHandler):
                return handler
        return None

    @property
    def logging_connected(self) -> bool:
        """Whether the logging handler has a live Valkey client.

        The external ``AsyncValkeyHandler`` owns its own ``GlideClient``
        (AR-018), independent of the worker's ``client``. This reports the
        logging client's health so a half-connected worker (task client up,
        logging client down, or vice versa) is observable.

        Returns:
            ``True`` only when a registered ``AsyncValkeyHandler`` exists and
            its ``client`` is not ``None``; ``False`` otherwise.
        """
        handler = self._valkey_logging_handler()
        return handler is not None and handler.client is not None

    def _log_connection_divergence(self) -> None:
        """Warn when the task and logging clients disagree in health.

        The worker and its ``AsyncValkeyHandler`` each own an independent
        ``GlideClient`` (AR-018), so their health can diverge (one up, one
        down). Surface the split so a half-connected worker is observable.
        """
        worker_connected = self._client is not None
        logging_connected = self.logging_connected
        if worker_connected == logging_connected:
            return
        if worker_connected:
            self.logger.warning(
                "Valkey worker connected, but the logging handler has no live client; "
                "log delivery is unavailable until it connects."
            )
        else:
            self.logger.warning(
                "Valkey logging handler is connected, but the worker client is not; "
                "task intake is unavailable until the worker connects."
            )

    async def connect(self) -> bool:
        """Establish an asynchronous connection to the Valkey server.

        Creates a new :class:`~glide.GlideClient` using the configured
        ``_client_config`` and verifies connectivity with ``PING``.

        ``_client`` is assigned only after ``PING`` succeeds, so a failed
        create or ping leaves ``_client`` as ``None`` and ``connect()``
        returns ``False``. This keeps the return value a reliable
        connectivity signal: callers that guard on ``self.client`` (e.g.
        ``initialize``) never see a half-connected worker.

        On success or failure it also reports the logging handler's client
        health via :meth:`logging_connected` and logs a WARNING when the two
        independent clients diverge (one up, one down) — AR-018. The return
        value still reflects only this worker's own client.

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
            self._log_connection_divergence()
            return False
        try:
            if await client.ping():
                self._client = client
                self.logger.log(logging.INFO, "Connected to Valkey")
                self._log_connection_divergence()
                return True
            self.logger.error("Error pinging Valkey")
        except (GlideConnectionError, GlideTimeoutError):
            self.logger.error("Error connecting to Valkey")
        # Ping failed or raised: never leave a half-connected client behind.
        try:
            await client.close()
        except Exception:
            pass  # best-effort close; the client is unusable either way
        self._log_connection_divergence()
        return False

    async def disconnect(self):
        """Gracefully close the connection to the Valkey server.

        Invokes :meth:`~glide.GlideClient.close` on the active client,
        logs the disconnection, and sets ``_client`` to ``None``.
        """
        if self._client is not None:
            await self._client.close()
            self.logger.info("Valkey client disconnected")
            self._client = None

    async def heartbeat(self) -> None:
        """Publish a heartbeat entry to the Valkey status key.

        Encodes a ``Heartbeat`` struct with service metadata and writes it
        to ``self._heartbeat_key`` with a TTL set to twice the heartbeat
        interval. Logs the duration at DEBUG and any failure at WARNING.
        """

        if self.client and self.start_time:
            heartbeat_data = Heartbeat(
                service=self.service_name,
                worker_id=self.worker_id,
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
            except Exception as exc:
                duration = (time.monotonic() - start_time) * 1000
                self.logger.log(
                    logging.WARNING,
                    "Failed to set heartbeat in Valkey: %s. Duration: %.2f ms",
                    exc,
                    duration,
                )

    async def initialize(self) -> bool:
        """Initialize the worker and prepare the Valkey task stream.

        Calls the parent ``AsyncTaskProcessor.initialize()`` to start
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

    async def cleanup(self):
        """Perform cleanup on shutdown.

        Drains the internal task queue and cancels running tasks via the
        parent ``AsyncTaskProcessor.cleanup()``, then closes the Valkey
        connection through :meth:`disconnect`.
        """
        await super().cleanup()
        await self.disconnect()

    async def purge_tasks(self):
        """Purge all pending and unacknowledged tasks from the Valkey task stream.

        Reads and acknowledges every entry in the task stream via
        ``XREADGROUP`` (both pending and unclaimed), then deletes them
        with ``XDEL``. Also purges any remaining entries via ``XREAD``.

        Returns:
            None. Logs a confirmation message on success or an error
            description on failure.
        """
        if self.client is None:
            self.logger.warning("No Valkey client available to purge tasks")
            return
        client = self.client
        try:
            # Entries already delivered to the group (pending + delivered).
            await self._purge_group_entries(client, "0-0")
            # Entries not yet delivered to the group.
            await self._purge_group_entries(client, ">")
            # Entries in the stream the group never saw.
            await self._purge_stream_entries(client)
            self.logger.log(logging.INFO, "All pending tasks purged from Valkey")
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to purge tasks from Valkey: %s", exc)

    async def _purge_group_entries(self, client: GlideClient, start: str) -> None:
        """Read, acknowledge, and delete group entries from the task stream.

        Reads entries via ``XREADGROUP`` from ``start``, acknowledges them
        with ``XACK`` so they leave the pending list, then deletes them
        with ``XDEL``. Loops until ``XREADGROUP`` returns no more entries.
        """
        while True:
            res = await client.xreadgroup(
                {self._task_stream_name: start},
                self._task_group_name,
                self._consumer_name,
            )
            entry_ids = self._stream_entry_ids(res)
            if not entry_ids:
                return
            await client.xack(self._task_stream_name, self._task_group_name, entry_ids)
            await client.xdel(self._task_stream_name, entry_ids)

    async def _purge_stream_entries(self, client: GlideClient) -> None:
        """Delete every remaining entry in the task stream.

        Reads all stream entries via ``XREAD`` (independent of the consumer
        group) and deletes them with ``XDEL``. Loops until ``XREAD`` returns
        no more entries.
        """
        while True:
            res = await client.xread({self._task_stream_name: "0-0"})
            entry_ids = self._stream_entry_ids(res)
            if not entry_ids:
                return
            await client.xdel(self._task_stream_name, entry_ids)

    def _stream_entry_ids(self, res) -> list[str | bytes | bytearray | memoryview]:
        """Extract stream entry ids from an XREADGROUP/XREAD result mapping."""
        if not res:
            return []
        entries = res[self._task_stream_name.encode("utf-8")]
        return list(entries.keys()) if entries else []

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

    async def _recover_pending_tasks(self) -> None:
        """Re-enqueue stream entries left pending by a previous run.

        Uses ``XAUTOCLAIM`` to claim every entry in the consumer group's
        pending list that is idle (``min_idle_time_ms=0``) and enqueue it, so
        tasks that were read but never acknowledged before a crash are
        redelivered (at-least-once). Called once from the first
        ``fetch_tasks``, before any ``'>'`` read, when no tasks are in flight.

        Returns:
            None. No-op if the Valkey client is ``None``.
        """
        if self.client is None:
            return
        try:
            start: str | bytes = "0-0"
            while True:
                res = await self.client.xautoclaim(
                    self._task_stream_name,
                    self._task_group_name,
                    self._consumer_name,
                    0,
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
                            return
                        self._task_entry_ids[UUID(task_id)] = entry_id
                if next_start == b"0-0" or next_start == "0-0":
                    break
                start = next_start
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to recover pending tasks: %s", exc)

    async def fetch_tasks(self):
        """Fetch a single new task from the Valkey task stream and enqueue it.

        Reads one entry from the task stream using ``XREADGROUP`` with
        ``block_ms=1000`` and the configured consumer group. Decodes the
        msgpack payload into a :class:`TaskData` struct and enqueues it via
        ``enqueue_task()`` as a ``(UUID, TaskData)`` tuple. The stream entry
        is NOT acknowledged here: it stays in the consumer group's pending
        list until the handler completes (see :meth:`on_task_completed`), so a
        crash after enqueue redelivers the task (at-least-once). The entry id
        is recorded in ``_task_entry_ids`` for the later acknowledgement.

        On read errors, disconnects and attempts to reconnect to Valkey.

        Returns:
            None. No-op if the Valkey client is ``None``.
        """
        if self.client is None:
            return
        if not self._recovered:
            self._recovered = True
            await self._recover_pending_tasks()
        try:
            res = await self.client.xreadgroup(
                {self._task_stream_name: ">"},
                self._task_group_name,
                self._consumer_name,
                StreamReadGroupOptions(count=1, block_ms=1000),
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
        except Exception as exc:
            self.logger.debug("Failed to fetch/parse task from Valkey stream: %s", exc)
            await self.disconnect()
            await self.connect()

    async def on_task_completed(
        self,
        task_id: UUID,
        task_data: TaskData,
        task_result: TaskResult | None,
    ) -> None:
        """Acknowledge and delete the stream entry for a completed task.

        Called by the base ``AsyncTaskProcessor.handle_task`` when a task's
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
