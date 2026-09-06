"""Valkey-backed async task processor for ``scietex.service``.

Provides ``ValkeyWorker`` — an async worker that extends ``AsyncTaskProcessor``
with Valkey stream-based task distribution, heartbeat publishing, and async
logging. Uses the ``glide`` client for all Valkey operations.

Requires the optional ``valkey-glide`` dependency.
"""

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


class ValkeyWorker(AsyncTaskProcessor):
    """
    Async worker backed by a Valkey (Redis) stream for task distribution.

    Extends ``AsyncTaskProcessor`` with Valkey-specific operations including
    connection management, stream-based task fetching, heartbeat publishing,
    and async logging to a Valkey stream via the ``glide`` client.

    Requires the optional ``valkey-glide`` dependency.

    Attributes:
        client (GlideClient | None): Valkey client instance, initialized
            during ``initialize()``.
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

    async def connect(self) -> bool:
        """Establish an asynchronous connection to the Valkey server.

        Creates a new :class:`~glide.GlideClient` using the configured
        ``_client_config`` and verifies connectivity with ``PING``.

        ``_client`` is assigned only after ``PING`` succeeds, so a failed
        create or ping leaves ``_client`` as ``None`` and ``connect()``
        returns ``False``. This keeps the return value a reliable
        connectivity signal: callers that guard on ``self.client`` (e.g.
        ``initialize``) never see a half-connected worker.

        Returns:
            ``True`` if the connection is established and ``PING``
            succeeds; ``False`` on connection failure or timeout.
        """
        if self._client is not None:
            return True
        try:
            client = await GlideClient.create(self._client_config)
        except (GlideConnectionError, GlideTimeoutError):
            print("Error connecting to Valkey")
            return False
        try:
            if await client.ping():
                self._client = client
                self.logger.log(logging.INFO, "Connected to Valkey")
                return True
            print("Error pinging Valkey")
        except (GlideConnectionError, GlideTimeoutError):
            print("Error connecting to Valkey")
        # Ping failed or raised: never leave a half-connected client behind.
        try:
            await client.close()
        except Exception:
            pass  # best-effort close; the client is unusable either way
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
        interval. Logs duration and any errors at DEBUG level.
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
                    logging.DEBUG,
                    "Failed to set heartbeat in Valkey: %s. Duration: %.2f ms",
                    exc,
                    duration,
                )

    async def initialize(self) -> bool:
        """Initialize the worker and prepare the Valkey task stream.

        Calls the parent ``AsyncTaskProcessor.initialize()`` to start
        registered task handlers, then connects to Valkey and creates
        the consumer group for the task stream (with ``make_stream=True``).
        Fails silently if the group already exists.

        Returns:
            ``True`` if the parent initialization and Valkey connection
            succeed, and the consumer group is ready. ``False`` if the
            parent initialization fails or the client is unavailable.
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
        except Exception as exc:
            self.logger.log(logging.DEBUG, "Valkey: %s", exc)
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
            print("No Valkey client available to purge tasks")
            return
        try:
            while True:
                # print("PURGING OLD TASKS")
                res = await self.client.xreadgroup(
                    {self._task_stream_name: "0-0"},
                    self._task_group_name,
                    self._consumer_name,
                    # StreamReadGroupOptions(count=100, block_ms=1000),
                )
                if not res:
                    break  # No results for stream_name, exit the loop
                # print("  OLD TASKS", res)
                entries = res[self._task_stream_name.encode("utf-8")]
                if not entries:
                    break  # No entries, exit the loop
                entries_ids: list[str | bytes | bytearray | memoryview[int]] = list(entries.keys())
                await self.client.xack(
                    self._task_stream_name,
                    self._task_group_name,
                    entries_ids,
                )
                await self.client.xdel(self._task_stream_name, entries_ids)
            while True:
                # print("PURGING PENDING TASKS")
                res = await self.client.xreadgroup(
                    {self._task_stream_name: ">"},
                    self._task_group_name,
                    self._consumer_name,
                    # StreamReadGroupOptions(count=100, block_ms=1000),
                )
                if not res:
                    break  # No results for stream_name, exit the loop
                # print("  PENDING TASKS", res)
                entries = res[self._task_stream_name.encode("utf-8")]
                if not entries:
                    break  # No entries, exit the loop
                entries_ids: list[str | bytes | bytearray | memoryview[int]] = list(entries.keys())
                await self.client.xack(
                    self._task_stream_name,
                    self._task_group_name,
                    entries_ids,
                )
                await self.client.xdel(self._task_stream_name, entries_ids)
            while True:
                # print("PURGING STREAM")
                res = await self.client.xread(
                    {self._task_stream_name: "0-0"},
                    # StreamReadOptions(count=100, block_ms=1000),
                )
                if not res:
                    break  # No results for stream_name, exit the loop
                # print("  STREAM DATA", res)
                entries = res[self._task_stream_name.encode("utf-8")]
                if not entries:
                    break  # No entries, exit the loop
                entries_ids: list[str | bytes | bytearray | memoryview[int]] = list(entries.keys())
                await self.client.xdel(self._task_stream_name, entries_ids)
            self.logger.log(logging.INFO, "All pending tasks purged from Valkey")
        except Exception as exc:
            self.logger.log(logging.DEBUG, "Failed to purge tasks from Valkey: %s", exc)

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
                            await self.task_queue.put((UUID(task_id), task_data))
                            self._task_entry_ids[UUID(task_id)] = entry_id
                        except Exception as exc:
                            self.logger.error("Failed to decode recovered task data: %s", exc)
                if next_start == b"0-0" or next_start == "0-0":
                    break
                start = next_start
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to recover pending tasks: %s", exc)

    async def fetch_tasks(self):
        """Fetch a single new task from the Valkey task stream and enqueue it.

        Reads one entry from the task stream using ``XREADGROUP`` with
        ``block_ms=1000`` and the configured consumer group. Decodes the
        msgpack payload into a :class:`TaskData` struct and puts it into
        ``self.task_queue`` as a ``(UUID, TaskData)`` tuple. The stream entry
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
                                await self.task_queue.put((UUID(task_id), task_data))
                                self._task_entry_ids[UUID(task_id)] = entry_id
                            except Exception as exc:
                                self.logger.error("Failed to decode task data: %s", exc)
                                continue
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
