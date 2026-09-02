"""
Module providing asynchronous worker, which communicates with the Valkey server using glide client.
Worker provides handling connections, disconnections, initialization, cleanups, and logging.
"""

import logging
import time
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
from ..task_handler import TaskData
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
        """
        Initialize the ValkeyWorker.

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
                to minimal defaults.
            log_stream_name: Name of the Valkey stream used for log entries.
            **kwargs: Additional keyword arguments passed to the parent
                ``AsyncTaskProcessor`` constructor.
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
        valkey_logging_handler = AsyncValkeyHandler(
            stream_name=self._log_stream_name,
            service_name=self.service_name,
            worker_id=self.worker_id,
            valkey_config=self._client_config,
            stdout_enable=False,
        )
        valkey_logging_handler.setLevel(self.logging_level)
        self.logger.addHandler(valkey_logging_handler)

        self._client: GlideClient | None = None
        self._heartbeat_key = f"scietex:{self.service_name}:{self.worker_id}:status"
        self._task_stream_name = f"scietex:{self.service_name}:{self.worker_id}:tasks"
        self._task_group_name = f"scietex:{self.service_name}:{self.worker_id}:task_group"
        self._consumer_name = f"scietex:{self.service_name}:{self.worker_id}"
        self.__encoder = msgspec.msgpack.Encoder()

    @property
    def valkey_config(self) -> ValkeyConfig | GlideClientConfiguration:
        """Valkey configuration property."""
        return self._valkey_config

    @property
    def client(self) -> GlideClient | None:
        """Valkey client property."""
        return self._client

    async def connect(self) -> bool:
        """
        Establishes an asynchronous connection to Valkey.

        Attempts to initialize the Valkey client using the specified configuration.
        Logs successful or unsuccessful connection attempt based on results.

        Returns:
            bool: True if successfully connected, otherwise False.
        """
        if self._client is None:
            try:
                self._client = await GlideClient.create(self._client_config)

                if await self._client.ping():
                    self.logger.log(logging.INFO, "Connected to Valkey")
                    return True
                print("Error pinging Valkey")
                return False
            except (GlideConnectionError, GlideTimeoutError):
                print("Error connecting to Valkey")
                return False
        return True

    async def disconnect(self):
        """Gracefully close the connection to Valkey.

        Closes the current Valkey client session and clears the internal
        client reference.
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
                self.logger.log(
                    logging.DEBUG, "Heartbeat set in Valkey, duration: %.2f ms", duration
                )
            except Exception as exc:
                duration = (time.monotonic() - start_time) * 1000
                self.logger.log(
                    logging.DEBUG,
                    "Failed to set heartbeat in Valkey: %s. Duration: %.2f ms",
                    exc,
                    duration,
                )

    async def initialize(self) -> bool:
        """
        Performs basic initialization steps along with establishing a connection to Valkey.

        Calls the base class's initialize method first, then connects to Valkey.

        Returns:
            bool: True if both initialization steps succeed, otherwise False.
        """

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

        Calls the parent ``AsyncTaskProcessor.cleanup()`` to handle task
        queue drainage and handler cleanup, then closes the Valkey
        connection via ``disconnect()``.
        """
        await super().cleanup()
        await self.disconnect()

    async def purge_tasks(self):
        """
        Purges all pending tasks from the Valkey task stream.

        This method is useful for clearing out any unprocessed tasks, especially during
        shutdown or when resetting the worker's state. It deletes all entries from the
        task stream and acknowledges them to ensure they are not reprocessed.
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
        """Return a task to the Valkey task stream.

        Encodes the ``TaskData`` using msgpack and appends it to the
        task stream identified by ``self._task_stream_name``.

        Args:
            task_id: The unique identifier of the task.
            task_data: The task data to return to the Valkey stream.
        """
        if self.client:
            t_id: bytes = str(task_id).encode("utf-8")
            packed = msgspec.msgpack.encode(task_data)  # bytes
            await self.client.xadd(self._task_stream_name, [(t_id, packed)])

    async def fetch_tasks(self):
        """Fetch a single task from the Valkey task stream and enqueue it.

        Reads one entry from the task stream using ``XREADGROUP`` with
        the configured consumer group, decodes the msgpack payload into
        a ``TaskData`` struct, and puts it into ``self.task_queue``.
        On read errors, attempts to reconnect to Valkey.
        """

        if self.client is None:
            return
        try:
            # Attempt to call the method in a forgiving way.
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
                            except Exception as exc:
                                self.logger.error("Failed to decode task data: %s", exc)
                                continue
                        await self.client.xack(
                            self._task_stream_name,
                            self._task_group_name,
                            [entry_id],
                        )
                        await self.client.xdel(self._task_stream_name, [entry_id])

        except Exception as exc:
            self.logger.debug("Failed to fetch/parse task from Valkey stream: %s", exc)
            await self.disconnect()
            await self.connect()
