"""Valkey-backed async task processor for ``scietex.service``.

Provides ``ValkeyWorker`` — an async worker that extends ``TaskProcessor``
with Valkey stream-based task distribution, heartbeat publishing, and async
logging. Uses the ``glide`` client for all Valkey operations.

Requires the optional ``valkey-glide`` dependency.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import ClassVar, cast
from uuid import UUID

import msgspec
from scietex.logging import AsyncValkeyHandler

from ..health import TransportHealth
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
)
from .config import (
    DEFAULT_TASK_TRACKING_TTL,
    ValkeyConfig,
    ValkeyWorkerConfig,
    generate_glide_config,
    logging_handler_config,
    read_valkey_config,
)
from .lease import TaskLeaseManager, derive_task_lease_ttl
from .schemas import Heartbeat
from .tracking import TaskStatusStore
from .transport import ValkeyTransport

# Client-construction injection seam (AR-074): connect() builds its client by
# awaiting this callable with the resolved GlideClientConfiguration, so tests
# and embedders can supply a fake or externally-built client.
ClientFactory = Callable[[GlideClientConfiguration], Awaitable[GlideClient]]


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
    connection (``valkey_config=`` mode), so the worker neither shares nor
    tears down the logging client.

    Client construction (AR-074): ``connect()`` builds its ``GlideClient`` by
    awaiting the ``client_factory=`` callable with the resolved configuration
    (defaulting to ``GlideClient.create``), so tests and embedders can inject a
    fake or externally-built client. The worker owns the returned client for its
    lifetime and calls ``close()`` on it during ``disconnect()``.

    Attributes:
        client (GlideClient | None): Valkey client instance, initialized
            during ``initialize()``.
    """

    # Concrete config struct for this worker. The base stores it into
    # ``self._config``, so ``config=None`` instantiates the concrete type here
    # (AR-069) and no re-store / double-instantiation is needed.
    _config_type: ClassVar[type[ValkeyWorkerConfig]] = ValkeyWorkerConfig

    def __init__(
        self,
        config: ValkeyWorkerConfig | None = None,
        *,
        client_factory: ClientFactory | None = None,
    ) -> None:
        """Initialize the ``ValkeyWorker``.

        Configures the Valkey client from ``config.valkey_config`` or, when
        that is ``None``, defers reading ``valkey.yml`` from the config
        directory to the first :meth:`connect` (AR-066), so construction is
        side-effect-free. Sets up stream names for tasks, heartbeat status, and
        logging. The :class:`~scietex.logging.AsyncValkeyHandler` for async log
        entries owns its own connection (``valkey_config=`` mode) and is built
        and registered on the first successful :meth:`connect`.

        Args:
            config: A :class:`~scietex.service.valkey.config.ValkeyWorkerConfig`
                holding the worker's service identity, task-queue settings, and
                Valkey-specific fields. ``None`` uses the struct defaults.
            client_factory: Optional async callable taking a
                :class:`~glide.GlideClientConfiguration` and returning a
                :class:`~glide.GlideClient`. Defaults to ``GlideClient.create``.
                Lets tests and embedders inject a fake or externally-built
                client without a live Valkey server.

        Attributes:
            _client (GlideClient | None): Valkey client, initialized during
                :meth:`initialize`.
            _valkey_logger_handler (AsyncValkeyHandler | None): The logging
                handler, built lazily on the first successful :meth:`connect`
                and reused across restarts. Owns its own connection
                (``valkey_config=`` mode); the worker never shares its client.
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
        # AR-066: when no explicit config was given, defer the filesystem read
        # (and the default valkey.yml write / config-dir mkdir it triggers) to
        # the first connect, so construction is side-effect-free. Both
        # attributes stay None until _ensure_client_config() loads them.
        if cfg.valkey_config is None:
            self._valkey_config: ValkeyConfig | None = None
            self._client_config: GlideClientConfiguration | None = None
        else:
            self._valkey_config = cfg.valkey_config
            self._client_config = generate_glide_config(
                cfg.valkey_config,
                service_name=self.service_name,
                worker_id=self.instance_id,
            )
        # The logging handler owns its own connection (AR-059/061); it is built
        # lazily on the first successful connect() and reused across restarts
        # (see _ensure_logging_handler).
        self._valkey_logger_handler: AsyncValkeyHandler | None = None

        self._client: GlideClient | None = None
        # Client-construction seam (AR-074): connect() awaits this factory with
        # the resolved client config; defaults to the real GlideClient.create.
        self._client_factory: ClientFactory = client_factory if client_factory is not None else GlideClient.create
        # Serializes connect()/disconnect() so only one task mutates _client at
        # a time (AR-059): intake reconnect and shutdown cannot race each other.
        self._client_lock: asyncio.Lock = asyncio.Lock()
        # Connection-health supervisor (AR-075): the single reconnect owner that
        # every glide-failure site reports into. Built before the collaborators
        # so lease/status/transport can all receive it by injection. The down
        # threshold and cooldown are derived from the intervals, not configured.
        self._health = TransportHealth(
            reconnect=self._reconnect,
            is_connected=lambda: self._client is not None,
            logger=self.logger,
            transport_name="Valkey",
            down_threshold=max(3 * self.watchdog_interval, self.heartbeat_interval),
            reconnect_cooldown=self.watchdog_interval,
        )
        self._heartbeat_key = f"scietex:{self.service_name}:{self.instance_id}:status"
        self._task_stream_name = f"scietex:{self.service_name}:tasks"
        self._task_group_name = f"scietex:{self.service_name}:task_group"
        self._consumer_name = f"scietex:{self.service_name}:{self.instance_id}"
        self._registry_key = f"scietex:{self.service_name}:workers"
        # Extracted collaborators (AR-073): the lease manager and tracking store
        # own the per-entry lease and status-record concerns this class used to
        # inline. Both reach the operational client through a late-bound
        # provider, so they see the client connect() assigns (or None before the
        # first successful connect).
        self._task_lease = TaskLeaseManager(
            service_name=self.service_name,
            consumer_name=self._consumer_name,
            lease_ttl=(
                cfg.task_lease_ttl
                if cfg.task_lease_ttl is not None
                else derive_task_lease_ttl(self.heartbeat_interval, self.watchdog_interval)
            ),
            client_provider=lambda: self._client,
            logger=self.logger,
            report_failure=self._health.report_failure,
        )
        self._task_status = TaskStatusStore(
            service_name=self.service_name,
            tracking_ttl=cfg.task_tracking_ttl if cfg.task_tracking_ttl is not None else DEFAULT_TASK_TRACKING_TTL,
            client_provider=lambda: self._client,
            logger=self.logger,
            report_failure=self._health.report_failure,
        )
        self.__encoder = msgspec.msgpack.Encoder()

        # Maps a task UUID to the stream entry id it was read from, so the
        # entry can be acknowledged when the handler completes (at-least-once).
        self._task_entry_ids: dict[UUID, str | bytes] = {}

        # Transport extension seam (AR-072): the stream operations this worker
        # used to override as TaskProcessor hooks now live on ValkeyTransport,
        # which receives the collaborators above by injection (ownership of the
        # lease manager, tracking store, and entry-id map stays here). The
        # InMemoryTransport built by super().__init__ is empty and discarded.
        self._valkey_transport = ValkeyTransport(
            config=cfg,
            service_name=self.service_name,
            consumer_name=self._consumer_name,
            stream_name=self._task_stream_name,
            group_name=self._task_group_name,
            client_provider=lambda: self._client,
            health=self._health,
            lease=self._task_lease,
            status=self._task_status,
            entry_ids=self._task_entry_ids,
            logger=self.logger,
        )
        self._transport = self._valkey_transport

    @property
    def valkey_config(self) -> ValkeyConfig | None:
        """The Valkey configuration used by this worker.

        Returns the :class:`ValkeyConfig` schema the worker was constructed
        with. When no explicit config was given at construction, the config is
        loaded lazily from disk at first connect, so this is ``None`` until
        :meth:`connect`/:meth:`initialize` has run (AR-066).

        Returns:
            The Valkey configuration instance, or ``None`` before the first
            connect when no explicit config was provided.
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

    @property
    def transport_health(self) -> TransportHealth:
        """The connection-health supervisor for this worker (read-only, AR-075).

        Exposes the :class:`~scietex.service.health.TransportHealth`
        aggregating every glide failure and owning the single reconnect path, so
        callers can observe degraded state without reaching into internals.
        """
        return self._health

    def _ensure_client_config(self) -> GlideClientConfiguration:
        """Load the Valkey config on first connect (AR-066).

        Defers the filesystem read — and the default ``valkey.yml`` write /
        config-dir ``mkdir`` it triggers — out of ``__init__`` to the first
        connect, so construction is side-effect-free. Populates
        ``_valkey_config`` and ``_client_config`` the same way ``__init__`` does
        for an explicitly-provided config, then no-ops on later calls.

        Returns:
            The ``GlideClientConfiguration`` used to create the client.
        """
        if self._client_config is not None:
            return self._client_config
        valkey_config = read_valkey_config(self.conf_dir)
        self._valkey_config = valkey_config
        self._client_config = generate_glide_config(
            valkey_config,
            service_name=self.service_name,
            worker_id=self.instance_id,
        )
        return self._client_config

    def _ensure_logging_handler(self) -> AsyncValkeyHandler | None:
        """Build and register the logging handler once, then reuse it.

        The handler is constructed with ``valkey_config=`` so it builds,
        closes, and reconnects its own ``GlideClient`` autonomously
        (``_owns_client`` is True); the worker never touches ``handler.client``
        (AR-059/061). ``self._valkey_config`` is set by ``_ensure_client_config``
        (or ``__init__``) before this runs, but ``ty`` cannot narrow that
        cross-method guarantee, so a local ``None`` guard documents that the
        handler is simply unavailable until the config is resolved.
        """
        if self._valkey_logger_handler is not None:
            return self._valkey_logger_handler

        config = self._valkey_config
        if config is None:
            return None
        self._valkey_logger_handler = AsyncValkeyHandler(
            stream_name=self._log_stream_name,
            valkey_config=logging_handler_config(config),
        )
        self._logging_lifecycle.register_logger_handler(self._valkey_logger_handler)
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

        Creates a new :class:`~glide.GlideClient` by awaiting the configured
        ``_client_factory`` with the resolved ``_client_config`` and verifies
        connectivity with ``PING``.

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
        client_config = self._ensure_client_config()
        try:
            client = await self._client_factory(client_config)
        except (GlideConnectionError, GlideTimeoutError):
            self.logger.error("Error connecting to Valkey")
            return False
        try:
            if await client.ping():
                self._client = client
                self._health.mark_connected()
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

        The logging handler owns its own client, so it is left untouched here;
        only the worker's operational client is closed. Invokes
        :meth:`~glide.GlideClient.close` on the active client, logs the
        disconnection, and sets ``_client`` to ``None``.
        """
        if self._client is not None:
            await self._client.close()
            self.logger.info("Valkey client disconnected")
            self._client = None
            self._health.mark_disconnected()

    async def _reconnect(self) -> None:
        """Tear down and re-establish the connection (intake error recovery)."""
        await self.disconnect()
        await self.connect()

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
            # Capture the client once: a concurrent _disconnect_locked may close
            # it mid-await, but glide errors are swallowed and reported to
            # TransportHealth, which drives the reconnect (AR-083).
            client = self.client
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
                await client.set(
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
                self._health.report_failure(exc)

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
        client = self.client
        if not client:
            return False

        try:
            await client.xgroup_create(
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
        status key). Only glide connection, request, and timeout errors are
        swallowed; other exceptions propagate.
        """
        client = self.client
        if client is None:
            return
        try:
            await client.sadd(self._registry_key, [self.instance_id])
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self.logger.log(
                logging.WARNING,
                "Failed to register instance %s in %s: %s",
                self.instance_id,
                self._registry_key,
                exc,
            )
            self._health.report_failure(exc)

    async def _unregister_instance(self) -> None:
        """Remove this instance id from the service-scoped worker registry set.

        Best-effort: a failed SREM must not fail shutdown (log WARNING and
        continue). Called by _shutdown() before cleanup() disconnects the
        client, so the client is still open here. Only glide connection,
        request, and timeout errors are swallowed; other exceptions propagate.
        """
        client = self.client
        if client is None:
            return
        try:
            await client.srem(self._registry_key, [self.instance_id])
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self.logger.log(
                logging.WARNING,
                "Failed to unregister instance %s from %s: %s",
                self.instance_id,
                self._registry_key,
                exc,
            )
            self._health.report_failure(exc)

    async def watchdog(self) -> None:
        """Refresh per-entry leases, supervise the connection, then run the base watchdog.

        Refreshing before ``super().watchdog()`` keeps leases fresh even when the
        base implementation blocks on a cancellation wait. Tasks the base watchdog
        cancels are removed from ``running_tasks``, so their leases stop being
        refreshed and expire, making the entries reclaimable.

        After the base watchdog runs, a degraded connection past its down
        threshold surfaces one CRITICAL message per down episode (AR-075); when
        healthy this emits nothing.
        """
        # refresh_leases is valkey-specific (not part of the core TaskTransport
        # protocol), so it is reached through the concrete transport.
        await self._valkey_transport.refresh_leases()
        await self._health.recover()
        await super().watchdog()
        msg = self._health.critical_report()
        if msg:
            self.logger.critical(msg)
