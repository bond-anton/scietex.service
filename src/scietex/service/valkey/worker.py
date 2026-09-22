"""Valkey-backed async task processor for ``scietex.service``.

Provides ``ValkeyWorker`` — an async worker that extends ``TransportWorker``
with Valkey stream-based task distribution, heartbeat publishing, and async
logging. Uses the ``glide`` client for all Valkey operations.

Requires the optional ``valkey-glide`` dependency.
"""

import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import ClassVar, Literal, cast
from uuid import UUID

import msgspec
from scietex.logging import AsyncValkeyHandler

from ..config_reload import CONFIG_SOURCE_UNAVAILABLE, ConfigApplyOutcome
from ..heartbeat import Heartbeat
from ..theme import Theme
from ..transport_worker import TransportWorker
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
from .config_source import ValkeyConfigSource
from .lease import TaskLeaseManager, derive_task_lease_ttl
from .tracking import TaskStatusStore
from .transport import ValkeyTransport

#: Client-construction injection seam (AR-074): connect() builds its client by
#: awaiting this callable with the resolved GlideClientConfiguration, so tests
#: and embedders can supply a fake or externally-built client.
ClientFactory = Callable[[GlideClientConfiguration], Awaitable[GlideClient]]


class ValkeyWorker(TransportWorker):
    """
    Async worker backed by a Valkey (Redis) stream for task distribution.

    Extends ``TransportWorker`` with Valkey-specific operations including
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

    # Transport label surfaced in the CRITICAL down message (AR-102); the base
    # reads it when building the TransportHealth supervisor.
    _transport_name: ClassVar[str] = "Valkey"

    def __init__(
        self,
        config: ValkeyWorkerConfig | None = None,
        *,
        client_factory: ClientFactory | None = None,
        theme: Theme | None = None,
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
            theme: The rendering theme for the startup banner and console log
                formatter. Defaults to :class:`ScietexMonochrome` when ``None``.
                It is a live object, not a config field, so it is injected via
                the constructor.

        Attributes:
            _client (GlideClient | None): Valkey client, initialized during
                :meth:`initialize`.
            _valkey_logger_handler (AsyncValkeyHandler | None): The logging
                handler, built lazily on the first successful :meth:`connect`
                and reused across restarts. Owns its own connection
                (``valkey_config=`` mode); the worker never shares its client.
            _heartbeat_key (str): Resolved Valkey key holding this worker's
                heartbeat, with ``{service}`` and ``{instance_id}`` substituted.
            _log_stream_name (str): Resolved Valkey stream name for log entries,
                with ``{service}`` substituted.
            _config_key (str): Resolved Valkey key holding the desired-state
                remote config, with ``{service}`` substituted.
            _control_stream_name (str): Resolved per-worker directed control
                stream name, with ``{service}`` and ``{instance_id}`` substituted.
            _control_broadcast_stream_name (str): Resolved service-scoped
                broadcast control stream name, with ``{service}`` substituted.
            _task_stream_name (str): Resolved Valkey stream name for task
                entries, with ``{service}`` substituted.
            _task_group_name (str): Resolved consumer group name for task
                fetching, with ``{service}`` substituted.
            _consumer_name (str): Resolved consumer identifier within the task
                group, with ``{service}`` and ``{instance_id}`` substituted.
            _control_entry_ids (dict[UUID, tuple[str, str | bytes]]): Maps a
                control task id to its directed-stream (stream name, entry id);
                control entries are never leased.
        """
        factory = client_factory if client_factory is not None else GlideClient.create
        super().__init__(config, client_factory=factory, theme=theme)
        # The base already stored the concrete config into ``self._config``
        # (AR-069); keep a typed local reference for the synchronous setup
        # reads below.
        cfg = cast(ValkeyWorkerConfig, self._config)

        # {service} is resolved here exactly as MQTT resolves log_topic; a name
        # without the placeholder passes through unchanged.
        self._log_stream_name = cfg.log_stream_name.format(service=self.service_name)
        # The durable key is the source of truth for remote config (design §2);
        # it is resolved here like log_stream_name.
        self._config_key = cfg.config_key.format(service=self.service_name)
        # The directed control stream embeds the instance id so the stream name
        # itself is the worker's address; the broadcast stream stays
        # service-scoped (AR-123).
        self._control_stream_name = cfg.control_stream_name.format(
            service=self.service_name, instance_id=self.instance_id
        )
        self._control_broadcast_stream_name = cfg.control_broadcast_stream_name.format(service=self.service_name)
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
            )
        # The logging handler owns its own connection (AR-059/061); it is built
        # lazily on the first successful connect() and reused across restarts
        # (see _ensure_logging_handler).
        self._valkey_logger_handler: AsyncValkeyHandler | None = None

        self._client: GlideClient | None = None
        # Client-construction seam (AR-074): connect() awaits this factory with
        # the resolved client config; defaults to the real GlideClient.create.
        self._client_factory: ClientFactory = factory
        self._heartbeat_key = cfg.heartbeat_key.format(service=self.service_name, instance_id=self.instance_id)
        self._task_stream_name = cfg.task_stream_name.format(service=self.service_name)
        self._task_group_name = cfg.task_group_name.format(service=self.service_name)
        self._consumer_name = cfg.consumer_name.format(service=self.service_name, instance_id=self.instance_id)
        # No registry Set: the heartbeat keys are the enumeration index. A
        # client SCANs ``scietex:{service}:*:status`` and reads each entry's
        # ``ttl``, so a crashed replica's key expires on its own instead of
        # leaving a stale member behind (AR-123).
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
            instance_id=self.instance_id,
            report_failure=self._health.report_failure,
        )
        self.__encoder = msgspec.msgpack.Encoder()

        # Maps a task UUID to the stream entry id it was read from, so the
        # entry can be acknowledged when the handler completes (at-least-once).
        self._task_entry_ids: dict[UUID, str | bytes] = {}
        # Maps a control task UUID to its (stream name, entry id) on the
        # group-less directed control stream (AR-123 §4.5). Kept parallel to
        # ``_task_entry_ids`` so the transport can branch ack/on_started/
        # on_drain/requeue on control ownership; control entries are never
        # leased.
        self._control_entry_ids: dict[UUID, tuple[str, str | bytes]] = {}

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
            control_stream_name=self._control_stream_name,
            control_broadcast_stream_name=self._control_broadcast_stream_name,
            control_entry_ids=self._control_entry_ids,
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
        """
        return self._valkey_config

    @property
    def client(self) -> GlideClient | None:
        """The Valkey :class:`~glide.GlideClient` instance.

        ``None`` until :meth:`initialize` completes successfully.
        """
        return self._client

    def _ensure_client_config(self) -> GlideClientConfiguration:
        """Load the Valkey config on first connect (AR-066).

        Defers the filesystem read — and the default ``valkey.yml`` write /
        config-dir ``mkdir`` it triggers — out of ``__init__`` to the first
        connect, so construction is side-effect-free. Populates
        ``_valkey_config`` and ``_client_config`` the same way ``__init__`` does
        for an explicitly-provided config, then no-ops on later calls.
        """
        if self._client_config is not None:
            return self._client_config
        valkey_config = read_valkey_config(self.conf_dir)
        self._valkey_config = valkey_config
        self._client_config = generate_glide_config(
            valkey_config,
            service_name=self.service_name,
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
        handler is unavailable until the config is resolved.
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

    async def heartbeat(self) -> None:
        """Publish a heartbeat entry to the Valkey status key.

        Encodes a ``Heartbeat`` struct with service metadata and writes it
        to ``self._heartbeat_key`` with a TTL of ``self.active_ttl``. On the
        same tick it refreshes the TTL of the directed control stream
        (``self._control_stream_name``), so a live worker keeps its directed
        stream alive while a departed worker's expires on its own (AR-123
        §4.3). Logs the duration at DEBUG and any failure at WARNING.

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
                ttl=self.active_ttl,
                timestamp=datetime.now(timezone.utc),
            )
            self.logger.log(logging.DEBUG, "Sending heartbeat to Valkey: %s", heartbeat_data)
            start_time = time.monotonic()
            try:
                await client.set(
                    self._heartbeat_key,
                    value=self.__encoder.encode(heartbeat_data),
                    expiry=ExpirySet(ExpiryType.SEC, int(self.active_ttl)),
                )
                duration = (time.monotonic() - start_time) * 1000
                self.logger.log(logging.DEBUG, "Heartbeat set in Valkey, duration: %.2f ms", duration)
                # The directed control stream is self-expiring state like the
                # status key: a live worker refreshes its TTL on the same tick,
                # so a departed worker's stream expires on its own (AR-123
                # §4.3). The broadcast stream is service-scoped and has no owner
                # to refresh it, so it has no TTL.
                await client.expire(self._control_stream_name, int(self.active_ttl))
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

        After a successful connect, the durable-key config source is attached
        and the local ``config.yml`` snapshot and remote envelope are applied
        (design §5 precedence: constructor < local file < remote). A missing,
        invalid, or unreachable remote config never fails startup.

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

        # Attach the durable-key source now that the client exists, then apply
        # the local snapshot and the remote source of truth. Startup must not
        # fail on a bad or unreachable remote config (availability-first).
        self._config_manager.attach_source(
            ValkeyConfigSource(
                client_provider=lambda: self._client,
                key=self._config_key,
                logger=self.logger,
            )
        )
        await self._apply_local_config()
        await self._reload_remote_config()

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

    async def _read_remote_outcome(self) -> ConfigApplyOutcome:
        """Read and apply the durable-key config envelope (design §5).

        A missing key, an invalid envelope, or an unreachable server must not
        fail startup: ``reload`` returns an outcome instead of raising, and the
        worker stays on its local/default config either way. An unavailable
        source maps to ``CONFIG_SOURCE_UNAVAILABLE``, so the base pipeline logs
        the "no config available" fallback instead of an error.
        """
        try:
            return await self._config_manager.reload_remote()
        except Exception as exc:
            self.logger.error("Failed to reload remote config: %s", exc)
            return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_UNAVAILABLE)

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
        """Publish an ``active`` heartbeat so the instance is discoverable.

        Called once by ``_startup()`` after ``initialize()`` succeeds and before
        managers start. The heartbeat manager's first beat refreshes the same
        entry; publishing here means a client that SCANs the status keys sees
        the instance from the moment it is reachable, without waiting for the
        first beat.

        Best-effort: a failed write must not fail startup (log WARNING and
        continue). Only glide connection, request, and timeout errors are
        swallowed; other exceptions propagate.
        """
        await self._publish_status("active", self.active_ttl)

    async def _unregister_instance(self) -> None:
        """Publish an ``inactive`` heartbeat on shutdown.

        Called once by ``_shutdown()`` after managers stop and before
        ``cleanup()`` disconnects the client, so the client is still open here.
        The entry is never deleted: it is left to expire under
        ``inactive_ttl``, which gives a monitoring client a window to observe
        the death before the key disappears (AR-123).

        Best-effort: a failed write must not fail shutdown (log WARNING and
        continue). Only glide connection, request, and timeout errors are
        swallowed; other exceptions propagate.
        """
        await self._publish_status("inactive", self.inactive_ttl)

    async def _publish_status(self, status: Literal["active", "inactive"], ttl: float) -> None:
        """Write a heartbeat entry with an explicit ``status`` and ``ttl``.

        Shared by ``_register_instance`` and ``_unregister_instance``, which
        differ only in the status they publish. ``start_time`` may be unset
        during a failed startup; fall back to the current time so the entry is
        still well-formed.
        """
        client = self.client
        if client is None:
            return
        heartbeat_data = Heartbeat(
            service=self.service_name,
            instance_id=self.instance_id,
            status=status,
            heartbeat_interval=self.heartbeat_interval,
            start_time=self.start_time or datetime.now(timezone.utc),
            ttl=ttl,
            timestamp=datetime.now(timezone.utc),
        )
        try:
            await client.set(
                self._heartbeat_key,
                value=self.__encoder.encode(heartbeat_data),
                expiry=ExpirySet(ExpiryType.SEC, int(ttl)),
            )
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self.logger.log(
                logging.WARNING,
                "Failed to publish %s status for instance %s: %s",
                status,
                self.instance_id,
                exc,
            )
            self._health.report_failure(exc)
