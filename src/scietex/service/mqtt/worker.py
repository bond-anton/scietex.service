"""MQTT-backed async task processor for ``scietex.service`` (v4.4.0).

Provides ``MqttWorker`` — an async worker that extends ``TransportWorker``
with MQTT topic-based task distribution, heartbeat publishing, and async
logging. Uses the ``aiomqtt`` client for all broker operations.

Requires the optional ``aiomqtt`` dependency.
"""

import asyncio
import logging
import random
import sqlite3
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar, Literal, cast

import msgspec
from scietex.logging import AsyncMqttHandler

from ..config import DEFAULT_CONFIG_STARTUP_TIMEOUT, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_WATCHDOG_INTERVAL
from ..config_reload import CONFIG_SOURCE_UNAVAILABLE, ConfigApplyOutcome
from ..heartbeat import Heartbeat
from ..task_handler.schemas import task_data_id
from ..task_handler.wire import decode_task_envelope
from ..theme import Theme
from ..transport_worker import TransportWorker
from ._aiomqtt import Client, Message, MqttError, PacketTypes, Properties, ProtocolVersion, Will
from .config import MqttConfig, MqttWorkerConfig, read_mqtt_config
from .config_source import MqttConfigSource
from .inbox import MemoryInbox, MqttInbox
from .inbox_sqlite import SqliteMqttInbox, derive_inbox_lease_ttl
from .logging import logging_handler_config
from .transport import MqttTransport

#: QoS for the retained heartbeat/registry messages. Retained liveness should
#: be at-least-once so the marker is reliably set; each beat refreshes it.
_REGISTRY_QOS: int = 1

#: Client-construction injection seam (AR-074): connect() builds its client by
#: awaiting this callable with the resolved MqttConfig, so tests and embedders
#: can supply a fake or externally-built client without a live broker. The
#: optional ``will`` carries the worker's last-will message (AR-123): the worker
#: owns the identity and TTL policy the Will needs, so it builds the Will and
#: hands it to the factory rather than the factory re-deriving it.
ClientFactory = Callable[..., Awaitable[Client]]


async def _create_client(config: MqttConfig, will: Will | None = None) -> Client:
    """Build and connect an ``aiomqtt.Client`` from a typed ``MqttConfig``.

    The default :data:`ClientFactory`. Maps the scalar ``MqttConfig`` fields to
    aiomqtt v2.5.1 ``Client`` kwargs, then enters the client's async context so
    the returned client is already connected (the analogue of Valkey's
    ``GlideClient.create``). MQTT 5 is the only protocol (design §10 #1); the task
    id rides inside the encoded ``TaskData``, not a user property. ``session_expiry_interval``
    has no scalar ``Client`` kwarg in aiomqtt v2.5.1 (it would require paho
    CONNECT properties), so it is deliberately omitted, matching the log handler
    translation in :mod:`scietex.service.mqtt.logging`.

    ``will`` is the worker's last-will message: the broker publishes it when the
    connection drops without a clean DISCONNECT, which is how a crashed worker
    is marked ``inactive`` (AR-123). ``None`` disables the Will.
    """
    client = Client(
        hostname=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        identifier=config.identifier,
        protocol=ProtocolVersion.V5,
        keepalive=config.keepalive,
        clean_start=config.clean_start,
        transport=config.transport,
        timeout=config.timeout,
        tls_insecure=config.tls_insecure,
        tls_context=config.tls_context,
        will=will,
    )
    await client.__aenter__()
    return client


class MqttWorker(TransportWorker):
    """
    Async worker backed by an MQTT broker for task distribution.

    Extends ``TransportWorker`` with MQTT-specific operations including
    connection management, topic-based task intake via a background message
    loop, heartbeat publishing, and async logging to an MQTT topic via the
    ``aiomqtt`` client.

    Requires the optional ``aiomqtt`` dependency.

    Connection lifecycle (AR-059): this worker runs one operational
    ``aiomqtt.Client`` for heartbeats, registry, intake, and task completion.
    ``connect()``/``disconnect()`` are serialized behind an ``asyncio.Lock`` so
    only one task mutates ``_client`` at a time. The logging handler
    (:class:`~scietex.logging.AsyncMqttHandler`) owns its own independent
    connection (``mqtt_config=`` mode), so the worker neither shares nor tears
    down the logging client.

    Client construction (AR-074): ``connect()`` builds its ``aiomqtt.Client``
    by awaiting the ``client_factory=`` callable with the resolved configuration
    (defaulting to :func:`_create_client`), so tests and embedders can inject a
    fake or externally-built client. The worker owns the returned client for its
    lifetime and exits its async context during ``disconnect()``.

    Delivery (design §3): the message loop persists every received message to
    the durable inbox before enqueueing it, restoring at-least-once delivery
    that aiomqtt v2.5.1's premature broker ack would otherwise lose.

    Attributes:
        client (Client | None): aiomqtt client instance, initialized
            during ``initialize()``.
        _control_inbox (MqttInbox | None): always ``None``: the control lane is
            in-memory only (event-only, never replayed across a restart).
        _control_intake_inbox (MqttInbox): the in-memory control intake target
            (a :class:`MemoryInbox`, one per process so broadcast still fans out).
    """

    # Concrete config struct for this worker. The base stores it into
    # ``self._config``, so ``config=None`` instantiates the concrete type here
    # (AR-069) and no re-store / double-instantiation is needed.
    _config_type: ClassVar[type[MqttWorkerConfig]] = MqttWorkerConfig

    # Transport label surfaced in the CRITICAL down message (AR-102); the base
    # reads it when building the TransportHealth supervisor.
    _transport_name: ClassVar[str] = "MQTT"

    def __init__(
        self,
        config: MqttWorkerConfig | None = None,
        *,
        client_factory: ClientFactory | None = None,
        theme: Theme | None = None,
    ) -> None:
        """Initialize the ``MqttWorker``.

        Configures the MQTT client from ``config.mqtt_config`` or, when that is
        ``None``, defers reading ``mqtt.yml`` from the config directory to the
        first :meth:`connect` (AR-066), so construction is side-effect-free with
        respect to the config read. Resolves the task/log/registry topics and
        builds the durable inbox and transport. The
        :class:`~scietex.logging.AsyncMqttHandler` for async log entries owns its
        own connection (``mqtt_config=`` mode) and is built and registered on the
        first successful :meth:`connect`.

        Args:
            config: A :class:`~scietex.service.mqtt.config.MqttWorkerConfig`
                holding the worker's service identity, task-queue settings, and
                MQTT-specific fields. ``None`` uses the struct defaults.
            client_factory: Optional async callable taking a
                :class:`~scietex.service.mqtt.config.MqttConfig` and returning a
                connected :class:`~aiomqtt.Client`. Defaults to
                :func:`_create_client`. Lets tests and embedders inject a fake
                or externally-built client without a live broker.
            theme: The rendering theme for the startup banner and console log
                formatter. Defaults to :class:`ScietexMonochrome` when ``None``.
                It is a live object, not a config field, so it is injected via
                the constructor.

        Attributes:
            _client (Client | None): aiomqtt client, initialized during
                :meth:`initialize`.
            _mqtt_logger_handler (AsyncMqttHandler | None): The logging
                handler, built lazily on the first successful :meth:`connect`
                and reused across restarts. Owns its own connection
                (``mqtt_config=`` mode); the worker never shares its client.
            _task_topic (str): Resolved topic tasks are consumed from.
            _log_topic (str): Resolved topic worker logs are published to.
            _registry_topic (str): Retained-message registry topic for this
                instance (heartbeat + liveness).
            _status_topic_prefix (str): Resolved ``{service}``-substituted
                prefix for the per-task status/progress topics (design §13.2).
            _config_topic (str): Resolved retained desired-state topic for
                remote config (design §2).
            _control_topic (str): Resolved directed control topic for this
                instance, embedding both the service name and instance id.
            _control_broadcast_topic (str): Resolved service-scoped broadcast
                control topic.
            _inbox (MqttInbox | None): Durable data inbox, or ``None`` for the
                ``inbox_backend="memory"``/``"none"`` at-most-once opt-out.
            _control_inbox (MqttInbox | None): Always ``None``: the control lane
                is in-memory only (event-only, never replayed across a restart).
            _control_intake_inbox (MqttInbox): The in-memory control intake
                target (a :class:`MemoryInbox`, one per process).
        """
        factory = client_factory if client_factory is not None else _create_client
        super().__init__(config, client_factory=factory, theme=theme)
        # The base already stored the concrete config into ``self._config``
        # (AR-069); keep a typed local reference for the synchronous setup
        # reads below.
        cfg = cast(MqttWorkerConfig, self._config)

        # AR-066: when no explicit config was given, defer the filesystem read
        # (and the default mqtt.yml write / config-dir mkdir it triggers) to the
        # first connect, so construction is side-effect-free.
        self._mqtt_config: MqttConfig | None = cfg.mqtt_config

        self._mqtt_logger_handler: AsyncMqttHandler | None = None
        self._client: Client | None = None
        # Client-construction seam (AR-074): connect() awaits this factory with
        # the resolved MqttConfig; defaults to _create_client (build + connect).
        self._client_factory: ClientFactory = factory

        self._task_topic = cfg.task_topic.format(service=self.service_name)
        self._log_topic = cfg.log_topic.format(service=self.service_name, instance_id=self.instance_id)
        self._registry_topic = f"scietex/{self.service_name}/workers/{self.instance_id}"
        # Status/progress topic prefix (design §13.2), resolved once here exactly
        # as task_topic is, so the transport receives the substituted form rather
        # than repeating the {service} formatting itself.
        self._status_topic_prefix = cfg.status_topic_prefix.format(service=self.service_name)
        # Retained desired-state topic for remote config (design §2), resolved
        # like task_topic. The source records snapshots from this topic and is
        # attached to the processor's config-manager source seam below.
        self._config_topic = cfg.config_topic.format(service=self.service_name)
        # The directed control topic embeds the instance id, so the topic is the
        # worker's address; the broadcast topic stays service-scoped (AR-123).
        self._control_topic = cfg.control_topic.format(service=self.service_name, instance_id=self.instance_id)
        self._control_broadcast_topic = cfg.control_broadcast_topic.format(service=self.service_name)
        # The concrete reference is retained for MQTT-native lifecycle calls
        # (record on message delivery, reset at the run boundary) and for the
        # bounded startup wait in _read_remote_outcome; those are outside the
        # core ConfigSource read/write contract (AR-110).
        self._mqtt_config_source = MqttConfigSource(
            topic=self._config_topic,
            qos=cfg.config_qos,
            ttl=cfg.config_ttl,
            publish=self._publish,
            logger=self.logger,
        )
        self._config_manager.attach_source(self._mqtt_config_source)

        # Durable inbox (design §10 #3). ``None`` for the "none" opt-out or a
        # failed inbox build; the transport receives a non-None inbox via
        # the null adapter below, while initialize()'s at-least-once guard
        # refuses to start when a real inbox was expected but could not be built.
        self._inbox: MqttInbox | None = self._build_inbox(cfg)
        # Control lane (design §5.1): always in-memory, per process. Control
        # commands are event-only and never replayed across a restart (design
        # §2.2/§4.7: "control commands are never retried"), so a durable control
        # store buys nothing the contract wants; keeping it per-process also
        # guarantees a broadcast command still fans out to every worker.
        self._control_inbox: MqttInbox | None = None

        # Next monotonic timestamp at which the watchdog may prune the inbox
        # (AR-115). Zero so the first watchdog tick reclaims tombstones left by
        # a previous run.
        self._next_inbox_prune: float = 0.0

        # The transport always receives a non-None inbox: the real one, or the
        # in-memory backend for the at-most-once opt-out. ``_intake_inbox`` is
        # that effective target, so the message loop persists through the same
        # path in both modes and ``fetch`` stays the single enqueue point.
        self._intake_inbox: MqttInbox = self._inbox if self._inbox is not None else MemoryInbox()
        self._control_intake_inbox: MqttInbox = MemoryInbox()

        # Transport extension seam (AR-072): the delivery/ack/drain hooks the
        # processor calls now live on MqttTransport, which receives the health
        # supervisor and publish seam by injection. The InMemoryTransport built
        # by super().__init__ is empty and discarded.
        self._mqtt_transport = MqttTransport(
            config=cfg,
            service_name=self.service_name,
            topic=self._task_topic,
            status_topic_prefix=self._status_topic_prefix,
            inbox=self._intake_inbox,
            control_inbox=self._control_intake_inbox,
            health=self._health,
            publish=self._publish,
            logger=self.logger,
            instance_id=self.instance_id,
            clock=time.monotonic,
        )
        self._transport = self._mqtt_transport

        # Background task running the aiomqtt message loop; created in
        # initialize() after connect + subscribe and stopped in cleanup().
        self._message_task: asyncio.Task[None] | None = None

    def _build_inbox(self, cfg: MqttWorkerConfig) -> MqttInbox | None:
        """Build the durable data inbox for the configured backend.

        ``inbox_backend="sqlite"`` builds a shared :class:`SqliteMqttInbox` at
        ``<conf_dir>/inbox.sqlite3`` (or ``inbox_path`` as the database file),
        safe for multiple processes. A build failure (e.g. the path is an
        existing file) returns ``None`` so :meth:`initialize`'s at-least-once
        guard can refuse to start loudly rather than silently dropping the
        durability guarantee (design §3.3). ``inbox_backend="memory"`` (or its
        alias ``"none"``) returns ``None`` as the explicit at-most-once opt-out;
        the transport then receives a :class:`MemoryInbox`.
        """
        if cfg.inbox_backend in ("memory", "none"):
            return None
        path = Path(cfg.inbox_path) if cfg.inbox_path is not None else self.conf_dir / "inbox.sqlite3"
        return self._build_sqlite_inbox(path, cfg)

    def _build_sqlite_inbox(self, path: Path, cfg: MqttWorkerConfig) -> MqttInbox | None:
        """Build a :class:`SqliteMqttInbox` at ``path``, returning ``None`` on failure.

        Builds the durable data inbox. The lease TTL defaults to
        :func:`derive_inbox_lease_ttl` over the heartbeat/watchdog intervals when
        ``inbox_lease_ttl`` is unset. A failure returns ``None`` rather than
        raising, so the caller's at-least-once guard can refuse to start.
        """
        try:
            lease_ttl = cfg.inbox_lease_ttl
            if lease_ttl is None:
                lease_ttl = derive_inbox_lease_ttl(
                    cfg.heartbeat_interval or DEFAULT_HEARTBEAT_INTERVAL,
                    cfg.watchdog_interval or DEFAULT_WATCHDOG_INTERVAL,
                )
            return SqliteMqttInbox(
                path,
                worker_id=self.instance_id,
                logger=self.logger,
                ttl=cfg.inbox_ttl,
                lease_ttl=lease_ttl,
            )
        except (OSError, sqlite3.Error) as exc:
            self.logger.error("Failed to build the MQTT sqlite inbox at %s: %s", path, exc)
            return None

    async def watchdog(self) -> None:
        """Run the shared watchdog, then maintain the durable inbox (AR-115).

        Pruning is throttled to the ``inbox_prune_interval`` config field: the
        watchdog fires every ``watchdog_interval`` (default 1s), but the
        tombstone scan is an indexed DELETE, so one maintenance pass runs per interval.
        The schedule is jittered (``inbox_prune_jitter``) so multiple workers
        sharing one store do not prune on the same tick; every worker prunes
        independently, and the DELETE is idempotent so the redundant maintenance
        is safe and self-healing. This decouples tombstone/entry expiry from the
        fetch poll, so the inbox self-bounds even when no task arrives.
        """
        await super().watchdog()
        await self._maybe_prune_inbox()

    async def _maybe_prune_inbox(self) -> None:
        """Prune the durable data inbox at most once per ``inbox_prune_interval``.

        The schedule is jittered (``inbox_prune_jitter``) so multiple workers
        sharing one store do not prune on the same tick; every worker prunes
        independently, and the DELETE is idempotent so the redundant maintenance
        is safe and self-healing. The first pass is not jittered (it reclaims
        tombstones left by a previous run immediately). The control lane is
        in-memory and holds no tombstones, so only the data inbox needs a
        maintenance pass.
        """
        if self._inbox is None:
            return  # at-most-once opt-out: no durable files to prune
        now = time.monotonic()
        if now < self._next_inbox_prune:
            return
        cfg = cast(MqttWorkerConfig, self._config)
        interval = cfg.inbox_prune_interval
        jitter = cfg.inbox_prune_jitter
        if jitter:
            interval *= 1.0 + random.uniform(-jitter, jitter)
        self._next_inbox_prune = now + interval
        await self._inbox.prune_expired()

    @property
    def mqtt_config(self) -> MqttConfig | None:
        """The MQTT configuration used by this worker.

        Returns the :class:`MqttConfig` schema the worker was constructed with.
        When no explicit config was given at construction, the config is loaded
        lazily from disk at first connect, so this is ``None`` until
        :meth:`connect`/:meth:`initialize` has run (AR-066).
        """
        return self._mqtt_config

    @property
    def client(self) -> Client | None:
        """The :class:`~aiomqtt.Client` instance.

        ``None`` until :meth:`initialize` completes successfully.
        """
        return self._client

    def _ensure_client_config(self) -> MqttConfig:
        """Load the MQTT config on first connect (AR-066).

        Defers the filesystem read — and the default ``mqtt.yml`` write /
        config-dir ``mkdir`` it triggers — out of ``__init__`` to the first
        connect, so construction is side-effect-free. Populates ``_mqtt_config``
        the same way ``__init__`` does for an explicitly-provided config, then
        no-ops on later calls.
        """
        if self._mqtt_config is not None:
            return self._mqtt_config
        mqtt_config = read_mqtt_config(self.conf_dir)
        self._mqtt_config = mqtt_config
        return mqtt_config

    def _ensure_logging_handler(self) -> AsyncMqttHandler | None:
        """Build and register the logging handler once, then reuse it.

        The handler is constructed with ``mqtt_config=`` so it builds, closes,
        and reconnects its own ``aiomqtt.Client`` autonomously (``_owns_client``
        is True); the worker never touches ``handler.client`` (AR-059/061).
        ``self._mqtt_config`` is set by ``_ensure_client_config`` (or
        ``__init__``) before this runs, but ``ty`` cannot narrow that
        cross-method guarantee, so a local ``None`` guard documents that the
        handler is unavailable until the config is resolved.
        """
        if self._mqtt_logger_handler is not None:
            return self._mqtt_logger_handler

        config = self._mqtt_config
        if config is None:
            return None
        cfg = cast(MqttWorkerConfig, self._config)
        self._mqtt_logger_handler = AsyncMqttHandler(
            topic=self._log_topic,
            mqtt_config=logging_handler_config(config),
            qos=cfg.log_qos,
            retain=cfg.log_retain,
            message_expiry=cfg.log_message_expiry,
        )
        self._logging_lifecycle.register_logger_handler(self._mqtt_logger_handler)
        return self._mqtt_logger_handler

    async def _connect_locked(self) -> bool:
        """Establish the connection; assumes ``_client_lock`` is held.

        Builds a connected :class:`~aiomqtt.Client` by awaiting the configured
        ``_client_factory`` with the resolved ``_mqtt_config``. ``_client`` is
        assigned only on success, so a failed build leaves ``_client`` as
        ``None`` and ``connect()`` returns ``False``. This keeps the return
        value a reliable connectivity signal: callers that guard on
        ``self.client`` (e.g. ``initialize``) never see a half-connected worker.

        On success the logging handler is ensured (constructed lazily here),
        its worker loop is started if not already running, and intake is
        restored via :meth:`_start_intake` (subscribe + message loop), so a
        reconnect re-subscribes and restarts the loop.

        Returns:
            ``True`` if the connection is established and intake restored;
            ``False`` on connection or subscription failure.
        """
        if self._client is not None:
            return True
        mqtt_config = self._ensure_client_config()
        try:
            client = await self._client_factory(mqtt_config, will=self._build_will())
        except MqttError as exc:
            self.logger.error("Error connecting to MQTT broker: %s", exc)
            return False
        self._client = client
        self._health.mark_connected()
        self.logger.log(logging.INFO, "Connected to MQTT broker")
        handler = self._ensure_logging_handler()
        if handler is not None and not handler.logging_running_event.is_set():
            await handler.start_logging()
        # Subscribe and start the message loop so this connection actually
        # receives tasks; a reconnect reaches this same path and restores
        # intake that the previous loop's exit tore down.
        return await self._start_intake()

    async def _disconnect_locked(self):
        """Close the connection; assumes ``_client_lock`` is held.

        The logging handler owns its own client, so it is left untouched here;
        only the worker's operational client is closed. Exits the client's async
        context, logs the disconnection, and sets ``_client`` to ``None``.
        """
        if self._client is not None:
            client = self._client
            self._client = None
            self._health.mark_disconnected()
            try:
                await client.__aexit__(None, None, None)
            except Exception:
                # Best-effort close: a broker that already dropped the connection
                # may raise on __aexit__; the client is unusable either way.
                pass
            self.logger.info("MQTT client disconnected")

    async def _start_intake(self) -> bool:
        """Subscribe to the task, config, and control topics and start the loop.

        Subscribes to ``_task_topic`` at ``task_qos``, to ``_config_topic`` at
        ``config_qos`` (the retained config snapshot is delivered on SUBACK,
        design §2), and to ``_control_topic``/``_control_broadcast_topic`` at
        ``control_qos`` (design §5.3), then starts the background message loop
        unless one is already running. Called from :meth:`_connect_locked` after
        the client is assigned and the logging handler is ensured, so both the
        initial connect and a reconnect restore intake. The loop is only
        (re)created when the previous task is missing or done, which is how a
        reconnect after a loop exit (``MqttError``) starts a fresh loop without
        double-starting one that is still running.

        Returns:
            ``True`` when subscribed and the loop is running; ``False`` when the
            client is missing or the subscribe failed, so ``initialize`` refuses
            to start a worker that cannot receive tasks.
        """
        cfg = cast(MqttWorkerConfig, self._config)
        client = self._client
        if client is None:
            return False
        try:
            await client.subscribe(self._task_topic, qos=cfg.task_qos)
            await client.subscribe(self._config_topic, qos=cfg.config_qos)
            await client.subscribe(self._control_topic, qos=cfg.control_qos)
            await client.subscribe(self._control_broadcast_topic, qos=cfg.control_qos)
        except MqttError as exc:
            self.logger.error("Failed to subscribe to task/control topics: %s", exc)
            return False
        if self._message_task is None or self._message_task.done():
            self._message_task = asyncio.create_task(self._message_loop(), name=f"mqtt-{self.instance_id}-messages")
        return True

    async def _publish(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        *,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> None:
        """Publish a payload to a topic (the transport's publish seam).

        Reaches the operational client through :attr:`client`, so the publish
        fails fast (raising :class:`~aiomqtt.MqttError`) when disconnected. The
        caller (``MqttTransport``) owns the failure policy: the envelope requeue
        lets ``handle_task`` log it without crashing intake, while the
        status/progress publishes swallow it and report to the health supervisor.
        ``retain`` mirrors ``Client.publish``: retained for the per-task status
        marker, never for the envelope requeue or progress ticks. ``properties``
        carries the MQTT 5 message-expiry interval for retained status publishes
        and is ``None`` (no expiry) for every other call site.
        """
        client = self.client
        if client is None:
            raise MqttError("No MQTT client is connected")
        await client.publish(topic, payload, qos=qos, retain=retain, properties=properties)

    async def heartbeat(self) -> None:
        """Publish a retained heartbeat message to the registry topic.

        The retained marker on ``scietex/{service}/workers/{instance_id}`` is the
        instance's liveness signal; each beat refreshes it. The write is guarded
        by ``self.client and self.start_time``: the start time is set in
        ``_startup`` before the managers start, so the first beat fires promptly
        (AR-049). A publish failure is reported to :class:`TransportHealth`,
        which drives the reconnect.

        The message carries a ``MessageExpiryInterval`` of ``self.active_ttl``
        (AR-123): the broker drops the retained marker once the worker stops
        beating, so a crashed instance disappears without any client-side
        cleanup. The same ``ttl`` is in the payload for clients that read it.
        """
        if self.client and self.start_time:
            # Capture the client once: a concurrent _disconnect_locked may close
            # it mid-await, but MqttError is swallowed and reported to
            # TransportHealth, which drives the reconnect (AR-083).
            client = self.client
            start_time = self.start_time
            metrics = self.task_metrics()
            payload = msgspec.msgpack.encode(
                Heartbeat(
                    service=self.service_name,
                    instance_id=self.instance_id,
                    status="active",
                    heartbeat_interval=self.heartbeat_interval,
                    start_time=start_time,
                    ttl=self.active_ttl,
                    queue_depth=metrics.queue_depth,
                    running_tasks=metrics.running,
                    tasks_per_second=metrics.rate,
                )
            )
            properties = Properties(PacketTypes.PUBLISH)
            properties.MessageExpiryInterval = int(self.active_ttl)
            try:
                await client.publish(
                    self._registry_topic,
                    payload,
                    qos=_REGISTRY_QOS,
                    retain=True,
                    properties=properties,
                )
            except MqttError as exc:
                self.logger.log(
                    logging.WARNING,
                    "Failed to publish heartbeat to %s: %s",
                    self._registry_topic,
                    exc,
                )
                self._health.report_failure(exc)

    def _build_will(self) -> Will:
        """Build the last-will message marking this instance ``inactive``.

        The broker publishes the Will when the connection drops without a clean
        DISCONNECT (crash, network loss), which is how a dead worker is
        distinguished from a live one (AR-123). ``WillDelayInterval`` delays the
        publish by ``inactive_ttl``: a reconnect within that window cancels the
        Will, so a brief blip does not flap the registry. The Will payload
        carries ``inactive_ttl`` as its own expiry, matching the shutdown path.
        """
        metrics = self.task_metrics()
        payload = msgspec.msgpack.encode(
            Heartbeat(
                service=self.service_name,
                instance_id=self.instance_id,
                status="inactive",
                heartbeat_interval=self.heartbeat_interval,
                start_time=self.start_time or datetime.now(timezone.utc),
                ttl=self.inactive_ttl,
                queue_depth=metrics.queue_depth,
                running_tasks=metrics.running,
                tasks_per_second=metrics.rate,
            )
        )
        properties = Properties(PacketTypes.WILLMESSAGE)
        properties.WillDelayInterval = int(self.inactive_ttl)
        properties.MessageExpiryInterval = int(self.inactive_ttl)
        return Will(
            topic=self._registry_topic,
            payload=payload,
            qos=_REGISTRY_QOS,
            retain=True,
            properties=properties,
        )

    async def initialize(self) -> bool:
        """Initialize the worker and connect.

        Calls the parent ``TaskProcessor.initialize()`` to start registered
        task handlers, then connects to the broker (``connect`` subscribes to
        the task and config topics and starts the background message loop via
        :meth:`_start_intake`). Recovery of non-terminal inbox entries left by a
        previous run is not performed here: it is owned by the shared
        :class:`~scietex.service.transport.RecoverableTransport` guard and runs
        on the first :meth:`fetch`. The at-least-once guard runs first: when a
        durable inbox was expected (``inbox_backend`` is ``"sqlite"``) but none
        could be built, the worker refuses to start rather than silently
        degrading to at-most-once (design §3.3).

        After a successful connect (and subscription) the local ``config.yml``
        snapshot is applied, then the retained remote snapshot is awaited for a
        bounded ``config_startup_timeout`` and applied if present (design §5
        precedence: constructor < local file < remote). A missing, invalid, or
        timed-out remote config never fails startup.

        Returns:
            ``True`` if the parent initialization and connection (including
            subscription and loop start) succeed. ``False`` if the parent
            initialization fails, the at-least-once guard trips, or the
            connection (or its subscription) fails.
        """
        cfg = cast(MqttWorkerConfig, self._config)
        # At-least-once guard (design §10 #3): refuse before starting handlers
        # or connecting when the durable data inbox was expected but could not
        # be built, so the durability guarantee is never silently lost. The
        # control lane is in-memory and needs no such guard.
        if cfg.inbox_backend == "sqlite" and self._inbox is None:
            self.logger.error(
                "MQTT worker configured with inbox_backend=%r but no inbox could be built; "
                "refusing to start rather than silently losing at-least-once delivery",
                cfg.inbox_backend,
            )
            return False

        if not await super().initialize():
            return False
        if not await self.connect():
            return False
        # Apply the persisted snapshot first, then the retained remote snapshot
        # (which arrives via the message loop started by connect). Startup must
        # not fail on a bad or absent remote config (availability-first).
        self._mqtt_config_source.reset()
        await self._apply_local_config()
        await self._reload_remote_config()
        return True

    async def _read_remote_outcome(self) -> ConfigApplyOutcome:
        """Read and apply the retained config snapshot (design §5).

        The retained message arrives after SUBACK, so startup waits a bounded
        ``config_startup_timeout`` for it. A missing snapshot, an invalid
        envelope, or an unreachable source must not fail startup: an
        unavailable source maps to ``CONFIG_SOURCE_UNAVAILABLE`` so the base
        pipeline logs the "no config available" fallback instead of an error.
        """
        cfg = cast(MqttWorkerConfig, self._config)
        timeout = (
            cfg.config_startup_timeout if cfg.config_startup_timeout is not None else DEFAULT_CONFIG_STARTUP_TIMEOUT
        )
        try:
            snapshot = await self._mqtt_config_source.wait_for_snapshot(timeout)
        except Exception as exc:
            self.logger.error("Failed to wait for remote config snapshot: %s", exc)
            return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_UNAVAILABLE)
        if snapshot is None:
            return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_UNAVAILABLE)
        try:
            return await self._config_manager.apply_envelope(snapshot, source="remote")
        except Exception as exc:
            self.logger.error("Failed to apply remote config: %s", exc)
            return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_UNAVAILABLE)

    async def cleanup(self):
        """Perform cleanup on shutdown.

        Drains the internal task queue and cancels running tasks via the parent
        ``TaskProcessor.cleanup()``, then stops the message loop, stops the MQTT
        logging handler so its worker drains remaining records, and finally
        closes the MQTT connection through :meth:`disconnect`. The sqlite data
        inbox owns a database connection, so it is closed here; the in-memory
        control lane holds no resources to close.
        """
        await super().cleanup()
        # Stop the message loop before disconnect() so the loop cannot observe
        # the client teardown as an unexpected disconnect (which would report a
        # spurious failure and request a reconnect).
        await self._stop_message_loop()
        if self._mqtt_logger_handler is not None:
            await self._mqtt_logger_handler.stop_logging()
        if self._inbox is not None:
            await self._inbox.close()
        await self.disconnect()

    async def _stop_message_loop(self) -> None:
        """Cancel the message loop task and await its teardown."""
        task = self._message_task
        self._message_task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # expected: the loop was cancelled during shutdown
        except Exception as exc:
            self.logger.warning("MQTT message loop raised during shutdown: %s", exc)

    async def _message_loop(self) -> None:
        """Iterate the client's message queue, persisting each task to the inbox.

        The loop exits cleanly on cancellation (shutdown) and on client
        disconnect, which :class:`~aiomqtt.MessagesIterator` signals by raising
        :class:`~aiomqtt.MqttError`. A disconnect is reported to
        :class:`TransportHealth` so the watchdog's ``recover()`` reconnects.
        """
        client = self._client
        if client is None:
            return
        try:
            async for message in client.messages:
                await self._handle_message(message)
        except asyncio.CancelledError:
            raise
        except MqttError as exc:
            self.logger.warning("MQTT message loop ended: %s", exc)
            self._health.report_failure(exc)

    async def _handle_message(self, message: Message) -> None:
        """Route one received MQTT message: config snapshot, control, or data task.

        The topic is the address (design §3.1), so routing is by topic first:

        - ``_config_topic`` is the retained remote-config snapshot (design §2)
          and is recorded by the config source — it is not a ``TaskData``
          envelope and must not follow the task path.
        - ``_control_topic`` (this instance's directed topic) and
          ``_control_broadcast_topic`` persist to the control inbox.
        - everything else persists to the data inbox.

        The payload is decoded as a versioned envelope first, which yields the
        ``TaskData`` (and its ``task_id``). A message carrying an undecodable
        envelope — including a pre-v5 payload without a ``task_id`` — is logged
        and skipped without crashing the loop. The loop persists only:
        :meth:`MqttTransport.fetch` is the single intake path that drains the
        inboxes into the processor queue, so persist-before-enqueue still holds
        (the inbox write precedes any enqueue via the transport's next poll) and
        a task is never enqueued twice (design §3.2).
        """
        topic = str(message.topic)
        if topic == self._config_topic:
            self._mqtt_config_source.record(message.payload)
            return
        task_data = decode_task_envelope(message.payload)
        if task_data is None:
            self.logger.warning("Skipping MQTT message with an undecodable envelope")
            return
        task_id = task_data_id(task_data)
        if topic in (self._control_topic, self._control_broadcast_topic):
            await self._control_intake_inbox.put(task_id, task_data)
            return
        if self._inbox is not None:
            await self._inbox.put(task_id, task_data)
        else:
            # At-most-once opt-out: buffer in memory so the transport's next
            # fetch drains it. Nothing survives a restart, by design.
            await self._intake_inbox.put(task_id, task_data)

    async def _register_instance(self) -> None:
        """Publish this instance's retained liveness marker to the registry topic.

        Uses the same :class:`Heartbeat` struct as :meth:`heartbeat`; at
        registration the start time is not yet set (``_startup`` assigns it
        after ``_register_instance``), so it falls back to the current instant
        and is replaced by the first beat. Best-effort: a failed publish must
        not fail startup (log WARNING and continue). Only
        :class:`~aiomqtt.MqttError` is swallowed; other exceptions propagate.
        """
        await self._publish_status("active", self.active_ttl)

    async def _unregister_instance(self) -> None:
        """Publish an ``inactive`` marker on shutdown.

        Called by ``_shutdown()`` before ``cleanup()`` disconnects the client,
        so the client is still open here. The marker is never cleared: it is
        left to expire under ``inactive_ttl``, which gives a monitoring client a
        window to observe the death before the retained message disappears
        (AR-123). A clean DISCONNECT also suppresses the Will, so this explicit
        publish is the only death signal on a graceful stop.

        Best-effort: a failed publish must not fail shutdown (log WARNING and
        continue). Only :class:`~aiomqtt.MqttError` is swallowed; other
        exceptions propagate.
        """
        await self._publish_status("inactive", self.inactive_ttl)

    async def _publish_status(self, status: Literal["active", "inactive"], ttl: float) -> None:
        """Publish a retained heartbeat entry with an explicit ``status``/``ttl``.

        Shared by ``_register_instance`` and ``_unregister_instance``, which
        differ only in the status they publish. ``start_time`` may be unset
        during a failed startup; fall back to the current time so the entry is
        still well-formed.
        """
        client = self.client
        if client is None:
            return
        metrics = self.task_metrics()
        payload = msgspec.msgpack.encode(
            Heartbeat(
                service=self.service_name,
                instance_id=self.instance_id,
                status=status,
                heartbeat_interval=self.heartbeat_interval,
                start_time=self.start_time or datetime.now(timezone.utc),
                ttl=ttl,
                queue_depth=metrics.queue_depth,
                running_tasks=metrics.running,
                tasks_per_second=metrics.rate,
            )
        )
        properties = Properties(PacketTypes.PUBLISH)
        properties.MessageExpiryInterval = int(ttl)
        try:
            await client.publish(
                self._registry_topic,
                payload,
                qos=_REGISTRY_QOS,
                retain=True,
                properties=properties,
            )
        except MqttError as exc:
            self.logger.log(
                logging.WARNING,
                "Failed to publish %s status for instance %s on %s: %s",
                status,
                self.instance_id,
                self._registry_topic,
                exc,
            )
            self._health.report_failure(exc)
