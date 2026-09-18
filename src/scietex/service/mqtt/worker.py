"""MQTT-backed async task processor for ``scietex.service`` (v4.4.0).

Provides ``MqttWorker`` — an async worker that extends ``TaskProcessor``
with MQTT topic-based task distribution, heartbeat publishing, and async
logging. Uses the ``aiomqtt`` client for all broker operations.

Requires the optional ``aiomqtt`` dependency.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar, cast
from uuid import UUID

import msgspec
from scietex.logging import AsyncMqttHandler

from ..health import TransportHealth
from ..task_handler.wire import decode_task_envelope
from ..task_processor import TaskProcessor
from ._aiomqtt import Client, Message, MqttError, Properties, ProtocolVersion
from .config import MqttConfig, MqttWorkerConfig, read_mqtt_config
from .inbox import FileMqttInbox, MemoryInbox, MqttInbox
from .logging import logging_handler_config
from .transport import MqttTransport

#: MQTT 5 user property carrying the task id alongside the envelope payload
#: (design §10 #2). The envelope stays the pure wire format; the id travels
#: here because the MQTT transport cannot read it from a stream entry key.
TASK_ID_PROPERTY: str = "scietex-task-id"

#: QoS for the retained heartbeat/registry messages. Retained liveness should
#: be at-least-once so the marker is reliably set; each beat refreshes it.
_REGISTRY_QOS: int = 1

# Client-construction injection seam (AR-074): connect() builds its client by
# awaiting this callable with the resolved MqttConfig, so tests and embedders
# can supply a fake or externally-built client without a live broker.
ClientFactory = Callable[[MqttConfig], Awaitable[Client]]


async def _create_client(config: MqttConfig) -> Client:
    """Build and connect an ``aiomqtt.Client`` from a typed ``MqttConfig``.

    The default :data:`ClientFactory`. Maps the scalar ``MqttConfig`` fields to
    aiomqtt v2.5.1 ``Client`` kwargs, then enters the client's async context so
    the returned client is already connected (the analogue of Valkey's
    ``GlideClient.create``). MQTT 5 is the only protocol (design §10 #1), so the
    user properties that carry the task id are available. ``session_expiry_interval``
    has no scalar ``Client`` kwarg in aiomqtt v2.5.1 (it would require paho
    CONNECT properties), so it is deliberately omitted, matching the log handler
    translation in :mod:`scietex.service.mqtt.logging`.
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
    )
    await client.__aenter__()
    return client


class MqttWorker(TaskProcessor):
    """
    Async worker backed by an MQTT broker for task distribution.

    Extends ``TaskProcessor`` with MQTT-specific operations including
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
    """

    # Concrete config struct for this worker. The base stores it into
    # ``self._config``, so ``config=None`` instantiates the concrete type here
    # (AR-069) and no re-store / double-instantiation is needed.
    _config_type: ClassVar[type[MqttWorkerConfig]] = MqttWorkerConfig

    def __init__(
        self,
        config: MqttWorkerConfig | None = None,
        *,
        client_factory: ClientFactory | None = None,
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
            _inbox (MqttInbox | None): Durable inbox, or ``None`` for the
                ``inbox_backend="memory"``/``"none"`` at-most-once opt-out.
        """
        super().__init__(config)
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
        self._client_factory: ClientFactory = client_factory if client_factory is not None else _create_client
        # Serializes connect()/disconnect() so only one task mutates _client at
        # a time (AR-059): the message-loop failure path and shutdown cannot
        # race each other.
        self._client_lock: asyncio.Lock = asyncio.Lock()
        # Connection-health supervisor (AR-075): the single reconnect owner that
        # every MQTT-failure site reports into. Built before the collaborators
        # so the transport can receive it by injection. The down threshold and
        # cooldown are derived from the intervals, not configured.
        self._health = TransportHealth(
            reconnect=self._reconnect,
            is_connected=lambda: self._client is not None,
            logger=self.logger,
            transport_name="MQTT",
            down_threshold=max(3 * self.watchdog_interval, self.heartbeat_interval),
            reconnect_cooldown=self.watchdog_interval,
        )

        self._task_topic = cfg.task_topic.format(service=self.service_name)
        self._log_topic = cfg.log_topic.format(service=self.service_name)
        self._registry_topic = f"scietex/{self.service_name}/workers/{self.instance_id}"
        # Status/progress topic prefix (design §13.2), resolved once here exactly
        # as task_topic is, so the transport receives the substituted form rather
        # than repeating the {service} formatting itself.
        self._status_topic_prefix = cfg.status_topic_prefix.format(service=self.service_name)

        # Durable inbox (design §10 #3). ``None`` for the "none" opt-out or a
        # failed file-inbox build; the transport receives a non-None inbox via
        # the null adapter below, while initialize()'s at-least-once guard
        # refuses to start when a real inbox was expected but could not be built.
        self._inbox: MqttInbox | None = self._build_inbox(cfg)

        # The transport always receives a non-None inbox: the real one, or the
        # in-memory backend for the at-most-once opt-out. ``_intake_inbox`` is
        # that effective target, so the message loop persists through the same
        # path in both modes and ``fetch`` stays the single enqueue point.
        self._intake_inbox: MqttInbox = self._inbox if self._inbox is not None else MemoryInbox()

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
            health=self._health,
            publish=self._publish,
            logger=self.logger,
            clock=time.monotonic,
        )
        self._transport = self._mqtt_transport

        # Background task running the aiomqtt message loop; created in
        # initialize() after connect + subscribe and stopped in cleanup().
        self._message_task: asyncio.Task[None] | None = None

    def _build_inbox(self, cfg: MqttWorkerConfig) -> MqttInbox | None:
        """Build the durable inbox for the configured backend.

        ``inbox_backend="file"`` builds a :class:`FileMqttInbox` under the
        resolved ``inbox_path`` (defaulting to ``<conf_dir>/inbox``). A build
        failure (e.g. the path is an existing file) returns ``None`` so
        :meth:`initialize`'s at-least-once guard can refuse to start loudly
        rather than silently dropping the durability guarantee (design §3.3).
        ``inbox_backend="memory"`` (or its alias ``"none"``) returns ``None``
        as the explicit at-most-once opt-out; the transport then receives a
        :class:`MemoryInbox`.
        """
        if cfg.inbox_backend in ("memory", "none"):
            return None
        path = Path(cfg.inbox_path) if cfg.inbox_path is not None else self.conf_dir / "inbox"
        try:
            return FileMqttInbox(path, logger=self.logger, ttl=cfg.inbox_ttl)
        except OSError as exc:
            self.logger.error("Failed to build the MQTT inbox at %s: %s", path, exc)
            return None

    @property
    def mqtt_config(self) -> MqttConfig | None:
        """The MQTT configuration used by this worker.

        Returns the :class:`MqttConfig` schema the worker was constructed with.
        When no explicit config was given at construction, the config is loaded
        lazily from disk at first connect, so this is ``None`` until
        :meth:`connect`/:meth:`initialize` has run (AR-066).

        Returns:
            The MQTT configuration instance, or ``None`` before the first
            connect when no explicit config was provided.
        """
        return self._mqtt_config

    @property
    def client(self) -> Client | None:
        """The :class:`~aiomqtt.Client` instance.

        ``None`` until :meth:`initialize` completes successfully.

        Returns:
            The active aiomqtt client, or ``None`` if not connected.
        """
        return self._client

    @property
    def transport_health(self) -> TransportHealth:
        """The connection-health supervisor for this worker (read-only, AR-075).

        Exposes the :class:`~scietex.service.health.TransportHealth` aggregating
        every MQTT failure and owning the single reconnect path, so callers can
        observe degraded state without reaching into internals.
        """
        return self._health

    def _ensure_client_config(self) -> MqttConfig:
        """Load the MQTT config on first connect (AR-066).

        Defers the filesystem read — and the default ``mqtt.yml`` write /
        config-dir ``mkdir`` it triggers — out of ``__init__`` to the first
        connect, so construction is side-effect-free. Populates ``_mqtt_config``
        the same way ``__init__`` does for an explicitly-provided config, then
        no-ops on later calls.

        Returns:
            The ``MqttConfig`` used to create the client.
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
        handler is simply unavailable until the config is resolved.
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
        )
        self._logging_lifecycle.register_logger_handler(self._mqtt_logger_handler)
        return self._mqtt_logger_handler

    async def connect(self) -> bool:
        """Establish an asynchronous connection to the MQTT broker.

        Serialized behind ``_client_lock`` so a concurrent ``disconnect()``
        (message-loop failure, shutdown) cannot race the create → assign
        sequence (AR-059). Delegates to :meth:`_connect_locked`.

        Returns:
            ``True`` if the connection is established; ``False`` on
            connection failure.
        """
        async with self._client_lock:
            return await self._connect_locked()

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
            client = await self._client_factory(mqtt_config)
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

    async def disconnect(self):
        """Gracefully close the connection to the MQTT broker.

        Serialized behind ``_client_lock`` so a concurrent ``connect()`` cannot
        race the close → null sequence (AR-059). Delegates to
        :meth:`_disconnect_locked`.
        """
        async with self._client_lock:
            await self._disconnect_locked()

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
        """Subscribe to the task topic and start the message loop if needed.

        Subscribes to ``_task_topic`` at ``task_qos`` and starts the background
        message loop unless one is already running. Called from
        :meth:`_connect_locked` after the client is assigned and the logging
        handler is ensured, so both the initial connect and a reconnect restore
        intake. The loop is only (re)created when the previous task is missing
        or done, which is how a reconnect after a loop exit (``MqttError``)
        starts a fresh loop without double-starting one that is still running.

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
        except MqttError as exc:
            self.logger.error("Failed to subscribe to task topic %s: %s", self._task_topic, exc)
            return False
        if self._message_task is None or self._message_task.done():
            self._message_task = asyncio.create_task(self._message_loop(), name=f"mqtt-{self.instance_id}-messages")
        return True

    async def _reconnect(self) -> None:
        """Tear down and re-establish the connection (message-loop error recovery)."""
        await self.disconnect()
        await self.connect()

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

    def _heartbeat_payload(self) -> bytes:
        """Encode the retained heartbeat/registry payload for this instance."""
        return msgspec.msgpack.encode(
            {
                "service": self.service_name,
                "instance_id": self.instance_id,
                "status": "active",
                "start_time": self.start_time.isoformat() if self.start_time else None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    async def heartbeat(self) -> None:
        """Publish a retained heartbeat message to the registry topic.

        The retained marker on ``scietex/{service}/workers/{instance_id}`` is the
        instance's liveness signal; each beat refreshes it. The write is guarded
        by ``self.client and self.start_time``: the start time is set in
        ``_startup`` before the managers start, so the first beat fires promptly
        (AR-049). A publish failure is reported to :class:`TransportHealth`,
        which drives the reconnect.
        """
        if self.client and self.start_time:
            # Capture the client once: a concurrent _disconnect_locked may close
            # it mid-await, but MqttError is swallowed and reported to
            # TransportHealth, which drives the reconnect (AR-083).
            client = self.client
            payload = self._heartbeat_payload()
            try:
                await client.publish(self._registry_topic, payload, qos=_REGISTRY_QOS, retain=True)
            except MqttError as exc:
                self.logger.log(
                    logging.WARNING,
                    "Failed to publish heartbeat to %s: %s",
                    self._registry_topic,
                    exc,
                )
                self._health.report_failure(exc)

    async def initialize(self) -> bool:
        """Initialize the worker, connect, and replay the inbox.

        Calls the parent ``TaskProcessor.initialize()`` to start registered
        task handlers, then connects to the broker (``connect`` now subscribes
        to the task topic and starts the background message loop via
        :meth:`_start_intake`) and replays any non-terminal inbox entries left
        by a previous run. The at-least-once guard runs first: when a durable
        inbox was expected (``inbox_backend == "file"``) but none could be
        built, the worker refuses to start rather than silently degrading to
        at-most-once (design §3.3).

        Returns:
            ``True`` if the parent initialization, connection (including
            subscription and loop start), and inbox replay succeed. ``False``
            if the parent initialization fails, the at-least-once guard trips,
            or the connection (or its subscription) fails.
        """
        cfg = cast(MqttWorkerConfig, self._config)
        # At-least-once guard (design §10 #3): refuse before starting handlers
        # or connecting when a durable inbox was expected but could not be
        # built, so the durability guarantee is never silently lost.
        if cfg.inbox_backend == "file" and self._inbox is None:
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
        # Replay non-terminal inbox entries from a previous run before managers
        # start. The first fetch() re-runs recovery if this was interrupted, so
        # marking recovered here only skips that redundant re-scan.
        recovered, _ = await self._mqtt_transport.recover_pending_tasks(self)
        if recovered:
            self._mqtt_transport.recovered = True
        return True

    async def cleanup(self):
        """Perform cleanup on shutdown.

        Drains the internal task queue and cancels running tasks via the parent
        ``TaskProcessor.cleanup()``, then stops the message loop, stops the MQTT
        logging handler so its worker drains remaining records, and finally
        closes the MQTT connection through :meth:`disconnect`. The file inbox
        has no close/flush: every write (``put``/``mark_terminal``) is awaited
        synchronously via ``to_thread``, so no buffered state remains.
        """
        await super().cleanup()
        # Stop the message loop before disconnect() so the loop cannot observe
        # the client teardown as an unexpected disconnect (which would report a
        # spurious failure and request a reconnect).
        await self._stop_message_loop()
        if self._mqtt_logger_handler is not None:
            await self._mqtt_logger_handler.stop_logging()
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
        """Persist one received MQTT message to the durable inbox.

        The task id is read from the ``scietex-task-id`` user property and the
        payload decoded as a versioned envelope. A message missing the property
        or carrying an undecodable envelope is logged and skipped without
        crashing the loop. The loop persists only: :meth:`MqttTransport.fetch`
        is the single intake path that drains the inbox into the processor
        queue, so persist-before-enqueue still holds (the inbox write precedes
        any enqueue via the transport's next poll) and a task is never enqueued
        twice (design §3.2).
        """
        task_id = self._extract_task_id(message)
        if task_id is None:
            self.logger.warning("Skipping MQTT message without a %s user property", TASK_ID_PROPERTY)
            return
        task_data = decode_task_envelope(message.payload)
        if task_data is None:
            self.logger.warning("Skipping MQTT message %s with an undecodable envelope", task_id)
            return
        if self._inbox is not None:
            await self._inbox.put(task_id, task_data)
        else:
            # At-most-once opt-out: buffer in memory so the transport's next
            # fetch drains it. Nothing survives a restart, by design.
            await self._intake_inbox.put(task_id, task_data)

    @staticmethod
    def _extract_task_id(message: Message) -> UUID | None:
        """Return the ``scietex-task-id`` user property value as a UUID, or ``None``."""
        properties = message.properties
        user_properties = getattr(properties, "UserProperty", None)
        if not user_properties:
            return None
        for key, value in user_properties:
            if key == TASK_ID_PROPERTY:
                try:
                    return UUID(value)
                except ValueError:
                    return None
        return None

    async def _register_instance(self) -> None:
        """Publish this instance's retained liveness marker to the registry topic.

        Best-effort: a failed publish must not fail startup (log WARNING and
        continue). Only :class:`~aiomqtt.MqttError` is swallowed; other
        exceptions propagate.
        """
        client = self.client
        if client is None:
            return
        try:
            await client.publish(self._registry_topic, self._heartbeat_payload(), qos=_REGISTRY_QOS, retain=True)
        except MqttError as exc:
            self.logger.log(
                logging.WARNING,
                "Failed to register instance %s on %s: %s",
                self.instance_id,
                self._registry_topic,
                exc,
            )
            self._health.report_failure(exc)

    async def _unregister_instance(self) -> None:
        """Clear this instance's retained liveness marker from the registry topic.

        Best-effort: a failed publish must not fail shutdown (log WARNING and
        continue). A retained message with an empty payload clears the retained
        state. Called by _shutdown() before cleanup() disconnects the client, so
        the client is still open here. Only :class:`~aiomqtt.MqttError` is
        swallowed; other exceptions propagate.
        """
        client = self.client
        if client is None:
            return
        try:
            await client.publish(self._registry_topic, None, qos=_REGISTRY_QOS, retain=True)
        except MqttError as exc:
            self.logger.log(
                logging.WARNING,
                "Failed to unregister instance %s from %s: %s",
                self.instance_id,
                self._registry_topic,
                exc,
            )
            self._health.report_failure(exc)

    async def watchdog(self) -> None:
        """Refresh inbox leases, supervise the connection, then run the base watchdog.

        ``refresh_leases`` is a no-op for the file inbox but kept for parity with
        ``ValkeyWorker``. ``health.recover()`` is the single reconnect owner for
        every failure the message loop, heartbeat, and publish sites reported.

        After the base watchdog runs, a degraded connection past its down
        threshold surfaces one CRITICAL message per down episode (AR-075); when
        healthy this emits nothing.
        """
        # refresh_leases is mqtt-specific (not part of the core TaskTransport
        # protocol), so it is reached through the concrete transport.
        await self._mqtt_transport.refresh_leases()
        await self._health.recover()
        await super().watchdog()
        msg = self._health.critical_report()
        if msg:
            self.logger.critical(msg)
