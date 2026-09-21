"""Transport-independent lifecycle scaffold for broker-backed workers (AR-102).

Owns the client-lock/reconnect pattern, the TransportHealth supervisor, the
startup config-apply pipeline, and the watchdog glue shared by ValkeyWorker and
MqttWorker. Broker-specific connect/disconnect/heartbeat/registry/cleanup and
the transport itself stay in the concrete worker.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, ClassVar

from .config import TaskProcessorConfig
from .config_reload import (
    CONFIG_SOURCE_UNAVAILABLE,
    REMOTE_CONFIG_DISABLED,
    STALE_CONFIG,
    ConfigApplyOutcome,
)
from .health import TransportHealth
from .task_processor import TaskProcessor


class TransportWorker(TaskProcessor):
    """Shared lifecycle scaffold for transport-backed workers (AR-102).

    Extends :class:`~scietex.service.task_processor.TaskProcessor` with the
    pieces every broker-backed worker repeats: the client-lock/reconnect
    pattern, the :class:`~scietex.service.health.TransportHealth` supervisor,
    the startup config-apply pipeline, and the watchdog glue. Concrete workers
    (:class:`~scietex.service.valkey.worker.ValkeyWorker` and
    :class:`~scietex.service.mqtt.worker.MqttWorker`) supply the broker-specific
    connect/disconnect/heartbeat/registry/cleanup and the transport itself.
    """

    #: Transport label surfaced in the CRITICAL down message; concrete workers
    #: override this (``"Valkey"``/``"MQTT"``).
    _transport_name: ClassVar[str] = "Transport"

    def __init__(
        self,
        config: TaskProcessorConfig | None = None,
        *,
        client_factory: Callable[[Any], Awaitable[Any]],
    ) -> None:
        """Initialize the worker with its broker client factory.

        Args:
            config: A :class:`~scietex.service.config.TaskProcessorConfig`
                holding the worker's service identity and task-queue settings.
                ``None`` uses the struct defaults.
            client_factory: Async factory that builds the broker client for
                :meth:`connect`; the concrete worker supplies its own (e.g.
                ``GlideClient.create`` or an ``aiomqtt.Client`` builder).
        """
        super().__init__(config)
        self._client_factory = client_factory
        self._client_lock: asyncio.Lock = asyncio.Lock()
        self._health = TransportHealth(
            reconnect=self._reconnect,
            is_connected=lambda: self.client is not None,
            logger=self.logger,
            transport_name=self._transport_name,
            down_threshold=max(3 * self.watchdog_interval, self.heartbeat_interval),
            reconnect_cooldown=self.watchdog_interval,
        )

    @property
    def client(self) -> object | None:
        """The broker client; ``None`` until connected. Overridden typed."""
        raise NotImplementedError

    @property
    def transport_health(self) -> TransportHealth:
        """The connection-health supervisor for this worker (read-only)."""
        return self._health

    async def connect(self) -> bool:
        """Establish a connection; serialized behind the client lock."""
        async with self._client_lock:
            return await self._connect_locked()

    async def disconnect(self) -> None:
        """Close the connection; serialized behind the client lock."""
        async with self._client_lock:
            await self._disconnect_locked()

    async def _connect_locked(self) -> bool:
        raise NotImplementedError

    async def _disconnect_locked(self) -> None:
        raise NotImplementedError

    async def _reconnect(self) -> None:
        await self.disconnect()
        await self.connect()

    async def _apply_local_config(self) -> None:
        """Apply the persisted config.yml snapshot at startup.

        Applied ahead of the remote read as a trusted, unsigned envelope; the
        remote source stays authoritative. A disabled feature skips the file
        without logging an error.
        """
        outcome = await self._config_manager.apply_local_file()
        if outcome is not None:
            self._log_config_outcome(outcome, "file")

    async def _reload_remote_config(self) -> None:
        """Apply the remote desired state, then log its outcome.

        The read itself is the pluggable hook: each broker supplies the
        transport-specific source (a live durable GET, or a retained snapshot
        wait) by overriding :meth:`_read_remote_outcome`. Startup must not fail
        on a bad or unavailable remote config (availability-first).
        """
        outcome = await self._read_remote_outcome()
        self._log_config_outcome(outcome, "remote")

    async def _read_remote_outcome(self) -> ConfigApplyOutcome:
        """Abstract: read and apply the remote desired-state envelope."""
        raise NotImplementedError

    def _log_config_outcome(self, outcome: ConfigApplyOutcome, source: str) -> None:
        if outcome.applied:
            self.logger.info("Applied %s config revision %d (hash %s)", source, outcome.revision, outcome.hash)
        elif outcome.error_code == STALE_CONFIG:
            self.logger.debug("Skipped stale %s config (revision %d)", source, outcome.revision)
        elif outcome.error_code == CONFIG_SOURCE_UNAVAILABLE:
            self.logger.debug("No %s config available; keeping local/default", source)
        elif outcome.error_code == REMOTE_CONFIG_DISABLED:
            self.logger.debug("Remote config is disabled; skipping %s config", source)
        else:
            self.logger.error("Failed to apply %s config: %s", source, outcome.error_code)

    async def watchdog(self) -> None:
        """Refresh claims, supervise the connection, then run the base watchdog.

        Refreshing before super() keeps claims fresh even when the base watchdog
        blocks on a cancellation wait; a task the base watchdog cancels stops
        being refreshed and its claim expires. After the base run, one CRITICAL
        per sustained outage is surfaced by TransportHealth.
        """
        await self._transport.refresh_leases()
        await self._health.recover()
        await super().watchdog()
        msg = self._health.critical_report()
        if msg:
            self.logger.critical(msg)
