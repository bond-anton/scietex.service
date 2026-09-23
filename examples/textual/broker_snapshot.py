"""Immutable broker snapshots and the monitor interface the TUI renders.

A monitor polls or streams its broker and publishes a frozen snapshot; the app
reads the latest snapshot on every poll tick. ``None`` on any metric means
"unavailable" — either the broker does not expose it (MQTT has no CPU metric,
mosquitto publishes heap only when built with memory tracking) or the value has
not arrived yet. Unavailable is distinct from stale: a stale value is a real
reading that has aged past the freshness threshold.
"""

from typing import Protocol, TypeVar

import msgspec


class BrokerSnapshot(msgspec.Struct, frozen=True):
    """Fields every broker snapshot carries regardless of transport."""

    connected: bool = False
    error: str | None = None
    #: ``time.monotonic()`` of the last successful refresh or message; ``0.0``
    #: means nothing has arrived yet. Monotonic so the render clock and the
    #: monitor clock cannot disagree across a wall-clock adjustment.
    received_at: float = 0.0


class ValkeyBrokerSnapshot(BrokerSnapshot, frozen=True):
    """Server-level metrics read from ``INFO`` and ``XLEN``."""

    version: str | None = None
    uptime_s: int | None = None
    used_memory: int | None = None
    used_memory_peak: int | None = None
    connected_clients: int | None = None
    ops_per_sec: float | None = None
    #: Cumulative CPU seconds since server start, not a percentage.
    used_cpu_sys: float | None = None
    used_cpu_user: float | None = None
    keys_total: int | None = None
    task_stream_len: int | None = None
    log_stream_len: int | None = None
    control_stream_len: int | None = None


class MqttBrokerSnapshot(BrokerSnapshot, frozen=True):
    """Broker metrics read from the mosquitto ``$SYS`` topic tree."""

    version: str | None = None
    uptime_s: int | None = None
    clients_connected: int | None = None
    clients_total: int | None = None
    messages_received: int | None = None
    messages_sent: int | None = None
    bytes_received: int | None = None
    bytes_sent: int | None = None
    subscriptions: int | None = None
    retained_messages: int | None = None
    store_messages: int | None = None
    load_messages_received_1min: float | None = None
    #: Absent unless mosquitto was compiled with ``WITH_MEMORY_TRACKING``.
    heap_current: int | None = None


SnapshotT = TypeVar("SnapshotT", bound=BrokerSnapshot)


class BrokerMonitor(Protocol[SnapshotT]):
    """Lifecycle and read surface the app depends on.

    ``start`` schedules background work and returns without awaiting the first
    successful connection, so an unreachable broker never blocks app startup.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def snapshot(self) -> SnapshotT: ...
