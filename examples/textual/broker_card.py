"""Cards rendering broker-level metrics in the BROKERS panel.

Each card owns a fixed set of metric rows and formats a snapshot into them.
Values are dimmed when stale (a real reading that has aged past the freshness
threshold) and when the broker is disconnected; a field the broker does not
expose renders an em dash and is never marked stale, because unavailable and
stale are different conditions.
"""

import time
from typing import Generic, TypeVar

from examples.textual.broker_snapshot import BrokerSnapshot, MqttBrokerSnapshot, ValkeyBrokerSnapshot
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Static

SnapshotT = TypeVar("SnapshotT", bound=BrokerSnapshot)

#: A value older than this reads as stale. Mosquitto refreshes ``$SYS`` every
#: ``sys_interval`` seconds (10 by default), so the threshold leaves room for
#: one missed update before the panel stops claiming the value is current.
STALE_AFTER_SECONDS = 15.0

UNAVAILABLE = "—"

_BYTE_UNITS = ("B", "KiB", "MiB", "GiB", "TiB")


def format_bytes(value: int | None) -> str:
    """Render a byte count with a binary unit, or an em dash when unavailable."""
    if value is None:
        return UNAVAILABLE
    size = float(value)
    for unit in _BYTE_UNITS:
        if size < 1024 or unit == _BYTE_UNITS[-1]:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return UNAVAILABLE


def format_duration(seconds: int | None) -> str:
    """Render a duration as ``1d 2h 3m``, dropping leading zero units."""
    if seconds is None:
        return UNAVAILABLE
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def format_count(value: int | None) -> str:
    """Render an integer with thousands separators, or an em dash."""
    return UNAVAILABLE if value is None else f"{value:,}"


def format_float(value: float | None, suffix: str = "") -> str:
    """Render a float to one decimal place, or an em dash."""
    return UNAVAILABLE if value is None else f"{value:.1f}{suffix}"


class BrokerCard(Static, Generic[SnapshotT]):
    """Base card: fixed chrome, a status line, and the stale/disconnected states."""

    can_focus = False

    def compose(self) -> ComposeResult:
        with Vertical(classes="broker-body"):
            yield Static("", classes="broker-title")
            for label, css_class in self.metric_rows():
                with Horizontal(classes="metric-row"):
                    yield Static(label, classes="metric-label")
                    yield Static("", classes=f"metric-value {css_class}")
            with Horizontal(classes="status-line"):
                yield Static("●", classes="link-dot")
                yield Static("", classes="broker-status")

    def metric_rows(self) -> tuple[tuple[str, str], ...]:
        """Return ``(label, value_css_class)`` pairs, one per rendered row."""
        raise NotImplementedError

    def set_snapshot(self, snapshot: SnapshotT) -> None:
        """Render a snapshot into the card.

        Resolves every child up front and bails if any is missing: Textual
        prunes a card's children during shutdown while the card itself is still
        attached, so a poll tick can land on a card whose subtree is gone.
        """
        title = self.query_one_optional(".broker-title", Static)
        dot = self.query_one_optional(".link-dot", Static)
        status = self.query_one_optional(".broker-status", Static)
        values = {css_class: self.query_one_optional(f".{css_class}", Static) for _, css_class in self.metric_rows()}
        if title is None or dot is None or status is None or any(value is None for value in values.values()):
            return

        stale = (
            snapshot.connected
            and snapshot.received_at > 0
            and (time.monotonic() - snapshot.received_at) > STALE_AFTER_SECONDS
        )
        self.set_class(stale, "stale")
        self.set_class(not snapshot.connected, "disconnected")

        title.update(self.title_text(snapshot))
        for css_class, value in values.items():
            if value is not None:
                value.update(self.metric_text(css_class, snapshot))

        dot.remove_class("healthy", "unhealthy")
        dot.add_class("healthy" if snapshot.connected else "unhealthy")
        if snapshot.connected:
            status.update("stale" if stale else "ok")
        else:
            status.update(snapshot.error or "disconnected")

    def title_text(self, snapshot: SnapshotT) -> str:
        raise NotImplementedError

    def metric_text(self, css_class: str, snapshot: SnapshotT) -> str:
        raise NotImplementedError


class ValkeyBrokerCard(BrokerCard[ValkeyBrokerSnapshot]):
    """Valkey server metrics: memory, clients, throughput, CPU, keys, streams."""

    def metric_rows(self) -> tuple[tuple[str, str], ...]:
        return (
            ("uptime", "valkey-uptime"),
            ("memory", "valkey-memory"),
            ("clients", "valkey-clients"),
            ("ops/s", "valkey-ops"),
            ("streams", "valkey-streams"),
        )

    def title_text(self, snapshot: ValkeyBrokerSnapshot) -> str:
        return f"Valkey {snapshot.version}" if snapshot.version else "Valkey"

    def metric_text(self, css_class: str, snapshot: ValkeyBrokerSnapshot) -> str:
        if not snapshot.connected:
            return UNAVAILABLE
        match css_class:
            case "valkey-uptime":
                return format_duration(snapshot.uptime_s)
            case "valkey-memory":
                return f"{format_bytes(snapshot.used_memory)} / {format_bytes(snapshot.used_memory_peak)}"
            case "valkey-clients":
                return format_count(snapshot.connected_clients)
            case "valkey-ops":
                return format_float(snapshot.ops_per_sec)
            case "valkey-streams":
                return (
                    f"{format_count(snapshot.task_stream_len)} / "
                    f"{format_count(snapshot.log_stream_len)} / "
                    f"{format_count(snapshot.control_stream_len)}"
                )
        return UNAVAILABLE


class MqttBrokerCard(BrokerCard[MqttBrokerSnapshot]):
    """MQTT broker metrics from ``$SYS``: clients, traffic, subscriptions, heap."""

    def metric_rows(self) -> tuple[tuple[str, str], ...]:
        return (
            ("uptime", "mqtt-uptime"),
            ("clients", "mqtt-clients"),
            ("messages", "mqtt-messages"),
            ("bytes", "mqtt-bytes"),
            ("subs", "mqtt-subs"),
            ("load 1m", "mqtt-load"),
        )

    def title_text(self, snapshot: MqttBrokerSnapshot) -> str:
        return f"MQTT {snapshot.version}" if snapshot.version else "MQTT"

    def metric_text(self, css_class: str, snapshot: MqttBrokerSnapshot) -> str:
        if not snapshot.connected:
            return UNAVAILABLE
        match css_class:
            case "mqtt-uptime":
                return format_duration(snapshot.uptime_s)
            case "mqtt-clients":
                return f"{format_count(snapshot.clients_connected)} / {format_count(snapshot.clients_total)}"
            case "mqtt-messages":
                return f"{format_count(snapshot.messages_received)} / {format_count(snapshot.messages_sent)}"
            case "mqtt-bytes":
                return f"{format_bytes(snapshot.bytes_received)} / {format_bytes(snapshot.bytes_sent)}"
            case "mqtt-subs":
                return format_count(snapshot.subscriptions)
            case "mqtt-load":
                return format_float(snapshot.load_messages_received_1min)
        return UNAVAILABLE
