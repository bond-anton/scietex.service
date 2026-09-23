"""Tests for the BROKERS panel: placement, rendering, staleness, and lifecycle."""

import time

import pytest

pytest.importorskip("textual")

from textual.widgets import Static  # noqa: E402

from examples.textual.broker_card import (  # noqa: E402
    STALE_AFTER_SECONDS,
    MqttBrokerCard,
    ValkeyBrokerCard,
    format_bytes,
    format_count,
    format_duration,
    format_float,
)
from examples.textual.broker_snapshot import MqttBrokerSnapshot, ValkeyBrokerSnapshot  # noqa: E402
from tests.textual.test_slot_app import FakeApp  # noqa: E402


def _text(widget, selector: str) -> str:
    """Read a Static child's rendered text."""
    return str(widget.query_one(selector, Static).content)


class FakeMonitor:
    """Monitor stub returning a fixed snapshot and recording lifecycle calls."""

    def __init__(self, snapshot) -> None:
        self._snapshot = snapshot
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def snapshot(self):
        return self._snapshot


class BrokerFakeApp(FakeApp):
    """App whose broker seam returns controllable fakes instead of real monitors."""

    def __init__(self, valkey=None, mqtt=None) -> None:
        super().__init__()
        self._valkey_snapshot = valkey if valkey is not None else ValkeyBrokerSnapshot()
        self._mqtt_snapshot = mqtt if mqtt is not None else MqttBrokerSnapshot()
        self.valkey_monitor = FakeMonitor(self._valkey_snapshot)
        self.mqtt_monitor = FakeMonitor(self._mqtt_snapshot)

    def _make_broker_monitors(self):
        return {"valkey": self.valkey_monitor, "mqtt": self.mqtt_monitor}


def test_format_bytes_units():
    assert format_bytes(None) == "—"
    assert format_bytes(0) == "0 B"
    assert format_bytes(1023) == "1023 B"
    assert format_bytes(1024) == "1.0 KiB"
    assert format_bytes(1048576) == "1.0 MiB"
    assert format_bytes(1073741824) == "1.0 GiB"


def test_format_duration_units():
    assert format_duration(None) == "—"
    assert format_duration(59) == "0m"
    assert format_duration(3600) == "1h 0m"
    assert format_duration(93784) == "1d 2h 3m"


def test_format_count_and_float():
    assert format_count(None) == "—"
    assert format_count(1200) == "1,200"
    assert format_float(None) == "—"
    assert format_float(12.5) == "12.5"


@pytest.mark.asyncio
async def test_panel_renders_below_the_log():
    app = BrokerFakeApp()
    async with app.run_test(size=(120, 50)) as pilot:
        await pilot.pause()
        app._poll()
        await pilot.pause()

        log = app.query_one("#logs")
        grid = app.query_one("#broker-grid")
        assert grid.region.y > log.region.y


@pytest.mark.asyncio
async def test_panel_has_both_cards():
    app = BrokerFakeApp()
    async with app.run_test(size=(120, 50)) as pilot:
        await pilot.pause()
        assert app.query_one("#broker-valkey", ValkeyBrokerCard) is not None
        assert app.query_one("#broker-mqtt", MqttBrokerCard) is not None


@pytest.mark.asyncio
async def test_fresh_snapshot_renders_values_and_is_not_stale():
    snapshot = ValkeyBrokerSnapshot(
        connected=True,
        received_at=time.monotonic(),
        version="8.0.1",
        uptime_s=93784,
        used_memory=1048576,
        used_memory_peak=2097152,
        connected_clients=3,
        ops_per_sec=42.0,
        task_stream_len=7,
        log_stream_len=3,
        control_stream_len=0,
    )
    app = BrokerFakeApp(valkey=snapshot)
    async with app.run_test(size=(120, 50)) as pilot:
        await pilot.pause()
        app._poll()
        await pilot.pause()

        card = app.query_one("#broker-valkey", ValkeyBrokerCard)
        assert not card.has_class("stale")
        assert not card.has_class("disconnected")
        assert "8.0.1" in str(_text(card, ".broker-title"))
        assert "1d 2h 3m" in str(_text(card, ".valkey-uptime"))
        assert "1.0 MiB / 2.0 MiB" in str(_text(card, ".valkey-memory"))
        assert "7 / 3 / 0" in str(_text(card, ".valkey-streams"))


@pytest.mark.asyncio
async def test_stale_snapshot_gets_the_stale_class():
    snapshot = MqttBrokerSnapshot(
        connected=True,
        received_at=time.monotonic() - (STALE_AFTER_SECONDS + 5),
        clients_connected=2,
    )
    app = BrokerFakeApp(mqtt=snapshot)
    async with app.run_test(size=(120, 50)) as pilot:
        await pilot.pause()
        app._poll()
        await pilot.pause()

        card = app.query_one("#broker-mqtt", MqttBrokerCard)
        assert card.has_class("stale")
        assert not card.has_class("disconnected")
        assert "stale" in str(_text(card, ".broker-status"))


@pytest.mark.asyncio
async def test_disconnected_snapshot_renders_dashes_and_error():
    snapshot = ValkeyBrokerSnapshot(connected=False, error="connection refused")
    app = BrokerFakeApp(valkey=snapshot)
    async with app.run_test(size=(120, 50)) as pilot:
        await pilot.pause()
        app._poll()
        await pilot.pause()

        card = app.query_one("#broker-valkey", ValkeyBrokerCard)
        assert card.has_class("disconnected")
        assert not card.has_class("stale")
        assert "connection refused" in str(_text(card, ".broker-status"))
        assert str(_text(card, ".valkey-memory")) == "—"


@pytest.mark.asyncio
async def test_unavailable_field_is_not_stale():
    """A field the broker never publishes is unavailable, not stale."""
    snapshot = MqttBrokerSnapshot(connected=True, received_at=time.monotonic(), clients_connected=2)
    app = BrokerFakeApp(mqtt=snapshot)
    async with app.run_test(size=(120, 50)) as pilot:
        await pilot.pause()
        app._poll()
        await pilot.pause()

        card = app.query_one("#broker-mqtt", MqttBrokerCard)
        assert not card.has_class("stale")
        assert str(_text(card, ".mqtt-bytes")) == "— / —"


@pytest.mark.asyncio
async def test_monitors_start_on_mount_and_stop_on_exit():
    app = BrokerFakeApp()
    async with app.run_test(size=(120, 50)) as pilot:
        await pilot.pause()
        assert app.valkey_monitor.started is True
        assert app.mqtt_monitor.started is True
        assert app.valkey_monitor.stopped is False

    assert app.valkey_monitor.stopped is True
    assert app.mqtt_monitor.stopped is True


@pytest.mark.asyncio
async def test_poll_renders_broker_snapshots():
    snapshot = MqttBrokerSnapshot(
        connected=True,
        received_at=time.monotonic(),
        version="mosquitto 2.0.18",
        clients_connected=2,
        clients_total=4,
        subscriptions=6,
    )
    app = BrokerFakeApp(mqtt=snapshot)
    async with app.run_test(size=(120, 50)) as pilot:
        await pilot.pause()
        app._poll()
        await pilot.pause()

        card = app.query_one("#broker-mqtt", MqttBrokerCard)
        assert "mosquitto 2.0.18" in str(_text(card, ".broker-title"))
        assert "2 / 4" in str(_text(card, ".mqtt-clients"))
        assert "6" in str(_text(card, ".mqtt-subs"))
