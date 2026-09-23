"""Tests for the PRODUCERS pane: placement, controls, and lifecycle."""

import pytest

pytest.importorskip("textual")

from typing import cast  # noqa: E402

import aiomqtt  # noqa: E402
from textual.widgets import Button, Input, Static, Switch  # noqa: E402

from examples.textual.producer import TaskProducer  # noqa: E402
from examples.textual.producer_card import ProducerCard  # noqa: E402
from scietex.service.valkey._glide import GlideClient  # noqa: E402
from tests.textual.test_slot_app import FakeApp  # noqa: E402


class FakeValkeyClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list]] = []

    async def xadd(self, key, values):
        self.calls.append((key, values))
        return b"1-1"

    async def close(self) -> None:
        pass


class FakeMqttClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, int]] = []

    async def publish(self, topic, payload, qos=0, **kwargs):
        self.calls.append((topic, payload, qos))

    async def __aexit__(self, *args) -> None:
        pass


async def _return(value):
    return value


class ProducerFakeApp(FakeApp):
    """App whose producer seam returns fakes wired to recording clients."""

    def __init__(self) -> None:
        super().__init__()
        self.valkey_client = FakeValkeyClient()
        self.mqtt_client = FakeMqttClient()

    def _make_producers(self) -> dict[str, TaskProducer]:
        # The app drives producers through a duck-typed surface (snapshot,
        # emitted, set_*, start, stop, emit), so an in-process producer with
        # fake clients stands in for the subprocess handle without spawning one.
        producers = {}
        for key, task_name, _label in (("fast", "fast_task", "FAST"), ("slow", "slow_task", "SLOW")):
            producer = TaskProducer(
                task_name,
                valkey_client_factory=lambda _config: _return(self.valkey_client),
                mqtt_client_factory=lambda _config: _return(self.mqtt_client),
            )
            producer._valkey_client = cast(GlideClient, self.valkey_client)
            producer._mqtt_client = cast(aiomqtt.Client, self.mqtt_client)
            producers[key] = producer
        return producers


def _card(app, key: str) -> ProducerCard:
    """The producer card for ``key``."""
    return next(card for card in app.query(ProducerCard) if card.producer == key)


async def _settle(pilot) -> None:
    """Wait for the app's thread workers (emit/stop) to finish.

    Emit and loop-stop run off the UI thread, so ``pilot.pause`` alone can
    return before the producer has been touched.
    """
    await pilot.pause()
    await pilot.app.workers.wait_for_complete()
    await pilot.pause()


@pytest.mark.asyncio
async def test_pane_renders_above_workers():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        producer_grid = app.query_one("#producer-grid")
        workers_grid = app.query_one("#card-grid")
        assert producer_grid.region.y < workers_grid.region.y


@pytest.mark.asyncio
async def test_two_cards_labelled_fast_and_slow():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        cards = list(app.query(ProducerCard))
        assert [card.producer for card in cards] == ["fast", "slow"]
        titles = [str(card.query_one(".producer-title", Static).content) for card in cards]
        assert titles == ["FAST", "SLOW"]


@pytest.mark.asyncio
async def test_defaults_are_1000ms():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        card = _card(app, "fast")
        assert card.query_one("#interval-fast", Input).value == "1000"
        assert card.query_one("#timeout-fast", Input).value == "1000"
        assert card.query_one("#batch-fast", Input).value == "1"


@pytest.mark.asyncio
async def test_switches_start_off():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        card = _card(app, "fast")
        assert card.query_one("#switch-valkey-fast", Switch).value is False
        assert card.query_one("#switch-mqtt-fast", Switch).value is False


@pytest.mark.asyncio
async def test_emit_with_no_broker_is_a_noop():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        await pilot.click("#producer-grid ProducerCard .emit")
        await pilot.pause()
        assert app._producers["fast"].emitted == 0


@pytest.mark.asyncio
async def test_valkey_switch_routes_to_valkey():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        await pilot.click("#switch-valkey-fast")
        await pilot.pause()
        assert app._producers["fast"].valkey_enabled is True
        await pilot.click("#producer-grid ProducerCard .emit")
        await _settle(pilot)
        assert app._producers["fast"].emitted == 1
        assert len(app.valkey_client.calls) == 1
        assert len(app.mqtt_client.calls) == 0


@pytest.mark.asyncio
async def test_both_switches_publish_to_both():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        await pilot.click("#switch-valkey-fast")
        await pilot.click("#switch-mqtt-fast")
        await pilot.pause()
        await pilot.click("#producer-grid ProducerCard .emit")
        await _settle(pilot)
        assert app._producers["fast"].emitted == 2
        assert len(app.valkey_client.calls) == 1
        assert len(app.mqtt_client.calls) == 1


@pytest.mark.asyncio
async def test_interval_input_updates_the_producer():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        interval = app.query_one("#interval-fast", Input)
        interval.value = "250"
        await pilot.pause()
        assert app._producers["fast"]._interval_ms == 250


@pytest.mark.asyncio
async def test_timeout_input_updates_the_producer():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        timeout = app.query_one("#timeout-fast", Input)
        timeout.value = "5000"
        await pilot.pause()
        assert app._producers["fast"]._timeout_ms == 5000


@pytest.mark.asyncio
async def test_batch_input_updates_the_producer():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        batch = app.query_one("#batch-fast", Input)
        batch.value = "25"
        await pilot.pause()
        assert app._producers["fast"].batch_size == 25


@pytest.mark.asyncio
async def test_run_button_starts_and_stops_the_loop():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        await pilot.click("#switch-valkey-fast")
        await pilot.pause()
        await pilot.click("#producer-grid ProducerCard .run")
        await pilot.pause()
        assert app._producers["fast"].running is True
        # Button.press() holds -active for 0.2s, so a second click inside that
        # window is swallowed; wait it out before toggling back.
        await pilot.pause(delay=0.3)
        await pilot.click("#producer-grid ProducerCard .run")
        await _settle(pilot)
        assert app._producers["fast"].running is False


@pytest.mark.asyncio
async def test_run_button_label_follows_state():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        card = _card(app, "fast")
        assert card.query_one(".run", Button).label == "Run"
        await pilot.click("#producer-grid ProducerCard .run")
        await pilot.pause()
        app._poll()
        await pilot.pause()
        assert card.query_one(".run", Button).label == "Stop"


@pytest.mark.asyncio
async def test_producers_stop_on_exit():
    app = ProducerFakeApp()
    async with app.run_test(size=(120, 60)) as pilot:
        await pilot.pause()
        await pilot.click("#switch-valkey-fast")
        await pilot.pause()
        await pilot.click("#producer-grid ProducerCard .run")
        await pilot.pause()
        assert app._producers["fast"].running is True

    assert app._producers_stopped is True
