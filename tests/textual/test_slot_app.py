"""Hermetic slot-model tests for the Textual TUI.

Drives :class:`TextualWorkerApp` headlessly with a :class:`FakeWorkerProcess`
standing in for the real subprocess-backed worker handles, so no server or
transport extra is required beyond ``textual``. The whole module skips when
``textual`` is not installed, so the default suite (without the ``textual``
extra) stays hermetic.
"""

from pathlib import Path

import pytest

pytest.importorskip("textual")

from scietex.textual import LogLine
from textual.containers import Grid
from textual.widgets import Button, ContentSwitcher, ProgressBar, Sparkline, Static

from examples.textual.app import MAX_LOG_LINES_PER_TICK, TextualWorkerApp
from examples.textual.slot import slot_key
from examples.textual.worker_process import LogRecordData, WorkerIdentity
from scietex.service.basic_worker import ServiceStatus
from scietex.service.task_metrics import TaskMetricsSnapshot

#: Path of the real app module, so subclasses defined here resolve the app's
#: relative ``CSS_PATH`` against it rather than against this test module.
_APP_PATH = Path(__file__).resolve().parents[2] / "examples" / "textual" / "app.py"


class FakeWorkerHealth:
    """Minimal transport-health stand-in exposing only the fields the app reads."""

    def __init__(self, connected: bool = True, degraded: bool = False) -> None:
        self.connected = connected
        self.degraded = degraded


class FakeWorkerProcess:
    """Minimal stand-in mirroring the :class:`WorkerProcess` surface the app reads.

    A fixed ``instance_id`` keeps the truncated-id assertion stable. Every read
    the state poller and status line perform maps onto a plain attribute or a
    deterministic accessor, so the app drives the fake without spawning a child
    process or opening a broker connection.
    """

    _KIND_LABELS = {"valkey": "Valkey", "mqtt": "MQTT"}

    def __init__(
        self,
        kind: str,
        *,
        total: int = 0,
        transport_health: FakeWorkerHealth | None = None,
        logs: list | None = None,
        memory: bool = False,
    ) -> None:
        self.kind = kind
        self.memory = memory
        self.instance_id = "deadbeefcafebabe"
        self.identity = WorkerIdentity(self.kind_label, self.instance_id)
        self.state = ServiceStatus.STOPPED
        self.queue_size = 16
        self.max_concurrent_tasks = 4
        self.failed_managers: tuple[str, ...] = ()
        self.transport_health = transport_health
        self.exited = False
        self.startup_error = None
        self._total = total
        self._logs = logs if logs is not None else []

    @property
    def kind_label(self) -> str:
        return self._KIND_LABELS.get(self.kind, self.kind)

    def task_metrics(self) -> TaskMetricsSnapshot:
        return TaskMetricsSnapshot(queue_depth=0, running=0, rate=0.0, total=self._total)

    def drain_logs(self) -> list:
        logs, self._logs = self._logs, []
        return logs

    def snapshot(self) -> None:
        return None

    def start_process(self) -> None:
        return None

    async def start(self) -> None:
        self.state = ServiceStatus.RUNNING

    async def stop(self) -> None:
        self.state = ServiceStatus.STOPPED

    async def exit(self) -> None:
        self.state = ServiceStatus.STOPPED
        self.exited = True


class FakeApp(TextualWorkerApp):
    """App whose worker seam returns in-memory fakes instead of subprocess handles."""

    _BASE_PATH = str(_APP_PATH)

    def _make_worker_process(self, kind: str):
        return FakeWorkerProcess(kind, memory=self._memory)

    def _make_broker_monitors(self):
        """No broker monitors: these tests exercise the worker slots, not the panel."""
        return {}

    def _make_producers(self):
        """No producers: spawning real children would leak processes into the test run."""
        return {}


class TransportFakeApp(FakeApp):
    """App whose seam returns a transport-backed fake with a controllable health state and total."""

    def __init__(
        self,
        *,
        connected: bool = True,
        degraded: bool = False,
        total: int = 0,
    ) -> None:
        super().__init__()
        self._fake_connected = connected
        self._fake_degraded = degraded
        self._fake_total = total

    def _make_worker_process(self, kind: str):
        return FakeWorkerProcess(
            kind,
            total=self._fake_total,
            transport_health=FakeWorkerHealth(self._fake_connected, self._fake_degraded),
            memory=self._memory,
        )


def _log_text(log) -> str:
    """Concatenate the rendered text of every line in a ``LogView``."""
    return "".join(strip.text for strip in log.lines)


def _record(message: str) -> LogRecordData:
    """A minimal INFO record for the fake worker's log buffer."""
    return LogRecordData(
        name="test",
        levelno=20,
        levelname="INFO",
        message=message,
        pathname="test.py",
        lineno=1,
        funcName="test",
        created=0.0,
        msecs=0.0,
        exc_text=None,
    )


async def _click_card(pilot, app, slot_index, selector) -> None:
    """Click a button inside a slot's card and let the app settle."""
    button = app.slots[slot_index].card.query_one(selector, Button)
    await pilot.click(button)
    await pilot.pause()


async def _select_slot(pilot, app, slot_index) -> None:
    """Click a non-button part of a card so the click bubbles up and selects it."""
    card = app.slots[slot_index].card
    state = "occupied-state" if card.has_class("occupied") else "empty-state"
    label = card.query_one(f".{state} .slot-number", Static)
    await pilot.click(label)
    await pilot.pause()


@pytest.mark.asyncio
async def test_create_valkey_occupies_slot():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        slot = app.slots[0]
        assert slot.worker is not None
        assert slot.card.has_class("occupied")
        assert slot.card.query_one(".worker-kind", Static).content == "Valkey"
        assert slot.card.query_one(".instance-id", Static).content == f"…{slot.worker.instance_id[-4:]}"


@pytest.mark.asyncio
async def test_create_mqtt_occupies_slot():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 1, ".kind-mqtt")
        slot = app.slots[1]
        assert slot.worker is not None
        assert slot.card.has_class("occupied")
        assert slot.card.query_one(".worker-kind", Static).content == "MQTT"


@pytest.mark.asyncio
async def test_memory_flag_is_forwarded_to_worker_process():
    app = FakeApp(memory=True)
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        assert app.slots[0].worker.memory is True


@pytest.mark.asyncio
async def test_select_empty_slot_shows_placeholder():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _select_slot(pilot, app, 2)
        assert app.query_one(ContentSwitcher).current == "log-slot-2"
        assert "Slot 3 is empty" in _log_text(app.slots[2].log)


@pytest.mark.asyncio
async def test_create_starts_worker_automatically():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        slot = app.slots[0]
        assert slot.running is True
        assert slot.worker.state == ServiceStatus.RUNNING
        app._sync_worker_states()
        assert slot.card.query_one(".toggle", Button).label == "Stop"


@pytest.mark.asyncio
async def test_metrics_line_populated_and_cleared():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        slot = app.slots[0]
        jobs_bar = slot.card.query_one(".jobs-bar", ProgressBar)
        queue_bar = slot.card.query_one(".queue-bar", ProgressBar)
        rate_bar = slot.card.query_one(".rate-bar", ProgressBar)
        jobs_value = slot.card.query_one(".jobs-value", Static)
        queue_value = slot.card.query_one(".queue-value", Static)
        rate_value = slot.card.query_one(".rate-value", Static)
        sparkline = slot.card.query_one(".rate-spark", Sparkline)

        # The left labels are constant; only the right-hand values carry state.
        assert slot.card.query_one(".jobs-label", Static).content == "jobs"
        assert slot.card.query_one(".queue-label", Static).content == "queue"
        assert slot.card.query_one(".rate-label", Static).content == "rate"

        # A poll tick fills the gauges from the fake's metrics surface; the fake
        # starts empty with a fixed concurrency ceiling of 4, queue capacity
        # 16, and a zero completion rate. The rate bar shares the sparkline's
        # fixed 0..10000 t/s scale.
        app._sync_worker_states()
        assert jobs_bar.progress == 0
        assert jobs_bar.total == 4
        assert queue_bar.progress == 0
        assert queue_bar.total == 16
        assert rate_bar.progress == 0
        assert rate_bar.total == 10000.0
        assert jobs_value.content == "0/4"
        assert queue_value.content == "0/16"
        assert rate_value.content == "0.0/s"
        # The sparkline pins its scale with two sentinels around the real
        # samples, and sits below the three metric rows. The poll timer may have
        # ticked more than once, so assert the sentinels and that every real
        # sample is the fake's zero rate rather than an exact sample count.
        assert sparkline.data[0] == 0.0
        assert sparkline.data[-1] == 10000.0
        assert all(sample == 0.0 for sample in sparkline.data[1:-1])
        assert sparkline.region.y > rate_bar.region.y
        # The fake has no transport, so the status line renders a grayed dot
        # and the (zero) completion count with a "done" suffix.
        assert slot.card.query_one(".link-dot", Static).has_class("no-transport")
        assert slot.card.query_one(".link-total", Static).content == "· 0 done"

        # Exiting restores the empty state and clears the gauges and history.
        await _click_card(pilot, app, 0, ".exit")
        assert jobs_bar.progress == 0
        assert jobs_bar.total == 1
        assert queue_bar.progress == 0
        assert queue_bar.total == 1
        assert rate_bar.progress == 0
        assert rate_bar.total == 10000.0
        assert jobs_value.content == ""
        assert queue_value.content == ""
        assert rate_value.content == ""
        assert sparkline.data == []
        assert slot.card.query_one(".link-total", Static).content == ""
        assert slot.card.query_one(".link-dot", Static).has_class("no-transport")


@pytest.mark.asyncio
async def test_sparkline_accumulates_rate_samples():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        slot = app.slots[0]
        sparkline = slot.card.query_one(".rate-spark", Sparkline)

        # Each poll tick appends one rate sample to the card-owned history, so
        # the sparkline's data (sentinels included) grows by one per tick.
        app._sync_worker_states()
        first_len = len(sparkline.data)
        app._sync_worker_states()
        assert len(sparkline.data) == first_len + 1
        # The two sentinels always bookend the real samples.
        assert sparkline.data[0] == 0.0
        assert sparkline.data[-1] == 10000.0


@pytest.mark.asyncio
async def test_toggle_start_and_stop():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        slot = app.slots[0]
        # A created worker is already running, so the first toggle stops it.
        app._sync_worker_states()
        assert slot.card.query_one(".toggle", Button).label == "Stop"

        # The button flashes its `-active` class for ~0.2s after a press and
        # swallows re-clicks while it is set, so wait out the effect first.
        await pilot.pause(delay=0.3)
        await _click_card(pilot, app, 0, ".toggle")
        app._sync_worker_states()
        assert slot.card.query_one(".toggle", Button).label == "Start"

        await pilot.pause(delay=0.3)
        await _click_card(pilot, app, 0, ".toggle")
        app._sync_worker_states()
        assert slot.card.query_one(".toggle", Button).label == "Stop"


@pytest.mark.asyncio
async def test_exit_restores_empty_slot():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        slot = app.slots[0]
        assert slot.worker is not None
        assert slot.identity is not None

        await _click_card(pilot, app, 0, ".exit")
        assert slot.worker is None
        assert slot.handler is None
        assert slot.identity is None
        assert slot.card.has_class("empty")


@pytest.mark.asyncio
async def test_create_clears_empty_placeholder():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _select_slot(pilot, app, 0)
        assert "is empty" in _log_text(app.slots[0].log)

        await _click_card(pilot, app, 0, ".kind-valkey")

        assert "is empty" not in _log_text(app.slots[0].log)


@pytest.mark.asyncio
async def test_exit_clears_log_and_restores_placeholder():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        app.on_log_line(LogLine(slot_key(0), "marker"))
        await pilot.pause()
        assert "marker" in _log_text(app.slots[0].log)

        await _click_card(pilot, app, 0, ".exit")

        text = _log_text(app.slots[0].log)
        assert "Slot 1 is empty" in text
        assert "marker" not in text


@pytest.mark.asyncio
async def test_recreate_clears_log_again():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        app.on_log_line(LogLine(slot_key(0), "marker"))
        await pilot.pause()

        await _click_card(pilot, app, 0, ".exit")
        await _click_card(pilot, app, 0, ".kind-valkey")

        text = _log_text(app.slots[0].log)
        assert "marker" not in text
        assert "is empty" not in text


@pytest.mark.asyncio
async def test_missing_extra_reports_error_without_worker(monkeypatch):
    monkeypatch.setattr("examples.textual.app.worker_unavailable", lambda kind: True)
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        slot = app.slots[0]
        assert slot.worker is None
        assert slot.card.has_class("empty")
        assert "unavailable" in _log_text(slot.log)


@pytest.mark.asyncio
async def test_status_line_shows_healthy_dot_and_total():
    app = TransportFakeApp(connected=True, degraded=False, total=1203)
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        app._sync_worker_states()
        card = app.slots[0].card
        dot = card.query_one(".link-dot", Static)
        assert dot.has_class("healthy")
        assert not dot.has_class("unhealthy")
        assert not dot.has_class("no-transport")
        # The cumulative count is rendered with a thousands separator and a
        # "done" suffix; the leading separator and dot margin complete the row.
        assert card.query_one(".link-total", Static).content == "· 1,203 done"


@pytest.mark.asyncio
async def test_status_line_shows_unhealthy_dot_when_degraded():
    app = TransportFakeApp(connected=False, degraded=True, total=7)
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        app._sync_worker_states()
        card = app.slots[0].card
        dot = card.query_one(".link-dot", Static)
        assert dot.has_class("unhealthy")
        assert not dot.has_class("healthy")
        assert card.query_one(".link-total", Static).content == "· 7 done"


@pytest.mark.asyncio
async def test_status_line_no_transport_renders_grayed_dot():
    # FakeApp's workers expose no ``transport_health``, so the dot reads as
    # "not applicable" rather than healthy, and the line is still present.
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        app._sync_worker_states()
        card = app.slots[0].card
        dot = card.query_one(".link-dot", Static)
        assert dot.has_class("no-transport")
        assert not dot.has_class("healthy")
        assert not dot.has_class("unhealthy")
        assert card.query_one(".link-total", Static).content == "· 0 done"


@pytest.mark.asyncio
async def test_card_grid_switches_to_4x1_at_wide_breakpoint():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        grid = app.query_one("#card-grid", Grid)
        # An 80-column terminal is below the 160-column breakpoint, so the grid
        # defaults to the 2x2 layout.
        assert (grid.styles.grid_size_columns, grid.styles.grid_size_rows) == (2, 2)

        await pilot.resize_terminal(160, 30)
        await pilot.pause()
        # At 160 columns the grid switches to a single 4x1 row.
        assert (grid.styles.grid_size_columns, grid.styles.grid_size_rows) == (4, 1)

        await pilot.resize_terminal(80, 30)
        await pilot.pause()
        assert (grid.styles.grid_size_columns, grid.styles.grid_size_rows) == (2, 2)


@pytest.mark.asyncio
async def test_drain_batches_records_into_one_write():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        worker = app.slots[0].worker
        worker._logs = [_record(f"line {i}") for i in range(5)]

        app._drain_worker_logs()
        await pilot.pause()

        text = _log_text(app.slots[0].log)
        for i in range(5):
            assert f"line {i}" in text


@pytest.mark.asyncio
async def test_drain_caps_lines_and_reports_the_drop():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        worker = app.slots[0].worker
        total = MAX_LOG_LINES_PER_TICK + 50
        worker._logs = [_record(f"line {i:04d}") for i in range(total)]

        app._drain_worker_logs()
        await pilot.pause()

        text = _log_text(app.slots[0].log)
        # The newest lines survive; the oldest are dropped.
        assert f"line {total - 1:04d}" in text
        assert "line 0000" not in text
        assert "50 log lines dropped" in text


@pytest.mark.asyncio
async def test_drain_keeps_the_newest_lines_when_capped():
    app = FakeApp()
    async with app.run_test(size=(80, 44)) as pilot:
        await _click_card(pilot, app, 0, ".kind-valkey")
        worker = app.slots[0].worker
        total = MAX_LOG_LINES_PER_TICK + 10
        # Zero-padded markers keep the substring checks unambiguous.
        worker._logs = [_record(f"line {i:04d}") for i in range(total)]

        app._drain_worker_logs()
        await pilot.pause()

        text = _log_text(app.slots[0].log)
        # The first kept line is the one just past the dropped prefix.
        first_kept = total - MAX_LOG_LINES_PER_TICK
        assert f"line {first_kept:04d}" in text
        assert f"line {first_kept - 1:04d}" not in text
