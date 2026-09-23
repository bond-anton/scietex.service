"""Textual TUI running four on-demand workers behind a 2x2 slot grid.

Each slot is one fixed grid position that is either empty or holds a single
worker of one kind (``"valkey"`` or ``"mqtt"``). A worker is created on demand
by clicking its card's create button and runs in its own child process; the
parent drains the child's log records into the slot's own ``RichLog`` through
the ``scietex_bridge`` handler, which posts each formatted record as a Textual
message tagged with the slot key. Selecting a card swaps the log panel to that
slot's stream; each stream keeps its history while hidden, while creating a
worker starts it clean and Exiting it clears the stream back to the empty-slot
hint. Textual owns the terminal, so the child removes the worker's default
console handler and the log colors follow the active Textual theme.
"""

import logging
import time

from rich.text import Text
from scietex.logging import (
    MONOCHROME,
    SCIETEX_DARK,
    SCIETEX_LIGHT,
    from_textual_theme,
)

from scietex.service.basic_worker import ServiceStatus
from textual import on
from textual.app import App, ComposeResult
from textual.containers import Grid, Horizontal, Vertical
from textual.events import Resize
from textual.timer import Timer
from textual.widgets import Collapsible, ContentSwitcher, Footer, RichLog

from .broker_card import BrokerCard, MqttBrokerCard, ValkeyBrokerCard
from .broker_snapshot import BrokerMonitor
from .mqtt_monitor import MqttBrokerMonitor
from .producer_card import ProducerCard
from .producer_process import ProducerProcess
from .scietex_bridge import LogLine, TextualLogHandler, to_textual_theme
from .shutdown_screen import ShutdownScreen
from .slot import Slot, slot_key
from .ui_worker import worker_unavailable
from .valkey_monitor import ValkeyBrokerMonitor
from .worker_card import WorkerCard
from .worker_process import LogRecordData, WorkerIdentity, WorkerProcess

#: How often each card's Start/Stop label is re-read from its worker's state.
STATE_POLL_INTERVAL = 0.5

#: Number of worker slots on the dashboard.
WORKER_COUNT = 4

#: The demo producers, keyed by the task name they emit. The label is the card
#: heading; the task name must match a registered handler on the workers.
PRODUCERS = (("fast", "fast_task", "FAST"), ("slow", "slow_task", "SLOW"))

#: Grid column count, used to translate up/down arrows into index deltas.
GRID_COLUMNS = 2

#: Terminal width (in columns) at or above which the card grid switches from a
#: 2x2 layout to a single 4x1 row.
GRID_WIDE_BREAKPOINT = 160

#: Most log lines rendered per slot per poll tick. A busy worker can emit
#: thousands of records between ticks; rendering them all blocks the event loop
#: for seconds. The cap keeps the newest lines and reports the dropped count, so
#: the UI stays responsive and the loss is visible rather than silent.
MAX_LOG_LINES_PER_TICK = 200


class TextualWorkerApp(App):
    """Textual app driving a 2x2 grid of on-demand worker slots."""

    TITLE = "scietex.service"
    SUB_TITLE = "4 slots"

    CSS_PATH = "app.tcss"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("left", "select_left", "Left"),
        ("right", "select_right", "Right"),
        ("up", "select_up", "Up"),
        ("down", "select_down", "Down"),
    ]

    #: The Scietex themes, registered as Textual themes so the log formatter
    #: stays in lockstep with the UI.
    SCIETEX_THEMES = (SCIETEX_DARK, SCIETEX_LIGHT, MONOCHROME)

    def __init__(self, *, log_level: int = logging.INFO) -> None:
        super().__init__()
        # Level handed to every worker child process's logger; the TUI renders
        # those records. INFO by default; ``--debug`` raises the volume.
        self._log_level = log_level
        self.slots = [Slot(index=i) for i in range(WORKER_COUNT)]
        self._selected_index = 0
        self._shutting_down = False
        self._workers_stopped = False
        self._poll_timer: Timer | None = None
        self._brokers: dict[str, BrokerMonitor] = {}
        self._brokers_stopped = False
        self._producers: dict[str, ProducerProcess] = {}
        self._producer_cards: dict[str, ProducerCard] = {}
        self._producer_rates: dict[str, tuple[int, float]] = {}
        self._producers_stopped = False
        for theme in self.SCIETEX_THEMES:
            self.register_theme(to_textual_theme(theme))

    def compose(self) -> ComposeResult:
        with Vertical(id="pane"):
            with Collapsible(title="PRODUCERS", collapsed=False, classes="section"):
                with Horizontal(id="producer-grid"):
                    for key, _task_name, label in PRODUCERS:
                        card = ProducerCard(key, label)
                        self._producer_cards[key] = card
                        yield card
            with Collapsible(title="WORKERS", collapsed=False, classes="section", id="workers-section"):
                with Grid(id="card-grid"):
                    for slot in self.slots:
                        slot.card = WorkerCard(slot.index)
                        yield slot.card
                with ContentSwitcher(initial=f"log-{slot_key(0)}", id="logs"):
                    for slot in self.slots:
                        slot.log = RichLog(
                            highlight=False,
                            markup=False,
                            wrap=True,
                            max_lines=2000,
                            id=f"log-{slot_key(slot.index)}",
                        )
                        # Keep arrow keys reserved for card navigation; a focusable
                        # RichLog would otherwise consume them for scrolling.
                        slot.log.can_focus = False
                        yield slot.log
            with Collapsible(title="BROKERS", collapsed=False, classes="section"):
                with Horizontal(id="broker-grid"):
                    yield ValkeyBrokerCard(id="broker-valkey")
                    yield MqttBrokerCard(id="broker-mqtt")
        yield Footer()

    async def on_mount(self) -> None:
        self.theme = SCIETEX_DARK.name
        self._select(0)
        self._poll_timer = self.set_interval(STATE_POLL_INTERVAL, self._poll)
        # The first Resize can arrive before the grid is queryable, so apply the
        # breakpoint here too instead of relying on on_resize alone.
        self._apply_grid_breakpoint(self.size.width)
        self._brokers = self._make_broker_monitors()
        for monitor in self._brokers.values():
            await monitor.start()
        self._producers = self._make_producers()

    def _make_broker_monitors(self) -> dict[str, BrokerMonitor]:
        """Build the broker monitors. Overridden in tests to avoid real brokers."""
        return {
            "valkey": ValkeyBrokerMonitor(),
            "mqtt": MqttBrokerMonitor(),
        }

    def _make_producers(self) -> dict[str, ProducerProcess]:
        """Build the task producers. Overridden in tests to avoid real brokers.

        Each producer runs in its own process so its interval loop is not starved
        by the worker handlers sharing the UI's event loop.
        """
        producers: dict[str, ProducerProcess] = {}
        for key, task_name, _label in PRODUCERS:
            producer = ProducerProcess(task_name)
            producer.start_process()
            producers[key] = producer
        return producers

    def _poll(self) -> None:
        """Refresh every live view on one tick: worker cards, broker cards, producers."""
        self._drain_worker_logs()
        self._sync_worker_states()
        self._push_broker_snapshots()
        self._push_producer_snapshots()

    def _drain_worker_logs(self) -> None:
        """Forward each slot's child log records into its log stream.

        Records are batched into one ``RichLog.write`` per slot per tick: a
        per-line write re-renders and re-scrolls the widget, so thousands of
        writes block the event loop for seconds. The newest
        ``MAX_LOG_LINES_PER_TICK`` lines are kept and the rest dropped with a
        note, bounding the tick's cost regardless of the worker's log rate.
        """
        for slot in self.slots:
            worker = slot.worker
            if worker is None:
                continue
            records = worker.drain_logs()
            if not records:
                continue
            dropped = len(records) - MAX_LOG_LINES_PER_TICK
            if dropped > 0:
                records = records[-MAX_LOG_LINES_PER_TICK:]
            lines = [self._format_record(slot, record) for record in records]
            if dropped > 0:
                lines.append(f"… {dropped} log lines dropped (UI backlog)")
            self.handle_log_line(LogLine(slot_key(slot.index), "\n".join(lines)))

    def _format_record(self, slot: Slot, record: LogRecordData) -> str:
        """Format a child log record with the slot handler's formatter."""
        if slot.handler is None:
            return record.message
        return slot.handler.formatter.format(record.to_log_record())

    def _push_producer_snapshots(self) -> None:
        """Render each producer's latest snapshot and sampled emission rate."""
        for key, producer in self._producers.items():
            card = self._producer_cards.get(key)
            # A tick can land after Textual pruned the card subtree but before
            # the producer is stopped; querying a detached card raises NoMatches.
            if card is None or not card.is_attached:
                continue
            card.set_snapshot(producer.snapshot(), self._producer_rate(key, producer))

    def _producer_rate(self, key: str, producer: ProducerProcess) -> float:
        """Emission rate in tasks/s from the emitted counter's delta since the last tick.

        The counter is cumulative, so the rate is a per-tick delta divided by the
        elapsed wall time. The first tick has no baseline and reports zero.
        """
        now = time.monotonic()
        previous = self._producer_rates.get(key)
        self._producer_rates[key] = (producer.emitted, now)
        if previous is None:
            return 0.0
        previous_emitted, previous_at = previous
        elapsed = now - previous_at
        if elapsed <= 0:
            return 0.0
        return max(0.0, (producer.emitted - previous_emitted) / elapsed)

    def _push_broker_snapshots(self) -> None:
        """Render each monitor's latest snapshot into its card."""
        for kind, monitor in self._brokers.items():
            card = self.query_one_optional(f"#broker-{kind}", BrokerCard)
            # A tick can land after Textual pruned the card subtree but before
            # the monitor is stopped; querying a detached card raises NoMatches.
            if card is not None and card.is_attached:
                card.set_snapshot(monitor.snapshot())

    def on_resize(self, event: Resize) -> None:
        """Reflow the card grid at the wide terminal breakpoint.

        ``event.size`` is the terminal size, so the breakpoint is in terminal
        columns rather than card columns.
        """
        self._apply_grid_breakpoint(event.size.width)

    def _apply_grid_breakpoint(self, width: int) -> None:
        """Switch the card grid between 2x2 and 4x1 at the breakpoint."""
        grid = self.query_one_optional("#card-grid", Grid)
        if grid is None:
            return
        wide = width >= GRID_WIDE_BREAKPOINT
        grid.styles.grid_size_columns = 4 if wide else 2
        grid.styles.grid_size_rows = 1 if wide else 2

    async def action_quit(self) -> None:
        """Block on a spinner overlay while the workers stop, then exit.

        Awaiting the screen mount guarantees the overlay is on screen before
        the first ``await worker.stop()`` yields, so the user sees feedback
        instead of a frozen UI for the whole teardown.
        """
        if self._shutting_down:
            return
        self._shutting_down = True
        await self.push_screen(ShutdownScreen())
        await self._stop_workers()
        self.exit()

    async def on_unmount(self) -> None:
        await self._stop_workers()

    async def _stop_workers(self) -> None:
        """Tear down every occupied slot once; safe from both the quit action and unmount."""
        if self._workers_stopped:
            return
        self._workers_stopped = True
        # Stop the state poll before tearing down: Textual prunes the card
        # subtree during shutdown, and a tick landing between the prune and the
        # worker being nulled would query a detached card and raise NoMatches.
        if self._poll_timer is not None:
            self._poll_timer.stop()
            self._poll_timer = None
        for slot in self.slots:
            await self._teardown_worker(slot)
        await self._stop_brokers()
        await self._stop_producers()

    async def _stop_brokers(self) -> None:
        """Stop every broker monitor once, bounded so an unreachable broker cannot hang shutdown."""
        if self._brokers_stopped:
            return
        self._brokers_stopped = True
        for monitor in self._brokers.values():
            await monitor.stop()
        self._brokers.clear()

    async def _stop_producers(self) -> None:
        """Stop every producer once, bounded so an unreachable broker cannot hang shutdown."""
        if self._producers_stopped:
            return
        self._producers_stopped = True
        for producer in self._producers.values():
            await producer.stop()
        self._producers.clear()

    @on(ProducerCard.EmitRequested)
    def on_producer_emit_requested(self, event: ProducerCard.EmitRequested) -> None:
        event.stop()
        producer = self._producers.get(event.producer)
        if producer is not None:
            self.run_worker(producer.emit(), name=f"emit-{event.producer}")

    @on(ProducerCard.Toggled)
    def on_producer_toggled(self, event: ProducerCard.Toggled) -> None:
        event.stop()
        producer = self._producers.get(event.producer)
        if producer is None:
            return
        if producer.running:
            self.run_worker(producer.stop_loop(), name=f"stop-producer-{event.producer}")
        else:
            producer.start()

    @on(ProducerCard.BrokerToggled)
    def on_producer_broker_toggled(self, event: ProducerCard.BrokerToggled) -> None:
        event.stop()
        producer = self._producers.get(event.producer)
        if producer is None:
            return
        if event.broker == "valkey":
            producer.set_valkey_enabled(event.enabled)
        else:
            producer.set_mqtt_enabled(event.enabled)

    @on(ProducerCard.IntervalChanged)
    def on_producer_interval_changed(self, event: ProducerCard.IntervalChanged) -> None:
        event.stop()
        producer = self._producers.get(event.producer)
        if producer is not None:
            producer.set_interval_ms(event.interval_ms)

    @on(ProducerCard.TimeoutChanged)
    def on_producer_timeout_changed(self, event: ProducerCard.TimeoutChanged) -> None:
        event.stop()
        producer = self._producers.get(event.producer)
        if producer is not None:
            producer.set_timeout_ms(event.timeout_ms)

    @on(ProducerCard.BatchChanged)
    def on_producer_batch_changed(self, event: ProducerCard.BatchChanged) -> None:
        event.stop()
        producer = self._producers.get(event.producer)
        if producer is not None:
            producer.set_batch_size(event.batch_size)

    def _select(self, slot_index: int) -> None:
        """Make the given slot the selection and show its log stream."""
        self._selected_index = slot_index
        for slot in self.slots:
            if slot.card is not None:
                slot.card.set_selected(slot.index == slot_index)
        self.query_one(ContentSwitcher).current = f"log-{slot_key(slot_index)}"
        selected = self.slots[slot_index]
        self._show_placeholder_if_empty(selected)
        if selected.card is not None:
            selected.card.focus()

    def _show_placeholder_if_empty(self, slot: Slot) -> None:
        """Write a dim hint into an empty slot's log so it is not blank.

        Guarded by the log being empty so reselecting a slot does not stack
        repeated placeholder lines.
        """
        if slot.worker is None and slot.log is not None and not slot.log.lines:
            slot.log.write(
                Text(f"Slot {slot.index + 1} is empty — create a worker to see its log.", style="dim italic")
            )

    def _move_selection(self, delta: int) -> None:
        """Shift the selection by a grid delta, clamped to the slot bounds."""
        index = max(0, min(self._selected_index + delta, len(self.slots) - 1))
        self._select(index)

    @on(WorkerCard.Selected)
    def on_worker_card_selected(self, event: WorkerCard.Selected) -> None:
        event.stop()
        if event.slot != self._selected_index:
            self._select(event.slot)

    def action_select_left(self) -> None:
        self._move_selection(-1)

    def action_select_right(self) -> None:
        self._move_selection(1)

    def action_select_up(self) -> None:
        self._move_selection(-self._grid_columns())

    def action_select_down(self) -> None:
        self._move_selection(self._grid_columns())

    def _grid_columns(self) -> int:
        """Live column count of the card grid, so up/down track the active layout."""
        grid = self.query_one_optional("#card-grid", Grid)
        if grid is None:
            return GRID_COLUMNS
        return grid.styles.grid_size_columns or GRID_COLUMNS

    def _make_worker_process(self, kind: str) -> WorkerProcess:
        """Build a worker handle for the requested kind (test seam)."""
        process = WorkerProcess(kind, log_level=self._log_level)
        process.start_process()
        return process

    def _create_worker(self, slot_index: int, kind: str) -> None:
        """Create a worker of ``kind`` in an empty slot, wiring its log handler."""
        slot = self.slots[slot_index]
        if slot.worker is not None:
            return
        if worker_unavailable(kind):
            self._write_slot_error(
                slot, f"Slot {slot.index + 1}: {kind} worker unavailable — install the '{kind}' extra"
            )
            return
        process = self._make_worker_process(kind)
        handler = TextualLogHandler(self, source=slot_key(slot_index))
        slot.worker = process
        slot.handler = handler
        slot.kind = kind
        # A newly created worker starts right away, so ``running`` tracks the
        # in-flight start until the state poller reconciles it from the worker.
        slot.running = True
        # Placeholder identity until the first snapshot carries the real id.
        slot.identity = process.identity
        if slot.card is not None:
            slot.card.set_worker(slot.identity)
            # Mirror the running state now so the label does not wait a poll
            # cycle; the poller leaves it alone while it agrees.
            slot.card.set_running(True)
        # Drop the empty-slot placeholder (and any stale lines) so the stream
        # starts clean; ``slot.worker`` is already set, so ``_select`` will not
        # write the placeholder back.
        if slot.log is not None:
            slot.log.clear()
        self._select(slot_index)
        self.run_worker(self._start_worker(slot_index), name=f"start-worker-{slot_index}")

    def _write_slot_error(self, slot: Slot, text: str) -> None:
        """Write a styled error into a slot's log and surface it."""
        if slot.log is not None:
            slot.log.write(Text(text, style="bold red"))
        if slot.index != self._selected_index:
            self._select(slot.index)

    @on(WorkerCard.CreateRequested)
    def on_worker_card_create_requested(self, event: WorkerCard.CreateRequested) -> None:
        event.stop()
        self._create_worker(event.slot, event.kind)

    @on(WorkerCard.Toggled)
    def on_worker_card_toggled(self, event: WorkerCard.Toggled) -> None:
        event.stop()
        slot = self.slots[event.slot]
        self.run_worker(
            self._stop_worker(event.slot) if slot.running else self._start_worker(event.slot),
            name=f"toggle-worker-{event.slot}",
        )

    async def _start_worker(self, slot_index: int) -> None:
        """Start the slot's worker off the UI path (the card label follows via the timer)."""
        slot = self.slots[slot_index]
        if slot.worker is None:
            return
        await slot.worker.start()

    async def _stop_worker(self, slot_index: int) -> None:
        """Stop the slot's worker off the UI path (the card label follows via the timer)."""
        slot = self.slots[slot_index]
        if slot.worker is None:
            return
        await slot.worker.stop()

    @on(WorkerCard.ExitRequested)
    def on_worker_card_exit_requested(self, event: WorkerCard.ExitRequested) -> None:
        event.stop()
        self.run_worker(self._teardown_worker(self.slots[event.slot]), name=f"exit-worker-{event.slot}")

    async def _teardown_worker(self, slot: Slot) -> None:
        """Exit a slot's worker and restore the slot to its empty state."""
        worker = slot.worker
        if worker is None:
            return
        await worker.exit()
        if not worker.exited:
            self.log.warning("Slot %d worker process did not exit cleanly", slot.index + 1)
        slot.worker = None
        slot.handler = None
        slot.identity = None
        slot.kind = None
        slot.running = False
        # Clear the exited worker's history and restore the empty-slot hint.
        if slot.log is not None:
            slot.log.clear()
            self._show_placeholder_if_empty(slot)
        # During shutdown the card's children are already pruned from the DOM,
        # so the visual reset is only safe (and only needed) while attached.
        if slot.card is not None and slot.card.is_attached:
            slot.card.set_worker(None)

    def _sync_worker_states(self) -> None:
        """Re-read each slot's worker state and reflect it in its card."""
        for slot in self.slots:
            worker = slot.worker
            if worker is None:
                continue
            running = worker.state in (ServiceStatus.RUNNING, ServiceStatus.STARTING)
            if running != slot.running:
                slot.running = running
                if slot.card is not None:
                    slot.card.set_running(running)
            # A tick can land after Textual pruned the card subtree but before
            # the worker is nulled; querying a detached card raises NoMatches.
            if slot.card is not None and slot.card.is_attached:
                identity: WorkerIdentity = worker.identity
                if identity != slot.identity:
                    slot.identity = identity
                    slot.card.set_worker(slot.identity)
                self._push_metrics(slot)

    def _push_metrics(self, slot: Slot) -> None:
        """Compute a slot's live metrics from the worker's public API and render them.

        The metrics are read directly from the worker (not gated behind the
        running-flag change), since in-flight tasks and queue state change
        without the worker's lifecycle state flipping. Each read is a cheap
        public accessor that is valid before and after start; ``task_metrics``
        yields queue depth, running count, and completion rate in one snapshot,
        while the concurrency ceiling, queue capacity, and failed managers are
        not part of it and are read from the worker directly.
        """
        worker = slot.worker
        card = slot.card
        if worker is None or card is None:
            return
        metrics = worker.task_metrics()
        card.set_metrics(
            running=metrics.running,
            max_concurrent=worker.max_concurrent_tasks,
            queue_depth=metrics.queue_depth,
            queue_capacity=worker.queue_size,
            rate=metrics.rate,
            failed_managers=len(worker.failed_managers),
            total=metrics.total,
            health=self._worker_health(worker),
        )

    def _worker_health(self, worker: WorkerProcess) -> bool | None:
        """Collapse the transport-health supervisor into a status-line value.

        ``None`` means the worker has no transport at all (a bare processor), so
        the card draws a grayed dot instead of implying a healthy link.
        """
        health = worker.transport_health
        if health is None:
            return None
        return health.connected and not health.degraded

    def watch_theme(self, theme_name: str) -> None:
        """Keep the log formatters in lockstep with the active Textual theme.

        Fires for any theme change, so the log colors always match the UI. A
        registered Scietex theme is used directly, which preserves its ``color``
        policy (monochrome emits no ANSI at all); any other Textual theme is
        converted on the fly.
        """
        for theme in self.SCIETEX_THEMES:
            if theme.name == theme_name:
                for slot in self.slots:
                    if slot.handler is not None:
                        slot.handler.set_theme(theme)
                break
        else:
            textual_theme = self.get_theme(theme_name)
            if textual_theme is None:
                return
            for slot in self.slots:
                if slot.handler is not None:
                    slot.handler.set_theme(from_textual_theme(textual_theme))

    def _log_for(self, source: str) -> RichLog | None:
        """Map a slot key back to its ``RichLog`` widget."""
        for slot in self.slots:
            if slot_key(slot.index) == source:
                return slot.log
        return None

    @on(LogLine)
    def handle_log_line(self, message: LogLine) -> None:
        """Render a posted log line into its slot's stream.

        ``text`` may hold several newline-joined lines from one drain batch;
        ``Text.from_ansi`` renders them as separate lines in a single write.
        """
        log = self._log_for(message.source)
        if log is not None:
            log.write(Text.from_ansi(message.text))
