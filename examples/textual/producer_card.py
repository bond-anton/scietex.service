"""Producer card widget for the PRODUCERS pane.

Each card drives one :class:`~examples.textual.producer.TaskProducer`: two broker
switches (Valkey / MQTT), an interval and a timeout input in milliseconds, a
rate bar and sparkline mirroring the worker cards, and Emit plus Run/Stop
buttons. The card posts messages upward so the app owns the producer lifecycle;
the app pushes state back through :meth:`set_snapshot`.
"""

from collections.abc import Sequence

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widgets import Button, Input, ProgressBar, Sparkline, Static, Switch

from .producer import DEFAULT_BATCH_SIZE, DEFAULT_INTERVAL_MS, DEFAULT_TIMEOUT_MS, ProducerSnapshot

#: Number of rate samples the sparkline keeps before dropping the oldest.
RATE_HISTORY_LIMIT = 60

#: Vertical-range sentinels pinning every producer sparkline to the same
#: 0..SPARK_SCALE_MAX t/s scale, matching the worker cards.
SPARK_SCALE_MIN = 0.0
SPARK_SCALE_MAX = 10000.0


def _spark_summary(partition: Sequence[float]) -> float:
    """Peak of a bucket, excluding the scale sentinels (see ``worker_card``)."""
    real = [value for value in partition if SPARK_SCALE_MIN < value < SPARK_SCALE_MAX]
    return max(real) if real else SPARK_SCALE_MIN


class ProducerCard(Static):
    """One producer's control and metrics card."""

    class EmitRequested(Message):
        """Posted when the Emit button is pressed."""

        def __init__(self, producer: str) -> None:
            self.producer = producer
            super().__init__()

    class Toggled(Message):
        """Posted when the Run/Stop button is pressed."""

        def __init__(self, producer: str) -> None:
            self.producer = producer
            super().__init__()

    class BrokerToggled(Message):
        """Posted when a broker switch changes state."""

        def __init__(self, producer: str, broker: str, enabled: bool) -> None:
            self.producer = producer
            self.broker = broker
            self.enabled = enabled
            super().__init__()

    class IntervalChanged(Message):
        """Posted when the interval input changes to a valid value."""

        def __init__(self, producer: str, interval_ms: int) -> None:
            self.producer = producer
            self.interval_ms = interval_ms
            super().__init__()

    class TimeoutChanged(Message):
        """Posted when the timeout input changes to a valid value."""

        def __init__(self, producer: str, timeout_ms: int) -> None:
            self.producer = producer
            self.timeout_ms = timeout_ms
            super().__init__()

    class BatchChanged(Message):
        """Posted when the batch-size input changes to a valid value."""

        def __init__(self, producer: str, batch_size: int) -> None:
            self.producer = producer
            self.batch_size = batch_size
            super().__init__()

    def __init__(self, producer: str, label: str) -> None:
        super().__init__()
        self._producer = producer
        self._label = label
        self._rate_history: list[float] = []
        self._last_emitted = 0

    @property
    def producer(self) -> str:
        return self._producer

    def compose(self) -> ComposeResult:
        with Vertical(classes="producer-body"):
            yield Static(self._label, classes="producer-title")
            with Horizontal(classes="switch-row"):
                yield Static("Valkey", classes="switch-label")
                yield Switch(value=False, id=f"switch-valkey-{self._producer}", classes="broker-switch")
                yield Static("MQTT", classes="switch-label")
                yield Switch(value=False, id=f"switch-mqtt-{self._producer}", classes="broker-switch")
            with Horizontal(classes="input-row"):
                yield Static("interval", classes="input-label")
                yield Input(
                    value=str(DEFAULT_INTERVAL_MS),
                    type="integer",
                    id=f"interval-{self._producer}",
                    classes="producer-input",
                )
                yield Static("ms", classes="input-unit")
                yield Static("timeout", classes="input-label")
                yield Input(
                    value=str(DEFAULT_TIMEOUT_MS),
                    type="integer",
                    id=f"timeout-{self._producer}",
                    classes="producer-input",
                )
                yield Static("ms", classes="input-unit")
                yield Static("batch", classes="input-label")
                yield Input(
                    value=str(DEFAULT_BATCH_SIZE),
                    type="integer",
                    id=f"batch-{self._producer}",
                    classes="producer-input",
                )
                yield Static("x", classes="input-unit")
            with Horizontal(classes="metric-row"):
                yield Static("rate", classes="metric-label rate-label")
                yield ProgressBar(total=SPARK_SCALE_MAX, show_percentage=False, show_eta=False, classes="rate-bar")
                yield Static("", classes="metric-value rate-value")
            yield Sparkline(classes="rate-spark", summary_function=_spark_summary)
            with Horizontal(classes="status-line"):
                yield Static("", classes="producer-status")
            with Horizontal(classes="controls"):
                yield Button("Emit", classes="emit", variant="primary")
                yield Button("Run", classes="run", variant="success")

    def set_snapshot(self, snapshot: ProducerSnapshot, rate: float) -> None:
        """Render a producer snapshot and the sampled emission rate.

        The rate is computed by the app from the emitted counter's delta between
        poll ticks, so the card only accumulates history and formats. A publish
        failure is surfaced in the status line rather than silently dropped.
        """
        # Textual prunes a card's children during shutdown while the card itself
        # is still attached, so a poll tick can land on a card whose subtree is
        # gone. Resolve every child up front and bail if any is missing.
        rate_bar = self.query_one_optional(".rate-bar", ProgressBar)
        rate_value = self.query_one_optional(".rate-value", Static)
        spark = self.query_one_optional(".rate-spark", Sparkline)
        status = self.query_one_optional(".producer-status", Static)
        run_button = self.query_one_optional(".run", Button)
        if rate_bar is None or rate_value is None or spark is None or status is None or run_button is None:
            return

        self._rate_history.append(rate)
        if len(self._rate_history) > RATE_HISTORY_LIMIT:
            self._rate_history = self._rate_history[-RATE_HISTORY_LIMIT:]
        spark.data = [SPARK_SCALE_MIN, *self._rate_history, SPARK_SCALE_MAX]
        rate_bar.update(progress=rate, total=SPARK_SCALE_MAX)
        rate_value.update(f"{rate:.1f}/s")

        run_button.label = "Stop" if snapshot.running else "Run"
        run_button.variant = "error" if snapshot.running else "success"

        if snapshot.last_error is not None:
            status.update(f"error: {snapshot.last_error}")
            status.add_class("error")
        else:
            status.update(f"{snapshot.emitted:,} emitted")
            status.remove_class("error")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.has_class("emit"):
            event.stop()
            self.post_message(self.EmitRequested(self._producer))
        elif event.button.has_class("run"):
            event.stop()
            self.post_message(self.Toggled(self._producer))

    def on_switch_changed(self, event: Switch.Changed) -> None:
        switch_id = event.switch.id or ""
        if switch_id.startswith("switch-valkey-"):
            broker = "valkey"
        elif switch_id.startswith("switch-mqtt-"):
            broker = "mqtt"
        else:
            return
        event.stop()
        self.post_message(self.BrokerToggled(self._producer, broker, event.value))

    def on_input_changed(self, event: Input.Changed) -> None:
        input_id = event.input.id or ""
        # An empty or non-numeric field is a transient edit state, not a value;
        # ignore it so the producer keeps its last valid setting.
        if not event.value.isdigit():
            return
        value = int(event.value)
        if input_id == f"interval-{self._producer}":
            event.stop()
            self.post_message(self.IntervalChanged(self._producer, value))
        elif input_id == f"timeout-{self._producer}":
            event.stop()
            self.post_message(self.TimeoutChanged(self._producer, value))
        elif input_id == f"batch-{self._producer}":
            event.stop()
            self.post_message(self.BatchChanged(self._producer, value))
