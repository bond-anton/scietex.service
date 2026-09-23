"""Worker card widget for a single slot in the center pane.

Each card renders two mutually exclusive states -- an empty state offering to
create a Valkey or MQTT worker, and an occupied state showing the worker's
kind, truncated instance id, live metrics, a transport-health/completion status
line, and Start/Stop plus Exit controls. A CSS class toggles which state is
visible, so nothing is mounted or unmounted dynamically.
"""

from collections.abc import Sequence

from textual import events
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.events import MouseEvent
from textual.message import Message
from textual.widgets import Button, ProgressBar, Sparkline, Static

from .worker_process import WorkerIdentity

#: Number of rate samples the sparkline keeps before dropping the oldest.
RATE_HISTORY_LIMIT = 60

#: Vertical-range sentinels pinned into the sparkline's data so every card
#: shares the same 0..SPARK_SCALE_MAX t/s scale instead of auto-scaling to its own peak.
SPARK_SCALE_MIN = 0.0
SPARK_SCALE_MAX = 10000.0

#: Maps the three health states to the CSS class coloring the status dot.
#: ``None`` is the no-transport case (a bare processor/worker), rendered as a
#: grayed error dot so it reads as "not applicable" rather than healthy.
_HEALTH_DOT_CLASS = {True: "healthy", False: "unhealthy", None: "no-transport"}


def _spark_summary(partition: Sequence[float]) -> float:
    """Peak of a bucket, excluding the scale sentinels.

    The renderable derives its vertical range from the raw data's min/max, so
    the sentinels still pin the scale to 0..SPARK_SCALE_MAX; the summary function only
    decides each bucket's rendered height. Dropping the sentinels here stops
    them from rendering as their own full-height (or empty) bars, so they fix
    the scale without distorting the shape.
    """
    real = [value for value in partition if SPARK_SCALE_MIN < value < SPARK_SCALE_MAX]
    return max(real) if real else SPARK_SCALE_MIN


class WorkerCard(Static):
    """One slot's empty/occupied card.

    Buttons post messages upward so the app (which owns the workers) drives the
    lifecycle off the UI path; the app pushes resulting state back through
    :meth:`set_worker` / :meth:`set_running` / :meth:`set_metrics`. Clicking the
    card (or giving it focus) posts a :class:`Selected` message so the app can
    swap the log panel to this slot's stream.
    """

    can_focus = True

    class CreateRequested(Message):
        """Posted when a create button is pressed, requesting a worker of ``kind``."""

        def __init__(self, slot: int, kind: str) -> None:
            self.slot = slot
            self.kind = kind
            super().__init__()

    class Toggled(Message):
        """Posted when the Start/Stop button is pressed."""

        def __init__(self, slot: int) -> None:
            self.slot = slot
            super().__init__()

    class ExitRequested(Message):
        """Posted when the Exit button is pressed."""

        def __init__(self, slot: int) -> None:
            self.slot = slot
            super().__init__()

    class Selected(Message):
        """Posted when the card is clicked or receives focus."""

        def __init__(self, slot: int) -> None:
            self.slot = slot
            super().__init__()

    def __init__(self, slot_index: int) -> None:
        super().__init__()
        self._slot_index = slot_index
        self._rate_history: list[float] = []
        self.add_class("empty")

    @property
    def slot_index(self) -> int:
        """The 0-based slot index this card displays."""
        return self._slot_index

    def compose(self) -> ComposeResult:
        n = self._slot_index + 1
        with Vertical(classes="occupied-state"):
            with Horizontal(classes="slot-header"):
                yield Static(f"Slot {n}", classes="slot-number")
                yield Static("", classes="worker-kind")
            yield Static("", classes="instance-id")
            with Horizontal(classes="metric-row"):
                yield Static("jobs", classes="metric-label jobs-label")
                yield ProgressBar(total=1, show_percentage=False, show_eta=False, classes="jobs-bar")
                yield Static("", classes="metric-value jobs-value")
            with Horizontal(classes="metric-row"):
                yield Static("queue", classes="metric-label queue-label")
                yield ProgressBar(total=1, show_percentage=False, show_eta=False, classes="queue-bar")
                yield Static("", classes="metric-value queue-value")
            with Horizontal(classes="metric-row"):
                yield Static("rate", classes="metric-label rate-label")
                yield ProgressBar(total=SPARK_SCALE_MAX, show_percentage=False, show_eta=False, classes="rate-bar")
                yield Static("", classes="metric-value rate-value")
            yield Sparkline(classes="rate-spark", summary_function=_spark_summary)
            with Horizontal(classes="status-line"):
                yield Static("link:", classes="link-label")
                yield Static("●", classes="link-dot")
                yield Static("", classes="link-total")
            with Horizontal(classes="controls"):
                yield Button("Start", classes="toggle", variant="default")
                yield Button("Exit", classes="exit", variant="error")
        with Vertical(classes="empty-state"):
            yield Static(f"Slot {n}", classes="slot-number")
            yield Static("NO WORKER", classes="empty-label")
            with Horizontal(classes="create-buttons"):
                yield Button("Valkey", classes="kind-valkey", variant="primary")
                yield Button("MQTT", classes="kind-mqtt", variant="primary")

    def set_worker(self, identity: WorkerIdentity | None) -> None:
        """Render the occupied state for a worker identity, or the empty state for ``None``."""
        if identity is None:
            self._rate_history = []
            self.query_one(".worker-kind", Static).update("")
            self.query_one(".instance-id", Static).update("")
            self.query_one(".jobs-value", Static).update("")
            self.query_one(".queue-value", Static).update("")
            self.query_one(".rate-value", Static).update("")
            self.query_one(".jobs-bar", ProgressBar).update(progress=0, total=1)
            self.query_one(".queue-bar", ProgressBar).update(progress=0, total=1)
            self.query_one(".rate-bar", ProgressBar).update(progress=0, total=SPARK_SCALE_MAX)
            self.query_one(".rate-spark", Sparkline).data = []
            self.query_one(".link-dot", Static).remove_class("healthy", "unhealthy", "no-transport")
            self.query_one(".link-dot", Static).add_class("no-transport")
            self.query_one(".link-total", Static).update("")
            self.add_class("empty")
            self.remove_class("occupied")
            self.set_running(False)
            return
        self.query_one(".worker-kind", Static).update(identity.kind_label)
        self.query_one(".instance-id", Static).update(f"…{identity.instance_id[-4:]}")
        self.add_class("occupied")
        self.remove_class("empty")

    def set_running(self, running: bool) -> None:
        """Reflect the worker's running state in the button label."""
        self.query_one(".toggle", Button).label = "Stop" if running else "Start"

    def set_metrics(
        self,
        *,
        running: int,
        max_concurrent: int,
        queue_depth: int,
        queue_capacity: int,
        rate: float,
        failed_managers: int,
        total: int,
        health: bool | None,
    ) -> None:
        """Render the worker's live monitoring metrics into the card.

        The app computes each value from the worker's public API on every poll,
        so this only formats. Running and queue depth become labelled progress
        bars whose share of capacity reads as a gauge, with the exact counts in a
        right-aligned value beside each bar; the completion rate gets its own bar
        on the shared 0..SPARK_SCALE_MAX t/s scale so rate and sparkline agree,
        and feeds a sparkline the card accumulates into a bounded history. A
        non-zero failed-manager count is appended to the rate value as a compact
        alert rather than reserving space for it when healthy. The status line
        renders the cumulative completion count beside a colored transport-health
        dot: ``health`` is ``True`` when connected and not degraded, ``False``
        when degraded or disconnected, and ``None`` when the worker has no
        transport (a bare processor), which draws a grayed dot so it reads as
        "not applicable" rather than healthy.
        """
        # Textual prunes a card's children during shutdown while the card itself
        # is still attached, so a poll tick can land on a card whose subtree is
        # gone. Resolve every child up front and bail if any is missing, so the
        # method never queries a subtree that is pruned mid-flight.
        jobs_bar = self.query_one_optional(".jobs-bar", ProgressBar)
        queue_bar = self.query_one_optional(".queue-bar", ProgressBar)
        rate_bar = self.query_one_optional(".rate-bar", ProgressBar)
        jobs_value = self.query_one_optional(".jobs-value", Static)
        queue_value = self.query_one_optional(".queue-value", Static)
        rate_value = self.query_one_optional(".rate-value", Static)
        spark = self.query_one_optional(".rate-spark", Sparkline)
        link_dot = self.query_one_optional(".link-dot", Static)
        link_total = self.query_one_optional(".link-total", Static)
        if (
            jobs_bar is None
            or queue_bar is None
            or rate_bar is None
            or jobs_value is None
            or queue_value is None
            or rate_value is None
            or spark is None
            or link_dot is None
            or link_total is None
        ):
            return
        # A zero (or negative) total would render the bar full via the
        # percentage clamp's ``total == 0 -> 1.0`` branch; clamp to at least one
        # so an empty capacity still reads as an empty gauge.
        jobs_bar.update(progress=running, total=max(1, max_concurrent))
        queue_bar.update(progress=queue_depth, total=max(1, queue_capacity))
        jobs_value.update(f"{running}/{max_concurrent}")
        queue_value.update(f"{queue_depth}/{queue_capacity}")

        self._rate_history.append(rate)
        if len(self._rate_history) > RATE_HISTORY_LIMIT:
            self._rate_history = self._rate_history[-RATE_HISTORY_LIMIT:]
        # Pin the sparkline's vertical range to a fixed 0..SPARK_SCALE_MAX t/s by including
        # sentinel bounds; the Sparkline renderable scales to its data's own
        # min/max, so without them each card would auto-scale to its own peak and
        # the cards would not be comparable. ``_spark_summary`` excludes the
        # sentinels from the rendered height so they set the scale without
        # drawing their own bars.
        spark.data = [SPARK_SCALE_MIN, *self._rate_history, SPARK_SCALE_MAX]

        # The rate bar and the sparkline share the same scale so the bar's fill
        # is a direct read of the sparkline's latest sample.
        rate_bar.update(progress=rate, total=SPARK_SCALE_MAX)
        text = f"{rate:.1f}/s"
        if failed_managers:
            text += f" · {failed_managers} mgr failed"
        rate_value.update(text)

        link_dot.remove_class("healthy", "unhealthy", "no-transport")
        link_dot.add_class(_HEALTH_DOT_CLASS[health])
        link_total.update(f"· {total:,} done")

    def set_selected(self, selected: bool) -> None:
        """Toggle the selection highlight border."""
        self.set_class(selected, "selected")

    def on_click(self, event: MouseEvent) -> None:
        event.stop()
        self.focus()
        self.post_message(self.Selected(self._slot_index))

    def on_focus(self, event: events.Focus) -> None:
        self.post_message(self.Selected(self._slot_index))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.has_class("toggle"):
            event.stop()
            self.post_message(self.Toggled(self._slot_index))
        elif event.button.has_class("exit"):
            event.stop()
            self.post_message(self.ExitRequested(self._slot_index))
        elif event.button.has_class("kind-valkey"):
            event.stop()
            self.post_message(self.CreateRequested(self._slot_index, "valkey"))
        elif event.button.has_class("kind-mqtt"):
            event.stop()
            self.post_message(self.CreateRequested(self._slot_index, "mqtt"))
