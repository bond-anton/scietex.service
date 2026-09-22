"""Textual TUI running one Valkey worker behind a live log panel.

The worker's asynchronous logging is routed into a ``RichLog`` through the
``scietex_bridge`` handler, which posts each formatted record as a Textual
message. Textual owns the terminal, so the worker's default ``ConsoleHandler``
is removed and the log colors follow the active Textual theme.
"""

from rich.text import Text
from scietex.logging import (
    MONOCHROME,
    SCIETEX_DARK,
    SCIETEX_LIGHT,
    ConsoleHandler,
    from_textual_theme,
)

from scietex.service import ScietexDark
from scietex.service.basic_worker import ServiceStatus
from textual import on
from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.widgets import Footer, RichLog

from .scietex_bridge import LogLine, TextualLogHandler, to_textual_theme
from .ui_worker import UiValkeyWorker
from .worker_card import WorkerCard

#: How often the card's Start/Stop label is re-read from the worker's state.
STATE_POLL_INTERVAL = 0.5


class TextualWorkerApp(App):
    """Textual app driving a single Valkey worker."""

    TITLE = "scietex.service"
    SUB_TITLE = "single Valkey worker"

    CSS = """
    #pane {
        height: 1fr;
    }
    WorkerCard {
        height: auto;
        border: round $panel-lighten-1;
        padding: 0 1;
    }
    WorkerCard:focus-within {
        border: round $accent;
    }
    #instance-id {
        text-style: bold;
    }
    /* The primary variant paints $primary behind the label. The id selector
       outranks the variant/state class rules, so one rule clears the fill in
       normal, hover, focus, active, and disabled states alike. */
    #toggle {
        background: transparent;
    }
    RichLog {
        border: round $panel-lighten-1;
        padding: 0 1;
        height: 1fr;
        min-height: 5;
    }
    RichLog:focus {
        border: round $accent;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
    ]

    #: The Scietex themes, registered as Textual themes so the log formatter
    #: stays in lockstep with the UI.
    SCIETEX_THEMES = (SCIETEX_DARK, SCIETEX_LIGHT, MONOCHROME)

    def __init__(self) -> None:
        super().__init__()
        # The worker's startup banner goes to stdout, which Textual owns; opt
        # the theme out of it so it cannot corrupt the screen.
        self.worker = UiValkeyWorker(theme=ScietexDark(show_banner=False))
        # Textual owns the terminal; the worker's ConsoleHandler writes to
        # sys.stdout, so drop it before it can corrupt the screen.
        self._remove_console_handler()
        self._handler = TextualLogHandler(self)
        self.worker.logger.addHandler(self._handler)
        for theme in self.SCIETEX_THEMES:
            self.register_theme(to_textual_theme(theme))
        self._worker_running = False

    def _remove_console_handler(self) -> None:
        """Remove the worker's stdout ``ConsoleHandler`` from its logger."""
        for handler in list(self.worker.logger.handlers):
            if isinstance(handler, ConsoleHandler):
                self.worker.logger.removeHandler(handler)

    def compose(self) -> ComposeResult:
        self._card = WorkerCard(self.worker.instance_id)
        with Vertical(id="pane"):
            yield self._card
            yield RichLog(
                highlight=False,
                markup=False,
                wrap=True,
                max_lines=2000,
                id="log-panel",
            )
        yield Footer()

    async def on_mount(self) -> None:
        self.theme = SCIETEX_DARK.name
        self.set_interval(STATE_POLL_INTERVAL, self._sync_worker_state)

    async def on_unmount(self) -> None:
        await self.worker.stop()
        await self._handler.stop_logging()
        self.worker.logger.removeHandler(self._handler)

    def _sync_worker_state(self) -> None:
        """Re-read the worker's lifecycle state and reflect it in the card."""
        running = self.worker.state in (ServiceStatus.RUNNING, ServiceStatus.STARTING)
        if running != self._worker_running:
            self._worker_running = running
            self._card.set_running(running)

    def watch_theme(self, theme_name: str) -> None:
        """Keep the log formatter in lockstep with the active Textual theme.

        Fires for any theme change, so the log colors always match the UI. A
        registered Scietex theme is used directly, which preserves its ``color``
        policy (monochrome emits no ANSI at all); any other Textual theme is
        converted on the fly.
        """
        for theme in self.SCIETEX_THEMES:
            if theme.name == theme_name:
                self._handler.set_theme(theme)
                break
        else:
            textual_theme = self.get_theme(theme_name)
            if textual_theme is None:
                return
            self._handler.set_theme(from_textual_theme(textual_theme))

    @on(LogLine)
    def handle_log_line(self, message: LogLine) -> None:
        """Render a posted log line, decoding its ANSI truecolor."""
        self.query_one("#log-panel", RichLog).write(Text.from_ansi(message.text))

    def on_worker_card_toggled(self, event: WorkerCard.Toggled) -> None:
        event.stop()
        if self._worker_running:
            self.run_worker(self._stop_worker(), name="stop-worker")
        else:
            self.run_worker(self._start_worker(), name="start-worker")

    async def _start_worker(self) -> None:
        """Start the worker off the UI path (the card label follows via the timer)."""
        await self.worker.start()

    async def _stop_worker(self) -> None:
        """Stop the worker off the UI path (the card label follows via the timer)."""
        await self.worker.stop()
