"""Worker card widget for the center pane."""

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import Button, Static


class WorkerCard(Static):
    """A single worker's instance id and Start/Stop control.

    The button posts a :class:`Toggled` message upward so the app (which owns
    the worker) drives the lifecycle off the UI path; the app pushes the
    resulting state back through :meth:`set_running`.
    """

    # The nested Vertical defaults to ``height: 1fr``; inside an auto-height
    # card that expands to the parent's full height and starves any sibling
    # (the log panel) of its fraction. Pin it to content instead.
    DEFAULT_CSS = """
    WorkerCard > Vertical {
        height: auto;
    }
    """

    class Toggled(Message):
        """Posted when the Start/Stop button is pressed."""

    def __init__(self, instance_id: str) -> None:
        super().__init__()
        self._instance_id = instance_id

    @property
    def instance_id(self) -> str:
        """The worker instance id this card displays."""
        return self._instance_id

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(f"id: {self._instance_id}", id="instance-id", markup=False)
            yield Button("Start", id="toggle", variant="primary")

    def set_running(self, running: bool) -> None:
        """Reflect the worker's running state in the button label."""
        self.query_one("#toggle", Button).label = "Stop" if running else "Start"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "toggle":
            event.stop()
            self.post_message(self.Toggled())
