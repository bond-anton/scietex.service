"""Modal overlay shown while the app stops its workers.

Pressing ``q`` stops every occupied slot before the app can exit; that teardown
is slow enough that the UI would otherwise look frozen. This screen is pushed
first so the user sees an animated spinner until the workers are down.
"""

from textual.app import ComposeResult
from textual.containers import Center, Middle, Vertical
from textual.screen import ModalScreen
from textual.widgets import LoadingIndicator, Static


class ShutdownScreen(ModalScreen):
    """A centered spinner blocking input until shutdown finishes."""

    def compose(self) -> ComposeResult:
        with Middle(), Center(), Vertical(id="shutdown-box"):
            yield LoadingIndicator()
            yield Static("Shutting down workers...", id="shutdown-label")
