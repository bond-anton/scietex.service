"""Bridge a worker's logging stream into a Textual app.

Routes the asynchronous logging machinery into a Textual widget: a custom
backend posts each formatted record into the app as a thread-safe message, and
the app renders it with ``Text.from_ansi`` so the theme's truecolor survives.
The Scietex palettes are mapped to Textual ``Theme`` objects so they drive the
whole UI and stay in lockstep with the log formatter.

Adapted from ``scietex.logging``'s ``examples/textual_log_viewer.py``.
"""

import logging

from scietex.logging import (
    SCIETEX_DARK,
    SCIETEX_LIGHT,
    LoggingTheme,
    ScietexFormatter,
)

from textual.app import App
from textual.message import Message
from textual.theme import Theme


def to_textual_theme(theme: LoggingTheme) -> Theme:
    """Build a Textual ``Theme`` from a scietex ``LoggingTheme``.

    The Scietex palettes are already Textual-shaped, so the mapping is direct:
    the level colors become the semantic slots and the background/foreground
    carry over. Registering these makes the Scietex themes first-class Textual
    themes that drive the whole UI.

    The brand yellow is an *accent*, not a surface: in the brand deck it appears
    only as the scietex.ru badge and the diagonal panel, while panels and the
    header sit on the brand's neutral tones. So the yellow drives ``accent``
    (borders, focus) while ``primary``/``secondary`` use the brand neutrals,
    which is what Textual tints the header, footer, and panel surfaces with.
    ``panel`` is left unset so Textual derives it from ``surface`` + ``primary``
    instead of painting a flat block of color.
    """
    palette = theme.palette
    dark = palette.background != SCIETEX_LIGHT.palette.background
    # Dark themes sit on the brand black with the dark-gray surface; the light
    # theme keeps a near-white surface so panels stay subtle on white.
    surface = palette.brand_dark_gray if dark else "#F5F5F5"
    return Theme(
        name=theme.name,
        primary=palette.brand_dark_gray,
        secondary=palette.brand_dark_gray,
        warning=palette.warning or palette.brand_yellow,
        error=palette.error or palette.brand_yellow,
        success=palette.info or palette.brand_yellow,
        accent=palette.brand_yellow,
        foreground=palette.foreground,
        background=palette.background,
        surface=surface,
        dark=dark,
    )


class LogLine(Message):
    """A formatted log line, safe to post from any thread."""

    def __init__(self, source: str, text: str) -> None:
        self.source = source
        self.text = text
        super().__init__()


class TextualLogHandler(logging.Handler):
    """Logging handler whose sink is a Textual app.

    The parent owns this handler: the child ships picklable records across the
    process boundary, and the parent formats each one and posts it to the app as
    a thread-safe message.
    """

    # Always non-None: __init__ installs a ScietexFormatter.
    formatter: logging.Formatter

    def __init__(self, app: App, *, source: str, theme: LoggingTheme = SCIETEX_DARK, **kwargs) -> None:
        super().__init__(**kwargs)
        self._app = app
        self._source = source
        self.formatter = ScietexFormatter(theme=theme)

    def emit(self, record: logging.LogRecord) -> None:
        """Format the record and post it to the app.

        ``post_message`` is thread-safe, so the app receives the line regardless
        of which thread emitted the record.
        """
        try:
            text = self.formatter.format(record)
            self._app.post_message(LogLine(self._source, text))
        except Exception:
            # A buggy formatter must not kill the emitting thread; the standard
            # handler error path reports it instead.
            self.handleError(record)

    def set_theme(self, theme: LoggingTheme) -> None:
        """Swap the formatter's theme.

        Replacing it takes effect on the next record. ``color`` is left to the
        theme, so a monochrome theme emits no ANSI.
        """
        self.formatter = ScietexFormatter(theme=theme)
