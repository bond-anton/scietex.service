"""Bridge a worker's logging stream into a Textual app.

Routes the asynchronous logging machinery into a Textual widget: a custom
backend posts each formatted record into the app as a thread-safe message, and
the app renders it with ``Text.from_ansi`` so the theme's truecolor survives.
The Scietex palettes are mapped to Textual ``Theme`` objects so they drive the
whole UI and stay in lockstep with the log formatter.

Adapted from ``scietex.logging``'s ``examples/textual_log_viewer.py``.
"""

import asyncio
import logging

from scietex.logging import (
    SCIETEX_DARK,
    SCIETEX_LIGHT,
    AsyncLoggingHandler,
    LoggingTheme,
    ScietexFormatter,
)
from scietex.logging.async_logging_handler import BackendDrainResult, DrainStatus

from textual.app import App
from textual.message import Message
from textual.theme import Theme

_QUEUE_TEXTUAL = "_textual"


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

    def __init__(self, text: str) -> None:
        self.text = text
        super().__init__()


class TextualLogHandler(AsyncLoggingHandler):
    """Async logging handler whose sink is a Textual app.

    Registers a single backend through the public ``register_backend`` API: a
    queue, a worker coroutine that formats each record and posts it to the app,
    and a drain hook that flushes the queue at shutdown.
    """

    # Always non-None: __init__ installs a ScietexFormatter, so the worker can
    # format records without a None guard.
    formatter: logging.Formatter

    def __init__(self, app: App, *, theme: LoggingTheme = SCIETEX_DARK, **kwargs) -> None:
        super().__init__(**kwargs)
        self._app = app
        self.formatter = ScietexFormatter(theme=theme)
        self._queue: asyncio.Queue[logging.LogRecord] = asyncio.Queue(maxsize=self.config.queue_maxsize)
        self.register_backend(_QUEUE_TEXTUAL, self._queue, self._worker, self._drain)

    async def _worker(self) -> None:
        """Drain the queue, posting each formatted record to the app.

        Formatting happens on the event-loop thread, so the shared record is
        never mutated off-loop. ``post_message`` is thread-safe, so the app
        receives the line regardless of which thread emitted the record.
        """
        while self.logging_running_event.is_set() or not self._queue.empty():
            try:
                record = await asyncio.wait_for(self._queue.get(), 1)
            except asyncio.TimeoutError:
                continue
            try:
                self._app.post_message(LogLine(self.formatter.format(record)))
            except Exception as exc:
                # A buggy formatter must not kill the worker; report and keep draining.
                self._report_error(record, exc)
            finally:
                self._queue.task_done()

    async def _drain(self, timeout: float) -> BackendDrainResult:
        """Wait for the queue to empty, reporting how the drain concluded."""
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout)
        except asyncio.TimeoutError:
            return BackendDrainResult(_QUEUE_TEXTUAL, DrainStatus.TIMEOUT)
        except Exception as exc:
            return BackendDrainResult(_QUEUE_TEXTUAL, DrainStatus.ERROR, exc)
        return BackendDrainResult(_QUEUE_TEXTUAL, DrainStatus.COMPLETED)

    def set_theme(self, theme: LoggingTheme) -> None:
        """Swap the formatter's theme.

        The worker reads ``self.formatter`` at work time, so replacing it takes
        effect on the next record without restarting the worker. ``color`` is
        left to the theme, so a monochrome theme emits no ANSI.
        """
        self.formatter = ScietexFormatter(theme=theme)
