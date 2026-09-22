"""Light, dark, and monochrome Scietex themes backed by ``scietex.logging``."""

import logging
import sys

from scietex.logging import (
    MONOCHROME,
    SCIETEX_DARK,
    SCIETEX_LIGHT,
    LoggingTheme,
    Palette,
    ScietexFormatter,
    resolve_color,
)
from scietex.logging.theme import BOLD, RESET, ansi_fg

from ..version import __version__
from .base import Theme

# The ASCII logo art, pasted as one rectangle. A drop-in asset: replace the
# block between the quotes with a designer's art directly (no hand-splitting);
# ragged rows, trailing spaces, and surrounding blank lines all normalize at
# render time. ``None`` (or ``""``) means "no logo column at all".
_LOGO: str | None = """\
          ########+
          #########+
          ##########-
          ###########-
           .##########-
              .+#######-
     +#+..        .#####-
   -##########.      .+##-
 -#################+-
 ####################
  .############-.    .-##-
    .####+.       .#####-
               -#######-
           .##########-
          ###########-
          ##########+
          ##########
          #########"""

# The identity column, one ``(line_number, slot, template)`` entry per label.
# ``line_number`` is 1-based (same coordinate space as the logo rows); ``slot``
# is the ``Palette`` attribute name each row is painted with (``None`` =
# unpainted); ``template`` is a ``str.format`` template taking ``service_name``,
# ``version``, and ``scietex_version``. A drop-in asset: add, remove, or move
# entries without touching the art.
_LABELS: tuple[tuple[int, str | None, str], ...] = (
    (3, "logger_name", "{service_name}"),
    (4, "foreground", "v{version}"),
    (10, "debug", "Powered by scietex.service v{scietex_version}"),
    (12, "debug", '(c) ООО "Научные технологии и сервис"'),
    (13, "debug", "https://scietex.ru"),
)


def _paint(text: str, hex_color: str | None, *, bold: bool = False) -> str:
    """Wrap ``text`` in ANSI styling, or return it plain when uncolored.

    Mirrors ``ScietexFormatter._paint``: ``BOLD`` precedes the truecolor
    foreground sequence, and the span is closed with ``RESET``.
    """
    if hex_color is None:
        return text
    prefix = BOLD + ansi_fg(hex_color) if bold else ansi_fg(hex_color)
    return prefix + text + RESET


def _render_banner(
    logo: str | None,
    labels: tuple[tuple[int, str | None, str], ...],
    *,
    palette: Palette,
    color: bool,
    service_name: str,
    version: str,
) -> str:
    """Compose the startup banner from a paste-ready logo block and a label column.

    Pure: no I/O and no theme state. ``logo`` is a rectangle of ASCII art
    (``None`` or ``""`` means no logo column): it is ``splitlines()``'d,
    right-stripped, freed of leading/trailing blank rows, then right-padded to
    its widest row only where a label sits. ``labels`` is a 1-based
    ``(line_number, slot, template)`` column; a ``line_number < 1`` or a
    duplicated line number raises ``ValueError``. The banner is
    ``max(logo rows, max line number)`` tall, so either side may run longer and
    the shorter contributes blanks. Rows that carry no label are emitted as bare
    right-stripped art (no trailing whitespace); label rows join the padded logo
    cell and label across a two-space gutter, which disappears entirely when
    there is no logo. When ``color`` is true, logo cells are painted with
    ``logger_name`` (falling back to ``foreground``) in bold and label rows with
    their slot's palette color.
    """
    label_by_line: dict[int, tuple[str | None, str]] = {}
    max_label_line = 0
    for line_number, slot, template in labels:
        if line_number < 1:
            raise ValueError(f"label line numbers are 1-based: got {line_number}")
        if line_number in label_by_line:
            raise ValueError(f"duplicate label line number: {line_number}")
        label_by_line[line_number] = (slot, template)
        max_label_line = max(max_label_line, line_number)

    if logo:
        logo_rows = [row.rstrip() for row in logo.splitlines()]
        while logo_rows and not logo_rows[0]:
            logo_rows.pop(0)
        while logo_rows and not logo_rows[-1]:
            logo_rows.pop()
    else:
        logo_rows = []
    logo_width = max(len(row) for row in logo_rows) if logo_rows else 0

    total_rows = max(len(logo_rows), max_label_line)
    rows: list[str] = []
    for line_number in range(1, total_rows + 1):
        logo_cell = logo_rows[line_number - 1] if line_number - 1 < len(logo_rows) else ""
        entry = label_by_line.get(line_number)
        if entry is None:
            if color and logo_cell:
                logo_cell = _paint(logo_cell, palette.logger_name or palette.foreground, bold=True)
            rows.append(logo_cell)
            continue
        slot, template = entry
        label = template.format(service_name=service_name, version=version, scietex_version=__version__)
        if color:
            label_color = getattr(palette, slot) if slot is not None else None
            if label_color is not None:
                label = _paint(label, label_color)
        if logo_rows:
            logo_cell = logo_cell.ljust(logo_width)
            if color:
                logo_cell = _paint(logo_cell, palette.logger_name or palette.foreground, bold=True)
            rows.append(logo_cell + "  " + label)
        else:
            rows.append(label)
    return "\n\n" + "\n".join(rows) + "\n \n"


class _LoggingBackedTheme:
    """Shared base for themes that delegate rendering to a ``LoggingTheme``.

    The banner is fixed (the Scietex logo); the color palette and console
    formatter come from the wrapped ``scietex.logging`` theme.
    """

    def __init__(self, logging_theme: LoggingTheme, *, show_banner: bool = True) -> None:
        self._logging_theme = logging_theme
        self._show_banner = show_banner

    @property
    def show_banner(self) -> bool:
        """Whether the startup banner should be printed."""
        return self._show_banner

    def banner(self, service_name: str, version: str, *, color: bool | None = None) -> str:
        """Return the startup banner text with service details substituted.

        When color is on, the ASCII logo art is painted with the palette's
        ``logger_name`` color (falling back to ``foreground``), and the label
        column row-by-row with the slot color each row declares — mirroring how
        ``ScietexFormatter`` paints a log record.
        """
        # The monochrome theme must stay plain even under an explicit
        # ``color=True``: its ``LoggingTheme.color`` is ``False`` (the source of
        # truth), yet its palette still carries brand colors (``foreground`` is
        # set), so resolving from the caller's flag alone would paint a
        # supposedly colorless theme. Effective color is the theme policy ANDed
        # with the flag.
        color_on = self._logging_theme.color if color is None else (self._logging_theme.color and color)
        return _render_banner(
            _LOGO,
            _LABELS,
            palette=self._logging_theme.palette,
            color=color_on,
            service_name=service_name,
            version=version,
        )

    @property
    def palette(self) -> Palette:
        """Return the color palette backing this theme."""
        return self._logging_theme.palette

    def console_formatter(self) -> logging.Formatter:
        """Return a fresh ``ScietexFormatter`` on every call."""
        return ScietexFormatter(
            theme=self._logging_theme,
            color=resolve_color(sys.stdout),
        )


class ScietexMonochrome(_LoggingBackedTheme):
    """Colorless Scietex theme on the brand-black background."""

    def __init__(self, *, show_banner: bool = True) -> None:
        super().__init__(MONOCHROME, show_banner=show_banner)


class ScietexLight(_LoggingBackedTheme):
    """Light Scietex brand theme on a white background."""

    def __init__(self, *, show_banner: bool = True) -> None:
        super().__init__(SCIETEX_LIGHT, show_banner=show_banner)


class ScietexDark(_LoggingBackedTheme):
    """Dark Scietex brand theme on the brand-black background."""

    def __init__(self, *, show_banner: bool = True) -> None:
        super().__init__(SCIETEX_DARK, show_banner=show_banner)


def print_banner(
    service_name: str,
    version: str,
    theme: Theme | None = None,
    *,
    color: bool | None = None,
) -> None:
    """Print the startup banner for ``service_name`` using ``theme`` (default monochrome)."""
    if theme is None:
        theme = ScietexMonochrome()
    # Suppress only on an explicit ``False``: themes predating this flag have no
    # attribute at all, and a ``Theme`` subclass that inherits the Protocol
    # property inherits a ``...`` body (``None``) — neither is an opt-out.
    if getattr(theme, "show_banner", True) is False:
        return
    if color is None:
        color = resolve_color(sys.stdout)
    print(theme.banner(service_name, version, color=color))
