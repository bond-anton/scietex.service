"""Theme contract for ``scietex.service``.

Defines the extension seam a theme must implement: a startup banner, a color
palette, and a console-log formatter. Concrete light, dark, and monochrome
themes backed by ``scietex.logging`` ship in :mod:`scietex.service.theme.scietex`.
"""

import logging
from typing import Protocol

from scietex.logging import Palette


class Theme(Protocol):
    """The rendering surface a theme provides to a worker.

    A theme supplies the startup banner text, the color palette, and the
    ``logging.Formatter`` the console log handler renders records with.
    """

    def banner(self, service_name: str, version: str) -> str:
        """Return the startup banner text (pure, no I/O)."""
        ...

    @property
    def show_banner(self) -> bool:
        """Whether ``print_banner`` should render this theme's startup banner. Defaults to True."""
        ...

    @property
    def palette(self) -> Palette:
        """Return the theme's color palette."""
        ...

    def console_formatter(self) -> logging.Formatter:
        """Return the formatter the console log handler renders records with."""
        ...
