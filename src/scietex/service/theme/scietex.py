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

from ..version import __version__
from .base import Theme

_BANNER = """

          ########+                                                            
          #########+                                                           
          ##########-         Service: {service_name}
          ###########-        Version: {version}
           .##########-                      
              .+#######-      
     +#+..        .#####-                                                      
   -##########.      .+##-                                                     
 -#################+-           
 ####################         Powered by scietex.service v{scietex_version}
  .############-.    .-##-      
    .####+.       .#####-     (c) ООО "Научные технологии и сервис"
               -#######-      https://scietex.ru
           .##########-                     
          ###########-                      
          ##########+                                                  
          ##########                                                           
          #########                                                            
 
"""


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

    def banner(self, service_name: str, version: str) -> str:
        """Return the startup banner text with service details substituted."""
        return _BANNER.format(
            service_name=service_name,
            version=version,
            scietex_version=__version__,
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
) -> None:
    """Print the startup banner for ``service_name`` using ``theme`` (default monochrome)."""
    if theme is None:
        theme = ScietexMonochrome()
    # Suppress only on an explicit ``False``: themes predating this flag have no
    # attribute at all, and a ``Theme`` subclass that inherits the Protocol
    # property inherits a ``...`` body (``None``) — neither is an opt-out.
    if getattr(theme, "show_banner", True) is False:
        return
    print(theme.banner(service_name, version))
