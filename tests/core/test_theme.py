"""Tests for the theme seam: banner rendering, formatter, and injection."""

import logging

import pytest
from scietex.logging import SCIETEX_DARK, Palette, ScietexFormatter
from scietex.logging.theme import ansi_fg

from scietex.service import (
    BasicWorker,
    ScietexDark,
    ScietexLight,
    ScietexMonochrome,
    TaskProcessor,
    Theme,
    print_banner,
)
from scietex.service.config import TaskProcessorConfig, WorkerConfig
from scietex.service.theme.scietex import _render_banner
from scietex.service.version import __version__

_BANNER_SENTINEL = "STUB_THEME_BANNER"

# The byte-for-byte plain startup banner for the fixed probe values "svc"/"1.2.3".
# Regenerated whenever the layout changes; guards the colorless render against drift.
_PLAIN_BANNER = """

          ########+
          #########+
          ##########-       svc
          ###########-      v1.2.3
           .##########-
              .+#######-
     +#+..        .#####-
   -##########.      .+##-
 -#################+-
 ####################       Powered by scietex.service v5.0.0
  .############-.    .-##-
    .####+.       .#####-   (c) ООО "Научные технологии и сервис"
               -#######-    https://scietex.ru
           .##########-
          ###########-
          ##########+
          ##########
          #########
 
"""


class _StubTheme(Theme):
    """Minimal theme proving injection over the Scietex default.

    No explicit ``show_banner``: inheriting the Protocol property (body ``...``,
    i.e. ``None``) must not count as an opt-out.
    """

    def __init__(self, formatter: logging.Formatter | None = None) -> None:
        self.formatter = formatter if formatter is not None else logging.Formatter()

    def banner(self, service_name: str, version: str, *, color: bool | None = None) -> str:
        return _BANNER_SENTINEL

    def console_formatter(self) -> logging.Formatter:
        return self.formatter


class _BannerOnlyTheme:
    """Minimal theme with no ``show_banner`` attribute (defensive default prints)."""

    def banner(self, service_name: str, version: str, *, color: bool | None = None) -> str:
        return _BANNER_SENTINEL


def test_scietex_banner_contains_service_and_versions() -> None:
    banner = ScietexMonochrome().banner("svc", "1.2.3")
    assert "svc" in banner
    assert "1.2.3" in banner
    assert __version__ in banner


def test_scietex_console_formatter_type() -> None:
    assert isinstance(ScietexMonochrome().console_formatter(), ScietexFormatter)


def test_console_formatter_returns_fresh_instance() -> None:
    for theme in (ScietexMonochrome(), ScietexLight(), ScietexDark()):
        assert isinstance(theme.console_formatter(), ScietexFormatter)
        assert theme.console_formatter() is not theme.console_formatter()


def test_print_banner_uses_given_theme(capsys) -> None:
    print_banner("svc", "1.2.3", theme=_StubTheme())
    assert _BANNER_SENTINEL in capsys.readouterr().out


def test_print_banner_defaults_to_scietex(capsys) -> None:
    print_banner("svc", "1.2.3")
    assert "scietex.ru" in capsys.readouterr().out


def test_builtin_themes_show_banner_by_default() -> None:
    for theme in (ScietexMonochrome(), ScietexLight(), ScietexDark()):
        assert theme.show_banner is True


def test_dark_theme_opt_out_still_renders_banner_text() -> None:
    theme = ScietexDark(show_banner=False)
    assert theme.show_banner is False
    assert theme.banner("svc", "1.2.3") != ""


def test_print_banner_suppressed_when_theme_opts_out(capsys) -> None:
    print_banner("svc", "1.2.3", theme=ScietexDark(show_banner=False))
    assert capsys.readouterr().out == ""


def test_print_banner_prints_when_theme_opt_in(capsys) -> None:
    print_banner("svc", "1.2.3", theme=ScietexDark())
    assert capsys.readouterr().out != ""


def test_print_banner_prints_for_theme_without_show_banner(capsys) -> None:
    print_banner("svc", "1.2.3", theme=_BannerOnlyTheme())
    assert _BANNER_SENTINEL in capsys.readouterr().out


def test_print_banner_prints_for_theme_subclass_inheriting_show_banner(capsys) -> None:
    """A ``Theme`` subclass with no explicit flag inherits ``None``, not ``False``."""
    stub = _StubTheme()
    assert stub.show_banner is None
    print_banner("svc", "1.2.3", theme=stub)
    assert _BANNER_SENTINEL in capsys.readouterr().out


def test_basic_worker_default_theme_is_scietex() -> None:
    assert isinstance(BasicWorker(WorkerConfig()).theme, ScietexMonochrome)


def test_basic_worker_uses_injected_theme() -> None:
    stub = _StubTheme()
    worker = BasicWorker(WorkerConfig(), theme=stub)
    assert worker.theme is stub
    console = next(h for h in worker.logger.handlers if h.__class__.__name__ == "ConsoleHandler")
    assert console.formatter is stub.formatter


def test_derived_worker_forwards_theme() -> None:
    stub = _StubTheme()
    assert TaskProcessor(TaskProcessorConfig(), theme=stub).theme is stub


def test_light_palette_brand_colors() -> None:
    palette = ScietexLight().palette
    assert isinstance(palette, Palette)
    assert palette.logger_name == "#31313B"
    assert palette.foreground == "#1F202A"


def test_dark_palette_brand_colors() -> None:
    palette = ScietexDark().palette
    assert isinstance(palette, Palette)
    assert palette.logger_name == "#FFDB1C"
    assert palette.warning == "#FFDB1C"
    assert palette.background == "#1F202A"


def test_monochrome_palette_has_no_level_colors() -> None:
    palette = ScietexMonochrome().palette
    assert isinstance(palette, Palette)
    assert palette.debug is None
    assert palette.info is None
    assert palette.warning is None
    assert palette.error is None
    assert palette.critical is None
    assert palette.critical_bg is None
    assert palette.logger_name is None


def test_colored_formatter_emits_ansi_escapes() -> None:
    record = logging.LogRecord(
        name="scietex.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg="hello",
        args=None,
        exc_info=None,
    )
    formatter = ScietexFormatter(theme=SCIETEX_DARK, color=True)
    assert "\x1b[" in formatter.format(record)


def test_banner_color_false_is_plain_and_byte_identical() -> None:
    banner = ScietexDark().banner("svc", "1.2.3", color=False)
    assert "\x1b[" not in banner
    assert banner == _PLAIN_BANNER


def test_banner_color_true_dark_uses_palette() -> None:
    banner = ScietexDark().banner("svc", "1.2.3", color=True)
    palette = ScietexDark().palette
    assert "\x1b[38;2;" in banner
    assert ansi_fg(palette.logger_name) in banner
    assert ansi_fg(palette.foreground) in banner
    assert ansi_fg(palette.debug) in banner


def test_banner_color_true_light_uses_palette() -> None:
    banner = ScietexLight().banner("svc", "1.2.3", color=True)
    palette = ScietexLight().palette
    assert "\x1b[38;2;" in banner
    assert ansi_fg(palette.logger_name) in banner
    assert ansi_fg(palette.foreground) in banner
    assert ansi_fg(palette.debug) in banner


def test_banner_monochrome_color_true_stays_plain() -> None:
    banner = ScietexMonochrome().banner("svc", "1.2.3", color=True)
    assert "\x1b[" not in banner
    assert banner == _PLAIN_BANNER


def test_banner_default_color_follows_theme() -> None:
    assert "\x1b[38;2;" in ScietexDark().banner("svc", "1.2.3")
    assert ScietexMonochrome().banner("svc", "1.2.3") == _PLAIN_BANNER


def test_print_banner_non_tty_stream_emits_no_ansi(capsys) -> None:
    print_banner("svc", "1.2.3", theme=ScietexDark())
    assert "\x1b[" not in capsys.readouterr().out


def test_print_banner_color_true_emits_ansi(capsys) -> None:
    print_banner("svc", "1.2.3", theme=ScietexDark(), color=True)
    assert "\x1b[38;2;" in capsys.readouterr().out


def test_print_banner_show_banner_false_prints_nothing_even_colored(capsys) -> None:
    print_banner("svc", "1.2.3", theme=ScietexDark(show_banner=False), color=True)
    assert capsys.readouterr().out == ""


def test_render_banner_without_logo_starts_labels_at_column_zero() -> None:
    """No logo column (None or "") means no gutter: labels begin at column 0."""
    labels = ((1, "foreground", "Service: {service_name}"),)
    for logo in (None, ""):
        rendered = _render_banner(
            logo,
            labels,
            palette=ScietexDark().palette,
            color=False,
            service_name="svc",
            version="1.2.3",
        )
        assert rendered == "\n\nService: svc\n \n"


def test_render_banner_label_past_logo_height_grows_banner() -> None:
    """A label line number beyond the logo's height pads with a blank logo cell."""
    rendered = _render_banner(
        "#",
        ((3, "foreground", "X"),),
        palette=ScietexDark().palette,
        color=False,
        service_name="svc",
        version="1.2.3",
    )
    lines = rendered.split("\n")
    assert len(lines) == 7  # "", "", "#", "", "   X", " ", ""
    assert lines[2] == "#"
    assert lines[3] == ""  # blank row beyond the 1-row logo
    assert lines[4] == "   X"  # blank logo cell (1) + 2-space gutter + label


def test_render_banner_ragged_logo_no_trailing_whitespace() -> None:
    """Ragged art, trailing spaces, and blank rows normalize to a clean rectangle."""
    logo = "\n\n#  \n##\n###   \n\n"
    rendered = _render_banner(
        logo,
        ((2, "foreground", "label"),),
        palette=ScietexDark().palette,
        color=False,
        service_name="svc",
        version="1.2.3",
    )
    lines = rendered.split("\n")
    content = lines[2:-2]  # drop the framing ("", "", ... , " ", "")
    assert all(line == line.rstrip() for line in content)  # no trailing whitespace
    assert content == ["#", "##   label", "###"]  # widest row (3) + 2-space gutter


def test_render_banner_duplicate_line_number_raises() -> None:
    """Two labels on one line would silently shadow; fail fast instead."""
    with pytest.raises(ValueError):
        _render_banner(
            "#",
            ((1, "foreground", "A"), (1, "debug", "B")),
            palette=ScietexDark().palette,
            color=False,
            service_name="svc",
            version="1.2.3",
        )


def test_render_banner_line_number_zero_raises() -> None:
    """Line numbers are 1-based; 0 is a programming error."""
    with pytest.raises(ValueError):
        _render_banner(
            "#",
            ((0, "foreground", "A"),),
            palette=ScietexDark().palette,
            color=False,
            service_name="svc",
            version="1.2.3",
        )


def test_render_banner_is_replaceable() -> None:
    """A custom logo/labels pair composes, proving either block is swappable."""
    rendered = _render_banner(
        "#\n##",
        ((2, "foreground", "{service_name}@{version}"),),
        palette=ScietexDark().palette,
        color=False,
        service_name="svc",
        version="1.2.3",
    )
    lines = rendered.split("\n")
    assert len(lines) == 6  # "", "", "#", "##  svc@1.2.3", " ", ""
    assert lines[2] == "#"  # bare art row, no trailing padding
    assert lines[3] == "##  svc@1.2.3"
