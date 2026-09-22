"""Tests for the theme seam: banner rendering, formatter, and injection."""

import logging

from scietex.logging import SCIETEX_DARK, Palette, ScietexFormatter

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
from scietex.service.version import __version__

_BANNER_SENTINEL = "STUB_THEME_BANNER"


class _StubTheme(Theme):
    """Minimal theme proving injection over the Scietex default.

    No explicit ``show_banner``: inheriting the Protocol property (body ``...``,
    i.e. ``None``) must not count as an opt-out.
    """

    def __init__(self, formatter: logging.Formatter | None = None) -> None:
        self.formatter = formatter if formatter is not None else logging.Formatter()

    def banner(self, service_name: str, version: str) -> str:
        return _BANNER_SENTINEL

    def console_formatter(self) -> logging.Formatter:
        return self.formatter


class _BannerOnlyTheme:
    """Minimal theme with no ``show_banner`` attribute (defensive default prints)."""

    def banner(self, service_name: str, version: str) -> str:
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
