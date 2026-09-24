"""Tests for the example's command-line argument parsing.

Importing ``examples.textual.__main__`` pulls in ``examples.textual.app``, which
needs Textual, so the module skips when the ``textual`` extra is absent.
"""

import pytest

pytest.importorskip("textual")

from examples.textual.__main__ import parse_args


def test_memory_flag_defaults_to_false():
    assert parse_args([]).memory is False


def test_short_memory_flag_is_set():
    assert parse_args(["-m"]).memory is True


def test_long_memory_flag_is_set():
    assert parse_args(["--memory"]).memory is True
