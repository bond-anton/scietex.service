"""Utility helpers for ``scietex.service``.

Provides configuration directory resolution via ``prepare_conf_dir``,
and the service logo printer via ``print_scietex_logo``.
"""

from .conf import prepare_conf_dir
from .logo import print_scietex_logo

__all__ = [
    "prepare_conf_dir",
    "print_scietex_logo",
]
