"""Configuration directory resolution for ``scietex.service``.

Implements a fallback-based configuration directory search that checks
paths in order: the user-supplied directory (if valid), an application-
specific environment variable, the XDG Base Directory path, then the
traditional fallback locations. If no existing directory is found,
``~/.config/scietex`` is created and returned.

Environment variables:
    ``SCIETEX_CONFIG_DIR`` — Application-specific override (checked after
        the ``conf_dir`` argument but before built-in paths).
    ``XDG_CONFIG_HOME`` — Standard XDG variable; if set,
        ``$XDG_CONFIG_HOME/scietex`` is inserted into the search path.
"""

import os
from pathlib import Path

_DEFAULT_XDG_DIR = Path.home() / ".config" / "scietex"
_ETC_DIR = Path("/etc") / "scietex"
_LOCAL_ETC_DIR = Path("/usr/local/etc") / "scietex"
_CWD_DIR = Path().cwd() / "config"


def _resolve_xdg_path() -> Path:
    """Return the XDG configuration path for scietex.service."""
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg) / "scietex"
    return _DEFAULT_XDG_DIR


def prepare_conf_dir(conf_dir: str | Path | None) -> Path:
    """Return the configuration directory path.

    Resolution order:
        1. ``conf_dir`` argument, if it is a valid existing directory.
        2. ``SCIETEX_CONFIG_DIR`` environment variable, if set and valid.
        3. ``$XDG_CONFIG_HOME/scietex`` (XDG Base Directory), if exists.
        4. ``~/.config/scietex``, if exists.
        5. ``/etc/scietex``, if exists.
        6. ``/usr/local/etc/scietex``, if exists.
        7. ``./config`` (current working directory), if exists.
        8. ``~/.config/scietex`` — created if needed.

    Args:
        conf_dir: User-supplied configuration directory path. May be a
            ``str``, :class:`pathlib.Path`, or ``None``.

    Returns:
        An existing directory path to use for configuration files.
    """
    # 1. Explicit argument
    if isinstance(conf_dir, (str, Path)):
        conf_dir_path = Path(conf_dir)
        if conf_dir_path.is_dir():
            return conf_dir_path

    # 2. Application-specific environment variable
    env_dir = os.environ.get("SCIETEX_CONFIG_DIR")
    if env_dir:
        env_path = Path(env_dir)
        if env_path.is_dir():
            return env_path

    # 3–7. Built-in search paths (XDG-aware)
    xdg_path = _resolve_xdg_path()
    for candidate in (xdg_path, _DEFAULT_XDG_DIR, _ETC_DIR, _LOCAL_ETC_DIR, _CWD_DIR):
        if candidate.is_dir():
            return candidate

    # 8. Create and return the default
    _DEFAULT_XDG_DIR.mkdir(parents=True, exist_ok=True)
    return _DEFAULT_XDG_DIR
