"""Configuration directory resolution for ``scietex.service``.

Provides ``prepare_conf_dir()`` which searches a predefined list of
paths (``~/.config/scietex``, ``/etc/scietex``, etc.) and creates the
first available directory, falling back to ``~/.config/scietex``.
"""

from pathlib import Path

CONF_DIR_PATHS = [
    Path.home() / ".config" / "scietex",
    Path("/etc") / "scietex",
    Path("/usr/local/etc") / "scietex",
    Path().cwd() / "config",
]


def prepare_conf_dir(conf_dir: str | Path | None) -> Path:
    if isinstance(conf_dir, (str, Path)):
        conf_dir_path = Path(conf_dir)
        if conf_dir_path.is_dir():
            return conf_dir_path
    if conf_dir is None:
        for conf_dir_path in CONF_DIR_PATHS:
            if conf_dir_path.is_dir():
                return conf_dir_path
    conf_dir = CONF_DIR_PATHS[0]
    conf_dir.mkdir(parents=True, exist_ok=True)
    return conf_dir
