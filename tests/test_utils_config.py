"""Tests for config-directory resolution in ``scietex.service.utils.config`` (AR-090)."""

from scietex.service.utils import config as config_module


def _isolate_search_paths(monkeypatch, tmp_path):
    """Redirect every environment-derived search candidate to a non-existent
    path under ``tmp_path`` so the tests are deterministic regardless of the
    host's ``~/.config/scietex``, ``/etc/scietex``, or ``/usr/local/etc/scietex``.

    ``_DEFAULT_XDG_DIR`` is monkeypatched rather than ``HOME`` because it is
    computed once at import time from ``Path.home()``; changing ``HOME`` at
    test time would not move it.
    """
    monkeypatch.delenv("SCIETEX_CONFIG_DIR", raising=False)
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr(config_module, "_DEFAULT_XDG_DIR", tmp_path / "default_xdg")
    monkeypatch.setattr(config_module, "_ETC_DIR", tmp_path / "etc_scietex")
    monkeypatch.setattr(config_module, "_LOCAL_ETC_DIR", tmp_path / "local_etc_scietex")


def test_prepare_conf_dir_resolves_cwd_config_after_import(monkeypatch, tmp_path):
    """The ``./config`` candidate is computed at call time: changing cwd after
    import still resolves the new cwd's ``config`` directory (AR-090)."""
    _isolate_search_paths(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()

    assert config_module.prepare_conf_dir(None) == tmp_path / "config"


def test_prepare_conf_dir_explicit_argument_wins(monkeypatch, tmp_path):
    """An existing ``conf_dir`` argument wins over the environment variable."""
    explicit = tmp_path / "explicit"
    explicit.mkdir()
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    monkeypatch.setenv("SCIETEX_CONFIG_DIR", str(env_dir))

    assert config_module.prepare_conf_dir(explicit) == explicit


def test_prepare_conf_dir_env_var_wins_over_search_paths(monkeypatch, tmp_path):
    """``SCIETEX_CONFIG_DIR`` wins over the built-in search paths when valid."""
    _isolate_search_paths(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    monkeypatch.setenv("SCIETEX_CONFIG_DIR", str(env_dir))

    assert config_module.prepare_conf_dir(None) == env_dir


def test_prepare_conf_dir_falls_back_to_default(monkeypatch, tmp_path):
    """With no existing candidate, the default is created and returned."""
    _isolate_search_paths(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)

    result = config_module.prepare_conf_dir(None)
    assert result == config_module._DEFAULT_XDG_DIR
    assert result.is_dir()


def test_prepare_conf_dir_nonexistent_conf_dir_falls_through(monkeypatch, tmp_path):
    """A non-existent ``conf_dir`` is not returned; resolution falls through to
    the search order and creates the default."""
    _isolate_search_paths(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    nonexistent = tmp_path / "nope"

    result = config_module.prepare_conf_dir(nonexistent)
    assert result != nonexistent
    assert result == config_module._DEFAULT_XDG_DIR
    assert result.is_dir()
