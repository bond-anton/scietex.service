"""ConfigManager local-file tests (AR-105)."""

import pytest

from scietex.service.config_reload import ConfigSections, read_local_config, write_local_config

from ._helpers import build_manager, make_settings


@pytest.mark.asyncio
async def test_apply_local_file_applies_persisted_snapshot(tmp_path):
    """A persisted ``config.yml`` applies as a revision-1 ``file`` source."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=make_settings(task_timeout=9.0)))
    manager = build_manager(tmp_path, enabled=True)

    outcome = await manager.apply_local_file()

    assert outcome is not None
    assert outcome.applied is True
    assert manager.source == "file"
    assert manager.revision == 1


@pytest.mark.asyncio
async def test_apply_local_file_absent_file_returns_none(tmp_path):
    """No local file yields ``None`` (the caller skips logging)."""
    manager = build_manager(tmp_path, enabled=True)

    assert await manager.apply_local_file() is None


@pytest.mark.asyncio
async def test_apply_local_file_disabled_returns_none(tmp_path):
    """A disabled feature skips the file without logging an error."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=make_settings()))
    manager = build_manager(tmp_path, enabled=False)

    assert await manager.apply_local_file() is None


@pytest.mark.asyncio
async def test_apply_local_file_invalid_file_returns_none(tmp_path):
    """An undecodable local file yields ``None`` rather than raising."""
    (tmp_path / "config.yml").write_bytes(b"not: [valid: yaml\n  base_config: broken")
    manager = build_manager(tmp_path, enabled=True)

    assert await manager.apply_local_file() is None


@pytest.mark.asyncio
async def test_apply_local_file_error_returns_none(tmp_path, monkeypatch):
    """A raising apply is caught and returns ``None`` (never propagates)."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=make_settings()))
    manager = build_manager(tmp_path, enabled=True)

    async def boom(payload, *, source):
        raise RuntimeError("boom")

    monkeypatch.setattr(manager._reloader, "apply_envelope", boom)

    assert await manager.apply_local_file() is None


def test_write_local_round_trips_atomically(tmp_path):
    """``write_local`` writes a decodable snapshot that round-trips back."""
    manager = build_manager(tmp_path, enabled=True)

    outcome = manager.write_local()

    assert outcome.stored is True
    assert outcome.target == "disk"
    assert outcome.path == str(tmp_path / "config.yml")
    assert read_local_config(tmp_path / "config.yml") == ConfigSections(core=make_settings())
    assert [entry.name for entry in tmp_path.iterdir()] == ["config.yml"]
