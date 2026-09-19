"""ConfigManager store tests (AR-105)."""

import pytest

from scietex.service.config_reload import (
    CONFIG_SOURCE_UNAVAILABLE,
    REMOTE_CONFIG_DISABLED,
)

from ._helpers import FakeConfigSource, build_manager


@pytest.mark.asyncio
async def test_store_disk_writes_local_file(tmp_path):
    """``config:store`` to disk writes ``<conf_dir>/config.yml``."""
    manager = build_manager(tmp_path, enabled=True)

    outcome = await manager.store_config("disk")

    assert outcome.stored is True
    assert (tmp_path / "config.yml").exists()


@pytest.mark.asyncio
async def test_store_remote_publishes_to_source(tmp_path):
    """``config:store`` to remote publishes to the attached source."""
    manager = build_manager(tmp_path, enabled=True)
    source = FakeConfigSource()
    manager.attach_source(source)

    outcome = await manager.store_config("remote")

    assert outcome.stored is True
    assert len(source.stored) == 1


@pytest.mark.asyncio
async def test_store_both_writes_disk_and_source(tmp_path):
    """``config:store`` to both writes the file and publishes to the source."""
    manager = build_manager(tmp_path, enabled=True)
    source = FakeConfigSource()
    manager.attach_source(source)

    outcome = await manager.store_config("both")

    assert outcome.stored is True
    assert (tmp_path / "config.yml").exists()
    assert len(source.stored) == 1


@pytest.mark.asyncio
async def test_store_remote_without_source_is_unavailable(tmp_path):
    """A remote target without an attached source is CONFIG_SOURCE_UNAVAILABLE."""
    manager = build_manager(tmp_path, enabled=True)

    outcome = await manager.store_config("remote")

    assert outcome.stored is False
    assert outcome.error_code == CONFIG_SOURCE_UNAVAILABLE


@pytest.mark.asyncio
async def test_store_disabled_returns_disabled(tmp_path):
    """With the master switch off, a remote store is rejected with
    REMOTE_CONFIG_DISABLED rather than publishing to the source."""
    manager = build_manager(tmp_path, enabled=False)
    source = FakeConfigSource()
    manager.attach_source(source)

    outcome = await manager.store_config("remote")

    assert outcome.stored is False
    assert outcome.error_code == REMOTE_CONFIG_DISABLED
    assert source.stored == []
