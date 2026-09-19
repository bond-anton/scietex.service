"""ConfigManager apply-pipeline tests (AR-105)."""

import pytest

from scietex.service.config_reload import (
    CONFIG_SOURCE_NOT_CONFIGURED,
    REMOTE_CONFIG_DISABLED,
)

from ._helpers import FakeConfigSource, build_manager, make_envelope


@pytest.mark.asyncio
async def test_apply_inline_payload_sets_source_inline(tmp_path):
    """An inline envelope applies with the ``inline`` source label."""
    manager = build_manager(tmp_path, enabled=True)

    outcome = await manager.apply_config(make_envelope(revision=1), False)

    assert outcome.applied is True
    assert manager.source == "inline"


@pytest.mark.asyncio
async def test_apply_none_payload_reloads_attached_source(tmp_path):
    """``payload=None`` with an attached source reloads and records the applied
    revision and ``remote`` source label."""
    manager = build_manager(tmp_path, enabled=True)
    manager.attach_source(FakeConfigSource(payload=make_envelope(revision=5)))

    outcome = await manager.apply_config(None, False)

    assert outcome.applied is True
    assert outcome.revision == 5
    assert manager.revision == 5
    assert manager.source == "remote"


@pytest.mark.asyncio
async def test_apply_none_payload_without_source_is_not_configured(tmp_path):
    """``payload=None`` with no attached source of truth is
    CONFIG_SOURCE_NOT_CONFIGURED."""
    manager = build_manager(tmp_path, enabled=True)

    outcome = await manager.apply_config(None, False)

    assert outcome.applied is False
    assert outcome.error_code == CONFIG_SOURCE_NOT_CONFIGURED


@pytest.mark.asyncio
async def test_apply_persist_writes_local_file(tmp_path):
    """``persist=True`` writes the local snapshot after a successful apply."""
    manager = build_manager(tmp_path, enabled=True)

    outcome = await manager.apply_config(make_envelope(revision=1), True)

    assert outcome.applied is True
    assert (tmp_path / "config.yml").exists()


@pytest.mark.asyncio
async def test_apply_disabled_short_circuits(tmp_path):
    """With the master switch off, an inline apply is rejected with
    REMOTE_CONFIG_DISABLED."""
    manager = build_manager(tmp_path, enabled=False)

    outcome = await manager.apply_config(make_envelope(revision=1), False)

    assert outcome.applied is False
    assert outcome.error_code == REMOTE_CONFIG_DISABLED
