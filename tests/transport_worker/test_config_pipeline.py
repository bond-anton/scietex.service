"""TransportWorker startup config-pipeline tests (AR-102a)."""

import logging

import pytest

from scietex.service.config import TaskProcessorConfig
from scietex.service.config_reload import (
    ConfigApplyOutcome,
    ConfigSections,
    ReloadableSettings,
    write_local_config,
)
from scietex.service.transport_worker import TransportWorker

from ._helpers import build_worker


def _sections(**overrides) -> ConfigSections:
    """Build a valid ConfigSections snapshot with in-range defaults."""
    core = {
        "max_concurrent_tasks": 1,
        "task_manager_sleep_time": 0.01,
        "task_queue_manager_sleep_time": 0.01,
        "task_handler_start_timeout": 1.0,
        "task_handler_stop_timeout": 1.0,
        "task_timeout": 3.0,
        "task_queue_fetch_timeout": 1.0,
        "task_cancellation_timeout": 1.0,
    }
    core.update(overrides)
    return ConfigSections(core=ReloadableSettings(**core))


@pytest.mark.asyncio
async def test_apply_local_config_applies_revision_one(tmp_path):
    """_apply_local_config applies a persisted config.yml as a revision-1 envelope."""
    write_local_config(tmp_path / "config.yml", _sections())
    worker = build_worker(tmp_path, config=TaskProcessorConfig(conf_dir=tmp_path, remote_config_enabled=True))

    await worker._apply_local_config()

    assert worker._config_manager.revision == 1


@pytest.mark.asyncio
async def test_apply_local_config_skipped_when_disabled(tmp_path):
    """_apply_local_config skips the file when remote config is disabled."""
    write_local_config(tmp_path / "config.yml", _sections())
    worker = build_worker(tmp_path)  # remote_config_enabled defaults to False

    await worker._apply_local_config()

    assert worker._config_manager.revision == 0


@pytest.mark.asyncio
async def test_reload_remote_config_delegates_and_logs(tmp_path, caplog):
    """_reload_remote_config delegates to _read_remote_outcome and logs the outcome."""
    worker = build_worker(tmp_path)
    worker.remote_outcome = ConfigApplyOutcome(applied=True, revision=5, hash="abc123")

    with caplog.at_level(logging.INFO):
        await worker._reload_remote_config()

    assert worker.read_calls == 1
    assert any("Applied remote config revision 5" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_abstract_read_remote_outcome_raises(tmp_path):
    """The abstract _read_remote_outcome raises NotImplementedError on the base class."""

    async def factory(_):
        return object()

    worker = TransportWorker(TaskProcessorConfig(conf_dir=tmp_path), client_factory=factory)

    with pytest.raises(NotImplementedError):
        await worker._read_remote_outcome()
