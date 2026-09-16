"""TaskProcessor handler start/stop lifecycle tests."""

import pytest

from scietex.service.config import TaskProcessorConfig

from ._helpers import (
    DemoProcessor,
    FailingStartHandler,
    RaisingStartHandler,
    StuckStopHandler,
)


@pytest.mark.asyncio
async def test_stop_task_handler_removes_handler_on_stop_timeout():
    """A handler whose stop() times out must still be removed from the active
    handlers dict, so it is not left in an ambiguous tracked-but-stuck state
    (AR-052)."""
    # task_handler_stop_timeout minimum is 1 (config.py), the smallest valid value.
    proc = DemoProcessor(TaskProcessorConfig(task_handler_stop_timeout=1))
    proc.add_task_handler(StuckStopHandler)
    await proc._start_task_handler("StuckStopHandler")
    assert "StuckStopHandler" in proc.task_handlers

    await proc._stop_task_handler("StuckStopHandler")
    assert "StuckStopHandler" not in proc.task_handlers


@pytest.mark.asyncio
async def test_initialize_returns_false_when_handler_start_fails():
    """A handler that fails to start must make initialize() return False (AR-010)."""
    proc = DemoProcessor()
    proc.add_task_handler(FailingStartHandler)
    ok = await proc.initialize()
    assert ok is False


@pytest.mark.asyncio
async def test_start_task_handler_removes_handler_on_start_failure():
    """A handler whose start() raises must be removed from the active handlers
    dict so dispatch never iterates a dead handler (AR-029)."""
    proc = DemoProcessor()
    proc.add_task_handler(RaisingStartHandler)
    ok = await proc._start_task_handler("RaisingStartHandler")
    assert ok is False
    assert "RaisingStartHandler" not in proc.task_handlers
