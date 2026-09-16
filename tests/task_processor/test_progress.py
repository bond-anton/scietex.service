"""TaskProcessor report_progress clamping and context-variable tests."""

import asyncio
import logging
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData

from ._helpers import DummyHandler, ProgressRecordingProcessor


@pytest.mark.asyncio
async def test_report_progress_inside_task_reaches_write_with_clamped_value():
    """report_progress inside a task must reach _write_task_progress with the
    value clamped to [0, 100]: 150.0 -> 100.0, -5.0 -> 0.0."""
    proc = ProgressRecordingProcessor()
    proc.add_task_handler(DummyHandler)
    await proc._start_task_handler("DummyHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="dummy", payload=b"{}"))
        for _ in range(100):
            if len(proc.progress_values) == 2:
                break
            await asyncio.sleep(0.01)
        assert proc.progress_values == [100.0, 0.0]
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_report_progress_outside_task_warns_and_noops(caplog):
    """report_progress outside a task context logs a warning and returns
    without raising or reaching _write_task_progress."""
    proc = ProgressRecordingProcessor()
    with caplog.at_level(logging.WARNING):
        await proc.report_progress(50.0)
    assert proc.progress_values == []
    assert "outside a task context" in caplog.text


@pytest.mark.asyncio
async def test_report_progress_context_var_reset_after_handle_task(caplog):
    """After handle_task completes, the ContextVar is reset: a further
    report_progress outside the task still warns and no-ops."""
    proc = ProgressRecordingProcessor()
    proc.add_task_handler(DummyHandler)
    await proc._start_task_handler("DummyHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="dummy", payload=b"{}"))
        for _ in range(100):
            if len(proc.progress_values) == 2:
                break
            await asyncio.sleep(0.01)
        assert proc.progress_values == [100.0, 0.0]

        with caplog.at_level(logging.WARNING):
            await proc.report_progress(42.0)
        assert proc.progress_values == [100.0, 0.0]
        assert "outside a task context" in caplog.text
    finally:
        await proc.exit()
        await proc.events["exit"].wait()
