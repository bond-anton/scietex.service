"""TaskProcessor progress reporting and TaskCapabilities clamping tests."""

import asyncio
from uuid import UUID, uuid4

import pytest

from scietex.service.task_handler.capabilities import TaskCapabilities
from scietex.service.task_handler.schemas import TaskData

from ._helpers import ProgressRecordingProcessor, ProgressReportingHandler


@pytest.mark.asyncio
async def test_report_progress_inside_task_reaches_write_with_clamped_value():
    """A handler reporting progress through its capabilities must reach
    _write_task_progress with the value clamped to [0, 100]: 150.0 -> 100.0,
    -5.0 -> 0.0."""
    proc = ProgressRecordingProcessor()
    proc.add_task_handler(ProgressReportingHandler)
    await proc._start_task_handler("ProgressReportingHandler")
    await proc.start()
    try:
        t_id = uuid4()
        proc.enqueue_task(t_id, TaskData(task="progress", payload=b"{}"))
        for _ in range(100):
            if len(proc.progress_values) == 2:
                break
            await asyncio.sleep(0.01)
        assert proc.progress_values == [100.0, 0.0]
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_task_capabilities_report_progress_clamps_and_forwards():
    """TaskCapabilities.report_progress clamps to [0, 100] and forwards the
    task_id: -5.0 -> 0.0, 150.0 -> 100.0, 42.5 -> 42.5."""
    task_id = uuid4()
    written: list[tuple[UUID, float]] = []

    async def write_progress(tid: UUID, value: float) -> None:
        written.append((tid, value))

    capabilities = TaskCapabilities(task_id=task_id, _write_progress=write_progress)
    await capabilities.report_progress(-5.0)
    await capabilities.report_progress(150.0)
    await capabilities.report_progress(42.5)

    assert written == [
        (task_id, 0.0),
        (task_id, 100.0),
        (task_id, 42.5),
    ]
