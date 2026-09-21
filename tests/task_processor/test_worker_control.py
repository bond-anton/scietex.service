"""Integration tests for the built-in ``worker:*`` control handlers.

These drive the commands through a real ``TaskProcessor`` control lane, proving
the handler is registered in the control registry and reachable end to end —
not merely that the handler class works in isolation.
"""

import asyncio
from uuid import uuid4

import msgspec
import pytest

from scietex.service.basic_worker import ServiceStatus
from scietex.service.task_handler.schemas import (
    WORKER_START_TASK_NAME,
    WORKER_STOP_TASK_NAME,
    TaskData,
)
from scietex.service.task_handler.worker import WorkerControlRequest, WorkerControlResponse

from ._helpers import CancelRecordingProcessor


@pytest.mark.asyncio
async def test_worker_control_handler_registered_in_control_registry():
    """The worker:* handler is registered unconditionally and starts into the
    control registry, not the data registry."""
    proc = CancelRecordingProcessor()
    await proc.start()
    try:
        for _ in range(200):
            if proc.state == ServiceStatus.RUNNING:
                break
            await asyncio.sleep(0.01)
        assert "WorkerControlHandler" in proc.control_task_handlers
        assert "WorkerControlHandler" not in proc.task_handlers
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_worker_start_command_runs_on_control_lane():
    """A worker:start command travels the control lane and returns an accepted
    response. start is idempotent, so this is safe to run against a live worker."""
    proc = CancelRecordingProcessor()
    await proc.start()
    try:
        command_id = uuid4()
        proc.enqueue_control_task(
            TaskData(
                task_id=str(command_id),
                task=WORKER_START_TASK_NAME,
                payload=msgspec.msgpack.encode(WorkerControlRequest(reason="test")),
            )
        )
        for _ in range(200):
            if any(tid == command_id for tid, *_ in proc.completed):
                break
            await asyncio.sleep(0.01)

        calls = [c for c in proc.completed if c[0] == command_id]
        assert len(calls) == 1
        assert calls[0][2].status == "success"
        response = msgspec.msgpack.decode(calls[0][2].payload, type=WorkerControlResponse)
        assert response.action == WORKER_START_TASK_NAME
        assert response.accepted is True
    finally:
        await proc.exit()
        await proc.events["exit"].wait()


@pytest.mark.asyncio
async def test_worker_stop_command_acks_before_shutdown():
    """A worker:stop command is acknowledged before the worker begins shutting
    down: the handler schedules the stop and returns, so the result is produced
    while the worker is still running. The scheduled stop then reaches STOPPED.

    A plain stop does not set the ``exit`` event (only ``exit()`` does), so the
    observable completion signal is the worker state, not the event."""
    proc = CancelRecordingProcessor()
    await proc.start()
    command_id = uuid4()
    proc.enqueue_control_task(
        TaskData(
            task_id=str(command_id),
            task=WORKER_STOP_TASK_NAME,
            payload=msgspec.msgpack.encode(WorkerControlRequest()),
        )
    )
    for _ in range(200):
        if any(tid == command_id for tid, *_ in proc.completed):
            break
        await asyncio.sleep(0.01)

    calls = [c for c in proc.completed if c[0] == command_id]
    assert len(calls) == 1
    assert calls[0][2].status == "success"
    response = msgspec.msgpack.decode(calls[0][2].payload, type=WorkerControlResponse)
    assert response.action == WORKER_STOP_TASK_NAME

    # The scheduled stop then completes on its own.
    for _ in range(500):
        if proc.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.01)
    assert proc.state == ServiceStatus.STOPPED
