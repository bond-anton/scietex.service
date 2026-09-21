"""End-to-end Valkey integration tests against a live server.

These tests drive a real ``ValkeyWorker`` against a real Valkey/Redis server:
the worker connects, creates its consumer group, drains a task XADDed to the
real task stream, processes it, and acks it. They are gated by
``SCIETEX_TEST_VALKEY_URL`` (see ``conftest.py``).

The mocked unit tests in ``tests/valkey/`` assert the exact wire calls the code
makes; this tier proves the complementary half — that those calls work against
a real server.
"""

import asyncio
import time
from uuid import uuid4

import pytest

from scietex.service import ValkeyWorker
from scietex.service.basic_worker import ServiceStatus
from scietex.service.task_handler.basic import TaskHandler
from scietex.service.task_handler.schemas import TaskData, TaskResult
from scietex.service.task_handler.wire import encode_task_envelope
from scietex.service.valkey.config import ValkeyWorkerConfig
from scietex.service.valkey.transport import TASK_FIELD


async def _wait_until(predicate, *, timeout: float = 10.0) -> None:
    """Poll ``predicate`` until it is true, or fail after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


async def _wait_for_async(predicate, *, timeout: float = 10.0) -> None:
    """Poll an async ``predicate`` until it is true, or fail after ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


class _RecordingHandler(TaskHandler):
    """Records the payloads it processes and signals each completion."""

    def __init__(self, *args, done: asyncio.Event, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._done = done
        self.processed: list[bytes] = []

    @property
    def supported_tasks(self) -> list[str]:
        return ["echo"]

    async def handle(self, task_data: TaskData, *, capabilities) -> TaskResult:
        self.processed.append(task_data.payload)
        self._done.set()
        return TaskResult(status="success", payload=task_data.payload)


def _task_data(task: str = "echo", payload: bytes = b"{}") -> TaskData:
    """Build a task with a fresh id for the given task name."""
    return TaskData(task_id=str(uuid4()), task=task, payload=payload)


@pytest.mark.asyncio
async def test_worker_processes_task_from_real_stream(worker_config: ValkeyWorkerConfig, valkey_client):
    """A task XADDed to the real task stream is drained, processed, and acked."""

    done = asyncio.Event()
    worker = ValkeyWorker(worker_config)
    worker.add_task_handler(_RecordingHandler, done=done)

    await worker.start()
    try:
        await _wait_until(lambda: worker.state is ServiceStatus.RUNNING)

        task_data = _task_data(payload=b"hello-real-server")
        await valkey_client.xadd(
            worker_config.task_stream_name.format(service=worker_config.service_name),
            [(TASK_FIELD, encode_task_envelope(task_data))],
        )

        await asyncio.wait_for(done.wait(), timeout=10.0)
        handler = worker._find_task_handler("echo")
        assert isinstance(handler, _RecordingHandler)
        assert handler.processed == [b"hello-real-server"]
    finally:
        await worker.exit()
        await asyncio.wait_for(worker.events["exit"].wait(), timeout=10.0)


@pytest.mark.asyncio
async def test_worker_creates_consumer_group_on_start(worker_config: ValkeyWorkerConfig, valkey_client):
    """Starting the worker creates its consumer group on the real task stream."""
    worker = ValkeyWorker(worker_config)
    await worker.start()
    try:
        await _wait_until(lambda: worker.state is ServiceStatus.RUNNING)

        groups = await valkey_client.xinfo_groups(
            worker_config.task_stream_name.format(service=worker_config.service_name)
        )
        names = {g[b"name"] if isinstance(g, dict) else g.name for g in groups}
        assert worker_config.task_group_name.format(service=worker_config.service_name).encode() in names
    finally:
        await worker.exit()
        await asyncio.wait_for(worker.events["exit"].wait(), timeout=10.0)


@pytest.mark.asyncio
async def test_worker_heartbeat_key_written(worker_config: ValkeyWorkerConfig, valkey_client):
    """A running worker writes its heartbeat key to the real server."""
    worker = ValkeyWorker(worker_config)
    await worker.start()
    try:
        await _wait_until(lambda: worker.state is ServiceStatus.RUNNING)

        key = worker_config.heartbeat_key.format(service=worker_config.service_name, instance_id=worker.instance_id)
        # The heartbeat manager writes on its own interval, so poll for the key.
        await _wait_for_async(lambda: valkey_client.get(key))
        value = await valkey_client.get(key)
        assert value is not None
    finally:
        await worker.exit()
        await asyncio.wait_for(worker.events["exit"].wait(), timeout=10.0)
