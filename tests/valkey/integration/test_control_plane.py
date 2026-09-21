"""End-to-end control-plane routing against a live Valkey server.

The mocked control-plane tests in ``tests/valkey/test_control.py`` assert the
exact XADD/XREAD calls the transport makes. This tier proves the complementary
half: that a command published through the real producer surface lands in the
right worker's stream on a real server, and that a broadcast reaches every
worker reading the shared broadcast stream.

Gated by ``SCIETEX_TEST_VALKEY_URL`` (see ``conftest.py``).
"""

import asyncio
import logging
import time
from typing import cast
from uuid import uuid4

import pytest

from scietex.service import ValkeyWorker
from scietex.service.basic_worker import ServiceStatus
from scietex.service.task_handler.basic import TaskHandler
from scietex.service.task_handler.schemas import TaskData, TaskResult
from scietex.service.valkey._glide import GlideClient
from scietex.service.valkey.config import ValkeyWorkerConfig
from scietex.service.valkey.control import ValkeyControlPublisher
from scietex.service.valkey.transport import ValkeyTransport


async def _wait_until(predicate, *, timeout: float = 10.0) -> None:
    """Poll ``predicate`` until it is true, or fail after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


class _BlockingHandler(TaskHandler):
    """Blocks inside ``handle`` until released, so a task stays running."""

    def __init__(self, *args, started: asyncio.Event, release: asyncio.Event, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._started = started
        self._release = release

    @property
    def supported_tasks(self) -> list[str]:
        return ["block"]

    async def handle(self, task_data: TaskData, *, capabilities) -> TaskResult:
        self._started.set()
        await self._release.wait()
        return TaskResult(status="success")


def _broadcast_cursor(worker: ValkeyWorker) -> str | bytes | None:
    """Read the worker's broadcast cursor from its concrete Valkey transport."""
    return cast(ValkeyTransport, worker._transport)._broadcast_cursor


def _publisher(
    worker: ValkeyWorker, client: object, config: ValkeyWorkerConfig, service_name: str
) -> ValkeyControlPublisher:
    """Build a publisher from a live worker's resolved control-plane names."""
    return ValkeyControlPublisher(
        client=cast(GlideClient, client),
        control_stream_name=worker._control_stream_name,
        control_broadcast_stream_name=worker._control_broadcast_stream_name,
        control_stream_maxlen=config.control_stream_maxlen,
        status_key_prefix=f"scietex:{service_name}:task",
        logger=logging.getLogger("itest-control"),
    )


async def _start_worker(config: ValkeyWorkerConfig, *handlers, **handler_kwargs) -> ValkeyWorker:
    """Start a worker with the given handlers and wait until it is RUNNING."""
    worker = ValkeyWorker(config)
    for handler in handlers:
        worker.add_task_handler(handler, **handler_kwargs)
    await worker.start()
    await _wait_until(lambda: worker.state is ServiceStatus.RUNNING)
    return worker


async def _stop_worker(worker: ValkeyWorker) -> None:
    """Exit a worker and wait for full shutdown."""
    await worker.exit()
    await asyncio.wait_for(worker.events["exit"].wait(), timeout=10.0)


@pytest.mark.asyncio
async def test_directed_cancel_reaches_only_target_worker(
    worker_config: ValkeyWorkerConfig, valkey_config, service_name: str, publisher_client
):
    """A cancel directed at worker A's instance_id cancels A's task; B is untouched."""
    started = asyncio.Event()
    release = asyncio.Event()

    worker_a = await _start_worker(worker_config, _BlockingHandler, started=started, release=release)
    worker_b = await _start_worker(
        ValkeyWorkerConfig(service_name=service_name, valkey_config=valkey_config),
        _BlockingHandler,
        started=asyncio.Event(),
        release=asyncio.Event(),
    )
    try:
        # Enqueue a blocking task and wait for A to pick it up.
        task_data = TaskData(task_id=str(uuid4()), task="block", payload=b"{}")
        assert worker_a.enqueue_task(task_data) is True
        await asyncio.wait_for(started.wait(), timeout=10.0)

        # Resolve the owner through the real tracking record, then cancel it.
        publisher = _publisher(worker_a, publisher_client, worker_config, service_name)
        owner = await publisher.resolve_owner(task_data.task_id)
        assert owner == worker_a.instance_id
        assert owner is not None

        cancel = TaskData(task_id=str(uuid4()), task="task:cancel", payload=b"{}")
        await publisher.direct(owner, cancel)

        # A's task is cancelled; B never saw the command.
        await _wait_until(lambda: task_data.task_id not in {str(t) for t in worker_a.running_tasks})
        assert worker_b.running_tasks == {}
    finally:
        release.set()
        await _stop_worker(worker_a)
        await _stop_worker(worker_b)


@pytest.mark.asyncio
async def test_broadcast_reaches_every_worker(
    worker_config: ValkeyWorkerConfig, valkey_config, service_name: str, publisher_client
):
    """A broadcast command is read by both workers sharing the broadcast stream."""
    worker_a = await _start_worker(worker_config)
    worker_b = await _start_worker(
        ValkeyWorkerConfig(
            service_name=service_name,
            valkey_config=valkey_config,
            task_queue_manager_sleep_time=worker_config.task_queue_manager_sleep_time,
        )
    )
    try:
        publisher = _publisher(worker_a, publisher_client, worker_config, service_name)
        command = TaskData(task_id=str(uuid4()), task="config:show", payload=b"{}")

        # The first broadcast read seeds the cursor at the stream tail, so a
        # command published before that read is skipped by design (§4.2). Wait
        # for both cursors to be seeded, then publish.
        await _wait_until(lambda: _broadcast_cursor(worker_a) is not None, timeout=15.0)
        await _wait_until(lambda: _broadcast_cursor(worker_b) is not None, timeout=15.0)
        await publisher.broadcast(command)

        # Both workers advance their cursor past the published entry.
        seeded_a = _broadcast_cursor(worker_a)
        seeded_b = _broadcast_cursor(worker_b)
        await _wait_until(lambda: _broadcast_cursor(worker_a) != seeded_a, timeout=15.0)
        await _wait_until(lambda: _broadcast_cursor(worker_b) != seeded_b, timeout=15.0)
    finally:
        await _stop_worker(worker_a)
        await _stop_worker(worker_b)
