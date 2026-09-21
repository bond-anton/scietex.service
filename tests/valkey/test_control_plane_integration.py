"""Multi-worker control-plane integration test (AR-123 §11 step 11).

End-to-end proof that the v5.0.0 control-plane split closes the AR-123 gaps: two
real ``ValkeyWorker`` instances share one in-process fake Valkey backend (no
server) and exchange control commands through the producer surface.

The three proofs, one test each:

- ``test_directed_cancel_crosses_workers`` — a ``cancel_task`` directed at
  worker A's ``instance_id`` reaches A's control lane and cancels A's running
  task, while worker B (reading the same backend) never sees the command.
- ``test_broadcast_reaches_both_workers`` — a broadcast ``config:apply``
  published to the shared broadcast stream is read by both A and B.
- ``test_resolve_owner_returns_owner_and_none`` — ``resolve_owner`` maps a
  running task's id to its owner's ``instance_id`` and returns ``None`` for an
  unknown id.

Sharing model (approach (a)): a ``_SharedStreams`` backend is injected into two
per-worker ``DummyClient`` instances. Each worker keeps its own client (so the
existing per-worker fakes and their recording attributes stay intact), but the
stream and key state they read is one shared object, making the stream name the
only routing decision, exactly as on a real broker. ``$`` is treated as the
stream start so a publish-then-read test is deterministic; tail-seek staleness
(design §4.2) is orthogonal to the routing gap and covered by the transport unit
tests.
"""

import asyncio
import logging
import time
from typing import cast
from uuid import UUID, uuid4

import msgspec
import pytest

from scietex.service import ValkeyWorker
from scietex.service.task_handler.basic import TaskHandler
from scietex.service.task_handler.cancel import CancelTaskRequest
from scietex.service.task_handler.capabilities import TaskCapabilities
from scietex.service.task_handler.schemas import (
    CONFIG_APPLY_TASK_NAME,
    TASK_CANCEL_TASK_NAME,
    TaskData,
    TaskResult,
    TaskStatus,
    TaskTimeout,
)
from scietex.service.valkey._glide import GlideClient
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig
from scietex.service.valkey.control import ValkeyControlPublisher

from ._helpers import DummyClient, _SharedStreams

_SERVICE = "svc"


def _make_worker(backend: _SharedStreams) -> tuple[ValkeyWorker, DummyClient]:
    """Build a ValkeyWorker whose client reads/writes the shared backend."""
    client = DummyClient(streams=backend)
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name=_SERVICE, valkey_config=ValkeyConfig()))
    worker._client = client
    # Skip startup recovery: the fake backend has no consumer-group pending list,
    # and XAUTOCLAIM on the canned client would return None.
    worker._transport.recovered = True
    return worker, client


def _publisher(client: DummyClient) -> ValkeyControlPublisher:
    """Build a publisher against the same control stream/status-key layout."""
    return ValkeyControlPublisher(
        client=cast(GlideClient, client),
        control_stream_name=f"scietex:{_SERVICE}:control:{{instance_id}}",
        control_broadcast_stream_name=f"scietex:{_SERVICE}:control",
        control_stream_maxlen=1000,
        status_key_prefix=f"scietex:{_SERVICE}:task",
        logger=logging.getLogger("test_control_plane_integration"),
    )


def _data_task(task_id: UUID) -> TaskData:
    """A data-plane task the blocking handler will start and then park."""
    return TaskData(task_id=str(task_id), task="long_task", payload=b"{}")


def _cancel_task(task_id: UUID, target_id: UUID) -> TaskData:
    """A directed ``cancel_task`` command targeting ``target_id``."""
    return TaskData(
        task_id=str(task_id),
        task=TASK_CANCEL_TASK_NAME,
        payload=msgspec.msgpack.encode(CancelTaskRequest(target_task_id=str(target_id))),
        timeout=TaskTimeout(timeout=None, timeout_action="discard"),
        canceled_action="discard",
    )


def _config_apply_task(task_id: UUID) -> TaskData:
    """A broadcast control command (``config:apply``)."""
    return TaskData(
        task_id=str(task_id),
        task=CONFIG_APPLY_TASK_NAME,
        payload=b"{}",
        timeout=TaskTimeout(timeout=None, timeout_action="discard"),
        canceled_action="discard",
    )


async def _wait_until(predicate, *, timeout: float = 5.0) -> None:
    """Poll ``predicate`` until it returns truthy, failing after ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not met within timeout")


class _BlockingHandler(TaskHandler):
    """Data handler that signals start, then blocks until released or cancelled.

    ``await self._release.wait()`` is cancellable: a ``CancelledError`` from a
    ``cancel_task`` propagates out of ``handle`` (``process_task`` catches only
    ``Exception``), so the worker task ends cancelled rather than ignoring it.
    """

    def __init__(self, name, context, *, started: asyncio.Event, release: asyncio.Event):
        super().__init__(name, context)
        self._started = started
        self._release = release

    @property
    def supported_tasks(self) -> list[str]:
        return ["long_task"]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        self._started.set()
        await self._release.wait()
        return TaskResult(status="success")


@pytest.mark.asyncio
async def test_directed_cancel_crosses_workers():
    """A directed ``cancel_task`` reaches A's control lane and cancels A's
    running task; B, reading the same backend, never sees the command."""
    backend = _SharedStreams()
    worker_a, _client_a = _make_worker(backend)
    worker_b, _client_b = _make_worker(backend)
    publisher = _publisher(DummyClient(streams=backend))

    data_id = uuid4()
    started = asyncio.Event()
    release = asyncio.Event()
    worker_a.add_task_handler(_BlockingHandler, started=started, release=release)
    await worker_a._start_task_handler("_BlockingHandler")
    await worker_a._start_task_handler("CancelTaskHandler")

    # Start the data task and let its handler park on release.
    assert worker_a.enqueue_task(_data_task(data_id)) is True
    await worker_a._executor.run_once()
    await started.wait()
    tracker = worker_a.running_tasks[data_id]
    assert not tracker.worker_task.done()

    # Publish the directed cancel to A's stream and drive A's control intake.
    await publisher.direct(worker_a.instance_id, _cancel_task(uuid4(), data_id))
    assert await worker_a._transport.fetch(worker_a) is True
    await worker_a._executor.run_once()

    await _wait_until(lambda: tracker.worker_task.done())
    assert tracker.worker_task.cancelled()
    assert data_id not in worker_a.running_tasks

    # B has no directed stream for the command: its fetch enqueues nothing.
    assert await worker_b._transport.fetch(worker_b) is False
    assert worker_b.control_queue_empty()


@pytest.mark.asyncio
async def test_broadcast_reaches_both_workers():
    """A broadcast ``config:apply`` is read from the shared broadcast stream by
    both workers and recorded against the broadcast stream name."""
    backend = _SharedStreams()
    worker_a, _client_a = _make_worker(backend)
    worker_b, _client_b = _make_worker(backend)
    publisher = _publisher(DummyClient(streams=backend))

    config_id = uuid4()
    await publisher.broadcast(_config_apply_task(config_id))

    assert await worker_a._transport.fetch(worker_a) is True
    assert await worker_b._transport.fetch(worker_b) is True

    assert config_id in worker_a._control_entry_ids
    assert config_id in worker_b._control_entry_ids
    assert worker_a._control_entry_ids[config_id][0] == worker_a._control_broadcast_stream_name
    assert worker_b._control_entry_ids[config_id][0] == worker_b._control_broadcast_stream_name


@pytest.mark.asyncio
async def test_resolve_owner_returns_owner_and_none():
    """resolve_owner reads the tracking key from the shared backend, returning
    the owner's instance_id, and None for an unknown task id."""
    backend = _SharedStreams()
    publisher = _publisher(DummyClient(streams=backend))

    task_id = str(uuid4())
    status = TaskStatus(
        task_id=task_id,
        service=_SERVICE,
        task="long_task",
        status="running",
        instance_id="worker-9",
    )
    backend.put(f"scietex:{_SERVICE}:task:{task_id}", msgspec.msgpack.encode(status))

    assert await publisher.resolve_owner(task_id) == "worker-9"
    assert await publisher.resolve_owner(str(uuid4())) is None
