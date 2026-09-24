"""End-to-end MQTT integration tests against a live broker.

These tests drive a real ``MqttWorker`` against a real MQTT broker: the worker
connects, subscribes to its task topic, drains a task published by a separate
client, processes it, and publishes retained per-task status. They are gated by
``SCIETEX_TEST_MQTT_HOST`` (see ``conftest.py``).

The mocked unit tests in ``tests/mqtt/`` assert the exact wire calls the code
makes; this tier proves the complementary half — that those calls work against
a real broker, and that the sqlite inbox survives a worker restart, which a
fake structurally cannot.
"""

import asyncio
import time
from uuid import uuid4

import aiomqtt
import msgspec
import pytest

from scietex.service.basic_worker import ServiceStatus
from scietex.service.mqtt import MqttWorker
from scietex.service.mqtt.config import MqttConfig, MqttWorkerConfig
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult, TaskStatus
from scietex.service.task_handler.wire import encode_task_envelope


async def _wait_until(predicate, *, timeout: float = 10.0) -> None:
    """Poll ``predicate`` until it is true, or fail after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


async def _wait_until_async(predicate, *, timeout: float = 10.0) -> None:
    """Poll async ``predicate`` until it is true, or fail after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


async def _pending_task_ids(inbox) -> set[str]:
    """The task ids still non-terminal in ``inbox``."""
    return {entry.task_id for entry in await inbox.pending()}


class _RecordingHandler(TaskHandler):
    """Records the ``TaskData`` it processes and signals each completion."""

    def __init__(self, *args, done: asyncio.Event, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._done = done
        self.processed: list[TaskData] = []

    @property
    def supported_tasks(self) -> list[str]:
        return ["echo"]

    async def handle(self, task_data: TaskData, *, capabilities) -> TaskResult:
        self.processed.append(task_data)
        self._done.set()
        return TaskResult(status="success", payload=task_data.payload)


def _task_data(task: str = "echo", payload: bytes = b"{}") -> TaskData:
    """Build a task with a fresh id for the given task name."""
    return TaskData(task_id=str(uuid4()), task=task, payload=payload)


def _task_topic(config: MqttWorkerConfig) -> str:
    """The task topic the worker subscribes to, with ``{service}`` resolved."""
    return config.task_topic.format(service=config.service_name)


@pytest.mark.asyncio
async def test_worker_processes_task_published_to_real_broker(worker_config: MqttWorkerConfig, mqtt_config: MqttConfig):
    """A task published to the real task topic is received, persisted, and processed."""

    done = asyncio.Event()
    worker = MqttWorker(worker_config)
    worker.add_task_handler(_RecordingHandler, done=done)

    await worker.start()
    try:
        await _wait_until(lambda: worker.state is ServiceStatus.RUNNING)

        task_data = _task_data(payload=b"hello-real-broker")
        async with aiomqtt.Client(hostname=mqtt_config.host, port=mqtt_config.port) as client:
            await client.publish(
                _task_topic(worker_config), encode_task_envelope(task_data), qos=worker_config.task_qos
            )

        await asyncio.wait_for(done.wait(), timeout=10.0)
        handler = worker._find_task_handler("echo")
        assert isinstance(handler, _RecordingHandler)
        assert handler.processed == [task_data]
    finally:
        await worker.exit()
        await asyncio.wait_for(worker.events["exit"].wait(), timeout=10.0)


@pytest.mark.asyncio
async def test_worker_publishes_retained_status_for_processed_task(
    worker_config: MqttWorkerConfig, mqtt_config: MqttConfig
):
    """A late subscriber to the status topic receives the retained ``completed`` status."""

    done = asyncio.Event()
    worker = MqttWorker(worker_config)
    worker.add_task_handler(_RecordingHandler, done=done)

    await worker.start()
    try:
        await _wait_until(lambda: worker.state is ServiceStatus.RUNNING)

        task_data = _task_data(payload=b"status-probe")
        async with aiomqtt.Client(hostname=mqtt_config.host, port=mqtt_config.port) as client:
            await client.publish(
                _task_topic(worker_config), encode_task_envelope(task_data), qos=worker_config.task_qos
            )

        await asyncio.wait_for(done.wait(), timeout=10.0)
        # The terminal status is published (retained) immediately before the
        # inbox entry is marked terminal, so the entry leaving ``pending`` is
        # the deterministic signal that the ``completed`` status is already on
        # the broker.
        inbox = worker._inbox
        assert inbox is not None

        async def task_terminal() -> bool:
            return task_data.task_id not in await _pending_task_ids(inbox)

        await _wait_until_async(task_terminal)

        status_topic = (
            f"{worker_config.status_topic_prefix.format(service=worker_config.service_name)}/{task_data.task_id}/status"
        )
        async with aiomqtt.Client(hostname=mqtt_config.host, port=mqtt_config.port) as client:
            await client.subscribe(status_topic, qos=worker_config.status_qos)
            message = await asyncio.wait_for(client.messages.__anext__(), timeout=10.0)

        assert message.retain is True
        status = msgspec.msgpack.decode(message.payload, type=TaskStatus)
        assert status.status == "completed"
        assert status.task_id == task_data.task_id
        assert status.result == b"status-probe"
    finally:
        await worker.exit()
        await asyncio.wait_for(worker.events["exit"].wait(), timeout=10.0)


@pytest.mark.asyncio
async def test_inbox_survives_reconnect(worker_config: MqttWorkerConfig, mqtt_config: MqttConfig):
    """A task persisted to the sqlite inbox by a stopped worker is recovered and processed on restart."""

    done = asyncio.Event()
    worker = MqttWorker(worker_config)
    worker.add_task_handler(_RecordingHandler, done=done)

    await worker.start()
    try:
        await _wait_until(lambda: worker.state is ServiceStatus.RUNNING)
        # Freeze intake so the published task is persisted to the inbox but never
        # drained into the processor queue: only the message loop writes the
        # inbox, and it is a plain asyncio task, not a manager.
        await worker.manager_runtime.stop_manager("TaskQueueManager")

        task_data = _task_data(payload=b"survives-restart")
        async with aiomqtt.Client(hostname=mqtt_config.host, port=mqtt_config.port) as client:
            await client.publish(
                _task_topic(worker_config), encode_task_envelope(task_data), qos=worker_config.task_qos
            )

        # The inbox entry is the durable proof the message reached storage
        # before the worker went away; it stays pending because nothing drained
        # it.
        inbox = worker._inbox
        assert inbox is not None

        async def task_pending() -> bool:
            return task_data.task_id in await _pending_task_ids(inbox)

        await _wait_until_async(task_pending)
    finally:
        await worker.exit()
        await asyncio.wait_for(worker.events["exit"].wait(), timeout=10.0)

    # A fresh worker over the same inbox path replays the pending entry on its
    # first fetch, so the task is processed without any broker redelivery.
    restarted_done = asyncio.Event()
    restarted = MqttWorker(worker_config)
    restarted.add_task_handler(_RecordingHandler, done=restarted_done)

    await restarted.start()
    try:
        await _wait_until(lambda: restarted.state is ServiceStatus.RUNNING)
        await asyncio.wait_for(restarted_done.wait(), timeout=10.0)
        handler = restarted._find_task_handler("echo")
        assert isinstance(handler, _RecordingHandler)
        assert handler.processed == [task_data]
    finally:
        await restarted.exit()
        await asyncio.wait_for(restarted.events["exit"].wait(), timeout=10.0)
