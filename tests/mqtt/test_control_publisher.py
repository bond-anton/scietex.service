"""MqttControlPublisher unit tests (AR-123 §7/§10.1)."""

import asyncio
import logging
from typing import cast
from uuid import uuid4

import pytest

from scietex.service.mqtt._aiomqtt import Client
from scietex.service.mqtt.control import MqttControlPublisher
from scietex.service.task_handler.schemas import TaskData
from scietex.service.task_handler.wire import encode_task_envelope

_LOGGER = "test_control_publisher"


class _FakeMessage:
    """Minimal aiomqtt ``Message``: a topic plus a payload."""

    def __init__(self, payload: bytes, topic: str) -> None:
        self.topic = topic
        self.payload = payload


class _FakeMessages:
    """Async iterator over a fake client's inbound message queue."""

    def __init__(self, queue: asyncio.Queue) -> None:
        self._queue = queue

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._queue.get()


class FakeClient:
    """Minimal aiomqtt ``Client`` recording publish/subscribe without a broker."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue = asyncio.Queue()
        self.published: list[tuple] = []
        self.subscriptions: list[tuple[str, int]] = []

    @property
    def messages(self):
        return _FakeMessages(self._queue)

    def feed(self, message: _FakeMessage) -> None:
        self._queue.put_nowait(message)

    async def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        self.published.append((topic, payload, qos, retain, properties))

    async def subscribe(self, topic, qos=0, *args, **kwargs):
        self.subscriptions.append((topic, qos))


def _publisher(client: FakeClient, **kwargs) -> MqttControlPublisher:
    return MqttControlPublisher(
        client=cast(Client, client),
        control_topic="scietex/svc/control/{instance_id}",
        control_broadcast_topic="scietex/svc/control",
        control_qos=1,
        status_topic_prefix="scietex/svc/tasks",
        logger=logging.getLogger(_LOGGER),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_direct_publishes_envelope_to_directed_topic():
    """direct publishes the enveloped command to the addressed worker's topic,
    event-only (not retained), formatting the {instance_id} placeholder."""
    client = FakeClient()
    publisher = _publisher(client)
    task_data = TaskData(task_id=str(uuid4()), task="task:cancel", payload=b"{}")

    await publisher.direct("worker-7", task_data)

    assert len(client.published) == 1
    topic, payload, qos, retain, _ = client.published[0]
    assert topic == "scietex/svc/control/worker-7"
    assert payload == encode_task_envelope(task_data)
    assert qos == 1
    assert retain is False


@pytest.mark.asyncio
async def test_broadcast_publishes_envelope_to_broadcast_topic():
    """broadcast publishes the enveloped command to the shared broadcast topic."""
    client = FakeClient()
    publisher = _publisher(client)
    task_data = TaskData(task_id=str(uuid4()), task="config:apply", payload=b"{}")

    await publisher.broadcast(task_data)

    assert len(client.published) == 1
    topic, payload, qos, retain, _ = client.published[0]
    assert topic == "scietex/svc/control"
    assert payload == encode_task_envelope(task_data)
    assert qos == 1
    assert retain is False


@pytest.mark.asyncio
async def test_resolve_owner_reads_retained_owner_marker():
    """resolve_owner subscribes to the owner topic and returns the marker payload
    (the raw instance_id) delivered as a retained message."""
    task_id = str(uuid4())
    client = FakeClient()
    publisher = _publisher(client)
    client.feed(_FakeMessage(b"worker-9", topic=f"scietex/svc/tasks/{task_id}/owner"))

    assert await publisher.resolve_owner(task_id) == "worker-9"
    assert client.subscriptions == [(f"scietex/svc/tasks/{task_id}/owner", 1)]


@pytest.mark.asyncio
async def test_resolve_owner_returns_none_on_timeout():
    """resolve_owner returns None when no owner marker arrives within the
    configured resolve_timeout (the task has no owner)."""
    task_id = str(uuid4())
    client = FakeClient()
    publisher = _publisher(client, resolve_timeout=0.01)

    assert await publisher.resolve_owner(task_id) is None
    assert client.subscriptions == [(f"scietex/svc/tasks/{task_id}/owner", 1)]
