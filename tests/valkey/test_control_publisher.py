"""ValkeyControlPublisher unit tests (AR-123 §7)."""

import logging
from typing import cast
from uuid import uuid4

import msgspec
import pytest

from scietex.service.task_handler.schemas import TaskData, TaskStatus
from scietex.service.task_handler.wire import encode_task_envelope
from scietex.service.valkey._glide import GlideClient, StreamAddOptions, TrimByMaxLen
from scietex.service.valkey.control import ValkeyControlPublisher
from scietex.service.valkey.transport import TASK_FIELD

from ._helpers import DummyClient

_LOGGER = "test_control_publisher"


def _publisher(client: DummyClient, **kwargs) -> ValkeyControlPublisher:
    return ValkeyControlPublisher(
        client=cast(GlideClient, client),
        control_stream_name="scietex:svc:control:{instance_id}",
        control_broadcast_stream_name="scietex:svc:control",
        control_stream_maxlen=1000,
        status_key_prefix="scietex:svc:task",
        logger=logging.getLogger(_LOGGER),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_direct_xadds_envelope_to_directed_stream_with_maxlen_trim():
    """direct XADDs the enveloped command to the addressed worker's stream with
    a MAXLEN ~ N trim, formatting the {instance_id} placeholder."""
    client = DummyClient()
    publisher = _publisher(client)
    task_data = TaskData(task_id=str(uuid4()), task="cancel_task", payload=b"{}")

    await publisher.direct("worker-7", task_data)

    assert len(client.added) == 1
    stream_name, fields, options = client.added[0]
    assert stream_name == "scietex:svc:control:worker-7"
    assert fields == [(TASK_FIELD, encode_task_envelope(task_data))]
    assert isinstance(options, StreamAddOptions)
    assert options.trim is not None
    assert isinstance(options.trim, TrimByMaxLen)
    assert options.trim.exact is False
    assert options.trim.threshold == 1000


@pytest.mark.asyncio
async def test_broadcast_xadds_envelope_to_broadcast_stream():
    """broadcast XADDs the enveloped command to the shared broadcast stream."""
    client = DummyClient()
    publisher = _publisher(client)
    task_data = TaskData(task_id=str(uuid4()), task="config:apply", payload=b"{}")

    await publisher.broadcast(task_data)

    assert len(client.added) == 1
    stream_name, fields, _ = client.added[0]
    assert stream_name == "scietex:svc:control"
    assert fields == [(TASK_FIELD, encode_task_envelope(task_data))]


@pytest.mark.asyncio
async def test_resolve_owner_returns_instance_id():
    """resolve_owner GETs the tracking key and returns the recorded instance_id."""
    task_id = str(uuid4())
    status = TaskStatus(task_id=task_id, service="svc", task="a", status="running", instance_id="worker-3")
    client = DummyClient(get_value=msgspec.msgpack.encode(status))
    publisher = _publisher(client)

    assert await publisher.resolve_owner(task_id) == "worker-3"
    assert client.gets == [f"scietex:svc:task:{task_id}"]


@pytest.mark.asyncio
async def test_resolve_owner_returns_none_when_key_absent():
    """resolve_owner returns None when the tracking key is absent (no owner)."""
    task_id = str(uuid4())
    client = DummyClient(get_value=None)
    publisher = _publisher(client)

    assert await publisher.resolve_owner(task_id) is None
    assert client.gets == [f"scietex:svc:task:{task_id}"]


@pytest.mark.asyncio
async def test_resolve_owner_returns_none_when_instance_id_empty():
    """resolve_owner maps an empty instance_id (owner unknown) to None."""
    task_id = str(uuid4())
    status = TaskStatus(task_id=task_id, service="svc", task="a", status="running", instance_id="")
    client = DummyClient(get_value=msgspec.msgpack.encode(status))
    publisher = _publisher(client)

    assert await publisher.resolve_owner(task_id) is None
