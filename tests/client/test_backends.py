"""Tests for the watch backends and heartbeat decoding."""

import asyncio
from datetime import datetime, timezone

import msgspec
import pytest

from scietex.service.client.backends import decode_heartbeat
from scietex.service.heartbeat import Heartbeat


def make_heartbeat(instance_id: str = "a") -> Heartbeat:
    return Heartbeat(
        service="svc",
        instance_id=instance_id,
        status="active",
        heartbeat_interval=10.0,
        start_time=datetime.now(timezone.utc),
        ttl=100.0,
        queue_depth=0,
        running_tasks=0,
        tasks_per_second=0.0,
        timestamp=datetime.now(timezone.utc),
    )


def test_decode_heartbeat_round_trip():
    heartbeat = make_heartbeat()
    payload = msgspec.msgpack.encode(heartbeat)

    decoded = decode_heartbeat(payload)

    assert decoded is not None
    assert decoded.instance_id == "a"
    assert decoded.ttl == 100.0


def test_decode_heartbeat_skips_empty_payload():
    assert decode_heartbeat(b"") is None
    assert decode_heartbeat(None) is None


def test_decode_heartbeat_skips_pre_v5_payload():
    """A heartbeat without ``ttl`` is rejected, not raised."""
    legacy = {
        "service": "svc",
        "instance_id": "a",
        "status": "active",
        "heartbeat_interval": 10.0,
        "start_time": datetime.now(timezone.utc),
        "timestamp": datetime.now(timezone.utc),
    }
    payload = msgspec.msgpack.encode(legacy)

    assert decode_heartbeat(payload) is None


def test_decode_heartbeat_skips_foreign_payload():
    assert decode_heartbeat(msgspec.msgpack.encode({"unrelated": True})) is None


class FakeGlideClient:
    """Minimal GlideClient stand-in for the polling backend."""

    def __init__(self, pages: list[tuple[bytes, list[bytes]]], values: list[bytes | None]) -> None:
        self._pages = pages
        self._values = values
        self.scan_calls: list[tuple[bytes, str, int]] = []
        self.mget_calls: list[list[bytes]] = []

    async def scan(self, cursor: bytes, *, match: str, count: int):
        self.scan_calls.append((cursor, match, count))
        return self._pages.pop(0)

    async def mget(self, keys):
        self.mget_calls.append(list(keys))
        return self._values


@pytest.mark.asyncio
async def test_polling_backend_scans_and_decodes():
    from scietex.service.valkey.watch import PollingBackend

    payload = msgspec.msgpack.encode(make_heartbeat())
    client = FakeGlideClient(
        pages=[(b"0", [b"scietex:svc:w1:status"])],
        values=[payload],
    )
    backend = PollingBackend(client, "svc")  # type: ignore[arg-type]

    heartbeats = await backend.poll()

    assert [h.instance_id for h in heartbeats] == ["a"]
    assert client.scan_calls == [(b"0", "scietex:svc:*:status", 100)]


@pytest.mark.asyncio
async def test_polling_backend_follows_cursor():
    from scietex.service.valkey.watch import PollingBackend

    payload = msgspec.msgpack.encode(make_heartbeat())
    client = FakeGlideClient(
        pages=[(b"7", [b"k1"]), (b"0", [b"k2"])],
        values=[payload, payload],
    )
    backend = PollingBackend(client, "svc")  # type: ignore[arg-type]

    await backend.poll()

    assert [call[0] for call in client.scan_calls] == [b"0", b"7"]


@pytest.mark.asyncio
async def test_polling_backend_skips_missing_and_undecodable():
    from scietex.service.valkey.watch import PollingBackend

    payload = msgspec.msgpack.encode(make_heartbeat())
    client = FakeGlideClient(
        pages=[(b"0", [b"k1", b"k2", b"k3"])],
        values=[payload, None, b"garbage"],
    )
    backend = PollingBackend(client, "svc")  # type: ignore[arg-type]

    heartbeats = await backend.poll()

    assert [h.instance_id for h in heartbeats] == ["a"]


@pytest.mark.asyncio
async def test_polling_backend_empty_scan_skips_mget():
    from scietex.service.valkey.watch import PollingBackend

    client = FakeGlideClient(pages=[(b"0", [])], values=[])
    backend = PollingBackend(client, "svc")  # type: ignore[arg-type]

    assert await backend.poll() == []
    assert client.mget_calls == []


class FakeMessage:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload


class FakeMqttClient:
    """Minimal aiomqtt.Client stand-in for the subscribe backend."""

    def __init__(self, messages: list[FakeMessage]) -> None:
        self._messages = messages
        self.subscribed: list[tuple[str, int]] = []
        self.unsubscribed: list[str] = []

    async def subscribe(self, topic: str, *, qos: int) -> None:
        self.subscribed.append((topic, qos))

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscribed.append(topic)

    @property
    def messages(self):
        async def _iter():
            for message in self._messages:
                yield message

        return _iter()


@pytest.mark.asyncio
async def test_subscribe_backend_buffers_and_drains():
    from scietex.service.mqtt.watch import SubscribeBackend

    payload = msgspec.msgpack.encode(make_heartbeat())
    client = FakeMqttClient([FakeMessage(payload), FakeMessage(b"")])
    backend = SubscribeBackend(client, "svc")  # type: ignore[arg-type]

    await backend.start()
    # Let the background consumer drain the finite message iterator.
    for _ in range(10):
        if backend._buffer:
            break
        await asyncio.sleep(0)

    heartbeats = await backend.poll()

    assert [h.instance_id for h in heartbeats] == ["a"]
    assert client.subscribed == [("scietex/svc/workers/+", 1)]
    # The buffer is cleared by poll.
    assert await backend.poll() == []

    await backend.close()
    assert client.unsubscribed == ["scietex/svc/workers/+"]
