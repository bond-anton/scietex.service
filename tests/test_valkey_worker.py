import asyncio
import json

import pytest

from scietex.service.valkey.valkey_async_worker import ValkeyWorker
from scietex.service.valkey.valkey_config import ValkeyBaseConfig


class DummyClient:
    def __init__(self, ping_ok=True):
        self._ping_ok = ping_ok
        self.closed = False

    async def xgroup_create(self, *args, **kwargs):
        pass

    async def xadd(self, *args, **kwargs):
        pass

    async def xack(self, *args, **kwargs):
        pass

    async def xdel(self, *args, **kwargs):
        pass

    async def xreadgroup(self, *args, **kwargs):
        return None

    async def ping(self):
        return self._ping_ok

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_connect_success(monkeypatch):
    # Mock GlideClient.create to return a DummyClient
    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    import scietex.service.valkey.valkey_async_worker as mod

    monkeypatch.setattr(
        mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)})
    )
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyBaseConfig())
    ok = await worker.connect()
    assert ok is True
    assert worker.client is not None


@pytest.mark.asyncio
async def test_disconnect_closes_client(monkeypatch):
    # Create a worker and attach a dummy client
    worker = ValkeyWorker(valkey_config=ValkeyBaseConfig())
    client = DummyClient()
    worker._client = client

    await worker.disconnect()
    assert client.closed is True
    assert worker.client is None


def _make_msg(channel: bytes | str, message: bytes | str):
    class Msg:
        def __init__(self, channel, message):
            self.channel = channel
            self.message = message

    return Msg(channel, message)


def test_parse_control_message_bytes(monkeypatch):
    worker = ValkeyWorker(valkey_config=ValkeyBaseConfig())

    # valid json bytes channel
    msg = _make_msg(b"scietex:service:1", b'{"a": 1}')
    # should not raise
    worker._parse_control_message(msg, context={"k": "v"})

    # invalid json should be caught and not raise
    bad = _make_msg(b"other", b"not-json")
    worker._parse_control_message(bad, context=None)
