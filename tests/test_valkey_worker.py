"""Valkey async task processor testing."""

import pytest
from scietex.logging import AsyncValkeyHandler

from scietex.service import ValkeyWorker
from scietex.service.valkey.valkey_config import (
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyUserCredentials,
)


class DummyClient:
    """Mocking Valkey client."""

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

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    ok = await worker.connect()
    assert ok is True
    assert worker.client is not None


@pytest.mark.asyncio
async def test_disconnect_closes_client(monkeypatch):
    # Create a worker and attach a dummy client
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
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


def _find_valkey_handler(worker: ValkeyWorker) -> AsyncValkeyHandler:
    for handler in worker.logger.handlers:
        if isinstance(handler, AsyncValkeyHandler):
            return handler
    raise AssertionError("AsyncValkeyHandler not registered on worker logger")


@pytest.mark.asyncio
async def test_log_handler_receives_credentials():
    cfg = ValkeyConfig(
        base_config=ValkeyBaseConfig(user_credentials=ValkeyUserCredentials(username="myuser", password="secret"))
    )
    worker = ValkeyWorker(service_name="creds-test", valkey_config=cfg)
    client_config = _find_valkey_handler(worker).client_config
    assert client_config["username"] == "myuser"
    assert client_config["password"] == "secret"


@pytest.mark.asyncio
async def test_log_handler_receives_no_credentials_by_default():
    worker = ValkeyWorker(service_name="nocreds-test", valkey_config=ValkeyConfig())
    client_config = _find_valkey_handler(worker).client_config
    assert client_config["username"] is None
    assert client_config["password"] is None


@pytest.mark.asyncio
async def test_connect_ping_failure_clears_client(monkeypatch):
    # A failed PING must leave _client as None so initialize() does not
    # treat the worker as connected (AR-006).
    async def create_mock(cfg):
        return DummyClient(ping_ok=False)

    import scietex.service.valkey.valkey_async_worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    ok = await worker.connect()
    assert ok is False
    assert worker.client is None, "failed ping must clear _client"


@pytest.mark.asyncio
async def test_connect_create_failure_leaves_client_none(monkeypatch):
    # A GlideClient.create exception must leave _client as None (AR-006).
    async def create_mock(cfg):
        raise RuntimeError("create failed")

    import scietex.service.valkey.valkey_async_worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    ok = await worker.connect()
    assert ok is False
    assert worker.client is None
