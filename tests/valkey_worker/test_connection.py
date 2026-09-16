"""ValkeyWorker connection, disconnection, and consumer-group initialization tests."""

import asyncio

import pytest

import scietex.service.valkey.worker as mod
from scietex.service import ValkeyWorker
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig

from ._helpers import DummyClient, _patch_glide, _patch_glide_and_handler


@pytest.mark.asyncio
async def test_connect_loads_config_from_disk(monkeypatch, tmp_path):
    """With no explicit config, ``valkey.yml`` is read (and the default written)
    at first connect, populating ``valkey_config``/``_client_config`` (AR-066)."""
    monkeypatch.setenv("SCIETEX_CONFIG_DIR", str(tmp_path))

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    _patch_glide_and_handler(monkeypatch)

    worker = ValkeyWorker(client_factory=factory)
    assert worker.valkey_config is None, "config must stay deferred before connect"

    ok = await worker.connect()
    assert ok is True
    assert (tmp_path / "valkey.yml").exists(), "connect must write the default valkey.yml"
    assert isinstance(worker.valkey_config, ValkeyConfig)
    assert worker._client_config is not None


@pytest.mark.asyncio
async def test_connect_success(monkeypatch):
    # Supply a DummyClient through the client_factory seam.
    async def factory(cfg):
        return DummyClient(ping_ok=True)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    ok = await worker.connect()
    assert ok is True
    assert worker.client is not None


@pytest.mark.asyncio
async def test_disconnect_closes_client(monkeypatch):
    # Create a worker and attach a dummy client
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    client = DummyClient()
    worker._client = client

    await worker.disconnect()
    assert client.closed is True
    assert worker.client is None


@pytest.mark.asyncio
async def test_connect_is_serialized_by_lock(monkeypatch):
    """Concurrent connect() calls must not double-create the client: the
    asyncio.Lock serializes the create→ping→assign sequence (AR-059)."""
    creates = []

    async def factory(cfg):
        creates.append(cfg)
        # Yield so a concurrent connect() would interleave without the lock.
        await asyncio.sleep(0)
        return DummyClient(ping_ok=True)

    _patch_glide_and_handler(monkeypatch)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    results = await asyncio.gather(worker.connect(), worker.connect(), worker.connect())

    assert results == [True, True, True]
    assert len(creates) == 1, "concurrent connect() must create exactly one client"
    assert worker.client is not None


@pytest.mark.asyncio
async def test_connect_ping_failure_clears_client(monkeypatch):
    # A failed PING must leave _client as None so initialize() does not
    # treat the worker as connected (AR-006).
    async def factory(cfg):
        return DummyClient(ping_ok=False)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    ok = await worker.connect()
    assert ok is False
    assert worker.client is None, "failed ping must clear _client"


@pytest.mark.asyncio
async def test_connect_create_failure_leaves_client_none(monkeypatch):
    # A factory exception must leave _client as None (AR-006).
    async def factory(cfg):
        raise RuntimeError("create failed")

    # Map the glide connection errors to Exception so the raising factory is
    # caught by connect()'s failure path.
    _patch_glide(monkeypatch)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    ok = await worker.connect()
    assert ok is False
    assert worker.client is None


@pytest.mark.asyncio
async def test_initialize_group_already_exists_succeeds(monkeypatch):
    # A BUSYGROUP error means the consumer group already exists and must be
    # ignored; initialize still reports success (AR-021).

    async def factory(cfg):
        return DummyClient(
            ping_ok=True,
            xgroup_create_error=mod.RequestError("BUSYGROUP Consumer Group name already exists"),
        )

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    ok = await worker.initialize()
    assert ok is True


@pytest.mark.asyncio
async def test_initialize_group_create_failure_fails(monkeypatch):
    # A genuine xgroup_create failure must fail initialize so the worker
    # does not run with no consumer group (AR-021).

    async def factory(cfg):
        return DummyClient(ping_ok=True, xgroup_create_error=mod.RequestError("NOAUTH Authentication required"))

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    ok = await worker.initialize()
    assert ok is False


@pytest.mark.asyncio
async def test_disconnect_closes_operational_client_but_not_owned_handler(monkeypatch):
    """disconnect closes the worker's operational client once; the owned
    logging handler's connection is independent and left untouched
    (AR-059/061)."""

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    _patch_glide_and_handler(monkeypatch)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    await worker.connect()
    client = worker.client
    handler = worker._valkey_logger_handler
    assert handler is not None
    assert handler.client is None  # owned, never injected

    await worker.disconnect()
    assert client.closed is True
    assert worker.client is None
    assert handler.client is None, "disconnect must not null an owned handler's client"
