"""Valkey async task processor testing."""

import logging

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

    def __init__(
        self,
        ping_ok=True,
        xreadgroup_result=None,
        xautoclaim_result=None,
        xgroup_create_error=None,
    ):
        self._ping_ok = ping_ok
        self.closed = False
        self.xreadgroup_result = xreadgroup_result
        self.xautoclaim_result = xautoclaim_result
        self.xgroup_create_error = xgroup_create_error
        self.acked: list = []
        self.deleted: list = []

    async def xgroup_create(self, *args, **kwargs):
        if self.xgroup_create_error is not None:
            raise self.xgroup_create_error

    async def xadd(self, *args, **kwargs):
        pass

    async def xack(self, *args, **kwargs):
        self.acked.append(args)

    async def xdel(self, *args, **kwargs):
        self.deleted.append(args)

    async def xreadgroup(self, *args, **kwargs):
        return self.xreadgroup_result

    async def xautoclaim(self, *args, **kwargs):
        return self.xautoclaim_result

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


@pytest.mark.asyncio
async def test_initialize_group_already_exists_succeeds(monkeypatch):
    # A BUSYGROUP error means the consumer group already exists and must be
    # ignored; initialize still reports success (AR-021).
    import scietex.service.valkey.valkey_async_worker as mod

    async def create_mock(cfg):
        return DummyClient(
            ping_ok=True,
            xgroup_create_error=mod.RequestError("BUSYGROUP Consumer Group name already exists"),
        )

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    ok = await worker.initialize()
    assert ok is True


@pytest.mark.asyncio
async def test_initialize_group_create_failure_fails(monkeypatch):
    # A genuine xgroup_create failure must fail initialize so the worker
    # does not run with no consumer group (AR-021).
    import scietex.service.valkey.valkey_async_worker as mod

    async def create_mock(cfg):
        return DummyClient(ping_ok=True, xgroup_create_error=mod.RequestError("NOAUTH Authentication required"))

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    ok = await worker.initialize()
    assert ok is False


def _entry(entry_id: bytes, task_id: str, payload: bytes):
    """Build an xreadgroup/xautoclaim result mapping for one stream entry."""
    return {b"stream": {entry_id: [[task_id.encode("utf-8"), payload]]}}


@pytest.mark.asyncio
async def test_fetch_tasks_does_not_ack_on_enqueue():
    """fetch_tasks must not XACK/XDEL on enqueue; it records the entry id so
    the entry stays pending until the handler completes (AR-005)."""
    import msgspec

    from scietex.service.task_handler.schemas import TaskData

    task_data = TaskData(task="dummy", payload=b"{}")
    payload = msgspec.msgpack.encode(task_data)
    client = DummyClient(xreadgroup_result=_entry(b"1-0", "11111111-1111-1111-1111-111111111111", payload))
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    worker._client = client

    await worker.fetch_tasks()

    assert client.acked == [], "fetch_tasks must not ack on enqueue"
    assert client.deleted == [], "fetch_tasks must not delete on enqueue"
    assert not worker.task_queue_empty()
    t_id, t_data = worker.dequeue_task()
    assert t_data.task == "dummy"
    assert worker._task_entry_ids[t_id] == b"1-0"


@pytest.mark.asyncio
async def test_on_task_completed_acks_and_deletes_entry():
    """on_task_completed must XACK+XDEL the recorded entry id and clear the map (AR-005)."""
    from uuid import UUID

    client = DummyClient()
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    worker._client = client
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    worker._task_entry_ids[t_id] = b"1-0"

    await worker.on_task_completed(t_id, None, None)

    assert client.acked == [(worker._task_stream_name, worker._task_group_name, [b"1-0"])]
    assert client.deleted == [(worker._task_stream_name, [b"1-0"])]
    assert t_id not in worker._task_entry_ids


@pytest.mark.asyncio
async def test_recover_pending_tasks_enqueues_pending_entries():
    """_recover_pending_tasks must claim idle pending entries and enqueue them,
    recording their entry ids for later ack (AR-005)."""
    import msgspec

    from scietex.service.task_handler.schemas import TaskData

    task_data = TaskData(task="dummy", payload=b"{}")
    payload = msgspec.msgpack.encode(task_data)
    # xautoclaim returns [next_start, {entry_id: [[field, value]]}, [deleted_ids]]
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ]
    )
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    worker._client = client

    await worker._recover_pending_tasks()

    assert not worker.task_queue_empty()
    t_id, t_data = worker.dequeue_task()
    assert t_data.task == "dummy"
    assert worker._task_entry_ids[t_id] == b"9-0"


@pytest.mark.asyncio
async def test_logging_connected_reflects_handler_client():
    """logging_connected mirrors the registered AsyncValkeyHandler's client:
    False when absent/None, True when a client is attached (AR-018)."""
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    # Handler registered but never connected: its client is None.
    assert worker.logging_connected is False

    _find_valkey_handler(worker).client = DummyClient()
    assert worker.logging_connected is True

    _find_valkey_handler(worker).client = None
    assert worker.logging_connected is False


@pytest.mark.asyncio
async def test_logging_connected_false_when_handler_absent():
    """logging_connected is False when no AsyncValkeyHandler is registered (AR-018)."""
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    worker.logger.removeHandler(_find_valkey_handler(worker))
    assert worker.logging_connected is False


@pytest.mark.asyncio
async def test_connect_warns_when_logging_client_down(monkeypatch, caplog):
    """A successful worker connect must WARN when the logging client is down,
    so a half-connected worker is observable (AR-018)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    import scietex.service.valkey.valkey_async_worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    with caplog.at_level(logging.WARNING):
        ok = await worker.connect()

    assert ok is True
    assert any(
        record.levelno == logging.WARNING and "logging handler has no live client" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_connect_warns_when_worker_client_down(monkeypatch, caplog):
    """A failed worker connect must WARN when the logging client is up, so the
    inverse divergence is also observable (AR-018)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=False)

    import scietex.service.valkey.valkey_async_worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    # Simulate the logging handler being connected while the worker fails.
    _find_valkey_handler(worker).client = DummyClient()
    with caplog.at_level(logging.WARNING):
        ok = await worker.connect()

    assert ok is False
    assert worker.client is None
    assert any(
        record.levelno == logging.WARNING
        and "logging handler is connected, but the worker client is not" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_share_glide_client_seam_warns_and_falls_back(caplog):
    """share_glide_client=True with a non-injecting handler must WARN and fall
    back to the config-dict path without crashing (AR-018)."""
    with caplog.at_level(logging.WARNING):
        worker = ValkeyWorker(valkey_config=ValkeyConfig(), share_glide_client=True)

    handler = _find_valkey_handler(worker)
    assert handler.client_config is not None
    assert any(
        record.levelno == logging.WARNING and "share_glide_client=True" in record.getMessage()
        for record in caplog.records
    )
