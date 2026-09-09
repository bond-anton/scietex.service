"""Valkey async task processor testing."""

import asyncio
import logging
from uuid import UUID

import pytest

from scietex.service import ValkeyWorker
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig


class DummyClient:
    """Mocking Valkey client."""

    def __init__(
        self,
        ping_ok=True,
        xreadgroup_result=None,
        xreadgroup_error=None,
        xautoclaim_result=None,
        xgroup_create_error=None,
    ):
        self._ping_ok = ping_ok
        self.closed = False
        self.xreadgroup_result = xreadgroup_result
        self.xreadgroup_error = xreadgroup_error
        self.xautoclaim_result = xautoclaim_result
        self.xgroup_create_error = xgroup_create_error
        self.acked: list = []
        self.deleted: list = []
        self.xautoclaim_calls: list = []
        self.xreadgroup_calls: list = []
        self.sets: list = []

    async def set(self, key, value=None, expiry=None, *args, **kwargs):
        self.sets.append((key, value, expiry))

    async def sadd(self, *args, **kwargs):
        pass

    async def srem(self, *args, **kwargs):
        pass

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
        if self.xreadgroup_error is not None:
            raise self.xreadgroup_error
        self.xreadgroup_calls.append(args)
        return self.xreadgroup_result

    async def xautoclaim(self, *args, **kwargs):
        self.xautoclaim_calls.append(args)
        return self.xautoclaim_result

    async def ping(self):
        return self._ping_ok

    async def close(self):
        self.closed = True


class FakeHandler(logging.Handler):
    """Stand-in for ``AsyncValkeyHandler`` in connection tests.

    The real handler's ``start_logging`` spawns a worker that connects to a
    live Valkey server; this fake records its construction arguments and
    start/stop calls without opening a connection, so tests stay deterministic.
    """

    def __init__(self, stream_name, *, valkey_config=None, client=None, **kwargs):
        super().__init__()
        self.stream_name = stream_name
        self.valkey_config = valkey_config
        self.client = client
        self._owns_client = client is None
        self.logging_running_event = asyncio.Event()
        self.start_calls = 0
        self.stop_calls = 0

    async def start_logging(self):
        self.start_calls += 1
        self.logging_running_event.set()

    async def stop_logging(self):
        self.stop_calls += 1
        self.logging_running_event.clear()

    def emit(self, record):
        # Drop records: the fake is only here to record construction/start/stop,
        # not to deliver logs.
        pass


def _patch_glide(monkeypatch, create_mock):
    """Point the worker's ``GlideClient.create`` at ``create_mock`` and make the
    glide connection errors plain ``Exception``s so connect() tests are serverless."""
    import scietex.service.valkey.worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)
    return mod


def _patch_glide_and_handler(monkeypatch, create_mock):
    """Like :func:`_patch_glide`, but also swap the real logging handler for a
    :class:`FakeHandler` so connect() never opens a second connection."""
    mod = _patch_glide(monkeypatch, create_mock)
    monkeypatch.setattr(mod, "AsyncValkeyHandler", FakeHandler)
    return mod


@pytest.mark.asyncio
async def test_connect_success(monkeypatch):
    # Mock GlideClient.create to return a DummyClient
    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    import scietex.service.valkey.worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
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

    async def create_mock(cfg):
        creates.append(cfg)
        # Yield so a concurrent connect() would interleave without the lock.
        await asyncio.sleep(0)
        return DummyClient(ping_ok=True)

    _patch_glide_and_handler(monkeypatch, create_mock)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    results = await asyncio.gather(worker.connect(), worker.connect(), worker.connect())

    assert results == [True, True, True]
    assert len(creates) == 1, "concurrent connect() must create exactly one client"
    assert worker.client is not None


def _make_msg(channel: bytes | str, message: bytes | str):
    class Msg:
        def __init__(self, channel, message):
            self.channel = channel
            self.message = message

    return Msg(channel, message)


@pytest.mark.asyncio
async def test_logging_handler_owns_its_own_connection(monkeypatch):
    """With a typed ValkeyConfig the handler is built once on connect with
    valkey_config= (owning its own connection); the worker no longer injects or
    re-points its client (AR-059/061)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    _patch_glide_and_handler(monkeypatch, create_mock)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    assert worker._valkey_logger_handler is None  # not built until connect

    ok = await worker.connect()
    assert ok is True
    handler = worker._valkey_logger_handler
    assert handler is not None
    assert isinstance(handler, FakeHandler)
    assert handler._owns_client is True, "typed ValkeyConfig -> handler owns its connection"
    assert handler.client is None, "handler must not be injected the worker's client"
    assert handler.valkey_config is not None, "handler must be handed a valkey_config dict"
    assert handler.valkey_config["addresses"] == [("localhost", 6379)]


@pytest.mark.asyncio
async def test_logging_handler_falls_back_to_client_injection_with_raw_config(monkeypatch):
    """A raw GlideClientConfiguration has no typed ValkeyConfig to hand the
    handler, so it keeps the shared-client injection seam (AR-059/061)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    import scietex.service.valkey.worker as mod
    from scietex.service.valkey._glide import GlideClientConfiguration, NodeAddress

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    raw_config = GlideClientConfiguration(addresses=[NodeAddress("localhost", 6379)])
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=raw_config))
    ok = await worker.connect()
    assert ok is True
    handler = worker._valkey_logger_handler
    assert handler is not None
    assert handler._owns_client is False, "raw config -> handler shares the worker's client"
    assert handler.client is worker.client


def test_logging_handler_config_translates_typed_config():
    """_logging_handler_config maps a typed ValkeyConfig onto the external
    handler's scalar dict schema (addresses + credentials + TLS + timeouts)."""
    from scietex.service.valkey.config import ValkeyBaseConfig, ValkeyNode, ValkeyUserCredentials
    from scietex.service.valkey.worker import _logging_handler_config

    cfg = ValkeyConfig(
        base_config=ValkeyBaseConfig(
            nodes=[ValkeyNode(host="redis.internal", port=6380)],
            user_credentials=ValkeyUserCredentials(username="svc", password="secret"),
            use_tls=True,
            request_timeout=7500,
            database_id=2,
            client_name="logger",
        )
    )
    assert _logging_handler_config(cfg) == {
        "addresses": [("redis.internal", 6380)],
        "username": "svc",
        "password": "secret",
        "use_tls": True,
        "request_timeout": 7500,
        "database_id": 2,
        "client_name": "logger",
        "inflight_requests_limit": None,
        "client_az": None,
        "lazy_connect": None,
    }


@pytest.mark.asyncio
async def test_connect_ping_failure_clears_client(monkeypatch):
    # A failed PING must leave _client as None so initialize() does not
    # treat the worker as connected (AR-006).
    async def create_mock(cfg):
        return DummyClient(ping_ok=False)

    import scietex.service.valkey.worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    ok = await worker.connect()
    assert ok is False
    assert worker.client is None, "failed ping must clear _client"


@pytest.mark.asyncio
async def test_connect_create_failure_leaves_client_none(monkeypatch):
    # A GlideClient.create exception must leave _client as None (AR-006).
    async def create_mock(cfg):
        raise RuntimeError("create failed")

    import scietex.service.valkey.worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    ok = await worker.connect()
    assert ok is False
    assert worker.client is None


@pytest.mark.asyncio
async def test_initialize_group_already_exists_succeeds(monkeypatch):
    # A BUSYGROUP error means the consumer group already exists and must be
    # ignored; initialize still reports success (AR-021).
    import scietex.service.valkey.worker as mod

    async def create_mock(cfg):
        return DummyClient(
            ping_ok=True,
            xgroup_create_error=mod.RequestError("BUSYGROUP Consumer Group name already exists"),
        )

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    ok = await worker.initialize()
    assert ok is True


@pytest.mark.asyncio
async def test_initialize_group_create_failure_fails(monkeypatch):
    # A genuine xgroup_create failure must fail initialize so the worker
    # does not run with no consumer group (AR-021).
    import scietex.service.valkey.worker as mod

    async def create_mock(cfg):
        return DummyClient(ping_ok=True, xgroup_create_error=mod.RequestError("NOAUTH Authentication required"))

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
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
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client

    await worker.fetch_tasks()

    assert client.acked == [], "fetch_tasks must not ack on enqueue"
    assert client.deleted == [], "fetch_tasks must not delete on enqueue"
    assert not worker.task_queue_empty()
    t_id, t_data = worker.dequeue_task()
    assert t_data.task == "dummy"
    assert worker._task_entry_ids[t_id] == b"1-0"


@pytest.mark.asyncio
async def test_fetch_tasks_reads_batch_and_reports_enqueued():
    """fetch_tasks must read up to task_fetch_batch_size entries per XREADGROUP
    and return True when it enqueued at least one task (AR-042)."""
    import msgspec

    from scietex.service.task_handler.schemas import TaskData

    task_data = TaskData(task="dummy", payload=b"{}")
    payload = msgspec.msgpack.encode(task_data)
    client = DummyClient(xreadgroup_result=_entry(b"1-0", "11111111-1111-1111-1111-111111111111", payload))
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig(), task_fetch_batch_size=25))
    worker._client = client

    enqueued = await worker.fetch_tasks()

    assert enqueued is True, "fetch_tasks must report that it enqueued a task"
    assert client.xreadgroup_calls, "fetch_tasks must call XREADGROUP"
    options = client.xreadgroup_calls[0][3]
    assert options.count == 25, "XREADGROUP must read task_fetch_batch_size entries per call"
    assert not worker.task_queue_empty()


@pytest.mark.asyncio
async def test_fetch_tasks_reports_nothing_when_stream_empty():
    """fetch_tasks must return False when no entries are read, so the intake
    manager backs off instead of busy-polling (AR-042)."""
    client = DummyClient(xreadgroup_result=None)
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client

    enqueued = await worker.fetch_tasks()

    assert enqueued is False, "fetch_tasks must report nothing enqueued on an empty read"
    assert worker.task_queue_empty()


@pytest.mark.asyncio
async def test_fetch_tasks_reconnects_on_glide_error(monkeypatch):
    """A glide error during XREADGROUP tears down the dead client and
    reconnects; the reconnect is limited to glide errors only (AR-054/059)."""
    import scietex.service.valkey.worker as mod

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    # Patch only the client factory and handler; leave the real glide error
    # classes intact so the narrowed except tuple is what actually runs.
    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "AsyncValkeyHandler", FakeHandler)

    client = DummyClient(xreadgroup_error=mod.RequestError("connection dropped"))
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client
    worker._recovered = True  # skip recovery; exercise the XREADGROUP path only

    enqueued = await worker.fetch_tasks()

    assert enqueued is False
    assert client.closed is True, "glide error must tear down the dead client"
    assert worker.client is not None, "glide error must reconnect"
    assert worker.client is not client, "a fresh client must be created on reconnect"


@pytest.mark.asyncio
async def test_fetch_tasks_propagates_non_glide_error():
    """A non-glide exception (e.g. a code bug) must propagate without tearing
    down the connection (AR-054/059)."""
    client = DummyClient(xreadgroup_error=ValueError("msgpack encode bug"))
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client
    worker._recovered = True

    with pytest.raises(ValueError, match="msgpack encode bug"):
        await worker.fetch_tasks()

    assert client.closed is False, "non-glide error must not trigger reconnect"
    assert worker.client is client, "client must survive a non-glide error"


@pytest.mark.asyncio
async def test_on_task_completed_acks_and_deletes_entry():
    """on_task_completed must XACK+XDEL the recorded entry id and clear the map (AR-005)."""
    from uuid import UUID

    client = DummyClient()
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
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
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client

    await worker._recover_pending_tasks()

    assert client.xautoclaim_calls[0][3] == 1000  # min_idle_time_ms
    assert not worker.task_queue_empty()
    t_id, t_data = worker.dequeue_task()
    assert t_data.task == "dummy"
    assert worker._task_entry_ids[t_id] == b"9-0"


@pytest.mark.asyncio
async def test_recover_pending_tasks_uses_configured_claim_min_idle_ms():
    """_recover_pending_tasks passes the configured claim_min_idle_ms to
    XAUTOCLAIM instead of the default floor (AR-062)."""
    import msgspec

    from scietex.service.task_handler.schemas import TaskData

    task_data = TaskData(task="dummy", payload=b"{}")
    payload = msgspec.msgpack.encode(task_data)
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ]
    )
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig(), claim_min_idle_ms=5000))
    worker._client = client

    await worker._recover_pending_tasks()

    assert client.xautoclaim_calls[0][3] == 5000  # min_idle_time_ms
    assert not worker.task_queue_empty()


@pytest.mark.asyncio
async def test_recover_pending_tasks_incomplete_when_queue_full_leaves_recovered_false():
    """A queue-full mid-recovery returns incomplete and must NOT let fetch_tasks
    mark recovery done, so the remaining pending entries are retried (AR-051)."""
    import msgspec

    from scietex.service.task_handler.schemas import TaskData

    task_data = TaskData(task="dummy", payload=b"{}")
    payload = msgspec.msgpack.encode(task_data)
    # Two pending entries but a queue that holds only one: the second enqueue
    # hits the full queue, so recovery stops before draining.
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {
                b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]],
                b"9-1": [[b"33333333-3333-3333-3333-333333333333", payload]],
            },
            [],
        ]
    )
    worker = ValkeyWorker(ValkeyWorkerConfig(queue_size=1, max_concurrent_tasks=1, valkey_config=ValkeyConfig()))
    worker._client = client
    assert worker._recovered is False

    # First entry is enqueued; the second finds the queue full -> incomplete.
    recovery_complete, enqueued = await worker._recover_pending_tasks()
    assert recovery_complete is False
    assert enqueued is True

    # fetch_tasks retries recovery instead of skipping it: incomplete recovery
    # must not set _recovered=True.
    await worker.fetch_tasks()
    assert worker._recovered is False


@pytest.mark.asyncio
async def test_recover_pending_tasks_complete_sets_recovered():
    """A fully-drained recovery reports complete and fetch_tasks marks
    _recovered once it has drained (AR-051)."""
    import msgspec

    from scietex.service.task_handler.schemas import TaskData

    task_data = TaskData(task="dummy", payload=b"{}")
    payload = msgspec.msgpack.encode(task_data)
    # Single pending entry and a default-sized queue: recovery drains fully.
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ]
    )
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client
    assert worker._recovered is False

    recovery_complete, enqueued = await worker._recover_pending_tasks()
    assert recovery_complete is True
    assert enqueued is True

    # fetch_tasks marks recovery done only on completion.
    worker._recovered = False
    ok = await worker.fetch_tasks()
    assert ok is True
    assert worker._recovered is True


def test_two_workers_share_stream_group_differ_in_consumer_status():
    a = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig()))
    b = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig()))

    assert a._task_stream_name == b._task_stream_name == "scietex:svc:tasks"
    assert a._task_group_name == b._task_group_name == "scietex:svc:task_group"
    assert a._consumer_name != b._consumer_name
    assert a._heartbeat_key != b._heartbeat_key
    assert a._consumer_name == f"scietex:svc:{a.instance_id}"
    assert a._heartbeat_key == f"scietex:svc:{a.instance_id}:status"


def test_auto_tune_derives_concurrency_from_cpu_count():
    """auto_tune is inherited from TaskProcessorConfig: a ValkeyWorkerConfig with
    auto_tune=True and no max_concurrent_tasks derives it from the CPU count."""
    import os

    worker = ValkeyWorker(ValkeyWorkerConfig(auto_tune=True, valkey_config=ValkeyConfig()))
    assert worker.max_concurrent_tasks == max(1, os.cpu_count() or 1)


@pytest.mark.asyncio
async def test_disconnect_closes_operational_client_but_not_owned_handler(monkeypatch):
    """disconnect closes the worker's operational client once; the owned
    logging handler's connection is independent and left untouched
    (AR-059/061)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    _patch_glide_and_handler(monkeypatch, create_mock)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    await worker.connect()
    client = worker.client
    handler = worker._valkey_logger_handler
    assert handler is not None
    assert handler.client is None  # owned, never injected

    await worker.disconnect()
    assert client.closed is True
    assert worker.client is None
    assert handler.client is None, "disconnect must not null an owned handler's client"


@pytest.mark.asyncio
async def test_cleanup_stops_logging_before_disconnect(monkeypatch):
    """cleanup must stop the valkey logging handler before disconnect() closes
    the shared client, so the handler drains remaining records through the
    still-open client instead of reconnecting to a closed one (AR-022)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    import scietex.service.valkey.worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    ok = await worker.connect()
    assert ok is True
    handler = worker._valkey_logger_handler
    client = worker.client
    assert handler is not None
    assert client is not None

    events: list[str] = []

    original_stop_logging = handler.stop_logging

    async def spy_stop_logging(*args, **kwargs):
        events.append("stop_logging")
        return await original_stop_logging(*args, **kwargs)

    original_close = client.close

    async def spy_close():
        events.append("close")
        return await original_close()

    handler.stop_logging = spy_stop_logging
    client.close = spy_close

    await worker.cleanup()

    assert "stop_logging" in events, "cleanup must stop the valkey logging handler"
    assert "close" in events, "cleanup must close the client via disconnect()"
    assert events.index("stop_logging") < events.index("close"), (
        "stop_logging must run before disconnect() closes the shared client"
    )
    assert client.closed is True
    assert worker.client is None


@pytest.mark.asyncio
async def test_cleanup_clears_pending_task_entry_ids(monkeypatch):
    """cleanup must clear _task_entry_ids so entries for tasks whose handlers
    ignored cancellation do not leak across repeated stop/start cycles
    (AR-050)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    import scietex.service.valkey.worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    ok = await worker.connect()
    assert ok is True

    # Simulate a task whose handler ignored cancellation and is still tracked.
    worker._task_entry_ids[UUID("12345678-1234-5678-1234-567812345678")] = b"1-0"

    await worker.cleanup()

    assert worker._task_entry_ids == {}


@pytest.mark.asyncio
async def test_first_heartbeat_writes_status_key_promptly():
    """The first ValkeyWorker heartbeat must write the status key promptly.

    ``heartbeat()`` is guarded by ``self.client and self.start_time``. To
    guarantee the heartbeat manager's immediate first beat is not skipped,
    ``_startup`` must set ``start_time`` before the managers start (AR-049).
    Because manager tasks only run once ``_startup`` yields to the event loop,
    an end-to-end timing check cannot distinguish the old from the new ordering,
    so this asserts the ordering invariant directly (start_time is already set
    when managers begin) and confirms the status-key write fires on startup.
    """

    class TestWorker(ValkeyWorker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._dummy = DummyClient()
            self.managers_saw_start_time = False

        async def initialize(self) -> bool:
            # Bypass connect()/xgroup_create and inject a DummyClient directly
            # so no real Valkey server is required (mirrors the file's pattern).
            self._client = self._dummy
            return True

        async def _startup(self):
            # AR-049: the heartbeat manager fires its first beat immediately, so
            # start_time must already be set when the managers begin. The base
            # _startup sets start_time just before it starts the managers; wrap
            # the manager-start call to capture the ordering invariant at the
            # moment the managers actually begin.
            original = self._manager_runtime.start_managers

            async def record_then_start():
                self.managers_saw_start_time = self.start_time is not None
                await original()

            self._manager_runtime.start_managers = record_then_start
            try:
                await super()._startup()
            finally:
                self._manager_runtime.start_managers = original

    worker = TestWorker(
        ValkeyWorkerConfig(
            service_name="hb_test",
            heartbeat_interval=1.0,
            valkey_config=ValkeyConfig(),
        )
    )
    await worker.start()
    try:
        # A status-key write must appear promptly after start (the immediate
        # first beat) rather than only on the second beat a full interval later.
        key = worker._heartbeat_key
        for _ in range(100):
            if any(k == key for k, _value, _expiry in worker._dummy.sets):
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("heartbeat() never wrote the status key (AR-049)")

        assert worker.managers_saw_start_time, (
            "start_time must be set before the managers start, or the first "
            "heartbeat is skipped by the start_time guard (AR-049)"
        )
    finally:
        await worker.stop()
