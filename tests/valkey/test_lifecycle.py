"""ValkeyWorker construction, configuration, lifecycle, and cleanup tests."""

import asyncio
import os
from datetime import datetime, timezone
from uuid import UUID

import pytest

import scietex.service.valkey.worker as mod
from scietex.service import ValkeyWorker
from scietex.service.task_handler.schemas import TaskData
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig

from ._helpers import DummyClient, _make_tracking_worker


def test_default_config_stored_as_concrete_type(tmp_path, monkeypatch):
    """When constructed with ``config=None`` the base must instantiate the
    concrete ``ValkeyWorkerConfig`` (not a bare ``WorkerConfig``/
    ``TaskProcessorConfig``), because the worker declares ``_config_type``
    (AR-069). Point ``SCIETEX_CONFIG_DIR`` at a tmp dir so the default
    ``valkey.yml`` bootstrap writes there, not into the user's home."""
    monkeypatch.setenv("SCIETEX_CONFIG_DIR", str(tmp_path))
    worker = ValkeyWorker()
    assert isinstance(worker._config, ValkeyWorkerConfig)


def test_construction_without_config_has_no_filesystem_side_effects(tmp_path, monkeypatch):
    """Constructing ``ValkeyWorker()`` with no explicit config must not write
    ``valkey.yml`` or create the config dir; the disk read is deferred to the
    first connect (AR-066)."""
    monkeypatch.setenv("SCIETEX_CONFIG_DIR", str(tmp_path))
    worker = ValkeyWorker()
    assert list(tmp_path.iterdir()) == [], "construction must not write valkey.yml or mkdir"
    assert worker.valkey_config is None
    assert worker._client_config is None


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
    worker = ValkeyWorker(ValkeyWorkerConfig(auto_tune=True, valkey_config=ValkeyConfig()))
    assert worker.max_concurrent_tasks == max(1, os.cpu_count() or 1)


@pytest.mark.asyncio
async def test_cleanup_stops_logging_before_disconnect(monkeypatch):
    """cleanup must stop the valkey logging handler before disconnect() closes
    the shared client, so the handler drains remaining records through the
    still-open client instead of reconnecting to a closed one (AR-022)."""

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
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

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
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


@pytest.mark.asyncio
async def test_heartbeat_refreshes_directed_control_stream_ttl():
    """heartbeat() refreshes the directed control stream TTL on the same tick
    as the status key, so a live worker keeps its stream alive (AR-123 §4.3)."""
    client = DummyClient()
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig()))
    worker._client = client
    worker._lifecycle.start_time = datetime.now(timezone.utc)

    await worker.heartbeat()

    assert client.expired == [(worker._control_stream_name, int(worker.active_ttl))]


@pytest.mark.asyncio
async def test_heartbeat_does_not_expire_broadcast_control_stream():
    """The broadcast control stream is service-scoped: it has no owner to
    refresh a TTL, so heartbeat() must leave it untouched (AR-123 §4.3)."""
    client = DummyClient()
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig()))
    worker._client = client
    worker._lifecycle.start_time = datetime.now(timezone.utc)

    await worker.heartbeat()

    assert client.expired
    assert all(name != worker._control_broadcast_stream_name for name, *_ in client.expired)


@pytest.mark.asyncio
async def test_heartbeat_expire_failure_is_reported_not_raised():
    """A glide error from the directed-stream EXPIRE is caught by the status
    write's handler and reported into health, never raised (AR-123 §4.3)."""
    client = DummyClient(expire_error=mod.RequestError("expire failed"))
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig()))
    worker._client = client
    worker._lifecycle.start_time = datetime.now(timezone.utc)

    await worker.heartbeat()

    assert worker.transport_health.degraded is True
    assert worker.transport_health.last_error == "expire failed"


@pytest.mark.asyncio
async def test_shutdown_drain_deletes_queued_lease():
    """cleanup() drains the queue: a still-queued task's lease is deleted, its
    stream entry is left pending (not XACK/XDEL'd), and ownership is cleared."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)
    worker.enqueue_task(TaskData(task_id=str(t_id), task="dummy", payload=b"{}"))
    worker._task_entry_ids[t_id] = b"1-0"

    await worker.cleanup()

    assert client.deleted_keys == [[worker._task_lease.key(t_id)]]
    assert client.acked == []
    assert client.deleted == []
    assert worker._task_entry_ids == {}
