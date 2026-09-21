"""ValkeyWorker task fetching, batching, and queue-full tests."""

from uuid import UUID

import pytest

import scietex.service.valkey.worker as mod
from scietex.service import ValkeyWorker
from scietex.service.task_handler import CANCEL_TASK_NAME
from scietex.service.task_handler.schemas import TaskData
from scietex.service.task_handler.wire import encode_task_envelope
from scietex.service.valkey._glide import ExpirySet, ExpiryType
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig

from ._helpers import DummyClient, FakeHandler, _entry


@pytest.mark.asyncio
async def test_fetch_tasks_does_not_ack_on_enqueue():
    """fetch_tasks must not XACK/XDEL on enqueue; it records the entry id so
    the entry stays pending until the handler completes (AR-005)."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    task_data = TaskData(task_id=str(t_id), task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    client = DummyClient(xreadgroup_result=_entry(b"1-0", payload))
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client

    await worker.fetch_tasks()

    assert client.acked == [], "fetch_tasks must not ack on enqueue"
    assert client.deleted == [], "fetch_tasks must not delete on enqueue"
    assert not worker.task_queue_empty()
    t_data = worker.dequeue_task()
    assert t_data is not None
    assert t_data.task == "dummy"
    assert worker._task_entry_ids[t_id] == b"1-0"


@pytest.mark.asyncio
async def test_fetch_tasks_reads_batch_and_reports_enqueued():
    """fetch_tasks must read up to task_fetch_batch_size entries per XREADGROUP
    and return True when it enqueued at least one task (AR-042)."""
    task_data = TaskData(task_id="11111111-1111-1111-1111-111111111111", task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    client = DummyClient(xreadgroup_result=_entry(b"1-0", payload))
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

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    # Patch only the handler; leave the real glide error classes intact so the
    # narrowed except tuple is what actually runs.
    monkeypatch.setattr(mod, "AsyncValkeyHandler", FakeHandler)

    client = DummyClient(xreadgroup_error=mod.RequestError("connection dropped"))
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    worker._client = client
    worker._transport.recovered = True  # skip recovery; exercise the XREADGROUP path only

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
    worker._transport.recovered = True

    with pytest.raises(ValueError, match="msgpack encode bug"):
        await worker.fetch_tasks()

    assert client.closed is False, "non-glide error must not trigger reconnect"
    assert worker.client is client, "client must survive a non-glide error"


@pytest.mark.asyncio
async def test_fetch_tasks_writes_lease_on_enqueue_accept():
    """fetch_tasks acquires the per-entry lease at enqueue-accept: the entry id
    is recorded in _task_entry_ids and the lease key is written with the
    consumer-name value and the derived TTL."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    task_data = TaskData(task_id=str(t_id), task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    client = DummyClient(xreadgroup_result=_entry(b"1-0", payload))
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client
    worker._transport.recovered = True  # skip recovery; exercise the XREADGROUP path only

    await worker.fetch_tasks()

    assert worker._task_entry_ids[t_id] == b"1-0"
    assert len(client.sets) == 1
    lease_key, lease_value, lease_expiry = client.sets[0]
    assert lease_key == worker._task_lease.key(t_id)
    assert lease_value == worker._consumer_name.encode("utf-8")
    assert lease_expiry == ExpirySet(ExpiryType.SEC, 20)


@pytest.mark.asyncio
async def test_fetch_tasks_queue_full_does_not_write_lease():
    """A full queue at fetch time leaves the stream entry pending: no lease is
    written, the entry is not recorded, and the stream is not acked."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    task_data = TaskData(task_id=str(t_id), task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    client = DummyClient(xreadgroup_result=_entry(b"1-0", payload))
    worker = ValkeyWorker(ValkeyWorkerConfig(queue_size=1, max_concurrent_tasks=1, valkey_config=ValkeyConfig()))
    worker._client = client
    worker._transport.recovered = True  # skip recovery; exercise the XREADGROUP path only
    filler = UUID("99999999-9999-9999-9999-999999999999")
    assert worker.enqueue_task(TaskData(task_id=str(filler), task="dummy", payload=b"{}")) is True

    enqueued = await worker.fetch_tasks()

    assert enqueued is False
    assert client.sets == []
    assert t_id not in worker._task_entry_ids
    assert client.acked == []


@pytest.mark.asyncio
async def test_fetch_tasks_defers_then_flushes_when_room():
    """A full data lane defers a claimed entry instead of dropping it (no lease
    written, entry id not recorded); once the lane has room, the next fetch
    flushes it (enqueued, entry id recorded, lease written)."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    task_data = TaskData(task_id=str(t_id), task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    client = DummyClient(xreadgroup_result=_entry(b"1-0", payload))
    worker = ValkeyWorker(ValkeyWorkerConfig(queue_size=1, max_concurrent_tasks=1, valkey_config=ValkeyConfig()))
    worker._client = client
    worker._transport.recovered = True  # skip recovery; exercise the XREADGROUP path only
    filler = UUID("99999999-9999-9999-9999-999999999999")
    assert worker.enqueue_task(TaskData(task_id=str(filler), task="dummy", payload=b"{}")) is True

    enqueued = await worker.fetch_tasks()

    # The claimed entry is held, not dropped: no lease, no entry id, one deferred.
    assert enqueued is False
    assert client.sets == []
    assert t_id not in worker._task_entry_ids
    assert len(worker._transport._deferred) == 1

    # Make room in the data lane; the next fetch flushes the deferred entry.
    assert worker.dequeue_task() is not None
    client.xreadgroup_result = None  # no new entries on the follow-up read
    enqueued = await worker.fetch_tasks()

    assert enqueued is True
    assert len(worker._transport._deferred) == 0
    assert worker._task_entry_ids[t_id] == b"1-0"
    assert len(client.sets) == 1


@pytest.mark.asyncio
async def test_fetch_tasks_stops_reading_when_deferred_full():
    """With the deferred buffer at task_fetch_batch_size, fetch returns without
    issuing a new XREADGROUP so unprocessable entries cannot accumulate without
    bound."""
    client = DummyClient(xreadgroup_result=None)
    worker = ValkeyWorker(
        ValkeyWorkerConfig(
            queue_size=1,
            max_concurrent_tasks=1,
            task_fetch_batch_size=2,
            valkey_config=ValkeyConfig(),
        )
    )
    worker._client = client
    worker._transport.recovered = True  # skip recovery; exercise the XREADGROUP path only
    filler = UUID("99999999-9999-9999-9999-999999999999")
    assert worker.enqueue_task(TaskData(task_id=str(filler), task="dummy", payload=b"{}")) is True
    # Seed the deferred buffer to capacity; the full data lane keeps the flush
    # from draining it, so the backpressure guard is what short-circuits the read.
    for i in range(2):
        d_id = UUID(f"aaaaaaaa-aaaa-aaaa-aaaa-{i:012d}")
        worker._transport._deferred.append((d_id, TaskData(task_id=str(d_id), task="dummy", payload=b"{}"), b"1-0"))

    enqueued = await worker.fetch_tasks()

    assert enqueued is False
    assert client.xreadgroup_calls == [], "fetch must not XREADGROUP while the deferred buffer is full"
    assert len(worker._transport._deferred) == 2


@pytest.mark.asyncio
async def test_fetch_tasks_delivers_control_when_data_full():
    """A control command read from the directed control stream is delivered even
    when the data lane is full; the data entry is deferred, and neither writes a
    lease (control entries are never leased)."""
    data_id = UUID("11111111-1111-1111-1111-111111111111")
    control_id = UUID("22222222-2222-2222-2222-222222222222")
    data_payload = encode_task_envelope(TaskData(task_id=str(data_id), task="dummy", payload=b"{}"))
    control_payload = encode_task_envelope(TaskData(task_id=str(control_id), task=CANCEL_TASK_NAME))
    # The data entry arrives on the XREADGROUP task stream; the control command
    # arrives on the directed control stream via the plain XREAD poll (the first
    # of the two control reads; the broadcast read returns nothing).
    client = DummyClient(
        xreadgroup_result=_entry(b"1-0", data_payload),
        xread_results=[_entry(b"1-0", control_payload), None],
    )
    worker = ValkeyWorker(ValkeyWorkerConfig(queue_size=1, max_concurrent_tasks=1, valkey_config=ValkeyConfig()))
    worker._client = client
    worker._transport.recovered = True  # skip recovery; exercise the read paths only
    filler = UUID("99999999-9999-9999-9999-999999999999")
    assert worker.enqueue_task(TaskData(task_id=str(filler), task="dummy", payload=b"{}")) is True

    enqueued = await worker.fetch_tasks()

    assert enqueued is True
    assert not worker.control_queue_empty(), "control task must be delivered despite a full data lane"
    assert control_id in worker._control_entry_ids
    assert data_id not in worker._task_entry_ids
    assert len(worker._transport._deferred) == 1
    assert client.sets == [], "deferred data and control entries write no lease"
