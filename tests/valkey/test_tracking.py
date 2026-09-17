"""ValkeyWorker task tracking-record tests: on_task_started, on_task_completed, and progress writes."""

from uuid import UUID

import msgspec
import pytest

import scietex.service.valkey.worker as mod
from scietex.service import ValkeyWorker
from scietex.service.task_handler.schemas import TaskData, TaskProgress, TaskResult, TaskStatus
from scietex.service.valkey._glide import ExpirySet, ExpiryType
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig

from ._helpers import DummyClient, _make_tracking_worker


@pytest.mark.asyncio
async def test_on_task_completed_acks_and_deletes_entry():
    """on_task_completed must XACK+XDEL the recorded entry id and clear the map (AR-005)."""
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
async def test_on_task_started_writes_running_tracking_record():
    """on_task_started writes a `running` TaskStatus record to the
    scietex:{service}:task:{task_id} key with the configured TTL."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)

    await worker.on_task_started(t_id, TaskData(task="dummy", payload=b"{}"))

    assert len(client.sets) == 2
    key, value, expiry = client.sets[0]
    assert key == f"scietex:svc:task:{t_id}"
    tracking = msgspec.msgpack.decode(value, type=TaskStatus)
    assert tracking.task_id == str(t_id)
    assert tracking.status == "running"
    assert tracking.task == "dummy"
    assert tracking.service == "svc"
    assert expiry == ExpirySet(ExpiryType.SEC, 3600)
    # The per-entry lease is written alongside the tracking record (AR-060).
    lease_key, lease_value, lease_expiry = client.sets[1]
    assert lease_key == worker._task_lease.key(t_id)
    assert lease_value == worker._consumer_name.encode("utf-8")
    assert lease_expiry == ExpirySet(ExpiryType.SEC, 20)


@pytest.mark.asyncio
async def test_on_task_completed_success_writes_completed_and_acks():
    """A success TaskResult writes a `completed` record carrying the payload,
    then still XACKs + XDELs the entry."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)
    worker._task_entry_ids[t_id] = b"1-0"

    await worker.on_task_completed(
        t_id, TaskData(task="dummy", payload=b"{}"), TaskResult(status="success", payload=b"done")
    )

    assert len(client.sets) == 1
    _key, value, _expiry = client.sets[0]
    tracking = msgspec.msgpack.decode(value, type=TaskStatus)
    assert tracking.status == "completed"
    assert tracking.result == b"done"
    assert client.acked == [(worker._task_stream_name, worker._task_group_name, [b"1-0"])]
    assert client.deleted == [(worker._task_stream_name, [b"1-0"])]


@pytest.mark.asyncio
async def test_on_task_completed_error_writes_failed_with_error():
    """An error TaskResult writes a `failed` record carrying error/error_code."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)

    await worker.on_task_completed(
        t_id,
        TaskData(task="dummy", payload=b"{}"),
        TaskResult(status="error", error="boom", error_code="PERMANENT"),
    )

    assert len(client.sets) == 1
    _key, value, _expiry = client.sets[0]
    tracking = msgspec.msgpack.decode(value, type=TaskStatus)
    assert tracking.status == "failed"
    assert tracking.error == "boom"
    assert tracking.error_code == "PERMANENT"
    assert tracking.result is None


@pytest.mark.asyncio
async def test_on_task_completed_none_writes_failed_canceled():
    """task_result=None (cancellation) writes a `failed` record with error
    'canceled'."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)

    await worker.on_task_completed(t_id, TaskData(task="dummy", payload=b"{}"), None)

    assert len(client.sets) == 1
    _key, value, _expiry = client.sets[0]
    tracking = msgspec.msgpack.decode(value, type=TaskStatus)
    assert tracking.status == "failed"
    assert tracking.error == "canceled"


@pytest.mark.asyncio
async def test_write_task_progress_updates_existing_record():
    """_write_task_progress reads the existing record and writes it back with
    progress={progress: True, value: <v>}, preserving the other fields."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    existing = TaskStatus(task_id=str(t_id), service="svc", task="dummy", status="running")
    client = DummyClient(get_value=msgspec.msgpack.encode(existing))
    worker = _make_tracking_worker(client)

    await worker._write_task_progress(t_id, 42.5)

    assert client.gets == [f"scietex:svc:task:{t_id}"]
    assert len(client.sets) == 1
    key, value, _expiry = client.sets[0]
    assert key == f"scietex:svc:task:{t_id}"
    tracking = msgspec.msgpack.decode(value, type=TaskStatus)
    assert tracking.status == "running"
    assert tracking.task == "dummy"
    assert tracking.progress == TaskProgress(progress=True, value=42.5)


@pytest.mark.asyncio
async def test_write_task_progress_missing_record_is_noop():
    """_write_task_progress drops the update when no tracking record exists:
    the read still happens but nothing is written (AR-095)."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)

    await worker._write_task_progress(t_id, 42.5)

    assert client.gets == [f"scietex:svc:task:{t_id}"]
    assert client.sets == []


@pytest.mark.asyncio
async def test_tracking_write_failure_does_not_raise():
    """A tracking write failure (client raises) must not propagate out of
    on_task_started/on_task_completed: tracking is observability, not
    correctness."""
    client = DummyClient(set_error=mod.RequestError("write failed"))
    worker = _make_tracking_worker(client)
    t_id = UUID("11111111-1111-1111-1111-111111111111")

    await worker.on_task_started(t_id, TaskData(task="dummy", payload=b"{}"))
    await worker.on_task_completed(t_id, TaskData(task="dummy", payload=b"{}"), None)
