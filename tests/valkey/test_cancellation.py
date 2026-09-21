"""ValkeyWorker task cancellation tests: cancel_reason handling, cancelled status, and data embedding."""

from uuid import UUID

import msgspec
import pytest

from scietex.service.task_handler.schemas import TaskData, TaskStatus
from scietex.service.task_handler.wire import decode_task_envelope, encode_task_envelope

from ._helpers import DummyClient, _make_tracking_worker


@pytest.mark.asyncio
async def test_on_task_completed_deliberate_cancel_writes_cancelled_with_data():
    """A deliberate cancel (cancel_reason='deliberate') writes a `cancelled`
    record embedding the original TaskData, then XACKs + XDELs the entry."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)
    worker._task_entry_ids[t_id] = b"1-0"
    task_data = TaskData(task_id=str(t_id), task="dummy", payload=b"original", canceled_action="requeue")

    await worker.on_task_completed(task_data, None, cancel_reason="deliberate")

    assert len(client.sets) == 1
    _key, value, _expiry = client.sets[0]
    tracking = msgspec.msgpack.decode(value, type=TaskStatus)
    assert tracking.status == "cancelled"
    assert tracking.error == "canceled"
    assert tracking.data == task_data
    assert client.acked == [(worker._task_stream_name, worker._task_group_name, [b"1-0"])]
    assert client.deleted == [(worker._task_stream_name, [b"1-0"])]


@pytest.mark.asyncio
async def test_on_task_completed_timeout_cancel_stays_failed():
    """A timeout/shutdown cancellation keeps the existing `failed` status and
    does not embed TaskData."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)

    await worker.on_task_completed(
        TaskData(task_id=str(t_id), task="dummy", payload=b"{}"), None, cancel_reason="timeout"
    )

    assert len(client.sets) == 1
    _key, value, _expiry = client.sets[0]
    tracking = msgspec.msgpack.decode(value, type=TaskStatus)
    assert tracking.status == "failed"
    assert tracking.error == "canceled"
    assert tracking.data is None


@pytest.mark.asyncio
async def test_cancelled_status_round_trips_through_msgpack():
    """A cancelled TaskStatus with embedded TaskData survives a msgpack
    round-trip, so an external process can decode it and resubmit."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    task_data = TaskData(task_id=str(t_id), task="dummy", payload=b"original", canceled_action="requeue")
    status = TaskStatus(
        task_id=str(t_id),
        service="svc",
        task="dummy",
        status="cancelled",
        data=task_data,
        error="canceled",
    )

    decoded = msgspec.msgpack.decode(msgspec.msgpack.encode(status), type=TaskStatus)

    assert decoded.status == "cancelled"
    assert decoded.data == task_data
    # The decoded TaskData can be re-enveloped for resubmission under a new id.
    assert decoded.data is not None
    envelope = encode_task_envelope(decoded.data)
    assert decode_task_envelope(envelope) == task_data


@pytest.mark.asyncio
async def test_cancel_queued_task_deletes_lease_and_acks_entry():
    """Cancelling a queued (undispatched) task XACKs + XDELs its stream entry,
    deletes its lease, and clears its ownership record."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)
    worker.enqueue_task(TaskData(task_id=str(t_id), task="dummy", payload=b"{}"))
    worker._task_entry_ids[t_id] = b"1-0"

    outcome = await worker._cancel_task(t_id)

    assert outcome == "cancelled"
    assert client.acked == [(worker._task_stream_name, worker._task_group_name, [b"1-0"])]
    assert client.deleted == [(worker._task_stream_name, [b"1-0"])]
    assert client.deleted_keys == [[worker._task_lease.key(t_id)]]
    assert t_id not in worker._task_entry_ids
