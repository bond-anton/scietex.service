"""ValkeyWorker pending-entry recovery (XAUTOCLAIM) tests, including AR-051 and AR-060 semantics."""

from uuid import UUID

import pytest

import scietex.service.valkey.worker as mod
from scietex.service import ValkeyWorker
from scietex.service.task_handler.schemas import TaskData
from scietex.service.task_handler.wire import encode_task_envelope
from scietex.service.valkey._glide import ExpirySet, ExpiryType
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig

from ._helpers import DummyClient


@pytest.mark.asyncio
async def test_recover_pending_tasks_enqueues_pending_entries():
    """_recover_pending_tasks must claim idle pending entries and enqueue them,
    recording their entry ids for later ack (AR-005)."""
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
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
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
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
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
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
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
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
    # The second pass re-runs recovery: the entry is already owned locally
    # (recorded in _task_entry_ids during the first pass), so it is skipped
    # without a lease check and nothing is enqueued (AR-060).
    assert ok is False
    assert worker._recovered is True


@pytest.mark.asyncio
async def test_recover_pending_tasks_skips_entry_with_held_lease():
    """A pending entry whose lease is held by a live worker is skipped: not
    enqueued, not recorded, and recovery reports incomplete (AR-060)."""
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    t_id = UUID("22222222-2222-2222-2222-222222222222")
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ],
        get_values={worker._task_lease.key(t_id): b"other"},
    )
    worker._client = client

    recovery_complete, enqueued = await worker._recover_pending_tasks()

    assert (recovery_complete, enqueued) == (False, False)
    assert worker.task_queue_empty()
    assert t_id not in worker._task_entry_ids
    assert client.xautoclaim_calls[0][3] == 1000


@pytest.mark.asyncio
async def test_recover_pending_tasks_reclaims_entry_when_lease_absent():
    """With no lease present, recovery reclaims the entry: enqueued, recorded,
    and recovery reports complete (AR-060)."""
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    t_id = UUID("22222222-2222-2222-2222-222222222222")
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ]
    )
    worker._client = client

    recovery_complete, enqueued = await worker._recover_pending_tasks()

    assert (recovery_complete, enqueued) == (True, True)
    assert not worker.task_queue_empty()
    dequeued_id, dequeued_data = worker.dequeue_task()
    assert dequeued_id == t_id
    assert dequeued_data.task == "dummy"
    assert worker._task_entry_ids[t_id] == b"9-0"


@pytest.mark.asyncio
async def test_recover_pending_tasks_skips_locally_inflight_entry():
    """An entry already owned locally (in _task_entry_ids) is skipped without
    marking recovery incomplete, and is not enqueued twice (AR-060)."""
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    t_id = UUID("22222222-2222-2222-2222-222222222222")
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ]
    )
    worker._client = client
    worker._task_entry_ids[t_id] = b"9-0"

    recovery_complete, enqueued = await worker._recover_pending_tasks()

    assert recovery_complete is True
    assert enqueued is False
    assert worker.task_queue_empty()
    assert worker._task_entry_ids[t_id] == b"9-0"


@pytest.mark.asyncio
async def test_recover_pending_tasks_incomplete_when_lease_held_leaves_recovered_false():
    """fetch_tasks must not mark _recovered done when recovery found a
    held-lease entry and left it pending (AR-060)."""
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    t_id = UUID("22222222-2222-2222-2222-222222222222")
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ],
        get_values={worker._task_lease.key(t_id): b"other"},
    )
    worker._client = client
    assert worker._recovered is False

    await worker.fetch_tasks()

    assert worker._recovered is False


@pytest.mark.asyncio
async def test_recover_pending_tasks_lease_acquire_error_fails_safe():
    """A glide error during atomic lease acquisition is fail-safe: the entry is
    still reclaimed (enqueued) and recovery reports complete. The atomic
    ``SET ... NX`` cannot distinguish "held" from "transport failed", so it
    defaults to proceeding rather than risking a stuck pending entry (AR-060)."""
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ],
        set_error=mod.RequestError("acquire failed"),
    )
    worker._client = client

    recovery_complete, enqueued = await worker._recover_pending_tasks()

    assert recovery_complete is True
    assert enqueued is True
    dequeued_id, _data = worker.dequeue_task()
    assert dequeued_id == UUID("22222222-2222-2222-2222-222222222222")


@pytest.mark.asyncio
async def test_recover_pending_tasks_lease_skip_and_reclaim_mixed():
    """In one batch, a held-lease entry is skipped while a free entry is
    reclaimed; recovery reports incomplete (AR-060)."""
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    leased_id = UUID("22222222-2222-2222-2222-222222222222")
    free_id = UUID("33333333-3333-3333-3333-333333333333")
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {
                b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]],
                b"9-1": [[b"33333333-3333-3333-3333-333333333333", payload]],
            },
            [],
        ],
        get_values={worker._task_lease.key(leased_id): b"other"},
    )
    worker._client = client

    recovery_complete, enqueued = await worker._recover_pending_tasks()

    assert recovery_complete is False
    assert enqueued is True
    assert leased_id not in worker._task_entry_ids
    assert worker._task_entry_ids[free_id] == b"9-1"
    dequeued_id, _data = worker.dequeue_task()
    assert dequeued_id == free_id
    assert worker.task_queue_empty()


@pytest.mark.asyncio
async def test_recover_pending_tasks_writes_lease_on_enqueue_accept():
    """_recover_pending_tasks writes the lease at enqueue-accept: the entry is
    enqueued, recorded, and its lease is atomically acquired."""
    t_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ]
    )
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = client

    recovery_complete, enqueued = await worker._recover_pending_tasks()

    assert (recovery_complete, enqueued) == (True, True)
    assert worker._task_entry_ids[t_id] == b"9-0"
    assert len(client.sets) == 1
    lease_key, lease_value, lease_expiry = client.sets[0]
    assert lease_key == worker._task_lease.key(t_id)
    assert lease_value == worker._consumer_name.encode("utf-8")
    assert lease_expiry == ExpirySet(ExpiryType.SEC, 20)


@pytest.mark.asyncio
async def test_recover_pending_tasks_queue_full_rolls_back_lease():
    """When recovery hits a full queue, the lease acquired for the rejected
    entry is rolled back (deleted) and the entry is not recorded."""
    t_id = UUID("22222222-2222-2222-2222-222222222222")
    task_data = TaskData(task="dummy", payload=b"{}")
    payload = encode_task_envelope(task_data)
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ]
    )
    worker = ValkeyWorker(ValkeyWorkerConfig(queue_size=1, max_concurrent_tasks=1, valkey_config=ValkeyConfig()))
    worker._client = client
    filler = UUID("99999999-9999-9999-9999-999999999999")
    assert worker.enqueue_task(filler, TaskData(task="dummy", payload=b"{}")) is True

    recovery_complete, enqueued = await worker._recover_pending_tasks()

    assert recovery_complete is False
    assert enqueued is False
    assert client.deleted_keys == [[worker._task_lease.key(t_id)]]
    assert t_id not in worker._task_entry_ids
