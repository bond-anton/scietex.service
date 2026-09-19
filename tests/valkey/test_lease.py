"""ValkeyWorker per-entry lease tests (AR-060): write, acquire, refresh, delete, and TTL derivation."""

import asyncio
import time
from uuid import UUID

import pytest

import scietex.service.valkey.worker as mod
from scietex.service import ValkeyWorker
from scietex.service.task_handler.runtime import TaskTracker
from scietex.service.task_handler.schemas import TaskData, TaskResult
from scietex.service.valkey._glide import ConditionalChange, ExpirySet, ExpiryType
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig
from scietex.service.valkey.lease import derive_task_lease_ttl

from ._helpers import DummyClient, _make_tracking_worker


@pytest.mark.asyncio
async def test_on_task_started_writes_lease_key():
    """on_task_started writes the per-entry lease alongside the tracking record
    (AR-060): the consumer-name bytes under a scietex:{service}:lease:{task_id}
    key with the derived 20s TTL."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)

    await worker.on_task_started(t_id, TaskData(task="dummy", payload=b"{}"))

    assert len(client.sets) == 2
    lease_key, lease_value, lease_expiry = client.sets[1]
    assert lease_key == worker._task_lease.key(t_id)
    assert lease_value == worker._consumer_name.encode("utf-8")
    assert lease_expiry == ExpirySet(ExpiryType.SEC, 20)


@pytest.mark.asyncio
async def test_on_task_completed_deletes_lease_key():
    """on_task_completed clears the per-entry lease in addition to the usual
    XACK + XDEL (AR-060)."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)
    worker._task_entry_ids[t_id] = b"1-0"

    await worker.on_task_completed(
        t_id, TaskData(task="dummy", payload=b"{}"), TaskResult(status="success", payload=b"done")
    )

    assert client.deleted_keys == [[worker._task_lease.key(t_id)]]
    assert client.acked == [(worker._task_stream_name, worker._task_group_name, [b"1-0"])]
    assert client.deleted == [(worker._task_stream_name, [b"1-0"])]


@pytest.mark.asyncio
async def test_on_task_completed_retryable_does_not_delete_lease():
    """A retryable-error ack must not delete the lease (AR-077b): the task was
    already requeued (and its lease released) before the ack, so deleting again
    would clobber a peer's fresh lease for the requeued copy."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)
    worker._task_entry_ids[t_id] = b"1-0"

    await worker.on_task_completed(
        t_id,
        TaskData(task="dummy", payload=b"{}"),
        TaskResult(status="error", error="transient", retryable=True),
    )

    assert client.deleted_keys == []
    assert client.acked == [(worker._task_stream_name, worker._task_group_name, [b"1-0"])]
    assert client.deleted == [(worker._task_stream_name, [b"1-0"])]


@pytest.mark.asyncio
async def test_requeue_deletes_lease():
    """requeue() releases the lease as part of re-queueing (AR-077b): the
    requeued copy reuses the same task_id, so the lease must be cleared for a
    peer to claim it."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)

    await worker._transport.requeue(t_id, TaskData(task="dummy", payload=b"{}"))

    assert client.deleted_keys == [[worker._task_lease.key(t_id)]]
    assert len(client.added) == 1


@pytest.mark.asyncio
async def test_watchdog_refreshes_leases_for_running_tasks():
    """watchdog() renews the per-entry lease for every owned task before
    delegating to the base watchdog. ``_task_entry_ids`` is the authoritative
    ownership map that the lease refresh iterates (AR-060)."""
    t1 = UUID("11111111-1111-1111-1111-111111111111")
    t2 = UUID("22222222-2222-2222-2222-222222222222")
    client = DummyClient()
    worker = _make_tracking_worker(client)

    task_a = asyncio.create_task(asyncio.sleep(100))
    task_b = asyncio.create_task(asyncio.sleep(100))
    try:
        # Seed the ownership map the lease refresh actually iterates, alongside
        # the running trackers the base watchdog inspects.
        worker._task_entry_ids[t1] = b"1-0"
        worker._task_entry_ids[t2] = b"2-0"
        worker._task_lifecycle.register(
            t1,
            TaskTracker(
                worker_task=task_a,
                data=TaskData(task="dummy", payload=b"{}"),
                started=time.monotonic(),
            ),
        )
        worker._task_lifecycle.register(
            t2,
            TaskTracker(
                worker_task=task_b,
                data=TaskData(task="dummy", payload=b"{}"),
                started=time.monotonic(),
            ),
        )

        await worker.watchdog()

        assert len(client.sets) == 2
        keys = {key for key, _value, _expiry in client.sets}
        assert keys == {worker._task_lease.key(t1), worker._task_lease.key(t2)}
        assert all(expiry == ExpirySet(ExpiryType.SEC, 20) for _key, _value, expiry in client.sets)
    finally:
        task_a.cancel()
        task_b.cancel()
        await asyncio.gather(task_a, task_b, return_exceptions=True)


@pytest.mark.asyncio
async def test_watchdog_lease_refresh_noop_without_client():
    """watchdog() must not raise when no client is connected: the lease refresh
    is a no-op and the base watchdog iterates an empty running set (AR-060)."""
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig()))
    worker._client = None

    await worker.watchdog()


def test_lease_ttl_derivation_default_and_configured():
    """The per-entry lease TTL is max(1, int(max(2*heartbeat, 3*watchdog)))
    (AR-060): 20s with defaults, floored at 1s for tiny intervals, and driven
    by the watchdog term when that interval is large."""
    default = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    assert derive_task_lease_ttl(default.heartbeat_interval, default.watchdog_interval) == 20

    tiny = ValkeyWorker(
        ValkeyWorkerConfig(heartbeat_interval=0.1, watchdog_interval=0.01, valkey_config=ValkeyConfig())
    )
    assert derive_task_lease_ttl(tiny.heartbeat_interval, tiny.watchdog_interval) == 1

    large_watchdog = ValkeyWorker(ValkeyWorkerConfig(watchdog_interval=600, valkey_config=ValkeyConfig()))
    assert derive_task_lease_ttl(large_watchdog.heartbeat_interval, large_watchdog.watchdog_interval) == 1800


@pytest.mark.asyncio
async def test_on_task_started_honours_configured_lease_ttl():
    """An explicit ``task_lease_ttl`` overrides the derived default (AR-077a)."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client, task_lease_ttl=5)

    await worker.on_task_started(t_id, TaskData(task="dummy", payload=b"{}"))

    assert client.sets[1][2] == ExpirySet(ExpiryType.SEC, 5)


@pytest.mark.asyncio
async def test_refresh_task_leases_covers_queued_tasks():
    """_refresh_task_leases iterates _task_entry_ids (not running_tasks), so a
    queued-but-undispatched task's lease is renewed too."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    client = DummyClient()
    worker = _make_tracking_worker(client)
    worker._task_entry_ids[t_id] = b"1-0"  # queued: no running tracker

    await worker._transport.refresh_leases()

    assert len(client.sets) == 1
    lease_key, _value, _expiry = client.sets[0]
    assert lease_key == worker._task_lease.key(t_id)


@pytest.mark.asyncio
async def test_refresh_task_leases_covers_queued_and_running():
    """_refresh_task_leases refreshes queued and running tasks alike, because
    _task_entry_ids spans both ownership states."""
    queued_id = UUID("11111111-1111-1111-1111-111111111111")
    running_id = UUID("22222222-2222-2222-2222-222222222222")
    client = DummyClient()
    worker = _make_tracking_worker(client)
    worker._task_entry_ids[queued_id] = b"1-0"  # queued: no running tracker
    worker._task_entry_ids[running_id] = b"2-0"  # running: has a tracker
    task = asyncio.create_task(asyncio.sleep(100))
    try:
        worker._task_lifecycle.register(
            running_id,
            TaskTracker(
                worker_task=task,
                data=TaskData(task="dummy", payload=b"{}"),
                started=time.monotonic(),
            ),
        )

        await worker._transport.refresh_leases()

        keys = {key for key, _value, _expiry in client.sets}
        assert keys == {worker._task_lease.key(queued_id), worker._task_lease.key(running_id)}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_acquire_task_lease_returns_false_when_held():
    """``TaskLeaseManager.acquire`` returns False when the lease key already
    exists (a peer holds it), simulated by seeding get_values."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    client = DummyClient(get_values={worker._task_lease.key(t_id): b"other"})
    worker._client = client

    acquired = await worker._task_lease.acquire(t_id)

    assert acquired is False


@pytest.mark.asyncio
async def test_acquire_task_lease_returns_true_when_absent():
    """``TaskLeaseManager.acquire`` returns True and issues a SET ... NX when
    the lease key is absent."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    client = DummyClient()
    worker._client = client

    acquired = await worker._task_lease.acquire(t_id)

    assert acquired is True
    assert client.set_calls == [(worker._task_lease.key(t_id), ConditionalChange.ONLY_IF_DOES_NOT_EXIST)]


@pytest.mark.asyncio
async def test_acquire_task_lease_defers_on_error():
    """A glide error during SET ... NX is not treated as a won claim:
    ``TaskLeaseManager.acquire`` returns False (defer) rather than raising or
    proceeding (AR-121)."""
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    client = DummyClient(set_error=mod.RequestError("set failed"))
    worker._client = client

    acquired = await worker._task_lease.acquire(t_id)

    assert acquired is False
