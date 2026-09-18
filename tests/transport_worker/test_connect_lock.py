"""TransportWorker.connect/disconnect serialization tests (AR-102a)."""

import asyncio

import pytest

from ._helpers import build_worker


@pytest.mark.asyncio
async def test_concurrent_connect_creates_one_client(tmp_path):
    """Concurrent connect() calls serialize behind the client lock, so the
    locked _connect_locked is idempotent and creates exactly one client."""
    worker = build_worker(tmp_path)
    barrier = asyncio.Event()
    worker.connect_barrier = barrier

    first = asyncio.create_task(worker.connect())
    # Yield so the first connect acquires the lock and parks inside
    # _connect_locked at the barrier before the others are scheduled.
    await asyncio.sleep(0)
    second = asyncio.create_task(worker.connect())
    third = asyncio.create_task(worker.connect())
    await asyncio.sleep(0)

    barrier.set()
    results = await asyncio.gather(first, second, third)

    assert results == [True, True, True]
    assert len(worker.created_clients) == 1


@pytest.mark.asyncio
async def test_disconnect_serialized_under_same_lock(tmp_path):
    """disconnect() is serialized under the same client lock as connect(): a
    second disconnect cannot enter _disconnect_locked while the first holds it."""
    worker = build_worker(tmp_path)
    await worker.connect()
    assert worker.client is not None

    barrier = asyncio.Event()
    worker.disconnect_barrier = barrier
    first = asyncio.create_task(worker.disconnect())
    await asyncio.sleep(0)
    assert worker.disconnect_calls == 1  # first disconnect holds the lock

    second = asyncio.create_task(worker.disconnect())
    await asyncio.sleep(0)
    assert worker.disconnect_calls == 1  # second is blocked on the lock

    barrier.set()
    await asyncio.gather(first, second)

    assert worker.disconnect_calls == 2
    assert worker.client is None
