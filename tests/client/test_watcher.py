"""Tests for the client-side worker registry and watcher (AR-123)."""

import asyncio
from datetime import datetime, timezone
from typing import Literal

import pytest

from scietex.service.client import (
    WorkerEventKind,
    WorkerRegistry,
    WorkerWatcher,
)
from scietex.service.heartbeat import Heartbeat


def make_heartbeat(
    instance_id: str,
    *,
    status: Literal["active", "inactive"] = "active",
    ttl: float = 100.0,
    timestamp: datetime | None = None,
) -> Heartbeat:
    return Heartbeat(
        service="svc",
        instance_id=instance_id,
        status=status,
        heartbeat_interval=10.0,
        start_time=datetime.now(timezone.utc),
        ttl=ttl,
        queue_depth=0,
        running_tasks=0,
        tasks_per_second=0.0,
        timestamp=timestamp or datetime.now(timezone.utc),
    )


class FakeBackend:
    """A backend returning queued batches, one per poll."""

    def __init__(self) -> None:
        self.batches: list[list[Heartbeat]] = []
        self.closed = False

    async def poll(self) -> list[Heartbeat]:
        return self.batches.pop(0) if self.batches else []

    async def close(self) -> None:
        self.closed = True


def test_registry_upsert_and_snapshot():
    registry = WorkerRegistry()
    registry.upsert(make_heartbeat("a"))
    registry.upsert(make_heartbeat("b"))

    assert [r.instance_id for r in registry.snapshot()] == ["a", "b"]
    assert len(registry) == 2


def test_registry_upsert_replaces_same_instance():
    registry = WorkerRegistry()
    registry.upsert(make_heartbeat("a", status="active"))
    registry.upsert(make_heartbeat("a", status="inactive"))

    assert len(registry) == 1
    record = registry.get("a")
    assert record is not None
    assert record.status == "inactive"


def test_registry_ignores_out_of_order_heartbeat():
    """An older heartbeat must not roll a worker's state backwards."""
    registry = WorkerRegistry()
    newer = make_heartbeat("a", status="inactive", timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc))
    older = make_heartbeat("a", status="active", timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))

    registry.upsert(newer)
    registry.upsert(older)

    record = registry.get("a")
    assert record is not None
    assert record.status == "inactive"


def test_registry_evicts_expired():
    registry = WorkerRegistry()
    registry.upsert(make_heartbeat("a", ttl=0.0))

    evicted = registry.evict_expired()

    assert [r.instance_id for r in evicted] == ["a"]
    assert registry.get("a") is None
    assert registry.snapshot() == []


def test_registry_get_evicts_expired_lazily():
    registry = WorkerRegistry()
    registry.upsert(make_heartbeat("a", ttl=0.0))

    assert registry.get("a") is None
    assert len(registry) == 0


@pytest.mark.asyncio
async def test_watcher_emits_added_then_updated():
    backend = FakeBackend()
    watcher = WorkerWatcher(backend, poll_interval=0.01)
    backend.batches.append([make_heartbeat("a")])

    first = await watcher._tick()
    assert [(e.kind, e.record.instance_id) for e in first] == [(WorkerEventKind.ADDED, "a")]

    backend.batches.append([make_heartbeat("a", status="inactive")])
    second = await watcher._tick()
    assert [(e.kind, e.record.instance_id) for e in second] == [(WorkerEventKind.UPDATED, "a")]


@pytest.mark.asyncio
async def test_watcher_emits_expired():
    backend = FakeBackend()
    watcher = WorkerWatcher(backend, poll_interval=0.01)
    backend.batches.append([make_heartbeat("a", ttl=0.05)])

    first = await watcher._tick()
    assert [(e.kind, e.record.instance_id) for e in first] == [(WorkerEventKind.ADDED, "a")]

    await asyncio.sleep(0.06)
    events = await watcher._tick()

    assert [(e.kind, e.record.instance_id) for e in events] == [(WorkerEventKind.EXPIRED, "a")]
    assert watcher.snapshot() == []


@pytest.mark.asyncio
async def test_watcher_expires_zero_ttl_immediately():
    """A zero-TTL record is evicted in the same tick that adds it."""
    backend = FakeBackend()
    watcher = WorkerWatcher(backend, poll_interval=0.01)
    backend.batches.append([make_heartbeat("a", ttl=0.0)])

    events = await watcher._tick()

    assert [e.kind for e in events] == [WorkerEventKind.ADDED, WorkerEventKind.EXPIRED]
    assert watcher.snapshot() == []


@pytest.mark.asyncio
async def test_watcher_snapshot_reflects_live_workers():
    backend = FakeBackend()
    watcher = WorkerWatcher(backend, poll_interval=0.01)
    backend.batches.append([make_heartbeat("a"), make_heartbeat("b")])

    await watcher._tick()

    assert [r.instance_id for r in watcher.snapshot()] == ["a", "b"]


@pytest.mark.asyncio
async def test_watcher_watch_yields_events():
    backend = FakeBackend()
    watcher = WorkerWatcher(backend, poll_interval=0.01)
    backend.batches.append([make_heartbeat("a")])

    events = []
    async for event in watcher.watch():
        events.append(event)
        break

    assert [(e.kind, e.record.instance_id) for e in events] == [(WorkerEventKind.ADDED, "a")]
    await watcher.close()


@pytest.mark.asyncio
async def test_watcher_close_closes_backend():
    backend = FakeBackend()
    watcher = WorkerWatcher(backend, poll_interval=0.01)

    await watcher.close()

    assert backend.closed is True
