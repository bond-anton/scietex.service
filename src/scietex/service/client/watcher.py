"""Push-shaped worker watcher: a snapshot plus a change stream.

The watcher is the client's read API over the worker registry. It owns a
:class:`~scietex.service.client.registry.WorkerRegistry` and a backend that
feeds it, and exposes two views:

- :meth:`WorkerWatcher.snapshot` — the current live workers, on demand.
- :meth:`WorkerWatcher.watch` — an async iterator of :class:`WorkerEvent`
  changes, for a monitoring loop.

The backend is swappable (polling, keyspace notifications, MQTT subscribe), so
the watcher's contract does not change when the delivery mechanism does. The
watcher drives the backend's ``poll`` on its own cadence and evicts expired
records between polls, so a worker that stops beating surfaces as a
:class:`WorkerEventKind.EXPIRED` event without the backend reporting anything.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from enum import Enum
from typing import Protocol, runtime_checkable

from ..heartbeat import Heartbeat
from .registry import WorkerRecord, WorkerRegistry

__all__ = [
    "WorkerEvent",
    "WorkerEventKind",
    "WorkerWatcher",
    "WatchBackend",
]


class WorkerEventKind(str, Enum):
    """What changed about a worker."""

    #: A worker was seen for the first time.
    ADDED = "added"
    #: A worker's heartbeat was refreshed (status or timestamp changed).
    UPDATED = "updated"
    #: A worker's record expired locally without a fresh heartbeat.
    EXPIRED = "expired"


class WorkerEvent:
    """A single change observed by the watcher.

    Attributes:
        kind: What changed.
        record: The worker's record. For :attr:`WorkerEventKind.EXPIRED` this is
            the last-known record, retained so a consumer can report which
            worker disappeared.
    """

    __slots__ = ("kind", "record")

    def __init__(self, kind: WorkerEventKind, record: WorkerRecord) -> None:
        self.kind = kind
        self.record = record

    def __repr__(self) -> str:
        return f"WorkerEvent(kind={self.kind.value!r}, instance_id={self.record.instance_id!r})"


@runtime_checkable
class WatchBackend(Protocol):
    """Delivery mechanism feeding heartbeats into a watcher.

    A backend is a pull source: :meth:`poll` returns whatever heartbeats it has
    observed since the last call, and the watcher reconciles them into its
    registry. This keeps the watcher's cadence independent of the backend's
    (a polling backend returns a SCAN page; a subscribe backend returns the
    messages buffered since the last poll).
    """

    async def poll(self) -> list[Heartbeat]:
        """Return heartbeats observed since the previous call.

        Returns:
            The heartbeats to reconcile, possibly empty.
        """
        ...

    async def close(self) -> None:
        """Release any resources held by the backend."""
        ...


class WorkerWatcher:
    """Watches worker heartbeats and emits changes as an async iterator.

    Args:
        backend: The delivery mechanism feeding heartbeats.
        poll_interval: Seconds between backend polls. Also bounds how quickly an
            expired worker is noticed: eviction runs on each poll tick.
    """

    def __init__(self, backend: WatchBackend, *, poll_interval: float = 1.0) -> None:
        self._backend = backend
        self._poll_interval = poll_interval
        self._registry = WorkerRegistry()
        self._closed = False

    @property
    def registry(self) -> WorkerRegistry:
        """The underlying registry, for direct inspection."""
        return self._registry

    def snapshot(self) -> list[WorkerRecord]:
        """Return the current live workers.

        Returns:
            The non-expired records, ordered by ``instance_id``.
        """
        return self._registry.snapshot()

    async def watch(self) -> AsyncIterator[WorkerEvent]:
        """Yield worker changes until the watcher is closed.

        Polls the backend on ``poll_interval``, reconciles heartbeats into the
        registry, and yields an event per change. Expired records are evicted
        each tick and yielded as :attr:`WorkerEventKind.EXPIRED`.

        Yields:
            A :class:`WorkerEvent` per observed change.
        """
        while not self._closed:
            for event in await self._tick():
                yield event
            await asyncio.sleep(self._poll_interval)

    async def _tick(self) -> list[WorkerEvent]:
        """Run one poll-and-reconcile cycle, returning the events it produced."""
        events: list[WorkerEvent] = []
        for heartbeat in await self._backend.poll():
            existing = self._registry.get(heartbeat.instance_id)
            record = self._registry.upsert(heartbeat)
            if existing is None:
                events.append(WorkerEvent(WorkerEventKind.ADDED, record))
            elif existing.heartbeat != record.heartbeat:
                events.append(WorkerEvent(WorkerEventKind.UPDATED, record))
        for record in self._registry.evict_expired():
            events.append(WorkerEvent(WorkerEventKind.EXPIRED, record))
        return events

    async def close(self) -> None:
        """Stop the watcher and close its backend."""
        self._closed = True
        await self._backend.close()
