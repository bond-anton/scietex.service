"""Client-side worker registry: the read model over published heartbeats.

A :class:`WorkerRegistry` holds the latest :class:`Heartbeat` seen per
``instance_id`` and evicts entries once their payload ``ttl`` has elapsed since
``timestamp``. It is transport-agnostic: backends feed it heartbeats, and it
answers :meth:`snapshot` and drives the change stream.

The registry is the client's own view, not a broker-side store. Each backend
delivers whatever it observes (a SCAN pass, a retained message), and the
registry reconciles by ``instance_id``. A worker that stops beating is evicted
locally once its ``ttl`` expires, so the client converges on the same view the
broker enforces independently.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from ..heartbeat import Heartbeat

__all__ = ["WorkerRecord", "WorkerRegistry"]


@dataclass(frozen=True, slots=True)
class WorkerRecord:
    """A worker's last-known state, as seen by a client.

    Attributes:
        heartbeat: The most recent :class:`Heartbeat` received for the worker.
        received_at: Monotonic timestamp when the client received it, used for
            local TTL eviction. Monotonic rather than wall-clock so a clock
            adjustment cannot resurrect or prematurely evict an entry.
    """

    heartbeat: Heartbeat
    received_at: float

    @property
    def instance_id(self) -> str:
        """The worker's instance id."""
        return self.heartbeat.instance_id

    @property
    def status(self) -> str:
        """The worker's last-reported status (``"active"`` or ``"inactive"``)."""
        return self.heartbeat.status

    @property
    def is_expired(self) -> bool:
        """Whether this record has outlived its payload ``ttl``."""
        return (time.monotonic() - self.received_at) >= self.heartbeat.ttl


class WorkerRegistry:
    """In-memory registry of worker heartbeats, keyed by ``instance_id``.

    Upserts on every heartbeat and evicts expired records lazily on read, so a
    worker that stops beating disappears from :meth:`snapshot` once its ``ttl``
    elapses without any explicit removal. Not thread-safe: it is driven from a
    single event loop by the owning watcher.
    """

    def __init__(self) -> None:
        self._records: dict[str, WorkerRecord] = {}

    def upsert(self, heartbeat: Heartbeat) -> WorkerRecord:
        """Record ``heartbeat`` as the latest state for its instance.

        A heartbeat older than the one already held is ignored, so an
        out-of-order delivery (a slow SCAN page, a replayed retained message)
        cannot roll a worker's state backwards.

        Args:
            heartbeat: The heartbeat to record.

        Returns:
            The record now held for the instance.
        """
        existing = self._records.get(heartbeat.instance_id)
        if existing is not None and heartbeat.timestamp < existing.heartbeat.timestamp:
            return existing
        record = WorkerRecord(heartbeat=heartbeat, received_at=time.monotonic())
        self._records[heartbeat.instance_id] = record
        return record

    def evict_expired(self) -> list[WorkerRecord]:
        """Drop every record whose ``ttl`` has elapsed.

        Returns:
            The evicted records, so a caller can emit removal events carrying
            the worker's last-known state.
        """
        expired = [record for record in self._records.values() if record.is_expired]
        for record in expired:
            del self._records[record.instance_id]
        return expired

    def snapshot(self) -> list[WorkerRecord]:
        """Return the live records, evicting expired ones first.

        Returns:
            The non-expired records, ordered by ``instance_id`` for a stable
            result across calls.
        """
        self.evict_expired()
        return [self._records[instance_id] for instance_id in sorted(self._records)]

    def get(self, instance_id: str) -> WorkerRecord | None:
        """Return the live record for ``instance_id``, or ``None``.

        An expired record is evicted and reported as absent.
        """
        record = self._records.get(instance_id)
        if record is None:
            return None
        if record.is_expired:
            del self._records[instance_id]
            return None
        return record

    def __len__(self) -> int:
        """Number of live records, evicting expired ones first."""
        self.evict_expired()
        return len(self._records)
