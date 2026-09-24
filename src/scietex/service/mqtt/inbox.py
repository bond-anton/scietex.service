"""Durable inbox for the MQTT transport (at-least-once delivery).

The inbox compensates for aiomqtt v2.5.1's premature broker acknowledgement
(design §3.1): paho auto-acks as soon as aiomqtt enqueues a message, before
the handler runs, so a crash mid-handler loses the message at the wire level.
Persisting every received message to the inbox *before* handing it to the
processor restores at-least-once delivery: on startup, every non-terminal
entry is replayed, and a tombstone dedupes tasks that already completed.

A worker runs two independent inbox instances (design §5.1): a data inbox and
a control inbox at distinct paths, so a saturated data lane cannot delay
control delivery. Both implement the same :class:`MqttInbox` contract.

The inbox is transitional. aiomqtt v3 exposes manual acknowledgement, which
removes the need for the durable backend entirely. The :class:`MqttInbox`
Protocol keeps that migration to an implementation swap: the transport depends
only on the Protocol. The shared multi-process backend is
:class:`~scietex.service.mqtt.inbox_sqlite.SqliteMqttInbox`; the in-memory
:class:`MemoryInbox` here is the single-process at-most-once opt-out.
"""

from collections.abc import Iterable
from typing import Protocol
from uuid import UUID

from ..task_handler.schemas import TaskData

__all__ = ["MemoryInbox", "MqttInbox"]


class MqttInbox(Protocol):
    """The durable at-least-once inbox contract the MQTT transport depends on.

    A task id passes through ``pending`` (persisted by :meth:`put`) to
    ``in-flight`` (:meth:`mark_in_flight`) to ``terminal``
    (:meth:`mark_terminal`). Only non-terminal entries are ever replayed
    (:meth:`recover`) or reported (:meth:`pending`), so a task that completed
    is never re-processed. :meth:`prune_expired` is the maintenance hook the
    worker schedules to bound on-disk growth.

    :meth:`claim` is the cross-process mutual-exclusion primitive a shared
    backend needs: it returns ``True`` only for the caller that won the entry,
    so two workers draining one store never process the same task id.
    :meth:`release` returns a claimed entry to the pool (a rejected or
    requeued task), :meth:`refresh` extends a live claim's lease, and
    :meth:`close` releases the backend's resources. Single-process backends
    implement :meth:`claim` as an unconditional ``True`` and the rest as
    no-ops.
    """

    async def put(self, task_id: UUID, task_data: TaskData) -> None: ...

    async def mark_in_flight(self, task_id: UUID) -> None: ...

    async def mark_terminal(self, task_id: UUID) -> None: ...

    async def pending(self) -> list[TaskData]: ...

    async def recover(self) -> list[TaskData]: ...

    async def prune_expired(self) -> None: ...

    async def claim(self, task_id: UUID) -> bool: ...

    async def release(self, task_id: UUID) -> None: ...

    async def refresh(self, task_ids: Iterable[UUID]) -> None: ...

    async def close(self) -> None: ...


class MemoryInbox:
    """An in-memory :class:`MqttInbox` for the at-most-once opt-out.

    Buffers entries in a dict so the transport's single-intake-path invariant
    holds without touching disk: the message loop still calls :meth:`put`, and
    :meth:`pending` hands the buffered entries to the next
    :meth:`~MqttTransport.fetch` drain. Nothing survives a restart --
    :meth:`recover` returns an empty list, which is exactly the at-most-once
    contract. There is no tombstone, so a re-delivered duplicate of an
    already-terminal task is buffered and processed again; use
    :class:`~scietex.service.mqtt.inbox_sqlite.SqliteMqttInbox` when durability
    or dedupe is required.
    """

    def __init__(self) -> None:
        self._entries: dict[UUID, TaskData] = {}

    async def put(self, task_id: UUID, task_data: TaskData) -> None:
        self._entries[task_id] = task_data

    async def mark_in_flight(self, task_id: UUID) -> None:
        return None

    async def mark_terminal(self, task_id: UUID) -> None:
        self._entries.pop(task_id, None)

    async def pending(self) -> list[TaskData]:
        return list(self._entries.values())

    async def recover(self) -> list[TaskData]:
        return []

    async def prune_expired(self) -> None:
        """No-op: the in-memory backend keeps no tombstones or durable files."""
        return None

    async def claim(self, task_id: UUID) -> bool:
        """Always wins: a single-process backend has no peer to contend with."""
        return True

    async def release(self, task_id: UUID) -> None:
        """No-op: there is no cross-process claim to release."""
        return None

    async def refresh(self, task_ids: Iterable[UUID]) -> None:
        """No-op: there is no cross-process lease to extend."""
        return None

    async def close(self) -> None:
        """No-op: the in-memory backend holds no external resources."""
        return None
