"""Durable inbox for the MQTT transport (at-least-once delivery).

The inbox compensates for aiomqtt v2.5.1's premature broker acknowledgement
(design §3.1): paho auto-acks as soon as aiomqtt enqueues a message, before
the handler runs, so a crash mid-handler loses the message at the wire level.
Persisting every received message to the inbox *before* handing it to the
processor restores at-least-once delivery: on startup, every non-terminal
entry is replayed, and a tombstone dedupes tasks that already completed.

The inbox is transitional. aiomqtt v3 exposes manual acknowledgement, which
removes the need for the durable backend entirely. The :class:`MqttInbox`
Protocol keeps that migration to an implementation swap: the transport depends
only on the Protocol, so retiring the file-backed store does not touch the
transport. The file-backed store is single-process; multi-replica deployments
would need a shared backend, which the Protocol preserves as a future option.
"""

import asyncio
import base64
import json
import logging
import time
from pathlib import Path
from typing import Protocol
from uuid import UUID

from ..task_handler.schemas import TaskData
from ..task_handler.wire import decode_task_envelope, encode_task_envelope

__all__ = ["FileMqttInbox", "MemoryInbox", "MqttInbox"]

# Entry lifecycle states. ``pending`` is a task persisted before it is handed
# to the processor; ``in-flight`` is one already handed over. Both are
# non-terminal and therefore replayed on recovery.
_STATE_PENDING: str = "pending"
_STATE_IN_FLIGHT: str = "in-flight"


class MqttInbox(Protocol):
    """The durable at-least-once inbox contract the MQTT transport depends on.

    A task id passes through ``pending`` (persisted by :meth:`put`) to
    ``in-flight`` (:meth:`mark_in_flight`) to ``terminal``
    (:meth:`mark_terminal`). Only non-terminal entries are ever replayed
    (:meth:`recover`) or reported (:meth:`pending`), so a task that completed
    is never re-processed. :meth:`prune_expired` is the maintenance hook the
    worker schedules to bound on-disk growth.
    """

    async def put(self, task_id: UUID, task_data: TaskData) -> None: ...

    async def mark_in_flight(self, task_id: UUID) -> None: ...

    async def mark_terminal(self, task_id: UUID) -> None: ...

    async def pending(self) -> list[tuple[UUID, TaskData]]: ...

    async def recover(self) -> list[tuple[UUID, TaskData]]: ...

    async def prune_expired(self) -> None: ...


class MemoryInbox:
    """An in-memory :class:`MqttInbox` for the at-most-once opt-out.

    Buffers entries in a dict so the transport's single-intake-path invariant
    holds without touching disk: the message loop still calls :meth:`put`, and
    :meth:`pending` hands the buffered entries to the next
    :meth:`~MqttTransport.fetch` drain. Nothing survives a restart --
    :meth:`recover` returns an empty list, which is exactly the at-most-once
    contract. There is no tombstone, so a re-delivered duplicate of an
    already-terminal task is buffered and processed again; use
    :class:`FileMqttInbox` when durability or dedupe is required.
    """

    def __init__(self) -> None:
        self._entries: dict[UUID, TaskData] = {}

    async def put(self, task_id: UUID, task_data: TaskData) -> None:
        self._entries[task_id] = task_data

    async def mark_in_flight(self, task_id: UUID) -> None:
        return None

    async def mark_terminal(self, task_id: UUID) -> None:
        self._entries.pop(task_id, None)

    async def pending(self) -> list[tuple[UUID, TaskData]]:
        return list(self._entries.items())

    async def recover(self) -> list[tuple[UUID, TaskData]]:
        return []

    async def prune_expired(self) -> None:
        """No-op: the in-memory backend keeps no tombstones or durable files."""
        return None


class FileMqttInbox:
    """A file-backed :class:`MqttInbox` storing one JSON file per entry.

    The directory ``path`` holds ``{task_id}.json`` entry files and
    ``{task_id}.done`` tombstone files. An entry file is a small JSON object
    carrying the task id, the lifecycle state, a creation epoch, and the
    base64-encoded versioned envelope (from
    :func:`~scietex.service.task_handler.wire.encode_task_envelope`).
    ``mark_terminal`` writes a tombstone rather than deleting the entry so a
    re-delivered duplicate of an already-terminal task is skipped instead of
    re-processed. Tombstones and expired entries are pruned on load, governed
    by ``ttl``.

    This store is single-process: it does not coordinate across replicas.
    """

    def __init__(self, path: Path, *, logger: logging.Logger, ttl: int | None = None) -> None:
        self._path = path
        self._logger = logger
        self._ttl = ttl
        self._lock = asyncio.Lock()
        self._path.mkdir(parents=True, exist_ok=True)

    def _entry_path(self, task_id: UUID) -> Path:
        return self._path / f"{task_id}.json"

    def _tombstone_path(self, task_id: UUID) -> Path:
        return self._path / f"{task_id}.done"

    @staticmethod
    def _parse_task_id(value: object) -> UUID | None:
        if not isinstance(value, str):
            return None
        try:
            return UUID(value)
        except ValueError:
            return None

    @staticmethod
    def _unlink_quietly(path: Path) -> None:
        """Best-effort delete; a missing or transiently-locked file is harmless."""
        try:
            path.unlink()
        except OSError:
            pass

    def _tombstone_is_active(self, task_id: UUID, now: float) -> bool:
        """Return ``True`` when a live (unexpired) tombstone exists for ``task_id``."""
        path = self._tombstone_path(task_id)
        if not path.exists():
            return False
        try:
            completed_at = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            # An unreadable tombstone is treated as live: a task that may have
            # completed must never be resurrected by a corrupt marker.
            self._logger.log(logging.WARNING, "Ignoring unreadable tombstone %s: %s", path, exc)
            return True
        if not isinstance(completed_at, (int, float)):
            self._logger.log(logging.WARNING, "Ignoring malformed tombstone %s", path)
            return True
        return self._ttl is None or now - completed_at <= self._ttl

    def _prune_expired_tombstones(self, now: float) -> None:
        """Delete tombstone files whose dedupe window has elapsed."""
        if self._ttl is None:
            return
        for path in self._path.glob("*.done"):
            try:
                completed_at = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(completed_at, (int, float)) and now - completed_at > self._ttl:
                self._unlink_quietly(path)

    def _prune_expired_entries(self, now: float) -> None:
        """Delete entry files whose ``created_at`` predates the TTL window."""
        if self._ttl is None:
            return
        for path in self._path.glob("*.json"):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            created_at = raw.get("created_at")
            if isinstance(created_at, (int, float)) and now - created_at > self._ttl:
                self._unlink_quietly(path)

    def _prune_sync(self) -> None:
        """One maintenance pass: expire tombstones and entries (AR-115)."""
        self._path.mkdir(parents=True, exist_ok=True)
        now = time.time()
        self._prune_expired_tombstones(now)
        self._prune_expired_entries(now)

    def _decode_envelope(self, envelope: str, entry_path: Path) -> TaskData | None:
        try:
            payload = base64.b64decode(envelope, validate=True)
        except ValueError as exc:
            self._logger.log(
                logging.WARNING, "Skipping inbox entry %s with invalid envelope encoding: %s", entry_path, exc
            )
            return None
        task_data = decode_task_envelope(payload)
        if task_data is None:
            self._logger.log(logging.WARNING, "Skipping inbox entry %s with an undecodable envelope", entry_path)
            return None
        return task_data

    def _put_sync(self, task_id: UUID, task_data: TaskData) -> None:
        self._path.mkdir(parents=True, exist_ok=True)
        if self._tombstone_is_active(task_id, time.time()):
            return  # duplicate delivery of an already-terminal task
        entry = {
            "task_id": str(task_id),
            "state": _STATE_PENDING,
            "created_at": time.time(),
            "envelope": base64.b64encode(encode_task_envelope(task_data)).decode("ascii"),
        }
        self._entry_path(task_id).write_text(json.dumps(entry), encoding="utf-8")

    def _mark_in_flight_sync(self, task_id: UUID) -> None:
        entry_path = self._entry_path(task_id)
        try:
            raw = json.loads(entry_path.read_text(encoding="utf-8"))
            raw["state"] = _STATE_IN_FLIGHT
            entry_path.write_text(json.dumps(raw), encoding="utf-8")
        except FileNotFoundError:
            return  # already terminal; nothing to mark
        except (OSError, json.JSONDecodeError) as exc:
            # The in-flight flag is observational only: the entry is already
            # durable and a pending entry is still replayed, so a failed mark
            # must not fail the task itself.
            self._logger.log(logging.WARNING, "Failed to mark inbox entry %s in-flight: %s", task_id, exc)

    def _mark_terminal_sync(self, task_id: UUID) -> None:
        self._path.mkdir(parents=True, exist_ok=True)
        # Write the tombstone before removing the entry so a crash between the
        # two leaves a terminal task skippable, never resurrected.
        self._tombstone_path(task_id).write_text(json.dumps(time.time()), encoding="utf-8")
        self._unlink_quietly(self._entry_path(task_id))

    def _load_entries(self) -> list[tuple[float, UUID, TaskData]]:
        """Scan the inbox and return non-terminal entries (``created_at``, id, data).

        Runs synchronously (called via ``asyncio.to_thread``). Corrupt, expired,
        and tombstoned entries are skipped; expired files are unlinked inline
        until the next :meth:`prune_expired` maintenance pass bounds growth.
        """
        self._path.mkdir(parents=True, exist_ok=True)
        now = time.time()
        entries: list[tuple[float, UUID, TaskData]] = []
        for entry_path in self._path.glob("*.json"):
            try:
                raw = json.loads(entry_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                self._logger.log(logging.WARNING, "Skipping corrupt inbox entry %s: %s", entry_path, exc)
                continue
            task_id = self._parse_task_id(raw.get("task_id"))
            if task_id is None:
                self._logger.log(logging.WARNING, "Skipping inbox entry %s with an invalid task_id", entry_path)
                continue
            if self._tombstone_is_active(task_id, now):
                continue
            created_at = raw.get("created_at")
            if not isinstance(created_at, (int, float)):
                self._logger.log(logging.WARNING, "Skipping inbox entry %s with an invalid created_at", entry_path)
                continue
            if self._ttl is not None and now - created_at > self._ttl:
                self._unlink_quietly(entry_path)
                continue
            envelope = raw.get("envelope")
            if not isinstance(envelope, str):
                self._logger.log(logging.WARNING, "Skipping inbox entry %s with an invalid envelope", entry_path)
                continue
            task_data = self._decode_envelope(envelope, entry_path)
            if task_data is None:
                continue
            entries.append((created_at, task_id, task_data))
        entries.sort(key=lambda item: item[0])
        return entries

    async def put(self, task_id: UUID, task_data: TaskData) -> None:
        """Persist ``task_data`` for ``task_id`` in the ``pending`` state.

        The write completes before this returns, so a crash after ``put`` but
        before processing cannot lose the task. A write failure propagates:
        durability is the inbox's only job.
        """
        async with self._lock:
            await asyncio.to_thread(self._put_sync, task_id, task_data)

    async def mark_in_flight(self, task_id: UUID) -> None:
        """Record that ``task_id`` was handed to the processor."""
        async with self._lock:
            await asyncio.to_thread(self._mark_in_flight_sync, task_id)

    async def mark_terminal(self, task_id: UUID) -> None:
        """Record that ``task_id`` completed (success, error, or cancellation)."""
        async with self._lock:
            await asyncio.to_thread(self._mark_terminal_sync, task_id)

    async def prune_expired(self) -> None:
        """Delete tombstones and entries whose TTL window has elapsed (AR-115).

        Decoupled from :meth:`_load_entries` so pruning is a maintenance pass
        the worker schedules, not a side effect of a fetch poll: the fetch scan
        no longer walks the ever-growing tombstone set. A no-op when ``ttl`` is
        ``None`` (the explicit unbounded-dedup opt-out).
        """
        async with self._lock:
            await asyncio.to_thread(self._prune_sync)

    async def pending(self) -> list[tuple[UUID, TaskData]]:
        """Return all non-terminal entries, oldest first (diagnostics/tests)."""
        return await self._snapshot()

    async def recover(self) -> list[tuple[UUID, TaskData]]:
        """Return all non-terminal entries for startup replay, oldest first."""
        return await self._snapshot()

    async def _snapshot(self) -> list[tuple[UUID, TaskData]]:
        async with self._lock:
            entries = await asyncio.to_thread(self._load_entries)
        return [(task_id, task_data) for _, task_id, task_data in entries]
