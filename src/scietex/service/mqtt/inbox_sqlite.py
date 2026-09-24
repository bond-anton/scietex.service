"""SQLite-backed shared inbox for the MQTT transport (at-least-once delivery).

A durable inbox must be safe for multiple processes: two workers sharing one
store must never race on the same entry. This backend uses a WAL-mode SQLite
database that multiple processes open safely, and adds a cross-process
claim/lease so two workers draining one store never process the same task id.

The claim is modeled on the Valkey ``TaskLeaseManager`` (AR-060): a row is
claimable when it is unclaimed or its lease has expired, and the winning
``UPDATE ... WHERE`` is atomic across processes under ``BEGIN IMMEDIATE``.
A crashed peer's entry is reclaimed once its lease lapses, so recovery-once is
not a correctness dependency.

The store is transitional: aiomqtt v3 exposes manual acknowledgement, which
removes the need for a durable inbox entirely. The
:class:`~scietex.service.mqtt.inbox.MqttInbox` Protocol keeps that migration to
an implementation swap.
"""

import asyncio
import logging
import sqlite3
import time
from collections.abc import Iterable
from pathlib import Path
from uuid import UUID

from ..task_handler.schemas import TaskData
from ..task_handler.wire import decode_task_envelope, encode_task_envelope

__all__ = ["SqliteMqttInbox", "derive_inbox_lease_ttl"]

#: Entry lifecycle states. ``pending`` is a task persisted before it is handed
#: to the processor; ``in-flight`` is one already handed over. Both are
#: non-terminal and therefore replayed on recovery.
_STATE_PENDING: str = "pending"
_STATE_IN_FLIGHT: str = "in-flight"

#: Lease-TTL derivation multipliers, mirroring ``valkey/lease.py`` (AR-060).
LEASE_TTL_HEARTBEAT_MULTIPLIER: int = 2
LEASE_TTL_WATCHDOG_MULTIPLIER: int = 3
MIN_INBOX_LEASE_TTL_SECONDS: int = 1

#: Default SQLite busy timeout (ms): how long a writer waits for a peer's lock
#: before raising ``SQLITE_BUSY``.
DEFAULT_BUSY_TIMEOUT_MS: int = 5000

_SCHEMA: str = """
CREATE TABLE IF NOT EXISTS entries (
    task_id          TEXT PRIMARY KEY,
    state            TEXT NOT NULL CHECK (state IN ('pending', 'in-flight')),
    created_at       REAL NOT NULL,
    envelope         BLOB NOT NULL,
    claimed_by       TEXT,
    lease_expires_at REAL
);
CREATE TABLE IF NOT EXISTS tombstones (
    task_id      TEXT PRIMARY KEY,
    completed_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entries_created
    ON entries (created_at);
CREATE INDEX IF NOT EXISTS idx_entries_lease
    ON entries (lease_expires_at)
    WHERE claimed_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tombstones_completed
    ON tombstones (completed_at);
"""


def derive_inbox_lease_ttl(heartbeat_interval: float, watchdog_interval: float) -> int:
    """max(1, int(max(2*heartbeat_interval, 3*watchdog_interval))) (AR-060).

    Duplicated from :func:`scietex.service.valkey.lease.derive_task_lease_ttl`
    rather than imported so the optional MQTT backend never imports the
    optional Valkey package (glide coupling).
    """
    return max(
        MIN_INBOX_LEASE_TTL_SECONDS,
        int(
            max(
                LEASE_TTL_HEARTBEAT_MULTIPLIER * heartbeat_interval,
                LEASE_TTL_WATCHDOG_MULTIPLIER * watchdog_interval,
            )
        ),
    )


class SqliteMqttInbox:
    """A shared, WAL-mode SQLite :class:`MqttInbox` with cross-process claims.

    One database file holds an ``entries`` table (non-terminal tasks) and a
    ``tombstones`` table (completed task ids, for dedupe). ``mark_terminal``
    inserts the tombstone and deletes the entry in one transaction, so a crash
    between the two can never resurrect a completed task.

    A single connection is opened with ``check_same_thread=False`` and
    ``isolation_level=None``; every access is serialized by an
    :class:`asyncio.Lock` and runs via :func:`asyncio.to_thread`, so the
    connection has exactly one waiter at a time. Write paths issue explicit
    ``BEGIN IMMEDIATE`` / ``COMMIT`` / ``ROLLBACK``.

    Args:
        path: Database file path. Its parent directory is created if missing.
        worker_id: This worker's identity, recorded as ``claimed_by`` so a
            worker only releases or refreshes its own claims.
        logger: Logger for warnings (corrupt rows, failed claims).
        ttl: Entry/tombstone TTL in seconds. ``None`` disables expiry.
        lease_ttl: Claim lifetime in seconds.
        busy_timeout_ms: SQLite busy timeout in milliseconds.
    """

    def __init__(
        self,
        path: Path,
        *,
        worker_id: str,
        logger: logging.Logger,
        ttl: int | None = None,
        lease_ttl: int = 1,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    ) -> None:
        self._path = path
        self._worker_id = worker_id
        self._logger = logger
        self._ttl = ttl
        self._lease_ttl = lease_ttl
        self._lock = asyncio.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(_SCHEMA)

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

    async def pending(self) -> list[TaskData]:
        """Return all non-terminal entries, oldest first (diagnostics/tests)."""
        return await self._snapshot()

    async def recover(self) -> list[TaskData]:
        """Return all non-terminal entries for startup replay, oldest first."""
        return await self._snapshot()

    async def prune_expired(self) -> None:
        """Delete tombstones and entries whose TTL window has elapsed (AR-115).

        A no-op when ``ttl`` is ``None`` (the explicit unbounded-dedup opt-out).
        Concurrent DELETEs across processes are idempotent.
        """
        async with self._lock:
            await asyncio.to_thread(self._prune_sync)

    async def claim(self, task_id: UUID) -> bool:
        """Try to take the cross-process claim on ``task_id``.

        Returns ``True`` only for the caller that won the entry (it was
        unclaimed or its lease had expired). A claim failure is logged and
        reported as ``False`` -- never raised -- so a transient DB fault defers
        the entry instead of breaking the task path.
        """
        async with self._lock:
            return await asyncio.to_thread(self._claim_sync, task_id)

    async def release(self, task_id: UUID) -> None:
        """Return ``task_id`` to the pool, clearing only this worker's claim."""
        async with self._lock:
            await asyncio.to_thread(self._release_sync, task_id)

    async def refresh(self, task_ids: Iterable[UUID]) -> None:
        """Extend the lease on this worker's live claims."""
        ids = list(task_ids)
        if not ids:
            return
        async with self._lock:
            await asyncio.to_thread(self._refresh_sync, ids)

    async def close(self) -> None:
        """Close the database connection."""
        async with self._lock:
            await asyncio.to_thread(self._conn.close)

    async def _snapshot(self) -> list[TaskData]:
        async with self._lock:
            return await asyncio.to_thread(self._load_entries)

    def _tombstone_is_active(self, task_id: UUID, now: float) -> bool:
        """Return ``True`` when a live (unexpired) tombstone exists for ``task_id``.

        An unreadable tombstone is treated as live: a task that may have
        completed must never be resurrected by a corrupt marker.
        """
        try:
            row = self._conn.execute(
                "SELECT completed_at FROM tombstones WHERE task_id = ?", (str(task_id),)
            ).fetchone()
        except sqlite3.Error as exc:
            self._logger.log(logging.WARNING, "Ignoring unreadable tombstone for %s: %s", task_id, exc)
            return True
        if row is None:
            return False
        completed_at = row[0]
        if not isinstance(completed_at, (int, float)):
            self._logger.log(logging.WARNING, "Ignoring malformed tombstone for %s", task_id)
            return True
        return self._ttl is None or now - completed_at <= self._ttl

    def _put_sync(self, task_id: UUID, task_data: TaskData) -> None:
        now = time.time()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            if self._tombstone_is_active(task_id, now):
                self._conn.execute("COMMIT")
                return  # duplicate delivery of an already-terminal task
            # INSERT OR IGNORE preserves an existing live claim on redelivery.
            self._conn.execute(
                "INSERT OR IGNORE INTO entries(task_id, state, created_at, envelope) VALUES(?, ?, ?, ?)",
                (str(task_id), _STATE_PENDING, now, encode_task_envelope(task_data)),
            )
            self._conn.execute("COMMIT")
        except sqlite3.Error:
            self._conn.execute("ROLLBACK")
            raise

    def _mark_in_flight_sync(self, task_id: UUID) -> None:
        try:
            self._conn.execute("UPDATE entries SET state = ? WHERE task_id = ?", (_STATE_IN_FLIGHT, str(task_id)))
        except sqlite3.Error as exc:
            # The in-flight flag is observational only: the entry is already
            # durable and a pending entry is still replayed, so a failed mark
            # must not fail the task itself.
            self._logger.log(logging.WARNING, "Failed to mark inbox entry %s in-flight: %s", task_id, exc)

    def _mark_terminal_sync(self, task_id: UUID) -> None:
        now = time.time()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            # Tombstone before delete so a crash between the two leaves a
            # terminal task skippable, never resurrected.
            self._conn.execute(
                "INSERT OR IGNORE INTO tombstones(task_id, completed_at) VALUES(?, ?)",
                (str(task_id), now),
            )
            self._conn.execute("DELETE FROM entries WHERE task_id = ?", (str(task_id),))
            self._conn.execute("COMMIT")
        except sqlite3.Error:
            self._conn.execute("ROLLBACK")
            raise

    def _prune_sync(self) -> None:
        if self._ttl is None:
            return
        cutoff = time.time() - self._ttl
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute("DELETE FROM tombstones WHERE completed_at < ?", (cutoff,))
            self._conn.execute("DELETE FROM entries WHERE created_at < ?", (cutoff,))
            self._conn.execute("COMMIT")
        except sqlite3.Error:
            self._conn.execute("ROLLBACK")
            raise

    def _claim_sync(self, task_id: UUID) -> bool:
        now = time.time()
        try:
            cursor = self._conn.execute(
                "UPDATE entries SET claimed_by = ?, lease_expires_at = ? "
                "WHERE task_id = ? AND (claimed_by IS NULL OR lease_expires_at < ?)",
                (self._worker_id, now + self._lease_ttl, str(task_id), now),
            )
        except sqlite3.Error as exc:
            # An unverifiable claim is not won (mirrors TaskLeaseManager.acquire).
            self._logger.log(logging.WARNING, "Failed to claim inbox entry %s: %s", task_id, exc)
            return False
        return cursor.rowcount > 0

    def _release_sync(self, task_id: UUID) -> None:
        try:
            self._conn.execute(
                "UPDATE entries SET claimed_by = NULL, lease_expires_at = NULL WHERE task_id = ? AND claimed_by = ?",
                (str(task_id), self._worker_id),
            )
        except sqlite3.Error as exc:
            self._logger.log(logging.WARNING, "Failed to release inbox entry %s: %s", task_id, exc)

    def _refresh_sync(self, task_ids: list[UUID]) -> None:
        expires_at = time.time() + self._lease_ttl
        try:
            self._conn.executemany(
                "UPDATE entries SET lease_expires_at = ? WHERE task_id = ? AND claimed_by = ?",
                [(expires_at, str(task_id), self._worker_id) for task_id in task_ids],
            )
        except sqlite3.Error as exc:
            self._logger.log(logging.WARNING, "Failed to refresh inbox leases: %s", exc)

    def _load_entries(self) -> list[TaskData]:
        """Return non-terminal entry payloads, oldest first.

        Runs synchronously (called via ``asyncio.to_thread``). Tombstoned and
        expired entries are excluded; a corrupt envelope row is skipped with a
        WARNING. A DB read fault returns ``[]`` -- no resurrection on error.
        """
        query = (
            "SELECT e.envelope FROM entries e WHERE NOT EXISTS (SELECT 1 FROM tombstones t WHERE t.task_id = e.task_id)"
        )
        params: tuple[object, ...] = ()
        if self._ttl is not None:
            query += " AND e.created_at >= ?"
            params = (time.time() - self._ttl,)
        query += " ORDER BY e.created_at ASC"
        try:
            rows = self._conn.execute(query, params).fetchall()
        except sqlite3.Error as exc:
            self._logger.log(logging.WARNING, "Failed to read inbox entries: %s", exc)
            return []
        entries: list[TaskData] = []
        for (envelope,) in rows:
            task_data = self._decode_envelope(envelope)
            if task_data is not None:
                entries.append(task_data)
        return entries

    def _decode_envelope(self, envelope: object) -> TaskData | None:
        if not isinstance(envelope, (bytes, bytearray)):
            self._logger.log(logging.WARNING, "Skipping inbox entry with an invalid envelope")
            return None
        task_data = decode_task_envelope(bytes(envelope))
        if task_data is None:
            self._logger.log(logging.WARNING, "Skipping inbox entry with an undecodable envelope")
            return None
        return task_data
