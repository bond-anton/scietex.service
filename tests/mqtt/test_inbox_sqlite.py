"""Tests for the SQLite-backed shared MQTT durable inbox (``SqliteMqttInbox``)."""

import asyncio
import logging
import sqlite3
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from scietex.service.mqtt.inbox_sqlite import SqliteMqttInbox, derive_inbox_lease_ttl
from scietex.service.task_handler.schemas import TaskData, task_data_id
from scietex.service.task_handler.wire import encode_task_envelope

_LOGGER = "test_inbox_sqlite"


def _inbox(tmp_path, *, worker_id="w1", ttl=None, lease_ttl=60) -> SqliteMqttInbox:
    return SqliteMqttInbox(
        tmp_path / "inbox.sqlite3",
        worker_id=worker_id,
        logger=logging.getLogger(_LOGGER),
        ttl=ttl,
        lease_ttl=lease_ttl,
    )


def _seed_entry(db_path: Path, task_id: UUID, task_data: TaskData, created_at: float) -> None:
    """Insert an entry row directly, bypassing the inbox, for deterministic setup."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO entries(task_id, state, created_at, envelope) VALUES(?, 'pending', ?, ?)",
            (str(task_id), created_at, encode_task_envelope(task_data)),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_tombstone(db_path: Path, task_id: UUID, completed_at: float) -> None:
    """Insert a tombstone row directly, bypassing the inbox, for deterministic setup."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO tombstones(task_id, completed_at) VALUES(?, ?)",
            (str(task_id), completed_at),
        )
        conn.commit()
    finally:
        conn.close()


def test_derive_inbox_lease_ttl_matches_valkey_formula():
    """The derivation mirrors max(1, int(max(2*heartbeat, 3*watchdog)))."""
    assert derive_inbox_lease_ttl(10.0, 1.0) == 20
    assert derive_inbox_lease_ttl(1.0, 10.0) == 30
    assert derive_inbox_lease_ttl(0.0, 0.0) == 1


@pytest.mark.asyncio
async def test_put_then_pending_roundtrips_task_data(tmp_path):
    """``put`` persists the envelope; ``pending`` returns the original TaskData."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b'{"to": "a@b.c"}')

    await inbox.put(task_id, task_data)

    assert await inbox.pending() == [task_data]
    await inbox.close()


@pytest.mark.asyncio
async def test_in_flight_is_non_terminal(tmp_path):
    """An in-flight entry is still returned by ``pending`` (non-terminal)."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b"x")

    await inbox.put(task_id, task_data)
    await inbox.mark_in_flight(task_id)

    assert await inbox.pending() == [task_data]
    await inbox.close()


@pytest.mark.asyncio
async def test_mark_terminal_hides_entry(tmp_path):
    """``mark_terminal`` tombstones the entry so pending/recover skip it."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()

    await inbox.put(task_id, TaskData(task_id=str(task_id), task="send_email", payload=b"x"))
    await inbox.mark_terminal(task_id)

    assert await inbox.pending() == []
    assert await inbox.recover() == []
    await inbox.close()


@pytest.mark.asyncio
async def test_recover_returns_oldest_first(tmp_path):
    """``recover`` returns non-terminal entries ordered by ``created_at``."""
    inbox = _inbox(tmp_path)
    first = uuid4()
    second = uuid4()
    _seed_entry(tmp_path / "inbox.sqlite3", first, TaskData(task_id=str(first), task="a", payload=b"1"), 200.0)
    _seed_entry(tmp_path / "inbox.sqlite3", second, TaskData(task_id=str(second), task="b", payload=b"2"), 100.0)

    assert [task_data_id(td) for td in await inbox.recover()] == [second, first]
    await inbox.close()


@pytest.mark.asyncio
async def test_duplicate_put_after_terminal_is_skipped(tmp_path):
    """A tombstone suppresses a duplicate ``put`` of the same task id."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b"x")

    await inbox.put(task_id, task_data)
    await inbox.mark_terminal(task_id)
    await inbox.put(task_id, task_data)

    assert await inbox.pending() == []
    assert await inbox.recover() == []
    await inbox.close()


@pytest.mark.asyncio
async def test_expired_entry_is_dropped(tmp_path):
    """An entry older than ``ttl`` is dropped by pending/recover."""
    inbox = _inbox(tmp_path, ttl=60)
    expired = uuid4()
    _seed_entry(
        tmp_path / "inbox.sqlite3",
        expired,
        TaskData(task_id=str(expired), task="old", payload=b"x"),
        time.time() - 120.0,
    )
    fresh = uuid4()
    fresh_data = TaskData(task_id=str(fresh), task="new", payload=b"y")

    await inbox.put(fresh, fresh_data)

    assert await inbox.pending() == [fresh_data]
    assert await inbox.recover() == [fresh_data]
    await inbox.close()


@pytest.mark.asyncio
async def test_expired_entry_preserved_when_ttl_none(tmp_path):
    """With ``ttl=None`` an old entry is still returned (unbounded opt-out)."""
    inbox = _inbox(tmp_path, ttl=None)
    old = uuid4()
    old_data = TaskData(task_id=str(old), task="old", payload=b"x")
    _seed_entry(tmp_path / "inbox.sqlite3", old, old_data, time.time() - 10_000.0)

    assert await inbox.pending() == [old_data]
    await inbox.close()


@pytest.mark.asyncio
async def test_prune_expired_deletes_old_keeps_fresh(tmp_path):
    """``prune_expired`` drops old tombstones/entries and keeps fresh ones."""
    inbox = _inbox(tmp_path, ttl=60)
    db_path = tmp_path / "inbox.sqlite3"
    old_id = uuid4()
    fresh_id = uuid4()
    _seed_entry(db_path, old_id, TaskData(task_id=str(old_id), task="old", payload=b"x"), time.time() - 120.0)
    _seed_tombstone(db_path, uuid4(), time.time() - 120.0)
    fresh_data = TaskData(task_id=str(fresh_id), task="new", payload=b"y")
    await inbox.put(fresh_id, fresh_data)

    await inbox.prune_expired()

    assert await inbox.pending() == [fresh_data]
    conn = sqlite3.connect(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) FROM tombstones").fetchone()[0] == 0
    finally:
        conn.close()
    await inbox.close()


@pytest.mark.asyncio
async def test_prune_expired_noop_when_ttl_none(tmp_path):
    """With ``ttl=None`` pruning leaves old rows untouched."""
    inbox = _inbox(tmp_path, ttl=None)
    db_path = tmp_path / "inbox.sqlite3"
    old_id = uuid4()
    _seed_entry(db_path, old_id, TaskData(task_id=str(old_id), task="old", payload=b"x"), time.time() - 10_000.0)
    _seed_tombstone(db_path, uuid4(), time.time() - 10_000.0)

    await inbox.prune_expired()

    conn = sqlite3.connect(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM tombstones").fetchone()[0] == 1
    finally:
        conn.close()
    await inbox.close()


@pytest.mark.asyncio
async def test_unreadable_tombstone_is_treated_live(tmp_path, monkeypatch, caplog):
    """A DB fault during a tombstone read must not resurrect the task."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b"x")
    await inbox.put(task_id, task_data)

    real_conn = inbox._conn

    class _FaultyConn:
        def __getattr__(self, name):
            return getattr(real_conn, name)

        def execute(self, sql, *args, **kwargs):
            if "FROM tombstones" in sql:
                raise sqlite3.OperationalError("boom")
            return real_conn.execute(sql, *args, **kwargs)

    monkeypatch.setattr(inbox, "_conn", _FaultyConn())
    with caplog.at_level(logging.WARNING):
        await inbox.put(task_id, task_data)

    assert any("unreadable tombstone" in record.getMessage() for record in caplog.records)
    await inbox.close()


@pytest.mark.asyncio
async def test_corrupt_envelope_row_is_skipped_with_warning(tmp_path, caplog):
    """A row with an undecodable envelope is logged at WARNING and skipped."""
    inbox = _inbox(tmp_path)
    db_path = tmp_path / "inbox.sqlite3"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO entries(task_id, state, created_at, envelope) VALUES(?, 'pending', ?, ?)",
            (str(uuid4()), time.time(), b"not-an-envelope"),
        )
        conn.commit()
    finally:
        conn.close()
    valid = uuid4()
    task_data = TaskData(task_id=str(valid), task="ok", payload=b"p")
    await inbox.put(valid, task_data)

    with caplog.at_level(logging.WARNING):
        entries = await inbox.pending()

    assert entries == [task_data]
    assert any("undecodable envelope" in record.getMessage() for record in caplog.records)
    await inbox.close()


@pytest.mark.asyncio
async def test_claim_is_exclusive_across_instances(tmp_path):
    """Two instances on one DB: only one wins the claim; release frees it."""
    db_path = tmp_path / "inbox.sqlite3"
    a = SqliteMqttInbox(db_path, worker_id="a", logger=logging.getLogger(_LOGGER), lease_ttl=60)
    b = SqliteMqttInbox(db_path, worker_id="b", logger=logging.getLogger(_LOGGER), lease_ttl=60)
    task_id = uuid4()
    await a.put(task_id, TaskData(task_id=str(task_id), task="t", payload=b"x"))

    assert await a.claim(task_id) is True
    assert await b.claim(task_id) is False

    await a.release(task_id)
    assert await b.claim(task_id) is True
    await a.close()
    await b.close()


@pytest.mark.asyncio
async def test_stale_lease_is_reclaimable(tmp_path):
    """A peer's expired lease admits a new claim."""
    db_path = tmp_path / "inbox.sqlite3"
    a = SqliteMqttInbox(db_path, worker_id="a", logger=logging.getLogger(_LOGGER), lease_ttl=1)
    b = SqliteMqttInbox(db_path, worker_id="b", logger=logging.getLogger(_LOGGER), lease_ttl=1)
    task_id = uuid4()
    await a.put(task_id, TaskData(task_id=str(task_id), task="t", payload=b"x"))
    assert await a.claim(task_id) is True

    # Force the lease into the past rather than sleeping.
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("UPDATE entries SET lease_expires_at = ? WHERE task_id = ?", (time.time() - 10.0, str(task_id)))
        conn.commit()
    finally:
        conn.close()

    assert await b.claim(task_id) is True
    await a.close()
    await b.close()


@pytest.mark.asyncio
async def test_refresh_extends_own_lease_only(tmp_path):
    """``refresh`` renews this worker's claim and leaves a peer's untouched."""
    db_path = tmp_path / "inbox.sqlite3"
    a = SqliteMqttInbox(db_path, worker_id="a", logger=logging.getLogger(_LOGGER), lease_ttl=60)
    b = SqliteMqttInbox(db_path, worker_id="b", logger=logging.getLogger(_LOGGER), lease_ttl=60)
    own = uuid4()
    peer = uuid4()
    await a.put(own, TaskData(task_id=str(own), task="t", payload=b"x"))
    await a.put(peer, TaskData(task_id=str(peer), task="t", payload=b"y"))
    assert await a.claim(own) is True
    assert await b.claim(peer) is True

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("UPDATE entries SET lease_expires_at = 1.0")
        conn.commit()
    finally:
        conn.close()

    await a.refresh([own, peer])

    conn = sqlite3.connect(str(db_path))
    try:
        own_expiry = conn.execute("SELECT lease_expires_at FROM entries WHERE task_id = ?", (str(own),)).fetchone()[0]
        peer_expiry = conn.execute("SELECT lease_expires_at FROM entries WHERE task_id = ?", (str(peer),)).fetchone()[0]
    finally:
        conn.close()
    assert own_expiry > 1.0
    assert peer_expiry == 1.0
    await a.close()
    await b.close()


@pytest.mark.asyncio
async def test_mark_terminal_after_claim_clears_row(tmp_path):
    """``mark_terminal`` deletes the claimed row, releasing the claim."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    await inbox.put(task_id, TaskData(task_id=str(task_id), task="t", payload=b"x"))
    assert await inbox.claim(task_id) is True

    await inbox.mark_terminal(task_id)

    assert await inbox.pending() == []
    await inbox.close()


@pytest.mark.asyncio
async def test_concurrent_puts_from_two_instances_all_persist(tmp_path):
    """Concurrent writers on one DB do not lose entries."""
    db_path = tmp_path / "inbox.sqlite3"
    a = SqliteMqttInbox(db_path, worker_id="a", logger=logging.getLogger(_LOGGER))
    b = SqliteMqttInbox(db_path, worker_id="b", logger=logging.getLogger(_LOGGER))
    ids = [uuid4() for _ in range(20)]

    async def _put(inbox, task_id):
        await inbox.put(task_id, TaskData(task_id=str(task_id), task="t", payload=b"x"))

    await asyncio.gather(*(_put(a if i % 2 == 0 else b, task_id) for i, task_id in enumerate(ids)))

    stored = {task_data_id(td) for td in await a.pending()}
    assert stored == set(ids)
    await a.close()
    await b.close()


@pytest.mark.asyncio
async def test_close_releases_connection(tmp_path):
    """``close`` closes the connection; a later read degrades to empty, not a hang."""
    inbox = _inbox(tmp_path)
    await inbox.close()

    assert await inbox.pending() == []
