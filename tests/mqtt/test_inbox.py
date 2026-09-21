"""Tests for the file-backed MQTT durable inbox (``FileMqttInbox``)."""

import base64
import json
import logging
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from scietex.service.mqtt.inbox import FileMqttInbox, MemoryInbox
from scietex.service.task_handler.schemas import TaskData, task_data_id
from scietex.service.task_handler.wire import encode_task_envelope

_LOGGER = "test_inbox"


def _inbox(tmp_path, *, ttl=None) -> FileMqttInbox:
    return FileMqttInbox(tmp_path / "inbox", logger=logging.getLogger(_LOGGER), ttl=ttl)


def _write_entry(inbox_dir: Path, task_id: UUID, task_data: TaskData, created_at: float) -> None:
    """Write an entry file directly, bypassing the inbox, for deterministic setup."""
    entry = {
        "task_id": str(task_id),
        "state": "pending",
        "created_at": created_at,
        "envelope": base64.b64encode(encode_task_envelope(task_data)).decode("ascii"),
    }
    (inbox_dir / f"{task_id}.json").write_text(json.dumps(entry), encoding="utf-8")


def _write_tombstone(inbox_dir: Path, task_id: UUID, completed_at: float) -> None:
    """Write a tombstone file directly, bypassing the inbox, for deterministic setup."""
    (inbox_dir / f"{task_id}.done").write_text(json.dumps(completed_at), encoding="utf-8")


@pytest.mark.asyncio
async def test_put_then_pending_roundtrips_task_data(tmp_path):
    """``put`` persists the envelope; ``pending`` returns the original TaskData."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b'{"to": "a@b.c"}')

    await inbox.put(task_id, task_data)

    assert await inbox.pending() == [task_data]


@pytest.mark.asyncio
async def test_in_flight_is_non_terminal(tmp_path):
    """An in-flight entry is still returned by ``pending`` (non-terminal)."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b"x")

    await inbox.put(task_id, task_data)
    await inbox.mark_in_flight(task_id)

    assert await inbox.pending() == [task_data]


@pytest.mark.asyncio
async def test_mark_terminal_hides_entry(tmp_path):
    """``mark_terminal`` tombstones the entry so pending/recover skip it."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()

    await inbox.put(task_id, TaskData(task_id=str(task_id), task="send_email", payload=b"x"))
    await inbox.mark_terminal(task_id)

    assert await inbox.pending() == []
    assert await inbox.recover() == []


@pytest.mark.asyncio
async def test_recover_returns_oldest_first(tmp_path):
    """``recover`` returns non-terminal entries ordered by ``created_at``."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER))
    first = uuid4()
    second = uuid4()
    _write_entry(inbox_dir, first, TaskData(task_id=str(first), task="a", payload=b"1"), created_at=200.0)
    _write_entry(inbox_dir, second, TaskData(task_id=str(second), task="b", payload=b"2"), created_at=100.0)

    assert [task_data_id(td) for td in await inbox.recover()] == [second, first]


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


@pytest.mark.asyncio
async def test_expired_entry_is_dropped(tmp_path):
    """An entry older than ``ttl`` is dropped by pending/recover."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER), ttl=60)
    expired = uuid4()
    _write_entry(
        inbox_dir, expired, TaskData(task_id=str(expired), task="old", payload=b"x"), created_at=time.time() - 120.0
    )
    fresh = uuid4()
    fresh_data = TaskData(task_id=str(fresh), task="new", payload=b"y")

    await inbox.put(fresh, fresh_data)

    assert await inbox.pending() == [fresh_data]
    assert await inbox.recover() == [fresh_data]


@pytest.mark.asyncio
async def test_corrupt_entry_is_skipped_with_warning(tmp_path, caplog):
    """A corrupt entry file is logged at WARNING and skipped without raising."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER))
    (inbox_dir / f"{uuid4()}.json").write_text("not json", encoding="utf-8")
    valid = uuid4()
    task_data = TaskData(task_id=str(valid), task="ok", payload=b"p")

    await inbox.put(valid, task_data)
    with caplog.at_level(logging.WARNING):
        entries = await inbox.pending()

    assert entries == [task_data]
    assert any("Skipping corrupt inbox entry" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
async def test_put_creates_directory(tmp_path):
    """``put`` (re)creates the inbox directory if it is missing."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER))
    inbox_dir.rmdir()  # remove the empty directory after construction
    task_id = uuid4()

    await inbox.put(task_id, TaskData(task_id=str(task_id), task="send_email", payload=b"x"))

    assert inbox_dir.is_dir()
    assert (inbox_dir / f"{task_id}.json").is_file()


@pytest.mark.asyncio
async def test_memory_inbox_put_then_pending_roundtrips():
    """``MemoryInbox.put`` buffers the entry; ``pending`` returns it."""
    inbox = MemoryInbox()
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b'{"to": "a@b.c"}')

    await inbox.put(task_id, task_data)

    assert await inbox.pending() == [task_data]


@pytest.mark.asyncio
async def test_memory_inbox_mark_terminal_removes_entry():
    """``mark_terminal`` drops the buffered entry (no tombstone)."""
    inbox = MemoryInbox()
    task_id = uuid4()
    await inbox.put(task_id, TaskData(task_id=str(task_id), task="send_email", payload=b"x"))

    await inbox.mark_terminal(task_id)

    assert await inbox.pending() == []


@pytest.mark.asyncio
async def test_memory_inbox_mark_in_flight_keeps_entry():
    """``mark_in_flight`` is a no-op: the entry stays non-terminal."""
    inbox = MemoryInbox()
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b"x")
    await inbox.put(task_id, task_data)

    await inbox.mark_in_flight(task_id)

    assert await inbox.pending() == [task_data]


@pytest.mark.asyncio
async def test_memory_inbox_recover_is_empty():
    """``recover`` returns nothing: the at-most-once contract (no restart replay)."""
    inbox = MemoryInbox()
    task_id = uuid4()
    await inbox.put(task_id, TaskData(task_id=str(task_id), task="send_email", payload=b"x"))

    assert await inbox.recover() == []


@pytest.mark.asyncio
async def test_prune_expired_removes_old_tombstone_keeps_fresh(tmp_path):
    """``prune_expired`` deletes a tombstone past ``ttl`` and keeps a fresh one."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER), ttl=60)
    expired = uuid4()
    fresh = uuid4()
    _write_tombstone(inbox_dir, expired, completed_at=time.time() - 120.0)
    _write_tombstone(inbox_dir, fresh, completed_at=time.time())

    await inbox.prune_expired()

    assert not (inbox_dir / f"{expired}.done").exists()
    assert (inbox_dir / f"{fresh}.done").is_file()


@pytest.mark.asyncio
async def test_prune_expired_removes_expired_entry_keeps_in_flight(tmp_path):
    """``prune_expired`` deletes an expired entry and keeps a fresh in-flight one."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER), ttl=60)
    expired = uuid4()
    _write_entry(
        inbox_dir, expired, TaskData(task_id=str(expired), task="old", payload=b"x"), created_at=time.time() - 120.0
    )
    fresh = uuid4()
    fresh_data = TaskData(task_id=str(fresh), task="new", payload=b"y")
    await inbox.put(fresh, fresh_data)
    await inbox.mark_in_flight(fresh)

    await inbox.prune_expired()

    assert not (inbox_dir / f"{expired}.json").exists()
    assert (inbox_dir / f"{fresh}.json").is_file()
    assert await inbox.pending() == [fresh_data]


@pytest.mark.asyncio
async def test_prune_expired_noop_when_ttl_is_none(tmp_path):
    """With ``ttl=None``, ``prune_expired`` leaves tombstones and entries on disk."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER))
    task_id = uuid4()
    entry_id = uuid4()
    _write_tombstone(inbox_dir, task_id, completed_at=time.time() - 999999.0)
    _write_entry(
        inbox_dir,
        entry_id,
        TaskData(task_id=str(entry_id), task="old", payload=b"x"),
        created_at=time.time() - 999999.0,
    )

    await inbox.prune_expired()

    assert (inbox_dir / f"{task_id}.done").is_file()
    assert (inbox_dir / f"{entry_id}.json").is_file()


@pytest.mark.asyncio
async def test_memory_inbox_prune_expired_is_noop():
    """``MemoryInbox.prune_expired`` returns cleanly and keeps entries intact."""
    inbox = MemoryInbox()
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b"x")
    await inbox.put(task_id, task_data)

    await inbox.prune_expired()

    assert await inbox.pending() == [task_data]


@pytest.mark.asyncio
async def test_pending_does_not_prune_expired_tombstone(tmp_path):
    """``pending`` no longer prunes: an expired tombstone survives until ``prune_expired``."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER), ttl=60)
    task_id = uuid4()
    _write_tombstone(inbox_dir, task_id, completed_at=time.time() - 120.0)

    assert await inbox.pending() == []

    assert (inbox_dir / f"{task_id}.done").is_file()


@pytest.mark.asyncio
async def test_prune_expired_reopens_dedup_window(tmp_path):
    """After ``prune_expired`` drops an expired tombstone, a duplicate ``put`` is accepted."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER), ttl=60)
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b"x")
    await inbox.put(task_id, task_data)
    await inbox.mark_terminal(task_id)
    _write_tombstone(inbox_dir, task_id, completed_at=time.time() - 120.0)

    await inbox.prune_expired()

    assert not (inbox_dir / f"{task_id}.done").exists()
    await inbox.put(task_id, task_data)
    assert await inbox.pending() == [task_data]


@pytest.mark.asyncio
async def test_prune_expired_keeps_corrupt_tombstone_active(tmp_path):
    """A corrupt tombstone survives ``prune_expired`` and still suppresses a duplicate."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER), ttl=60)
    task_id = uuid4()
    (inbox_dir / f"{task_id}.done").write_text("not json", encoding="utf-8")

    await inbox.prune_expired()

    assert (inbox_dir / f"{task_id}.done").is_file()
    await inbox.put(task_id, TaskData(task_id=str(task_id), task="send_email", payload=b"x"))
    assert await inbox.pending() == []
