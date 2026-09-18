"""Tests for the file-backed MQTT durable inbox (``FileMqttInbox``)."""

import base64
import json
import logging
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from scietex.service.mqtt.inbox import FileMqttInbox, MemoryInbox
from scietex.service.task_handler.schemas import TaskData
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


@pytest.mark.asyncio
async def test_put_then_pending_roundtrips_task_data(tmp_path):
    """``put`` persists the envelope; ``pending`` returns the original TaskData."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task="send_email", payload=b'{"to": "a@b.c"}')

    await inbox.put(task_id, task_data)

    assert await inbox.pending() == [(task_id, task_data)]


@pytest.mark.asyncio
async def test_in_flight_is_non_terminal(tmp_path):
    """An in-flight entry is still returned by ``pending`` (non-terminal)."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task="send_email", payload=b"x")

    await inbox.put(task_id, task_data)
    await inbox.mark_in_flight(task_id)

    assert await inbox.pending() == [(task_id, task_data)]


@pytest.mark.asyncio
async def test_mark_terminal_hides_entry(tmp_path):
    """``mark_terminal`` tombstones the entry so pending/recover skip it."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()

    await inbox.put(task_id, TaskData(task="send_email", payload=b"x"))
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
    _write_entry(inbox_dir, first, TaskData(task="a", payload=b"1"), created_at=200.0)
    _write_entry(inbox_dir, second, TaskData(task="b", payload=b"2"), created_at=100.0)

    assert [task_id for task_id, _ in await inbox.recover()] == [second, first]


@pytest.mark.asyncio
async def test_duplicate_put_after_terminal_is_skipped(tmp_path):
    """A tombstone suppresses a duplicate ``put`` of the same task id."""
    inbox = _inbox(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task="send_email", payload=b"x")

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
    _write_entry(inbox_dir, expired, TaskData(task="old", payload=b"x"), created_at=time.time() - 120.0)
    fresh = uuid4()
    fresh_data = TaskData(task="new", payload=b"y")

    await inbox.put(fresh, fresh_data)

    assert await inbox.pending() == [(fresh, fresh_data)]
    assert await inbox.recover() == [(fresh, fresh_data)]


@pytest.mark.asyncio
async def test_corrupt_entry_is_skipped_with_warning(tmp_path, caplog):
    """A corrupt entry file is logged at WARNING and skipped without raising."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER))
    (inbox_dir / f"{uuid4()}.json").write_text("not json", encoding="utf-8")
    valid = uuid4()
    task_data = TaskData(task="ok", payload=b"p")

    await inbox.put(valid, task_data)
    with caplog.at_level(logging.WARNING):
        entries = await inbox.pending()

    assert entries == [(valid, task_data)]
    assert any("Skipping corrupt inbox entry" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
async def test_put_creates_directory(tmp_path):
    """``put`` (re)creates the inbox directory if it is missing."""
    inbox_dir = tmp_path / "inbox"
    inbox = FileMqttInbox(inbox_dir, logger=logging.getLogger(_LOGGER))
    inbox_dir.rmdir()  # remove the empty directory after construction
    task_id = uuid4()

    await inbox.put(task_id, TaskData(task="send_email", payload=b"x"))

    assert inbox_dir.is_dir()
    assert (inbox_dir / f"{task_id}.json").is_file()


@pytest.mark.asyncio
async def test_memory_inbox_put_then_pending_roundtrips():
    """``MemoryInbox.put`` buffers the entry; ``pending`` returns it."""
    inbox = MemoryInbox()
    task_id = uuid4()
    task_data = TaskData(task="send_email", payload=b'{"to": "a@b.c"}')

    await inbox.put(task_id, task_data)

    assert await inbox.pending() == [(task_id, task_data)]


@pytest.mark.asyncio
async def test_memory_inbox_mark_terminal_removes_entry():
    """``mark_terminal`` drops the buffered entry (no tombstone)."""
    inbox = MemoryInbox()
    task_id = uuid4()
    await inbox.put(task_id, TaskData(task="send_email", payload=b"x"))

    await inbox.mark_terminal(task_id)

    assert await inbox.pending() == []


@pytest.mark.asyncio
async def test_memory_inbox_mark_in_flight_keeps_entry():
    """``mark_in_flight`` is a no-op: the entry stays non-terminal."""
    inbox = MemoryInbox()
    task_id = uuid4()
    task_data = TaskData(task="send_email", payload=b"x")
    await inbox.put(task_id, task_data)

    await inbox.mark_in_flight(task_id)

    assert await inbox.pending() == [(task_id, task_data)]


@pytest.mark.asyncio
async def test_memory_inbox_recover_is_empty():
    """``recover`` returns nothing: the at-most-once contract (no restart replay)."""
    inbox = MemoryInbox()
    await inbox.put(uuid4(), TaskData(task="send_email", payload=b"x"))

    assert await inbox.recover() == []
