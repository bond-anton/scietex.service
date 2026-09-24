"""Tests for the in-memory MQTT inbox (``MemoryInbox``)."""

from uuid import uuid4

import pytest

from scietex.service.mqtt.inbox import MemoryInbox
from scietex.service.task_handler.schemas import TaskData


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
async def test_memory_inbox_prune_expired_is_noop():
    """``MemoryInbox.prune_expired`` returns cleanly and keeps entries intact."""
    inbox = MemoryInbox()
    task_id = uuid4()
    task_data = TaskData(task_id=str(task_id), task="send_email", payload=b"x")
    await inbox.put(task_id, task_data)

    await inbox.prune_expired()

    assert await inbox.pending() == [task_data]


@pytest.mark.asyncio
async def test_memory_inbox_claim_always_wins_and_release_is_noop():
    """The in-memory backend claims unconditionally and no-ops the rest."""
    inbox = MemoryInbox()
    task_id = uuid4()

    assert await inbox.claim(task_id) is True
    await inbox.release(task_id)
    await inbox.refresh([task_id])
    await inbox.close()
