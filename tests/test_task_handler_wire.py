"""Tests for the versioned task transport envelope wire helpers (AR-064)."""

import msgspec

from scietex.service.task_handler.schemas import TaskData, TaskEnvelope
from scietex.service.task_handler.wire import decode_task_envelope, encode_task_envelope


def test_encode_decode_roundtrip():
    """encode_task_envelope → decode_task_envelope must round-trip a TaskData."""
    task_data = TaskData(task="send_email", payload=b'{"to": "a@b.c"}')
    encoded = encode_task_envelope(task_data)

    assert isinstance(encoded, bytes)
    assert decode_task_envelope(encoded) == task_data


def test_unknown_version_returns_none():
    """A future/unknown envelope version must decode to None (not raise)."""
    envelope = TaskEnvelope(version=99, data=msgspec.msgpack.encode(TaskData(task="x")))
    encoded = msgspec.msgpack.encode(envelope)

    assert decode_task_envelope(encoded) is None


def test_garbage_bytes_returns_none():
    """Non-msgpack bytes must decode to None without raising."""
    assert decode_task_envelope(b"not-msgpack") is None
