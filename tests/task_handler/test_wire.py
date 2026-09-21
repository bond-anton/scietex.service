"""Tests for the versioned task transport envelope wire helpers (AR-064)."""

from uuid import uuid4

import msgspec

from scietex.service.task_handler.schemas import TaskData, TaskEnvelope
from scietex.service.task_handler.wire import (
    decode_task_envelope,
    decode_task_envelope_version,
    encode_task_envelope,
)


def test_encode_decode_roundtrip():
    """encode_task_envelope → decode_task_envelope must round-trip a TaskData."""
    task_data = TaskData(task_id=str(uuid4()), task="send_email", payload=b'{"to": "a@b.c"}')
    encoded = encode_task_envelope(task_data)

    assert isinstance(encoded, bytes)
    assert decode_task_envelope(encoded) == task_data


def test_unknown_version_returns_none():
    """A future/unknown envelope version must decode to None (not raise)."""
    envelope = TaskEnvelope(version=99, data=msgspec.msgpack.encode(TaskData(task_id=str(uuid4()), task="x")))
    encoded = msgspec.msgpack.encode(envelope)

    assert decode_task_envelope(encoded) is None


def test_garbage_bytes_returns_none():
    """Non-msgpack bytes must decode to None without raising."""
    assert decode_task_envelope(b"not-msgpack") is None


def test_decode_task_envelope_version_distinguishes_unknown_from_malformed():
    """The version peek reports the envelope version for a well-formed payload
    and None for a malformed one, so transports can tell an unsupported version
    from a corrupt payload (AR-098)."""
    envelope = TaskEnvelope(version=99, data=msgspec.msgpack.encode(TaskData(task_id=str(uuid4()), task="x")))
    encoded = msgspec.msgpack.encode(envelope)

    assert decode_task_envelope_version(encoded) == 99
    assert decode_task_envelope_version(b"not-msgpack") is None


def test_decode_task_envelope_version_reports_supported_version():
    """A v1 envelope reports version 1."""
    encoded = encode_task_envelope(TaskData(task_id=str(uuid4()), task="send_email"))

    assert decode_task_envelope_version(encoded) == 1


def test_pre_v5_payload_without_task_id_is_rejected():
    """A v1 envelope whose inner TaskData lacks the required task_id field is
    rejected at decode (returns None) rather than decoded with a bogus id.

    Pre-v5 payloads carried ``task``/``payload``/``timeout``/``canceled_action``
    but no ``task_id``; v5 made ``task_id`` a required field, so the inner
    decode raises ``msgspec.ValidationError`` and the envelope decoder maps it
    to ``None``.
    """
    pre_v5_inner = msgspec.msgpack.encode({"task": "send_email", "payload": b"{}"})
    envelope = TaskEnvelope(version=1, data=pre_v5_inner)
    encoded = msgspec.msgpack.encode(envelope)

    assert decode_task_envelope(encoded) is None
