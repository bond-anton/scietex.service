"""Versioned transport wire helpers for the task handler subsystem.

The durable on-the-wire format for a task is a versioned
:class:`~scietex.service.task_handler.schemas.TaskEnvelope`, not a bare
``TaskData``. These helpers encode and decode that envelope so every
transport (Valkey now, MQTT/Kafka later) shares one versioned format
(AR-064).
"""

import msgspec

from .schemas import TaskData, TaskEnvelope


def encode_task_envelope(task_data: TaskData) -> bytes:
    """Encode a ``TaskData`` into a versioned transport envelope (msgpack bytes).

    Args:
        task_data: The :class:`TaskData` to wrap and serialize.

    Returns:
        The msgpack-encoded :class:`TaskEnvelope` bytes.
    """
    envelope = TaskEnvelope(version=1, data=msgspec.msgpack.encode(task_data))
    return msgspec.msgpack.encode(envelope)


def decode_task_envelope(payload: bytes) -> TaskData | None:
    """Decode a versioned envelope back to a ``TaskData``.

    Returns ``None`` when the payload is not a valid envelope or carries an
    unknown version, so callers can skip the entry without crashing intake.

    Args:
        payload: The msgpack-encoded envelope bytes read from the transport.

    Returns:
        The decoded :class:`TaskData`, or ``None`` on an invalid payload or
        unknown version.
    """
    try:
        envelope = msgspec.msgpack.decode(payload, type=TaskEnvelope)
        if envelope.version == 1:
            return msgspec.msgpack.decode(envelope.data, type=TaskData)
        return None  # unknown version
    except msgspec.DecodeError:
        return None
