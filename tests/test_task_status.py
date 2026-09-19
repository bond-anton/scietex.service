"""Tests for the pure ``TaskStatus`` builders (AR-114)."""

import logging
from datetime import datetime, timezone
from uuid import UUID

import msgspec
import pytest
from mqtt.test_transport import _transport
from valkey._helpers import DummyClient

from scietex.service.task_handler.schemas import TaskData, TaskProgress, TaskResult, TaskStatus
from scietex.service.task_status import build_running_status, build_terminal_status
from scietex.service.valkey.tracking import TaskStatusStore

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
TASK_ID = UUID("11111111-1111-1111-1111-111111111111")

# The fields both transports must populate identically for a terminal task
# (AR-114). ``created_at``/``updated_at`` are stamped per builder call, so the
# cross-transport comparison excludes them (each is checked separately below).
_TERMINAL_FIELDS = ("task_id", "service", "task", "status", "progress", "result", "data", "error", "error_code")

_TERMINAL_CASES = [
    (TaskResult(status="success", payload=b"done"), None),
    (TaskResult(status="error", error="boom", error_code="PERMANENT"), None),
    (None, "deliberate"),
    (None, "timeout"),
    (None, "shutdown"),
]


def _task_data() -> TaskData:
    return TaskData(task="dummy", payload=b"{}")


def _terminal_fields(status: TaskStatus) -> tuple:
    """Project a record onto the fields both transports populate identically."""
    return tuple(getattr(status, field) for field in _TERMINAL_FIELDS)


def test_build_running_status_default():
    status = build_running_status(TASK_ID, "svc", _task_data(), now=NOW)

    assert status.status == "running"
    assert status.progress == TaskProgress()
    assert status.result is None
    assert status.data is None
    assert status.error == ""
    assert status.error_code == ""
    assert status.task_id == str(TASK_ID)
    assert status.service == "svc"
    assert status.task == "dummy"
    assert status.created_at == NOW
    assert status.updated_at == NOW


def test_build_running_status_queued():
    status = build_running_status(TASK_ID, "svc", _task_data(), status="queued", now=NOW)

    assert status.status == "queued"
    assert status.created_at == NOW
    assert status.updated_at == NOW


def test_build_terminal_status_success_result():
    result = TaskResult(status="success", payload=b"done")
    status = build_terminal_status(TASK_ID, "svc", _task_data(), result, now=NOW)

    assert status.status == "completed"
    assert status.result == b"done"
    assert status.error == result.error
    assert status.error_code == result.error_code
    assert status.data is None
    assert status.created_at == NOW
    assert status.updated_at == NOW


def test_build_terminal_status_error_result():
    result = TaskResult(status="error", error="boom", error_code="PERMANENT")
    status = build_terminal_status(TASK_ID, "svc", _task_data(), result, now=NOW)

    assert status.status == "failed"
    assert status.result is None
    assert status.error == "boom"
    assert status.error_code == "PERMANENT"
    assert status.data is None
    assert status.created_at == NOW
    assert status.updated_at == NOW


def test_build_terminal_status_deliberate_cancel():
    task_data = _task_data()
    status = build_terminal_status(TASK_ID, "svc", task_data, None, "deliberate", now=NOW)

    assert status.status == "cancelled"
    assert status.data == task_data
    assert status.error == "canceled"
    assert status.error_code == ""
    assert status.created_at == NOW
    assert status.updated_at == NOW


def test_build_terminal_status_timeout_cancel():
    status = build_terminal_status(TASK_ID, "svc", _task_data(), None, "timeout", now=NOW)

    assert status.status == "failed"
    assert status.data is None
    assert status.error == "canceled"
    assert status.created_at == NOW
    assert status.updated_at == NOW


def test_build_terminal_status_shutdown_cancel():
    status = build_terminal_status(TASK_ID, "svc", _task_data(), None, "shutdown", now=NOW)

    assert status.status == "failed"
    assert status.data is None
    assert status.error == "canceled"
    assert status.created_at == NOW
    assert status.updated_at == NOW


def test_build_terminal_status_none_cancel_fallback():
    status = build_terminal_status(TASK_ID, "svc", _task_data(), None, None, now=NOW)

    assert status.status == "failed"
    assert status.data is None
    assert status.error == "canceled"
    assert status.created_at == NOW
    assert status.updated_at == NOW


def test_build_terminal_status_missing_task_data():
    status = build_terminal_status(TASK_ID, "svc", None, None, None, now=NOW)

    assert status.task == ""
    assert status.status == "failed"
    assert status.created_at == NOW
    assert status.updated_at == NOW


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_result, cancel_reason",
    _TERMINAL_CASES,
    ids=["success", "error", "cancel-deliberate", "cancel-timeout", "cancel-shutdown"],
)
async def test_transports_produce_equivalent_terminal_status(task_result, cancel_reason):
    """Valkey tracking and MQTT status publishing must build field-equal
    ``TaskStatus`` records for the same terminal input (AR-114)."""
    task_data = _task_data()

    # Valkey path: TaskStatusStore.record_terminal persists via client.set(msgpack).
    client = DummyClient()
    store = TaskStatusStore(
        service_name="svc",
        tracking_ttl=3600,
        client_provider=lambda: client,
        logger=logging.getLogger("test_task_status"),
    )
    await store.record_terminal(TASK_ID, task_data, task_result, cancel_reason)
    assert len(client.sets) == 1
    valkey_status = msgspec.msgpack.decode(client.sets[0][1], type=TaskStatus)

    # MQTT path: MqttTransport.ack publishes a retained terminal status.
    transport, _, published = _transport()
    await transport.ack(TASK_ID, task_data, task_result, cancel_reason=cancel_reason)
    assert len(published) == 1
    mqtt_status = msgspec.msgpack.decode(published[0][1], type=TaskStatus)

    # Timestamps are stamped per builder call (datetime.now), so they differ
    # between transports; each record must still stamp created_at == updated_at.
    assert valkey_status.created_at == valkey_status.updated_at
    assert mqtt_status.created_at == mqtt_status.updated_at

    assert _terminal_fields(valkey_status) == _terminal_fields(mqtt_status)
