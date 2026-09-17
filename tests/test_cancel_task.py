"""Tests for the built-in ``cancel_task`` handler and its wire structs."""

import logging
from uuid import UUID, uuid4

import msgspec
import pytest

from scietex.service.task_handler.cancel import (
    CancelOutcome,
    CancelTaskHandler,
    CancelTaskRequest,
    CancelTaskResponse,
)
from scietex.service.task_handler.capabilities import TaskCapabilities
from scietex.service.task_handler.context import TaskHandlerContext
from scietex.service.task_handler.schemas import CANCEL_TASK_TYPE, TaskData


def _context() -> TaskHandlerContext:
    return TaskHandlerContext(service_name="svc", instance_id="inst", logger=logging.getLogger("test"))


async def _noop_progress(_task_id: UUID, _value: float) -> None:
    pass


def _capabilities() -> TaskCapabilities:
    return TaskCapabilities(task_id=uuid4(), _write_progress=_noop_progress)


def test_cancel_task_request_round_trips_through_msgpack():
    """CancelTaskRequest survives a msgpack round-trip."""
    request = CancelTaskRequest(target_task_id="abc-123", reason="operator note")
    decoded = msgspec.msgpack.decode(msgspec.msgpack.encode(request), type=CancelTaskRequest)
    assert decoded == request


def test_cancel_task_request_reason_defaults_empty():
    """The optional reason defaults to an empty string."""
    request = CancelTaskRequest(target_task_id="abc-123")
    assert request.reason == ""


def test_cancel_task_response_round_trips_through_msgpack():
    """CancelTaskResponse survives a msgpack round-trip."""
    response = CancelTaskResponse(target_task_id="abc-123", outcome="cancelled")
    decoded = msgspec.msgpack.decode(msgspec.msgpack.encode(response), type=CancelTaskResponse)
    assert decoded == response


def test_cancel_task_handler_supports_cancel_task_type():
    """The handler declares exactly the cancel_task task type."""
    handler = CancelTaskHandler("CancelTaskHandler", _context(), cancel=_noop_cancel)
    assert handler.supported_tasks == [CANCEL_TASK_TYPE]
    assert handler.supports(CANCEL_TASK_TYPE)
    assert not handler.supports("dummy")


async def _noop_cancel(_target_id: UUID) -> CancelOutcome:
    return "cancelled"


@pytest.mark.asyncio
async def test_cancel_task_handler_success_returns_response_payload():
    """A cancelled outcome yields a success result with a CancelTaskResponse."""
    target_id = uuid4()
    seen: list[UUID] = []

    async def cancel(tid: UUID) -> CancelOutcome:
        seen.append(tid)
        return "cancelled"

    handler = CancelTaskHandler("CancelTaskHandler", _context(), cancel=cancel)
    payload = msgspec.msgpack.encode(CancelTaskRequest(target_task_id=str(target_id)))
    result = await handler.handle(TaskData(task=CANCEL_TASK_TYPE, payload=payload), capabilities=_capabilities())

    assert seen == [target_id]
    assert result.status == "success"
    response = msgspec.msgpack.decode(result.payload, type=CancelTaskResponse)
    assert response.target_task_id == str(target_id)
    assert response.outcome == "cancelled"


@pytest.mark.asyncio
async def test_cancel_task_handler_not_running_returns_error():
    """A not_running outcome yields a non-retryable TASK_NOT_RUNNING error."""

    async def cancel(_tid: UUID) -> CancelOutcome:
        return "not_running"

    handler = CancelTaskHandler("CancelTaskHandler", _context(), cancel=cancel)
    payload = msgspec.msgpack.encode(CancelTaskRequest(target_task_id=str(uuid4())))
    result = await handler.handle(TaskData(task=CANCEL_TASK_TYPE, payload=payload), capabilities=_capabilities())

    assert result.status == "error"
    assert result.error_code == "TASK_NOT_RUNNING"
    assert result.retryable is False


@pytest.mark.asyncio
async def test_cancel_task_handler_ignored_returns_error():
    """An ignored outcome yields a non-retryable CANCEL_IGNORED error."""

    async def cancel(_tid: UUID) -> CancelOutcome:
        return "ignored"

    handler = CancelTaskHandler("CancelTaskHandler", _context(), cancel=cancel)
    payload = msgspec.msgpack.encode(CancelTaskRequest(target_task_id=str(uuid4())))
    result = await handler.handle(TaskData(task=CANCEL_TASK_TYPE, payload=payload), capabilities=_capabilities())

    assert result.status == "error"
    assert result.error_code == "CANCEL_IGNORED"
    assert result.retryable is False


@pytest.mark.asyncio
async def test_cancel_task_handler_malformed_payload_returns_invalid_code():
    """A payload that is not a CancelTaskRequest yields INVALID_CANCEL_PAYLOAD
    without raising."""
    handler = CancelTaskHandler("CancelTaskHandler", _context(), cancel=_noop_cancel)
    result = await handler.handle(TaskData(task=CANCEL_TASK_TYPE, payload=b"not-msgpack"), capabilities=_capabilities())

    assert result.status == "error"
    assert result.error_code == "INVALID_CANCEL_PAYLOAD"
    assert result.retryable is False


@pytest.mark.asyncio
async def test_cancel_task_handler_invalid_uuid_returns_invalid_code():
    """A well-formed request with a non-UUID target id yields
    INVALID_CANCEL_PAYLOAD without raising."""
    handler = CancelTaskHandler("CancelTaskHandler", _context(), cancel=_noop_cancel)
    payload = msgspec.msgpack.encode(CancelTaskRequest(target_task_id="not-a-uuid"))
    result = await handler.handle(TaskData(task=CANCEL_TASK_TYPE, payload=payload), capabilities=_capabilities())

    assert result.status == "error"
    assert result.error_code == "INVALID_CANCEL_PAYLOAD"
    assert result.retryable is False
