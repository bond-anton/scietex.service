"""Tests for the built-in ``worker:*`` control handler and its wire structs."""

import logging
from uuid import uuid4

import msgspec
import pytest

from scietex.service.task_handler.capabilities import TaskCapabilities
from scietex.service.task_handler.context import TaskHandlerContext
from scietex.service.task_handler.schemas import (
    WORKER_EXIT_TASK_NAME,
    WORKER_RESTART_TASK_NAME,
    WORKER_START_TASK_NAME,
    WORKER_STOP_TASK_NAME,
    TaskData,
)
from scietex.service.task_handler.worker import (
    WorkerControlHandler,
    WorkerControlRequest,
    WorkerControlResponse,
)


def _context() -> TaskHandlerContext:
    return TaskHandlerContext(service_name="svc", instance_id="inst", logger=logging.getLogger("test"))


async def _noop_progress(_task_id, _value: float) -> None:
    pass


def _capabilities() -> TaskCapabilities:
    return TaskCapabilities(task_id=uuid4(), _write_progress=_noop_progress)


class _Recorder:
    """Records which lifecycle callbacks fired."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def start(self) -> None:
        self.calls.append("start")

    async def stop(self) -> None:
        self.calls.append("stop")

    async def restart(self) -> None:
        self.calls.append("restart")

    async def exit(self) -> None:
        self.calls.append("exit")


def _handler(recorder: _Recorder) -> WorkerControlHandler:
    return WorkerControlHandler(
        "WorkerControlHandler",
        _context(),
        start=recorder.start,
        stop=recorder.stop,
        restart=recorder.restart,
        exit=recorder.exit,
    )


def test_worker_control_request_round_trips_through_msgpack():
    """WorkerControlRequest survives a msgpack round-trip."""
    request = WorkerControlRequest(reason="operator note")
    decoded = msgspec.msgpack.decode(msgspec.msgpack.encode(request), type=WorkerControlRequest)
    assert decoded == request


def test_worker_control_request_reason_defaults_empty():
    """The optional reason defaults to an empty string."""
    assert WorkerControlRequest().reason == ""


def test_worker_control_response_round_trips_through_msgpack():
    """WorkerControlResponse survives a msgpack round-trip."""
    response = WorkerControlResponse(action=WORKER_STOP_TASK_NAME)
    decoded = msgspec.msgpack.decode(msgspec.msgpack.encode(response), type=WorkerControlResponse)
    assert decoded == response
    assert decoded.accepted is True


def test_worker_control_handler_is_control_plane():
    """The handler declares itself control-plane so it lands in the control registry."""
    assert WorkerControlHandler.control is True


def test_worker_control_handler_supports_all_worker_names():
    """The handler declares exactly the four worker:* task names."""
    handler = _handler(_Recorder())
    assert set(handler.supported_tasks) == {
        WORKER_START_TASK_NAME,
        WORKER_STOP_TASK_NAME,
        WORKER_RESTART_TASK_NAME,
        WORKER_EXIT_TASK_NAME,
    }
    assert handler.supports(WORKER_START_TASK_NAME)
    assert not handler.supports("dummy")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_name", "expected"),
    [
        (WORKER_START_TASK_NAME, "start"),
        (WORKER_STOP_TASK_NAME, "stop"),
        (WORKER_RESTART_TASK_NAME, "restart"),
        (WORKER_EXIT_TASK_NAME, "exit"),
    ],
)
async def test_worker_control_handler_dispatches_to_matching_callback(task_name: str, expected: str):
    """Each worker:* name invokes exactly its own callback and returns a response."""
    recorder = _Recorder()
    handler = _handler(recorder)
    payload = msgspec.msgpack.encode(WorkerControlRequest())
    result = await handler.handle(
        TaskData(task_id=str(uuid4()), task=task_name, payload=payload), capabilities=_capabilities()
    )

    assert recorder.calls == [expected]
    assert result.status == "success"
    response = msgspec.msgpack.decode(result.payload, type=WorkerControlResponse)
    assert response.action == task_name
    assert response.accepted is True


@pytest.mark.asyncio
async def test_worker_control_handler_unknown_action_returns_error():
    """A task name outside the four worker:* names yields UNKNOWN_WORKER_ACTION."""
    recorder = _Recorder()
    handler = _handler(recorder)
    result = await handler.handle(
        TaskData(task_id=str(uuid4()), task="worker:bogus", payload=b""), capabilities=_capabilities()
    )

    assert recorder.calls == []
    assert result.status == "error"
    assert result.error_code == "UNKNOWN_WORKER_ACTION"
    assert result.retryable is False


@pytest.mark.asyncio
async def test_worker_control_handler_malformed_payload_returns_invalid_code():
    """A payload that is not a WorkerControlRequest yields INVALID_WORKER_PAYLOAD
    without raising, and the callback does not fire."""
    recorder = _Recorder()
    handler = _handler(recorder)
    result = await handler.handle(
        TaskData(task_id=str(uuid4()), task=WORKER_STOP_TASK_NAME, payload=b"not-msgpack"),
        capabilities=_capabilities(),
    )

    assert recorder.calls == []
    assert result.status == "error"
    assert result.error_code == "INVALID_WORKER_PAYLOAD"
    assert result.retryable is False
