"""TaskProcessor process_task dispatch and error-taxonomy tests."""

from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData, TaskResult

from ._helpers import (
    DemoProcessor,
    DummyHandler,
    RaisingHandler,
    ReturningErrorHandler,
)


@pytest.mark.asyncio
async def test_process_task_with_dummy_handler():
    proc = DemoProcessor()
    proc.add_task_handler(DummyHandler)
    await proc._start_task_handler("DummyHandler")

    result: TaskResult = await proc.process_task(TaskData(task_id=str(uuid4()), task="dummy", payload=b'{"value": 5}'))

    assert result.status == "success"
    assert result.payload.decode("utf-8") == '{"value": 5}'


@pytest.mark.asyncio
async def test_process_task_empty_task_returns_error_result():
    """An empty task type must yield an error TaskResult, not raise (AR-010)."""
    proc = DemoProcessor()
    result: TaskResult = await proc.process_task(TaskData(task_id=str(uuid4()), task="", payload=b"{}"))
    assert result.status == "error"
    assert "task" in result.error
    assert result.retryable is False


@pytest.mark.asyncio
async def test_process_task_handler_exception_is_permanent():
    """A handler that raises must yield an error TaskResult with retryable
    False (permanent) by default, with the exception message in ``error``
    (AR-022 v4)."""
    proc = DemoProcessor()
    proc.add_task_handler(RaisingHandler)
    await proc._start_task_handler("RaisingHandler")

    result: TaskResult = await proc.process_task(TaskData(task_id=str(uuid4()), task="raiser", payload=b"{}"))

    assert result.status == "error"
    assert result.error == "boom"
    assert result.retryable is False
    assert result.error_code == ""
    assert result.partial is False


@pytest.mark.asyncio
async def test_process_task_preserves_handler_returned_result_fields():
    """A handler that returns a TaskResult controls its own error-taxonomy
    fields; process_task must pass them through unchanged (AR-022)."""
    proc = DemoProcessor()
    proc.add_task_handler(ReturningErrorHandler)
    await proc._start_task_handler("ReturningErrorHandler")

    result: TaskResult = await proc.process_task(TaskData(task_id=str(uuid4()), task="error_returner", payload=b"{}"))

    assert result.status == "error"
    assert result.error == "x"
    assert result.retryable is False
    assert result.error_code == "PERMANENT"
    assert result.partial is True


@pytest.mark.asyncio
async def test_process_task_no_handler_is_not_retryable():
    """The 'no handler found' framework error is permanent: retryable stays
    False (AR-022)."""
    proc = DemoProcessor()
    result: TaskResult = await proc.process_task(TaskData(task_id=str(uuid4()), task="unknown", payload=b"{}"))

    assert result.status == "error"
    assert "No handler" in result.error
    assert result.retryable is False
    assert result.error_code == ""
