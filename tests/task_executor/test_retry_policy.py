"""TaskExecutor retry-policy tests: requeue-once, budget lifecycle, terminal ack."""

from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData, TaskResult

from ._helpers import Recording, build_executor


def _retryable_error() -> TaskResult:
    return TaskResult(status="error", error="transient", retryable=True)


def _permanent_error() -> TaskResult:
    return TaskResult(status="error", error="permanent", retryable=False)


@pytest.mark.asyncio
async def test_retryable_error_requeues_and_bumps_budget():
    """A retryable error under budget is requeued and its budget bumped (not acked)."""
    recording = Recording()
    retry_attempts = {}
    executor = build_executor(recording, retry_attempts=retry_attempts)
    task_id = uuid4()
    task_data = TaskData(task="retryable")
    result = _retryable_error()

    ack = await executor._apply_retry_policy(task_id, task_data, result)

    assert ack is not None
    assert ack is result
    assert ack.retryable is True
    assert recording.requeued == [(task_id, task_data)]
    assert retry_attempts == {task_id: 1}


@pytest.mark.asyncio
async def test_retryable_error_exhausts_budget_and_acks_terminal():
    """The second consecutive retryable failure is acked as retryable=False with
    an empty budget (the transports leave a retryable entry pending, so the
    terminal ack must not present it as retryable)."""
    recording = Recording()
    task_id = uuid4()
    retry_attempts = {task_id: 1}
    executor = build_executor(recording, retry_attempts=retry_attempts)
    task_data = TaskData(task="retryable")

    ack = await executor._apply_retry_policy(task_id, task_data, _retryable_error())

    assert ack is not None
    assert ack.retryable is False
    assert not recording.requeued
    assert retry_attempts == {}


@pytest.mark.asyncio
async def test_non_retryable_error_pops_budget():
    """A permanent error is dropped without requeue and clears the budget."""
    recording = Recording()
    task_id = uuid4()
    retry_attempts = {task_id: 1}
    executor = build_executor(recording, retry_attempts=retry_attempts)
    task_data = TaskData(task="permanent")
    result = _permanent_error()

    ack = await executor._apply_retry_policy(task_id, task_data, result)

    assert ack is result
    assert not recording.requeued
    assert retry_attempts == {}


@pytest.mark.asyncio
async def test_success_result_pops_budget():
    """A success is acked unchanged and clears any stale budget."""
    recording = Recording()
    task_id = uuid4()
    retry_attempts = {task_id: 1}
    executor = build_executor(recording, retry_attempts=retry_attempts)
    task_data = TaskData(task="ok")
    result = TaskResult(status="success")

    ack = await executor._apply_retry_policy(task_id, task_data, result)

    assert ack is result
    assert not recording.requeued
    assert retry_attempts == {}


@pytest.mark.asyncio
async def test_none_result_pops_budget():
    """A cancelled task (result None) clears the budget and acks nothing."""
    recording = Recording()
    task_id = uuid4()
    retry_attempts = {task_id: 1}
    executor = build_executor(recording, retry_attempts=retry_attempts)
    task_data = TaskData(task="cancelled")

    ack = await executor._apply_retry_policy(task_id, task_data, None)

    assert ack is None
    assert not recording.requeued
    assert retry_attempts == {}


@pytest.mark.asyncio
async def test_requeue_failure_pops_budget_and_returns_result():
    """A failed requeue loses the retry copy but still acks: the budget is popped
    so a phantom second retry is never granted, and the result is returned as-is."""

    class FailingRecording(Recording):
        async def requeue(self, task_id, task_data):
            self.requeued.append((task_id, task_data))
            raise RuntimeError("requeue boom")

    recording = FailingRecording()
    retry_attempts = {}
    executor = build_executor(recording, retry_attempts=retry_attempts)
    task_id = uuid4()
    task_data = TaskData(task="retryable")
    result = _retryable_error()

    ack = await executor._apply_retry_policy(task_id, task_data, result)

    assert ack is not None
    assert ack is result
    assert ack.retryable is True
    assert retry_attempts == {}
    assert recording.requeued == [(task_id, task_data)]


@pytest.mark.asyncio
async def test_timeout_cancel_preserves_timeout_budget():
    """A timeout cancel (result None) leaves the watchdog-owned timeout budget
    intact so the redelivery cycle keeps its count, while clearing the
    error-path retry budget."""
    recording = Recording()
    task_id = uuid4()
    retry_attempts = {task_id: 1}
    executor = build_executor(recording, retry_attempts=retry_attempts)
    executor._timeout_requeues = {task_id: 2}
    task_data = TaskData(task="timeout")

    ack = await executor._apply_retry_policy(task_id, task_data, None, cancel_reason="timeout")

    assert ack is None
    assert executor._timeout_requeues == {task_id: 2}
    assert retry_attempts == {}


@pytest.mark.asyncio
async def test_non_timeout_cancel_clears_timeout_budget():
    """A deliberate or shutdown cancel (result None) clears both budgets."""
    for reason in ("deliberate", "shutdown"):
        recording = Recording()
        task_id = uuid4()
        retry_attempts = {task_id: 1}
        executor = build_executor(recording, retry_attempts=retry_attempts)
        executor._timeout_requeues = {task_id: 2}
        task_data = TaskData(task="cancelled")

        ack = await executor._apply_retry_policy(task_id, task_data, None, cancel_reason=reason)

        assert ack is None
        assert executor._timeout_requeues == {}
        assert retry_attempts == {}
