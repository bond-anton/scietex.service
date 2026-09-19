"""Shared fakes and factory helpers for the isolated TaskExecutor suite (AR-101)."""

import asyncio
import logging
from uuid import UUID

from scietex.service.config_reload import ReloadableSettings
from scietex.service.task_executor import TaskExecutor
from scietex.service.task_handler.runtime import TaskTracker
from scietex.service.task_handler.schemas import TaskData, TaskResult
from scietex.service.task_lifecycle import TaskLifecycle

_logger = logging.getLogger("scietex.service.task_executor.tests")


def make_settings(**overrides) -> ReloadableSettings:
    """Build a ReloadableSettings snapshot with fast, test-friendly defaults."""
    defaults = {
        "max_concurrent_tasks": 10,
        "task_manager_sleep_time": 0.0,
        "task_queue_manager_sleep_time": 0.0,
        "task_handler_start_timeout": 1.0,
        "task_handler_stop_timeout": 1.0,
        "task_timeout": 3.0,
        "task_queue_fetch_timeout": 0.05,
        "task_cancellation_timeout": 0.05,
    }
    defaults.update(overrides)
    return ReloadableSettings(**defaults)


class Recording:
    """Records every hook invocation the executor makes into a shared list.

    ``process_result`` is the fixed value the fake ``process_task`` returns
    (a ``TaskResult``, or an ``Exception`` to raise); ``None`` defaults to a
    success result.
    """

    def __init__(self, *, process_result=None):
        self.started: list = []
        self.completed: list = []
        self.requeued: list = []
        self.drained: list = []
        self.processed: list = []
        self.process_result = process_result

    async def process_task(self, task_id: UUID, task_data: TaskData) -> TaskResult:
        self.processed.append((task_id, task_data))
        if isinstance(self.process_result, Exception):
            raise self.process_result
        return self.process_result if self.process_result is not None else TaskResult(status="success")

    async def on_started(self, task_id: UUID, task_data: TaskData) -> None:
        self.started.append((task_id, task_data))

    async def on_completed(self, task_id, task_data, result, *, cancel_reason=None):
        self.completed.append((task_id, task_data, result, cancel_reason))

    async def requeue(self, task_id: UUID, task_data: TaskData) -> None:
        self.requeued.append((task_id, task_data))

    async def on_drain(self, task_id: UUID, task_data: TaskData) -> None:
        self.drained.append((task_id, task_data))


def build_executor(
    recording,
    *,
    queue=None,
    lifecycle=None,
    retry_attempts=None,
    settings=None,
    max_retries=1,
    max_timeout_requeues=1,
) -> TaskExecutor:
    """Wire a TaskExecutor to a Recording and caller-provided storage."""
    return TaskExecutor(
        queue=queue if queue is not None else asyncio.Queue(),
        lifecycle=lifecycle if lifecycle is not None else TaskLifecycle(),
        retry_attempts=retry_attempts if retry_attempts is not None else {},
        process_task=recording.process_task,
        on_started=recording.on_started,
        on_completed=recording.on_completed,
        requeue=recording.requeue,
        on_drain=recording.on_drain,
        settings=(lambda: settings) if settings is not None else (lambda: make_settings()),
        logger=_logger,
        max_retries=max_retries,
        max_timeout_requeues=max_timeout_requeues,
    )


async def _blocker() -> None:
    await asyncio.Event().wait()


def register_running(lifecycle: TaskLifecycle, task_id: UUID, task_data: TaskData, *, started=0.0) -> TaskTracker:
    """Register a tracker whose worker task blocks until cancelled."""
    task = asyncio.create_task(_blocker())
    tracker = TaskTracker(worker_task=task, data=task_data, started=started)
    lifecycle.register(task_id, tracker)
    return tracker


async def register_finished(
    lifecycle: TaskLifecycle, task_id: UUID, task_data: TaskData, *, started=0.0
) -> TaskTracker:
    """Register a tracker whose worker task is already done (no lingering task)."""
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    tracker = TaskTracker(worker_task=task, data=task_data, started=started)
    lifecycle.register(task_id, tracker)
    return tracker
