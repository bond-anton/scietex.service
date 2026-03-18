import asyncio

import pytest

from uuid import uuid4
from scietex.service.async_tasks_processor import AsyncTaskProcessor
from scietex.service.task_handlers.basic import TaskHandler
from scietex.service.task_handlers.schemas import TaskData, TaskResult, TaskTimeout


class DummyHandler(TaskHandler):
    def __init__(self, worker):
        super().__init__(worker)
        self._is_initialized = True

    async def handle(self, task_data: TaskData) -> TaskResult:
        result = task_data.payload.decode("utf-8")
        return TaskResult(
            status="success", error="No error", payload=result.encode("utf-8")
        )

    def supports(self, task_type: str) -> bool:
        return task_type == "dummy"


class SlowHandler(TaskHandler):
    def __init__(self, worker):
        super().__init__(worker)
        self._is_initialized = True

    async def handle(self, task_data: TaskData) -> TaskResult:
        # simulate long running task
        await asyncio.sleep(2)
        return TaskResult(payload=task_data.payload, status="success", error="No error")

    def supports(self, task_type: str) -> bool:
        return task_type == "slow"


class DemoProcessor(AsyncTaskProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.requeued: list = []

    async def fetch_tasks(self) -> None:  # pragma: no cover - stub
        return None

    async def return_task_to_queue(self, task_id, task_data):
        # record requeued tasks for assertions
        self.requeued.append((task_id, task_data))

    async def _logger_init_handlers(self) -> None:  # disable real logging start
        return None

    async def _logger_shut_down_handlers(self) -> None:  # disable real logging stop
        return None


@pytest.mark.asyncio
async def test_process_task_with_dummy_handler():
    proc = DemoProcessor()
    proc.register_task_handler("dummy", DummyHandler)

    result: TaskResult = await proc.process_task(
        uuid4(), TaskData(task="dummy", payload=b'{"value": 5}')
    )

    assert result.status == "success"
    assert result.payload.decode("utf-8") == '{"value": 5}'


@pytest.mark.asyncio
async def test_watchdog_requeues_timed_out_task():
    proc = DemoProcessor()
    proc.register_task_handler("slow", SlowHandler)

    # start managers (task_manager, task_queue_manager, watchdog)
    await proc.start()

    # push a task that will timeout quickly
    t_id = uuid4()
    await proc.task_queue.put(
        (
            t_id,
            TaskData(
                task="slow",
                payload=b'{"value": 5}',
                timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"),
            ),
        )
    )

    # allow some time for task_manager to pick up and watchdog to act
    # Need to wait longer than watchdog sleep interval to ensure it runs at least once
    await asyncio.sleep(1.5)

    # task should have been requeued by watchdog
    assert any(tid == t_id for tid, _ in proc.requeued)

    # stop processor to cleanup background tasks
    await proc.stop()
