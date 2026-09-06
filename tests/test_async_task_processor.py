import asyncio
from uuid import uuid4

import pytest

from scietex.service.async_tasks_processor import AsyncTaskProcessor
from scietex.service.task_handler.basic import TaskHandler
from scietex.service.task_handler.schemas import TaskData, TaskResult, TaskTimeout


class DummyHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        result = task_data.payload.decode("utf-8")
        return TaskResult(status="success", error="No error", payload=result.encode("utf-8"))

    @property
    def supported_tasks(self) -> list[str]:
        return ["dummy"]


class SlowHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        # simulate long running task
        await asyncio.sleep(2)
        return TaskResult(payload=task_data.payload, status="success", error="No error")

    @property
    def supported_tasks(self) -> list[str]:
        return ["slow"]


class DemoProcessor(AsyncTaskProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.requeued: list = []

    async def fetch_tasks(self) -> None:  # pragma: no cover - stub
        return None

    async def return_task_to_queue(self, task_id, task_data):
        # record requeued tasks for assertions
        self.requeued.append((task_id, task_data))

    async def _logger_shut_down_handlers(self) -> None:  # disable real logging stop
        return None


@pytest.mark.asyncio
async def test_process_task_with_dummy_handler():
    proc = DemoProcessor()
    proc.add_task_handler("dummy", DummyHandler)
    await proc._start_task_handler("dummy")

    result: TaskResult = await proc.process_task(uuid4(), TaskData(task="dummy", payload=b'{"value": 5}'))

    assert result.status == "success"
    assert result.payload.decode("utf-8") == '{"value": 5}'


@pytest.mark.asyncio
async def test_watchdog_requeues_timed_out_task():
    proc = DemoProcessor()
    proc.add_task_handler("slow", SlowHandler)

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


@pytest.mark.asyncio
async def test_process_task_empty_task_returns_error_result():
    """An empty task type must yield an error TaskResult, not raise (AR-010)."""
    proc = DemoProcessor()
    result: TaskResult = await proc.process_task(uuid4(), TaskData(task="", payload=b"{}"))
    assert result.status == "error"
    assert "task" in result.error


class ExplodingSupportsHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="success", error="No error")

    @property
    def supported_tasks(self) -> list[str]:
        return ["exploding"]

    def supports(self, task_type: str) -> bool:
        raise RuntimeError("supports() exploded")


@pytest.mark.asyncio
async def test_task_manager_consumes_handler_exception_without_leaking():
    """An exception raised outside process_task's try/except (e.g. in a
    handler's supports()) must be caught by handle_task so it does not surface
    as an unretrieved task exception (AR-010)."""
    import gc

    loop = asyncio.get_running_loop()
    leaked: list[str] = []
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, ctx: leaked.append(str(ctx.get("message", ""))))
    proc = DemoProcessor()
    proc.add_task_handler("exploding", ExplodingSupportsHandler)
    # Start the handler first so it is ready before the managers consume the
    # task; otherwise _find_task_handler would see an empty registry and the
    # exploding supports() path would never run.
    await proc._start_task_handler("exploding")
    await proc.start()
    try:
        t_id = uuid4()
        await proc.task_queue.put((t_id, TaskData(task="exploding", payload=b"{}")))
        # Wait until task_manager has consumed the task from the queue.
        for _ in range(100):
            if proc.task_queue.empty():
                break
            await asyncio.sleep(0.01)
        # Let the spawned handle_task coroutine run to completion.
        await asyncio.sleep(0.05)
        # Force GC so any unretrieved task exception is reported deterministically.
        gc.collect()
        await asyncio.sleep(0.05)
        assert not any("never retrieved" in m for m in leaked), f"leaked: {leaked}"
    finally:
        loop.set_exception_handler(old_handler)
        await proc.stop()


class FailingStartHandler(TaskHandler):
    async def initialize(self) -> bool:
        return False

    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="error", error="never ready")

    @property
    def supported_tasks(self) -> list[str]:
        return ["failing"]


@pytest.mark.asyncio
async def test_initialize_returns_false_when_handler_start_fails():
    """A handler that fails to start must make initialize() return False (AR-010)."""
    proc = DemoProcessor()
    proc.add_task_handler("failing", FailingStartHandler)
    ok = await proc.initialize()
    assert ok is False


class RecordingProcessor(DemoProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.completed: list = []

    async def on_task_completed(self, task_id, task_data, task_result):
        self.completed.append((task_id, task_data, task_result))


@pytest.mark.asyncio
async def test_handle_task_invokes_completion_hook():
    """handle_task must invoke on_task_completed with the final result (AR-005)."""
    proc = RecordingProcessor()
    proc.add_task_handler("dummy", DummyHandler)
    await proc._start_task_handler("dummy")
    await proc.start()
    try:
        t_id = uuid4()
        await proc.task_queue.put((t_id, TaskData(task="dummy", payload=b'{"value": 5}')))
        for _ in range(100):
            if proc.completed:
                break
            await asyncio.sleep(0.01)
        assert len(proc.completed) == 1
        cid, cdata, cresult = proc.completed[0]
        assert cid == t_id
        assert cdata.task == "dummy"
        assert cresult.status == "success"
    finally:
        await proc.stop()


class StubbornHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        try:
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            # Swallow cancellation and keep running briefly.
            await asyncio.sleep(0.3)
        return TaskResult(status="success", error="No error")

    @property
    def supported_tasks(self) -> list[str]:
        return ["stubborn"]


@pytest.mark.asyncio
async def test_watchdog_does_not_requeue_when_handler_ignores_cancellation(monkeypatch):
    """A handler that swallows CancelledError must not be requeued by the
    watchdog: it is still running, so requeueing would run it twice (AR-005)."""
    import scietex.service.async_tasks_processor as mod

    # Shorten the cancellation wait so the test does not block for 5s.
    monkeypatch.setattr(mod, "WORKER_TASK_CANCELLATION_TIMEOUT", 0.05)
    proc = DemoProcessor()
    proc.add_task_handler("stubborn", StubbornHandler)
    await proc._start_task_handler("stubborn")
    await proc.start()
    try:
        t_id = uuid4()
        await proc.task_queue.put(
            (
                t_id,
                TaskData(
                    task="stubborn",
                    payload=b"{}",
                    timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"),
                ),
            )
        )
        # Wait past the watchdog interval (default 1s) plus the cancel wait so
        # the watchdog has acted and decided not to requeue.
        await asyncio.sleep(1.6)
        assert not any(tid == t_id for tid, _ in proc.requeued)
        # Let the stubborn handler finish so no dangling task remains.
        await asyncio.sleep(0.5)
    finally:
        await proc.stop()


@pytest.mark.asyncio
async def test_cleanup_drain_does_not_requeue_queued_tasks():
    """cleanup must drop queued-but-undispatched tasks without requeueing
    them: their transport entries stay pending and are redelivered on
    restart, so an XADD here would duplicate them (AR-005)."""
    proc = DemoProcessor()
    t_id = uuid4()
    await proc.task_queue.put((t_id, TaskData(task="dummy", payload=b"{}")))
    await proc.cleanup()
    assert not any(tid == t_id for tid, _ in proc.requeued)
    assert proc.task_queue.empty()
