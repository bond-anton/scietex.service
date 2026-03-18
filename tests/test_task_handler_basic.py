"""Tests for TaskHandler base class and a simple concrete implementation."""

import types
import pytest


from scietex.service.task_handlers.basic import TaskHandler
from scietex.service.task_handlers.schemas import TaskData, TaskResult


class DummyWorker(types.SimpleNamespace):
    pass


class DummyHandler(TaskHandler):
    def __init__(self, worker):
        super().__init__(worker)
        self.cleaned = False

    async def handle(self, task_data: TaskData) -> TaskResult:
        # echo back a computed result
        return TaskResult(
            status="success",
            error="",
            payload=f"{task_data.payload.decode('utf-8')}".encode("utf-8"),
        )

    def supports(self, task_type: str) -> bool:
        return task_type == "dummy"

    async def cleanup(self) -> None:
        self.cleaned = True


@pytest.mark.asyncio
async def test_taskhandler_is_abstract():
    # Trying to instantiate abstract TaskHandler should raise TypeError
    with pytest.raises(TypeError):
        TaskHandler(None)  # abstract methods not implemented


@pytest.mark.asyncio
async def test_dummyhandler_lifecycle():
    worker = DummyWorker()
    handler = DummyHandler(worker)

    # initially not ready
    assert not handler.is_ready

    # initialize should set is_ready
    await handler.initialize()
    assert handler.is_ready

    # supports should work
    assert handler.supports("dummy")
    assert not handler.supports("other")

    # handle should return expected result
    res = await handler.handle(TaskData(task="dummy", payload=b'{"value": 123}'))
    assert res.status == "success"
    assert res.payload.decode("utf-8") == '{"value": 123}'

    # cleanup should be callable and set the flag
    await handler.cleanup()
    assert handler.cleaned
