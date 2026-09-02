"""Tests for TaskHandler base class and a simple concrete implementation."""

import logging
import types

import pytest

from scietex.service.task_handler.basic import TaskHandler
from scietex.service.task_handler.schemas import TaskData, TaskResult


class DummyWorker(types.SimpleNamespace):
    logger = logging.getLogger(__name__)
    pass


class DummyHandler(TaskHandler):
    def __init__(self, name, worker):
        super().__init__(name, worker)
        self.cleaned = False

    async def handle(self, task_data: TaskData) -> TaskResult:
        # echo back a computed result
        return TaskResult(
            status="success",
            error="",
            payload=f"{task_data.payload.decode('utf-8')}".encode(),
        )

    @property
    def supported_tasks(self) -> list[str]:
        return ["dummy"]

    async def cleanup(self) -> None:
        self.cleaned = True


@pytest.mark.asyncio
async def test_taskhandler_is_abstract():
    # Trying to instantiate abstract TaskHandler should raise TypeError
    with pytest.raises(TypeError):
        worker = DummyWorker()
        TaskHandler("handler", worker)  # abstract methods not implemented


@pytest.mark.asyncio
async def test_dummyhandler_lifecycle():
    worker = DummyWorker()
    handler = DummyHandler("dummy", worker)

    # initially not ready
    assert not handler.is_ready

    # initialize should set is_ready
    await handler.start()
    assert handler.is_ready

    # supports should work
    assert handler.supports("dummy")
    assert not handler.supports("other")

    # handle should return expected result
    res = await handler.handle(TaskData(task="dummy", payload=b'{"value": 123}'))
    assert res.status == "success"
    assert res.payload.decode("utf-8") == '{"value": 123}'

    # cleanup should be callable and set the flag
    await handler.stop()
    assert handler.cleaned
