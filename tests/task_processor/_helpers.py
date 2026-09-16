"""Shared handlers and processor subclasses for TaskProcessor tests."""

import asyncio
import logging
from uuid import uuid4

import msgspec

from scietex.service.config import TaskProcessorConfig
from scietex.service.task_handler.basic import TaskHandler
from scietex.service.task_handler.cancel import CancelTaskRequest
from scietex.service.task_handler.context import TaskHandlerContext
from scietex.service.task_handler.schemas import TaskData, TaskResult
from scietex.service.task_processor import TaskProcessor
from scietex.service.transport import InMemoryTransport


class RecordingInMemoryTransport(InMemoryTransport):
    """In-memory transport that records every requeue into a shared list, so
    tests can assert on requeue behaviour at the transport seam (AR-001)."""

    def __init__(self, *, requeued: list, logger: logging.Logger) -> None:
        super().__init__(logger=logger)
        self._requeued = requeued

    async def requeue(self, task_id, task_data) -> None:
        self._requeued.append((task_id, task_data))
        await super().requeue(task_id, task_data)


class DurableInMemoryTransport(InMemoryTransport):
    """In-memory transport with durable drain semantics: a still-queued task is
    not re-enqueued on shutdown (its entry stays pending and redelivers on
    restart), mirroring a Valkey stream (AR-041)."""

    async def on_drain(self, task_id, task_data) -> None:
        pass


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


class StuckStopHandler(TaskHandler):
    async def stop(self) -> None:
        # Block far past the stop timeout so _stop_task_handler hits
        # asyncio.TimeoutError instead of a clean stop.
        await asyncio.sleep(10)

    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="success", error="No error", payload=task_data.payload)

    @property
    def supported_tasks(self) -> list[str]:
        return ["stuck_stop"]


class NameDerivedHandler(TaskHandler):
    """A handler whose task support is derived from its lifecycle name, so one
    class can be split across instances that each serve a disjoint task set
    (AR-053)."""

    _TASKS_BY_NAME = {
        "alpha": ["alpha_task"],
        "beta": ["beta_task"],
    }

    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="success", error="No error", payload=task_data.payload)

    @property
    def supported_tasks(self) -> list[str]:
        return self._TASKS_BY_NAME[self.name]


class ThresholdHandler(TaskHandler):
    """Handler that accepts a keyword-only constructor kwarg, verifying the
    handler_kwargs passthrough reaches the constructor."""

    def __init__(self, name: str, context: TaskHandlerContext, *, threshold: int) -> None:
        super().__init__(name, context)
        self.threshold = threshold

    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="success", error="No error", payload=task_data.payload)

    @property
    def supported_tasks(self) -> list[str]:
        return ["threshold"]


class SharedStateHandler(TaskHandler):
    """Handler that receives a shared mutable object via handler_kwargs, so the
    injected state outlives a single start/stop cycle."""

    def __init__(self, name: str, context: TaskHandlerContext, *, shared: dict) -> None:
        super().__init__(name, context)
        self.shared = shared

    async def handle(self, task_data: TaskData) -> TaskResult:
        self.shared["count"] = self.shared.get("count", 0) + 1
        return TaskResult(status="success", error="No error", payload=task_data.payload)

    @property
    def supported_tasks(self) -> list[str]:
        return ["shared"]


class RaisingHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        raise ValueError("boom")

    @property
    def supported_tasks(self) -> list[str]:
        return ["raiser"]


class ReturningErrorHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(
            status="error",
            error="x",
            retryable=False,
            error_code="PERMANENT",
            partial=True,
        )

    @property
    def supported_tasks(self) -> list[str]:
        return ["error_returner"]


class RetryableErrorHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="error", error="transient", retryable=True)

    @property
    def supported_tasks(self) -> list[str]:
        return ["retryable_err"]


class PermanentErrorHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="error", error="permanent", retryable=False)

    @property
    def supported_tasks(self) -> list[str]:
        return ["permanent_err"]


class ExplodingSupportsHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="success", error="No error")

    @property
    def supported_tasks(self) -> list[str]:
        return ["exploding"]

    def supports(self, task_type: str) -> bool:
        raise RuntimeError("supports() exploded")


class FailingStartHandler(TaskHandler):
    async def initialize(self) -> bool:
        return False

    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="error", error="never ready")

    @property
    def supported_tasks(self) -> list[str]:
        return ["failing"]


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


class RaisingStartHandler(TaskHandler):
    async def initialize(self) -> bool:
        raise RuntimeError("start failed")

    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="error", error="never ready")

    @property
    def supported_tasks(self) -> list[str]:
        return ["raising"]


class NeverFinishesHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        # Block until cancelled so the task stays running for the test.
        await asyncio.Event().wait()
        return TaskResult(status="success", error="No error")

    @property
    def supported_tasks(self) -> list[str]:
        return ["never"]


class SelfCancelHandler(TaskHandler):
    """A handler that asks the processor to cancel its own task."""

    def __init__(self, name, context, *, cancel):
        super().__init__(name, context)
        self._cancel = cancel

    async def handle(self, task_data: TaskData) -> TaskResult:
        outcome = await self._cancel(uuid4())
        return TaskResult(status="success", payload=outcome.encode())

    @property
    def supported_tasks(self) -> list[str]:
        return ["self_cancel"]


class DemoProcessor(TaskProcessor):
    def __init__(self, *args, **kwargs):
        self.requeued: list = []
        super().__init__(*args, **kwargs)
        # Record requeues at the transport seam so tests assert on self.requeued
        # without overriding return_task_to_queue (AR-001).
        self._transport = RecordingInMemoryTransport(requeued=self.requeued, logger=self.logger)

    async def fetch_tasks(self) -> bool:  # pragma: no cover - stub
        return False


class RecordingProcessor(DemoProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.completed: list = []

    async def on_task_completed(self, task_id, task_data, task_result, *, cancel_reason=None):
        self.completed.append((task_id, task_data, task_result))


class RequeueRecordingProcessor(RecordingProcessor):
    async def return_task_to_queue(self, task_id, task_data):
        self.requeued.append((task_id, task_data))


class DurableProcessor(TaskProcessor):
    """A processor whose transport keeps items pending after enqueue (e.g. a
    Valkey stream), so drained tasks must NOT be re-enqueued on shutdown —
    they redeliver on restart (AR-041)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.requeued: list = []
        # The durable transport's on_drain is a no-op, so a drained task is not
        # re-enqueued (AR-001).
        self._transport = DurableInMemoryTransport(logger=self.logger)

    async def fetch_tasks(self) -> bool:  # pragma: no cover - stub
        return False

    async def return_task_to_queue(self, task_id, task_data):
        # record requeued tasks for assertions
        self.requeued.append((task_id, task_data))


class ReportingProcessor(TaskProcessor):
    """Processor whose fetch_tasks reports productivity without enqueuing, so
    the task_queue_manager sleep-skip decision can be tested in isolation."""

    def __init__(self, config: TaskProcessorConfig | None = None, *, fetch_result: bool = False):
        super().__init__(config)
        self._fetch_result = fetch_result

    async def fetch_tasks(self) -> bool:
        return self._fetch_result


class OrderRecordingProcessor(DemoProcessor):
    """Processor that records the order of the task lifecycle hooks, so the
    on_task_started-before-process_task contract is observable."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.call_order: list[str] = []

    async def on_task_started(self, task_id, task_data):
        self.call_order.append("started")

    async def process_task(self, task_id, task_data):
        self.call_order.append("process")
        return await super().process_task(task_id, task_data)


class ProgressRecordingProcessor(DemoProcessor):
    """Processor that reports progress from within process_task and records the
    clamped values that reach _write_task_progress."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.progress_values: list[float] = []

    async def _write_task_progress(self, task_id, value):
        self.progress_values.append(value)

    async def process_task(self, task_id, task_data):
        await self.report_progress(150.0)
        await self.report_progress(-5.0)
        return await super().process_task(task_id, task_data)


class CancelRecordingProcessor(DemoProcessor):
    """Records terminal hook calls with their cancel reason."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.completed: list = []

    async def on_task_completed(self, task_id, task_data, task_result, *, cancel_reason=None):
        self.completed.append((task_id, task_data, task_result, cancel_reason))


def _cancel_payload(target_id) -> bytes:
    return msgspec.msgpack.encode(CancelTaskRequest(target_task_id=str(target_id)))
