"""Example demonstrating task handler usage with AsyncTaskProcessor.

Shows how to:
- Register multiple task handlers
- Handle different task types
- Use TaskData, TaskResult, and TaskTimeout
- Implement handler lifecycle (initialize, handle, cleanup)
- Process tasks concurrently
"""

import asyncio
import json
import logging
from uuid import UUID

from scietex.service import AsyncTaskProcessor
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult, TaskTimeout

# ── Handler implementations ──────────────────────────────────────────────


class DataProcessingHandler(TaskHandler):
    """Processes raw data payloads (e.g. transform, aggregate, validate)."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["process_data", "validate_data"]

    async def initialize(self) -> bool:
        self.logger.info("DataProcessingHandler initialized")
        return True

    async def handle(self, task_data: TaskData) -> TaskResult:
        self.logger.info("Processing task '%s' with payload: %s", task_data.task, task_data.payload)
        try:
            # Decode payload
            payload_str = task_data.payload.decode("utf-8")
            data = json.loads(payload_str)

            if task_data.task == "validate_data":
                # Simple validation: check for required fields
                required = {"name", "value"}
                missing = required - set(data.keys())
                if missing:
                    raise ValueError(f"Missing required fields: {missing}")
                result = {"validated": True, "fields": list(data.keys())}
            elif task_data.task == "process_data":
                # Transform: double the value
                data["value"] = data.get("value", 0) * 2
                data["processed"] = True
                result = data
            else:
                self.logger.error("Task %s not supported", task_data.task)
                raise ValueError(f"Task {task_data.task} is not supported")

            return TaskResult(
                status="success",
                error="",
                payload=json.dumps(result).encode("utf-8"),
            )
        except Exception as exc:
            return TaskResult(status="error", error=str(exc))

    async def cleanup(self) -> None:
        self.logger.info("DataProcessingHandler cleaned up")
        await super().cleanup()


class ReportGenerationHandler(TaskHandler):
    """Generates reports from task data."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["generate_report"]

    async def initialize(self) -> bool:
        self.logger.info("ReportGenerationHandler initialized")
        return True

    async def handle(self, task_data: TaskData) -> TaskResult:
        self.logger.info("Generating report for task: %s", task_data.task)
        try:
            # Simulate report generation
            await asyncio.sleep(0.5)

            payload_str = task_data.payload.decode("utf-8") if task_data.payload else "{}"
            report_data = json.loads(payload_str)

            report = {
                "type": "summary",
                "entries": report_data.get("entries", 0),
                "generated": True,
            }

            return TaskResult(
                status="success",
                error="",
                payload=json.dumps(report).encode("utf-8"),
            )
        except Exception as exc:
            return TaskResult(status="error", error=str(exc))


class ImageProcessingHandler(TaskHandler):
    """Handles image processing tasks with custom timeout."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["resize_image", "compress_image", "convert_image"]

    async def initialize(self) -> bool:
        self.logger.info("ImageProcessingHandler initialized")
        return True

    async def handle(self, task_data: TaskData) -> TaskResult:
        operation = task_data.task
        self.logger.info("Image operation '%s' started", operation)

        try:
            # Simulate image processing (these are slow operations)
            await asyncio.sleep(2)

            result = {
                "operation": operation,
                "output_format": "png" if operation == "convert_image" else None,
                "processed": True,
            }

            return TaskResult(
                status="success",
                error="",
                payload=json.dumps(result).encode("utf-8"),
            )
        except Exception as exc:
            return TaskResult(status="error", error=str(exc))


# ── Task source (simulated) ─────────────────────────────────────────────


class InMemoryTaskSource:
    """Simulates an external task source (e.g. database, message queue)."""

    def __init__(self) -> None:
        self._tasks: list[tuple[UUID, TaskData]] = []

    def add_task(self, task_data: TaskData) -> None:
        task_id = UUID(int=len(self._tasks))
        self._tasks.append((task_id, task_data))
        self.logger.info("Task source: queued task '%s' (id=%s)", task_data.task, task_id)

    @property
    def logger(self):
        return logging.getLogger("TaskSource")

    def clear(self) -> None:
        self._tasks.clear()


# ── Processor ────────────────────────────────────────────────────────────


class TaskProcessorService(AsyncTaskProcessor):
    """Service that fetches tasks from an in-memory source and processes them."""

    def __init__(self, task_source: InMemoryTaskSource, **kwargs) -> None:
        super().__init__(**kwargs)
        self._task_source = task_source

    async def fetch_tasks(self) -> None:
        """Pull pending tasks from the source and enqueue them."""
        while self._task_source._tasks and not self.task_queue_full():
            task_id, task_data = self._task_source._tasks.pop(0)
            self.enqueue_task(task_id, task_data)

    async def return_task_to_queue(self, task_id: UUID, task_data: TaskData) -> None:
        """Re-queue tasks that timed out or were canceled."""
        self.logger.warning("Re-queuing task '%s' (id=%s)", task_data.task, task_id)
        self._task_source.add_task(task_data)

    async def cleanup(self) -> None:
        """Requeue drained tasks, then run the base cleanup.

        The in-memory source is non-durable: tasks already drained from the
        source into the processor's queue would be silently lost if dropped on
        shutdown. Requeue them before ``super().cleanup()``, which drains the
        queue without requeueing (correct only for durable transports).
        """
        while True:
            item = self.dequeue_task()
            if item is None:
                break
            task_id, task_data = item
            await self.return_task_to_queue(task_id, task_data)
        await super().cleanup()


# ── Main ─────────────────────────────────────────────────────────────────


async def main() -> None:

    # Create task source and populate with sample tasks
    task_source = InMemoryTaskSource()
    task_source.add_task(TaskData(task="process_data", payload=b'{"name": "sensor_readings", "value": 42}'))
    task_source.add_task(TaskData(task="validate_data", payload=b'{"name": "user", "value": 100}'))
    task_source.add_task(TaskData(task="generate_report", payload=b'{"entries": 5}', timeout=TaskTimeout(timeout=5)))
    task_source.add_task(TaskData(task="resize_image", payload=b"", timeout=TaskTimeout(timeout=1)))  # Will timeout!

    # Create processor and register handlers
    processor = TaskProcessorService(
        task_source=task_source,
        service_name="task_handler_demo",
        version="1.0.0",
        logging_level=logging.INFO,
        queue_size=10,
        max_concurrent_tasks=3,
    )

    # One registration per handler class covers all of its supported_tasks:
    # dispatch is driven by supported_tasks membership, so each task type is
    # routed to its handler regardless of the registration key used here.
    processor.add_task_handler("process_data", DataProcessingHandler)
    processor.add_task_handler("generate_report", ReportGenerationHandler)
    processor.add_task_handler("resize_image", ImageProcessingHandler)

    # Start processing
    await processor.start()
    await processor.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
