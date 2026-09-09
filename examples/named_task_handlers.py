"""Example: one handler class registered as multiple named instances (AR-053).

``add_task_handler`` normally keys a handler by its class name, so one class
yields one instance. Passing the optional keyword-only ``name`` lets the same
class be registered several times under distinct lifecycle keys. Each instance
is constructed with that key as its ``name``, so a subclass can compute its
``supported_tasks`` from ``self.name`` and split one class's task types across
several non-overlapping instances. Dispatch picks the first active instance
whose ``supports(task)`` is True, so per-instance task sets must NOT overlap —
this is the caller's responsibility when choosing names.
"""

import asyncio
import logging
from uuid import UUID

from scietex.service import TaskProcessor, TaskProcessorConfig
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult

# ── Handler ──────────────────────────────────────────────────────────────


class NamedTaskHandler(TaskHandler):
    """Handles tasks for a single named slice; the task set comes from the name.

    One class, many instances: each registered ``name`` maps to its own
    disjoint set of task types, so a single implementation can be specialised
    per instance without subclassing.
    """

    _TASKS_BY_NAME = {
        "alpha": ["alpha_task"],
        "beta": ["beta_task"],
    }

    async def handle(self, task_data: TaskData) -> TaskResult:
        self.logger.info(
            "Handler '%s' processed task '%s' with payload %r",
            self.name,
            task_data.task,
            task_data.payload,
        )
        return TaskResult(status="success", error="", payload=task_data.payload)

    @property
    def supported_tasks(self) -> list[str]:
        return self._TASKS_BY_NAME[self.name]


# ── Task source (simulated) ─────────────────────────────────────────────


class InMemoryTaskSource:
    """Simulates an external task source (e.g. database, message queue)."""

    def __init__(self) -> None:
        self._tasks: list[tuple[UUID, TaskData]] = []

    def add_task(self, task_data: TaskData) -> None:
        task_id = UUID(int=len(self._tasks))
        self._tasks.append((task_id, task_data))
        logging.getLogger("TaskSource").info("Task source: queued task '%s' (id=%s)", task_data.task, task_id)


# ── Processor ────────────────────────────────────────────────────────────


class NamedTaskProcessor(TaskProcessor):
    """Service that fetches tasks from an in-memory source and processes them."""

    def __init__(self, task_source: InMemoryTaskSource, config: TaskProcessorConfig | None = None) -> None:
        super().__init__(config)
        self._task_source = task_source

    async def fetch_tasks(self) -> bool:
        enqueued = False
        while self._task_source._tasks and not self.task_queue_full():
            task_id, task_data = self._task_source._tasks.pop(0)
            self.enqueue_task(task_id, task_data)
            enqueued = True
        return enqueued


# ── Main ─────────────────────────────────────────────────────────────────


async def main() -> None:
    # Enqueue tasks of both types; each is handled by the matching named slice.
    task_source = InMemoryTaskSource()
    task_source.add_task(TaskData(task="alpha_task", payload=b"hello alpha"))
    task_source.add_task(TaskData(task="beta_task", payload=b"hello beta"))

    processor = NamedTaskProcessor(
        task_source=task_source,
        config=TaskProcessorConfig(
            service_name="named_task_handler_demo",
            version="1.0.0",
            logging_level=logging.INFO,
            queue_size=10,
            max_concurrent_tasks=2,
        ),
    )

    # Register the SAME class twice under distinct names (AR-053).
    processor.add_task_handler(NamedTaskHandler, name="alpha")
    processor.add_task_handler(NamedTaskHandler, name="beta")

    await processor.start()
    await processor.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
