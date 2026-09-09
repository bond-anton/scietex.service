"""Example: a stateful handler with shared state injected via ``**handler_kwargs``.

``add_task_handler`` accepts arbitrary keyword-only ``**handler_kwargs`` that
are forwarded to the handler constructor on EVERY (re)instantiation. Injecting
a SHARED mutable object (a counter, a cache, a client) at registration time
makes a handler stateful: the handler holds a reference to the object and
mutates it across tasks, and the same object is handed to every fresh instance
the processor constructs on a restart, so its state outlives a single
start/stop cycle. Runtime-mutated *instance* state would be lost on restart —
only injected shared objects survive.

This example registers a ``CountingHandler`` with ``counter=shared_counter``.
``main()`` owns the ``SharedCounter``; the handler only reads and increments
it. Each handled task bumps the counter, and the constructor logs the current
value, making both the injection and what a re-instantiation would see visible
in the logs.
"""

import asyncio
import logging
from uuid import UUID

from scietex.service import TaskProcessor, TaskProcessorConfig
from scietex.service.task_handler import TaskData, TaskHandler, TaskHandlerContext, TaskResult

# ── Shared state ─────────────────────────────────────────────────────────


class SharedCounter:
    """A mutable object created once at registration and shared with the handler.

    The handler increments this object on every task; the count persists
    because the object — not the handler instance — owns the state.
    """

    def __init__(self) -> None:
        self._value = 0

    def increment(self) -> int:
        self._value += 1
        return self._value

    @property
    def value(self) -> int:
        return self._value


# ── Handler ──────────────────────────────────────────────────────────────


class CountingHandler(TaskHandler):
    """Increments a shared counter on every handled task.

    The counter is NOT created here — it is injected by the processor via
    ``add_task_handler(CountingHandler, counter=shared_counter)``. Because the
    processor stores the kwargs alongside the class and re-applies them on
    every instantiation, each fresh ``CountingHandler`` receives the same
    ``SharedCounter`` object.
    """

    def __init__(self, name: str, context: TaskHandlerContext, *, counter: SharedCounter) -> None:
        super().__init__(name, context)
        self._counter = counter
        # Logging the injected value at construction proves the handler sees
        # the SHARED object: a re-instantiation would resume from here, not 0.
        self.logger.info("CountingHandler constructed; shared counter currently at %d", self._counter.value)

    @property
    def supported_tasks(self) -> list[str]:
        return ["count"]

    async def handle(self, task_data: TaskData) -> TaskResult:
        total = self._counter.increment()
        self.logger.info("Handled task '%s'; shared counter now at %d", task_data.task, total)
        return TaskResult(status="success", payload=str(total).encode())


# ── Task source (simulated) ──────────────────────────────────────────────


class InMemoryTaskSource:
    """Simulates an external task source (e.g. database, message queue)."""

    def __init__(self) -> None:
        self._tasks: list[tuple[UUID, TaskData]] = []

    def add_task(self, task_data: TaskData) -> None:
        task_id = UUID(int=len(self._tasks))
        self._tasks.append((task_id, task_data))
        logging.getLogger("TaskSource").info("Task source: queued task '%s' (id=%s)", task_data.task, task_id)


# ── Processor ────────────────────────────────────────────────────────────


class StatefulTaskProcessor(TaskProcessor):
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
    # The shared object is created here, at registration time, and owned by
    # main() — the handler only ever holds a reference to it.
    shared_counter = SharedCounter()

    task_source = InMemoryTaskSource()
    for _ in range(5):
        task_source.add_task(TaskData(task="count"))

    processor = StatefulTaskProcessor(
        task_source=task_source,
        config=TaskProcessorConfig(
            service_name="stateful_handler_demo",
            version="1.0.0",
            logging_level=logging.INFO,
            queue_size=10,
            max_concurrent_tasks=2,
        ),
    )

    # Inject the shared counter as a handler kwarg. It is re-injected on every
    # (re)instantiation, so its value survives a stop/start cycle.
    processor.add_task_handler(CountingHandler, counter=shared_counter)

    await processor.start()
    await processor.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
