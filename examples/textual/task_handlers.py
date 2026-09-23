"""FAST and SLOW demonstration handlers for the Textual TUI workers.

Every UI worker registers these two handlers so tasks submitted to it can be
processed without a real backend: ``fast_task`` completes immediately and
``slow_task`` takes five seconds, exercising both the instant path and the
concurrent-execution path on the dashboard.
"""

import asyncio

from scietex.service.task_handler import TaskCapabilities, TaskData, TaskHandler, TaskResult


class FastTaskHandler(TaskHandler):
    """Returns success immediately, exercising the instant-task path."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["fast_task"]

    async def initialize(self) -> bool:
        self.logger.info("FastTaskHandler initialized")
        return True

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        self.logger.info("Processing fast task '%s' (id=%s)", task_data.task, task_data.task_id)
        return TaskResult(status="success", payload=b"ok")


class SlowTaskHandler(TaskHandler):
    """Takes five seconds to return success, exercising the slow-task path."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["slow_task"]

    async def initialize(self) -> bool:
        self.logger.info("SlowTaskHandler initialized")
        return True

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        self.logger.info("Processing slow task '%s' (id=%s)", task_data.task, task_data.task_id)
        await asyncio.sleep(5)
        self.logger.info("Completed slow task '%s' (id=%s)", task_data.task, task_data.task_id)
        return TaskResult(status="success", payload=b"ok")
