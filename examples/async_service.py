"""Example of BasicAsyncWorker based service."""

from typing import Any
import asyncio
import logging
import random

from scietex.service import BasicAsyncWorker

# pylint: disable=duplicate-code

TASKS = [
    (1, {"data": "Task data 1", "timeout": 6.0}),
    (2, {"data": "Task data 2", "timeout": 6.0}),
    (3, {"data": "Task data 3", "timeout": 6.0}),
    (4, {"data": "Task data 4", "timeout": 0.0}),
    (5, {"data": "Task data 5", "timeout": 0.0}),
]


class MyAsyncWorker(BasicAsyncWorker):
    """
    Custom Worker based on BasicAsyncWorker.
    """

    tasks = asyncio.Queue()

    async def initialize(self) -> bool:
        if not await super().initialize():
            return False
        for task in TASKS:
            await self.tasks.put(task)
        return True

    async def fetch_tasks(self) -> None:
        try:
            task_id, task_data = await asyncio.wait_for(self.tasks.get(), timeout=1)
            await self.task_queue.put((task_id, task_data))
        except TimeoutError:
            pass

    async def process_task(
        self, task_id: int | str, task_data: dict[str, Any]
    ) -> dict[str, Any]:
        task_time = random.randint(5, 20)
        await asyncio.sleep(task_time)
        return {"data": task_time}

    async def return_task_to_queue(
        self, task_id: int | str, task_data: dict[str, Any]
    ) -> None:
        await self.tasks.put((task_id, task_data))

    async def process_result(self, task_id: int | str, result: dict[str, Any]) -> None:
        await asyncio.sleep(1)


async def main() -> None:
    """Main function."""
    worker = MyAsyncWorker(
        service_name="MyAsyncWorker", version="0.0.1", delay=1, log_level=logging.DEBUG
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
