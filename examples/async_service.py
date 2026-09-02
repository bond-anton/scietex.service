"""Example of BasicAsyncWorker service."""

import asyncio
import random

from scietex.service import BasicAsyncWorker, Manager


class MyService(BasicAsyncWorker):
    """A simple daemon service."""

    async def initialize(self) -> bool:
        """Connect to external services."""
        self.logger.info("Initializing MyService...")
        # self.db = await connect_database(self.conf_dir / "db.yaml")
        return True

    async def heartbeat(self) -> None:
        """Override for custom heartbeat behavior."""
        self.logger.debug("Heartbeat — all systems nominal")
        # await self.db.ping()

    async def watchdog(self) -> None:
        """Override for custom watchdog behavior."""
        self.logger.debug("Watchdog check")
        # Check disk space, memory, dependencies, etc.

    async def cleanup(self) -> None:
        """Release resources on shutdown."""
        self.logger.info("Cleaning up MyService...")
        # await self.db.close()

    async def pull_numbers(self) -> list[float]:
        """Simulates data pulling from external store."""
        return [random.random() for _ in range(10)]

    async def push_data(self, data: float) -> None:
        """Simulates data pushing to external store."""
        self.logger.info("PUSHING Result %.3f", data)

    @Manager(name="cruncher")
    async def number_cruncher(self) -> None:
        """Custom manager implementation example."""
        numbers = await self.pull_numbers()
        result = 0
        for item in numbers:
            result += item
        await self.push_data(result)
        await asyncio.sleep(1)


async def main():
    worker = MyService(
        service_name="my_daemon",
        version="1.0.0",
        worker_id=1,
        heartbeat_interval=15,
        watchdog_interval=5,
        logging_level="DEBUG",
    )

    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
