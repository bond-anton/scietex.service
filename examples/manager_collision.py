"""Example of the @Manager name-collision warning (AR-068)."""

import asyncio

from scietex.service import BasicWorker, Manager, WorkerConfig


class CollidingService(BasicWorker):
    """A service whose two managers accidentally share the same ``name=``."""

    @Manager(name="worker")
    async def _primary_loop(self) -> None:
        """The manager that wins the ``worker`` name (first in the class body)."""
        while True:
            await self.tick("primary")
            await asyncio.sleep(1)

    @Manager(name="worker")
    async def _duplicate_loop(self) -> None:
        """A second manager that independently picked ``name="worker"``; skipped."""
        while True:
            await self.tick("duplicate")
            await asyncio.sleep(1)

    async def tick(self, which: str) -> None:
        """Log a tick from the named manager loop."""
        self.logger.info("[%s] tick", which)


async def main():
    worker = CollidingService(
        WorkerConfig(
            service_name="collision_service",
            version="1.0.0",
            heartbeat_interval=15,
            watchdog_interval=5,
            logging_level="INFO",
        )
    )

    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
