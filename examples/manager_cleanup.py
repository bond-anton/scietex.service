"""Example of a @Manager with a cleanup callable (AR-067)."""

import asyncio
import random

from scietex.service import BasicWorker, Manager, WorkerConfig


class DataService(BasicWorker):
    """A service whose manager tears down a session on shutdown."""

    def __init__(self, config: WorkerConfig | None = None):
        super().__init__(config)
        # Simulated external session the manager depends on and its cleanup closes.
        self.session_open = False

    async def initialize(self) -> bool:
        """Open the simulated session before the manager starts."""
        self.logger.info("Opening session...")
        self.session_open = True
        return True

    async def close_session(worker) -> None:
        """Manager cleanup: visible teardown of the simulated session."""
        worker.session_open = False
        worker.logger.info("Manager cleanup: session closed")

    @Manager(name="data_pump", cleanup=close_session)
    async def data_pump(self) -> None:
        """Periodically push a simulated batch while the session is open."""
        while self.session_open:
            await self.push_batch()
            await asyncio.sleep(1)

    async def push_batch(self) -> None:
        """Simulate pushing a batch of records downstream."""
        self.logger.info("Pushing batch of %d records", random.randint(1, 10))


async def main():
    worker = DataService(
        WorkerConfig(
            service_name="data_service",
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
