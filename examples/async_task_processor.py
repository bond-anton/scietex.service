"""Example of BasicAsyncWorker service."""

import asyncio
import logging

from scietex.service import AsyncTaskProcessor


async def main() -> None:
    """Main function."""
    worker = AsyncTaskProcessor(
        service_name="MyAsyncTaskProcessor", version="0.0.1", logging_level=logging.DEBUG
    )
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
