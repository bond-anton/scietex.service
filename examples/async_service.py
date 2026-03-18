"""Example of BasicAsyncWorker service."""

import asyncio
import logging

from scietex.service import BasicAsyncWorker

# pylint: disable=duplicate-code


async def main() -> None:
    """Main function."""
    worker = BasicAsyncWorker(
        service_name="MyAsyncWorker", version="0.0.1", log_level=logging.DEBUG
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
