"""
Example of ValkeyWorker based service
"""

# pylint: disable=duplicate-code
from tox.config.loader.api import V

import asyncio
import logging

from glide import GlideClientConfiguration, AdvancedGlideClientConfiguration, NodeAddress

from scietex.service import ValkeyWorker, ValkeyBaseConfig


class MyAsyncWorker(ValkeyWorker):
    """Custom worker implementation."""


async def main() -> None:
    """Main function."""

    worker = MyAsyncWorker(
        service_name="MyValkeyService",
        version="0.0.1",
        worker_id=1,
        log_level=logging.DEBUG,
        heartbeat_interval=3,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
