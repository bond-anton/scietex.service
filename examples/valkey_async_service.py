"""
Example of ValkeyWorker based service
"""

import asyncio
import logging

from scietex.service import (
    ValkeyAdvancedConfig,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyWorker,
)


async def main(config: ValkeyConfig | None) -> None:
    """Main function."""

    worker = ValkeyWorker(
        service_name="MyValkeyService",
        version="0.0.1",
        worker_id=1,
        log_level=logging.DEBUG,
        heartbeat_interval=4,
        valkey_config=config,
        queue_size=100,
        max_concurrent_tasks=100,
    )
    await worker.run()


if __name__ == "__main__":
    valkey_config = ValkeyConfig(
        base_config=ValkeyBaseConfig(
            nodes=[ValkeyNode(host="localhost", port=6379)],
            request_timeout=10_000,
        ),
        advanced_config=ValkeyAdvancedConfig(
            connection_timeout=10000,
            tcp_nodelay=True,
        ),
    )

    asyncio.run(main(None))
