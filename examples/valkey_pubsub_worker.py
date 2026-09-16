"""
Example of a Valkey worker that listens for PubSub control messages.

``ValkeyWorker`` honours ``ValkeyConfig.pubsub_config`` directly: setting
``ValkeyPubSubConfig(listening=True)`` subscribes the worker's own client to
the service-specific and broadcast channels, delivering each message to the
``parse_control_message`` callback. The directed channel uses the worker's own
``instance_id``.

The worker subscribes to two channels:
    - ``scietex:{service_name}:{instance_id}``  (directed at this instance)
    - ``scietex:broadcast``                     (sent to every instance)

Any message published to either channel is delivered to the
``parse_control_message`` callback supplied at config time.
"""

import asyncio
import logging

from scietex.service import (
    ValkeyAdvancedConfig,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyPubSubConfig,
    ValkeyWorker,
    ValkeyWorkerConfig,
)


def parse_control_message(message, context) -> None:
    """Handle a PubSub control message.

    Args:
        message: The received ``PubSubMsg`` (has ``message``, ``channel``
            and optional ``pattern`` attributes).
        context: Optional context passed at subscription time (``None`` here).
    """
    logging.getLogger("control").info("control message on %s: %s", message.channel, message.message)


async def main(config: ValkeyConfig) -> None:
    """Main function."""

    worker = ValkeyWorker(
        ValkeyWorkerConfig(
            service_name="MyPubSubValkeyService",
            version="0.0.1",
            logging_level=logging.DEBUG,
            heartbeat_interval=4,
            valkey_config=config,
            queue_size=100,
            max_concurrent_tasks=100,
        )
    )
    await worker.start()
    await worker.events["exit"].wait()


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
        pubsub_config=ValkeyPubSubConfig(
            listening=True,
            parse_control_message=parse_control_message,
        ),
    )

    asyncio.run(main(valkey_config))
