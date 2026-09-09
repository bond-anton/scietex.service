"""
Example of a custom Valkey worker that listens for PubSub control messages.

``ValkeyWorker`` itself always builds its client with ``listening=False``.
To subscribe to the service-specific and broadcast channels, subclass it and
pass a pre-built ``GlideClientConfiguration`` (created with
``generate_glide_config(..., listening=True)``) as ``valkey_config``.

The worker subscribes to two channels:
    - ``scietex:{service_name}:{instance_id}``  (directed at this instance)
    - ``scietex:broadcast``                     (sent to every instance)

Any message published to either channel is delivered to the
``parse_control_message`` callback supplied at config time.
"""

import asyncio
import logging

import msgspec
from glide import GlideClientConfiguration

from scietex.service import (
    ValkeyAdvancedConfig,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyWorker,
    ValkeyWorkerConfig,
)
from scietex.service.valkey.config import generate_glide_config


def parse_control_message(message, context) -> None:
    """Handle a PubSub control message.

    Args:
        message: The received ``PubSubMsg`` (has ``message``, ``channel``
            and optional ``pattern`` attributes).
        context: Optional context passed at subscription time (``None`` here).
    """
    logging.getLogger("control").info("control message on %s: %s", message.channel, message.message)


class PubSubValkeyWorker(ValkeyWorker):
    """A Valkey worker that also listens on the PubSub control channels."""

    def __init__(self, config: ValkeyWorkerConfig | None = None) -> None:
        # Build a client configuration that subscribes to the control
        # channels instead of letting ValkeyWorker build a non-listening one.
        cfg = config if config is not None else ValkeyWorkerConfig()
        if isinstance(cfg.valkey_config, ValkeyConfig):
            client_config: GlideClientConfiguration = generate_glide_config(
                cfg.valkey_config,
                service_name=cfg.service_name,
                worker_id="pubsub-example",
                listening=True,
                parse_control_message=parse_control_message,
            )
            cfg = msgspec.structs.replace(cfg, valkey_config=client_config)
        super().__init__(cfg)


async def main(config: ValkeyConfig) -> None:
    """Main function."""

    worker = PubSubValkeyWorker(
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
    )

    asyncio.run(main(valkey_config))
