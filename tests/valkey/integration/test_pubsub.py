"""End-to-end PubSub integration tests against a live Valkey/Redis server.

These tests exercise the real PubSub capability (AR-044) via
``ValkeyConfig(pubsub_config=ValkeyPubSubConfig(listening=True))`` against a
running server. They are gated by ``SCIETEX_TEST_VALKEY_URL`` (see
``conftest.py``), so the suite stays green in environments without a server.
"""

import asyncio

import pytest

from scietex.service.valkey.config import ValkeyConfig, ValkeyPubSubConfig, generate_glide_config


@pytest.mark.asyncio
async def test_pubsub_directed_and_broadcast_delivery(valkey_config: ValkeyConfig, service_name: str):
    """A client built with ``listening=True`` receives directed and broadcast
    messages published on its subscribed channels."""

    from glide import GlideClient

    received: list[tuple[bytes, bytes]] = []

    def parse_control_message(message, context) -> None:
        received.append((message.channel, message.message))

    client = await GlideClient.create(
        generate_glide_config(
            ValkeyConfig(
                base_config=valkey_config.base_config,
                pubsub_config=ValkeyPubSubConfig(
                    listening=True,
                    parse_control_message=parse_control_message,
                ),
            ),
            service_name=service_name,
        )
    )
    try:
        # Directed channel: scietex:{service}
        directed = f"scietex:{service_name}"
        # Broadcast channel: scietex:broadcast
        broadcast = "scietex:broadcast"

        directed_receivers = await client.publish("hello-directed", directed)
        broadcast_receivers = await client.publish("hello-broadcast", broadcast)
        assert directed_receivers >= 1
        assert broadcast_receivers >= 1

        # The callback fires on a background reader thread; poll briefly.
        deadline = asyncio.get_event_loop().time() + 5.0
        while len(received) < 2 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)

        assert (directed.encode(), b"hello-directed") in received
        assert (broadcast.encode(), b"hello-broadcast") in received
    finally:
        await client.close()
