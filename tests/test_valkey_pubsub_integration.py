"""End-to-end PubSub integration tests against a live Valkey/Redis server.

These tests exercise the real ``generate_glide_config(..., listening=True)``
PubSub capability (AR-044) against a running server. They are skipped when no
server is reachable on ``localhost:6379``, so the suite stays green in
environments without one (plain local dev). CI provides a ``redis`` service
container on port 6379, so they run there.
"""

import asyncio

import pytest

from scietex.service.valkey.config import ValkeyConfig, generate_glide_config


def _server_reachable() -> bool:
    """Return ``True`` if a Valkey/Redis server answers on localhost:6379."""

    async def probe() -> bool:
        from glide import GlideClient

        client = None
        try:
            client = await GlideClient.create(generate_glide_config(ValkeyConfig(), "probe", "probe"))
            return await client.ping() == b"PONG"
        except Exception:
            return False
        finally:
            if client is not None:
                await client.close()

    try:
        return asyncio.run(probe())
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _server_reachable(),
    reason="no Valkey/Redis server reachable on localhost:6379",
)


@pytest.mark.asyncio
async def test_pubsub_directed_and_broadcast_delivery():
    """A client built with ``listening=True`` receives directed and broadcast
    messages published on its subscribed channels."""

    from glide import GlideClient

    received: list[tuple[bytes, bytes]] = []

    def parse_control_message(message, context) -> None:
        received.append((message.channel, message.message))

    service_name = "pubsub-itest"
    worker_id = "worker-1"
    client = await GlideClient.create(
        generate_glide_config(
            ValkeyConfig(),
            service_name=service_name,
            worker_id=worker_id,
            listening=True,
            parse_control_message=parse_control_message,
        )
    )
    try:
        # Directed channel: scietex:{service}:{worker_id}
        directed = f"scietex:{service_name}:{worker_id}"
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
