"""Fixtures for the Valkey end-to-end integration tier.

These tests run against a real Valkey/Redis server and are opt-in: set
``SCIETEX_TEST_VALKEY_URL`` to a ``host:port`` address to enable them. When the
variable is unset the whole directory skips, so the default suite stays
hermetic and CI without a broker stays green.

The mocked unit tests in ``tests/valkey/`` remain the primary coverage; this
tier proves only what a fake structurally cannot — that the worker drives a real
server end to end.
"""

import os
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import pytest
import pytest_asyncio

from scietex.service.valkey.config import (
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyWorkerConfig,
)

#: Env var holding the ``host:port`` of the Valkey server under test.
VALKEY_URL_ENV = "SCIETEX_TEST_VALKEY_URL"


def _parse_address(raw: str) -> tuple[str, int]:
    """Split a ``host:port`` string, defaulting the port to 6379."""
    host, _, port = raw.rpartition(":")
    if not host:
        return raw, 6379
    return host, int(port)


@pytest.fixture(scope="session")
def valkey_address() -> tuple[str, int]:
    """The ``(host, port)`` of the server under test, or skip when unset."""
    raw = os.environ.get(VALKEY_URL_ENV)
    if not raw:
        pytest.skip(f"{VALKEY_URL_ENV} not set; skipping Valkey integration tests")
    return _parse_address(raw)


@pytest.fixture
def valkey_config(valkey_address: tuple[str, int]) -> ValkeyConfig:
    """A ``ValkeyConfig`` pointing at the server under test."""
    host, port = valkey_address
    return ValkeyConfig(base_config=ValkeyBaseConfig(nodes=[ValkeyNode(host=host, port=port)]))


@pytest.fixture
def service_name() -> Iterator[str]:
    """A per-test service name, so stream and key names never collide."""
    yield f"itest-{uuid4().hex[:12]}"


@pytest.fixture
def worker_config(service_name: str, valkey_config: ValkeyConfig) -> ValkeyWorkerConfig:
    """A ``ValkeyWorkerConfig`` bound to the server under test.

    The queue-manager poll interval is raised from its 10 ms default: the
    transport reads four streams per iteration with a non-blocking ``XREAD``,
    and two idle workers at the default rate saturate a single-threaded dev
    server until the 5 s heartbeat write times out.
    """
    return ValkeyWorkerConfig(
        service_name=service_name,
        valkey_config=valkey_config,
        task_queue_manager_sleep_time=0.2,
    )


@pytest_asyncio.fixture
async def valkey_client(valkey_config: ValkeyConfig) -> AsyncIterator[object]:
    """A raw ``GlideClient`` for seeding streams and asserting server state."""
    from glide import GlideClient

    from scietex.service.valkey.config import generate_glide_config

    client = await GlideClient.create(generate_glide_config(valkey_config, "itest-probe"))
    try:
        yield client
    finally:
        await client.close()


@pytest_asyncio.fixture
async def publisher_client(valkey_config: ValkeyConfig) -> AsyncIterator[object]:
    """A dedicated ``GlideClient`` for control-plane publishers.

    A publisher must not share a worker's client: ``GlideClient`` serializes
    requests per connection, so a publisher call issued from the test coroutine
    would block the worker's heartbeat and queue-manager reads until it times
    out.
    """
    from glide import GlideClient

    from scietex.service.valkey.config import generate_glide_config

    client = await GlideClient.create(generate_glide_config(valkey_config, "itest-publisher"))
    try:
        yield client
    finally:
        await client.close()
