"""ValkeyWorker logging-handler ownership and configuration tests."""

import pytest

from scietex.service import ValkeyWorker
from scietex.service.valkey._glide import GlideClientConfiguration, NodeAddress
from scietex.service.valkey.config import (
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyUserCredentials,
    ValkeyWorkerConfig,
)
from scietex.service.valkey.worker import _logging_handler_config

from ._helpers import DummyClient, FakeHandler, _patch_glide_and_handler


@pytest.mark.asyncio
async def test_logging_handler_owns_its_own_connection(monkeypatch):
    """With a typed ValkeyConfig the handler is built once on connect with
    valkey_config= (owning its own connection); the worker no longer injects or
    re-points its client (AR-059/061)."""

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    _patch_glide_and_handler(monkeypatch)

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    assert worker._valkey_logger_handler is None  # not built until connect

    ok = await worker.connect()
    assert ok is True
    handler = worker._valkey_logger_handler
    assert handler is not None
    assert isinstance(handler, FakeHandler)
    assert handler._owns_client is True, "typed ValkeyConfig -> handler owns its connection"
    assert handler.client is None, "handler must not be injected the worker's client"
    assert handler.valkey_config is not None, "handler must be handed a valkey_config dict"
    assert handler.valkey_config["addresses"] == [("localhost", 6379)]


@pytest.mark.asyncio
async def test_logging_handler_falls_back_to_client_injection_with_raw_config(monkeypatch):
    """A raw GlideClientConfiguration has no typed ValkeyConfig to hand the
    handler, so it keeps the shared-client injection seam (AR-059/061)."""

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    raw_config = GlideClientConfiguration(addresses=[NodeAddress("localhost", 6379)])
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=raw_config), client_factory=factory)
    ok = await worker.connect()
    assert ok is True
    handler = worker._valkey_logger_handler
    assert handler is not None
    assert handler._owns_client is False, "raw config -> handler shares the worker's client"
    assert handler.client is worker.client


def test_logging_handler_config_translates_typed_config():
    """_logging_handler_config maps a typed ValkeyConfig onto the external
    handler's scalar dict schema (addresses + credentials + TLS + timeouts)."""
    cfg = ValkeyConfig(
        base_config=ValkeyBaseConfig(
            nodes=[ValkeyNode(host="redis.internal", port=6380)],
            user_credentials=ValkeyUserCredentials(username="svc", password="secret"),
            use_tls=True,
            request_timeout=7500,
            database_id=2,
            client_name="logger",
        )
    )
    assert _logging_handler_config(cfg) == {
        "addresses": [("redis.internal", 6380)],
        "username": "svc",
        "password": "secret",
        "use_tls": True,
        "request_timeout": 7500,
        "database_id": 2,
        "client_name": "logger",
        "inflight_requests_limit": None,
        "client_az": None,
        "lazy_connect": None,
    }
