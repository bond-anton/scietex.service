"""ValkeyWorker logging-handler ownership and configuration tests."""

import pytest

from scietex.service import ValkeyWorker
from scietex.service.valkey.config import (
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyPubSubConfig,
    ValkeyUserCredentials,
    ValkeyWorkerConfig,
    logging_handler_config,
)

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
async def test_logging_handler_ignores_pubsub_listening(monkeypatch):
    """A pubsub-listening ValkeyConfig still yields a handler built by
    logging_handler_config; listening does not leak into the logging handler."""

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    def parse_control_message(msg, context):
        pass

    _patch_glide_and_handler(monkeypatch)

    cfg = ValkeyConfig(pubsub_config=ValkeyPubSubConfig(listening=True, parse_control_message=parse_control_message))
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=cfg), client_factory=factory)
    ok = await worker.connect()
    assert ok is True
    handler = worker._valkey_logger_handler
    assert handler is not None
    assert isinstance(handler, FakeHandler)
    assert handler._owns_client is True
    assert handler.client is None
    assert handler.valkey_config is not None
    assert handler.valkey_config["addresses"] == [("localhost", 6379)]


def test_logging_handler_config_translates_typed_config():
    """logging_handler_config maps a typed ValkeyConfig onto the external
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
    assert logging_handler_config(cfg) == {
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
