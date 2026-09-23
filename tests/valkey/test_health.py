"""ValkeyWorker health-integration tests (AR-075).

The pure ``TransportHealth`` unit tests live in ``tests/core/test_transport_health.py`` (the
supervisor moved to core in AR-089). These tests drive the real wiring:
heartbeat/registry failures report into health, the watchdog reconnects, and a
sustained outage surfaces CRITICAL once.
"""

import logging
from datetime import datetime, timezone

import pytest

import scietex.service.valkey.worker as mod
from scietex.service import ValkeyWorker
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig

from ._helpers import DummyClient, FakeHandler


@pytest.mark.asyncio
async def test_watchdog_logs_critical_once_then_recovers(caplog, monkeypatch):
    """A forced failure keeping the client down past the threshold surfaces one
    CRITICAL; once the connection recovers, the next watchdog tick is quiet."""
    monkeypatch.setattr(mod, "AsyncValkeyHandler", FakeHandler)
    reconnecting = {"down": True}

    async def factory(cfg):
        return DummyClient(ping_ok=not reconnecting["down"])

    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    worker._client = DummyClient(set_error=mod.RequestError("down"))
    worker._lifecycle.start_time = datetime.now(timezone.utc)
    # Near-zero threshold/cooldown so the test stays deterministic with no sleeps.
    worker._health._down_threshold = 0.0
    worker._health._reconnect_cooldown = 0.0

    await worker.heartbeat()  # fails -> reports into health
    assert worker.transport_health.degraded is True

    with caplog.at_level(logging.CRITICAL):
        await worker.watchdog()  # reconnect fails -> CRITICAL emitted

    criticals = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(criticals) == 1

    reconnecting["down"] = False  # next reconnect succeeds
    caplog.clear()
    with caplog.at_level(logging.CRITICAL):
        await worker.watchdog()

    assert [r for r in caplog.records if r.levelno == logging.CRITICAL] == []
    assert worker.transport_health.degraded is False
    assert worker.client is not None


@pytest.mark.asyncio
async def test_heartbeat_failure_marks_degraded_and_watchdog_reconnects(monkeypatch):
    """A heartbeat glide failure reports into health; the following watchdog
    reconnect replaces the dead client and clears the degraded state."""
    monkeypatch.setattr(mod, "AsyncValkeyHandler", FakeHandler)

    async def factory(cfg):
        return DummyClient(ping_ok=True)

    client = DummyClient(set_error=mod.RequestError("heartbeat failed"))
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()), client_factory=factory)
    worker._client = client
    worker._lifecycle.start_time = datetime.now(timezone.utc)

    await worker.heartbeat()

    assert worker.transport_health.degraded is True
    assert worker.transport_health.last_error == "heartbeat failed"
    assert worker.transport_health.failure_count == 1

    await worker.watchdog()

    assert worker.client is not None
    assert worker.client is not client, "watchdog must replace the dead client"
    assert worker.transport_health.degraded is False
    assert worker.transport_health.connected is True


@pytest.mark.asyncio
async def test_register_instance_failure_reports_into_health():
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = DummyClient(set_error=mod.RequestError("set failed"))

    await worker._register_instance()

    assert worker.transport_health.degraded is True
    assert worker.transport_health.last_error == "set failed"
    assert worker.transport_health.failure_count == 1


@pytest.mark.asyncio
async def test_unregister_instance_failure_reports_into_health():
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = DummyClient(set_error=mod.RequestError("set failed"))

    await worker._unregister_instance()

    assert worker.transport_health.degraded is True
    assert worker.transport_health.last_error == "set failed"
    assert worker.transport_health.failure_count == 1


def test_logging_handler_receives_stream_maxlen(monkeypatch):
    """_ensure_logging_handler forwards log_stream_maxlen to the handler."""
    monkeypatch.setattr(mod, "AsyncValkeyHandler", FakeHandler)
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig(), log_stream_maxlen=250))

    handler = worker._ensure_logging_handler()

    assert handler is not None
    assert handler.stream_maxlen == 250
    assert handler.stream_name == worker._log_stream_name
