"""TransportHealth (AR-075) unit tests and ValkeyWorker health-integration tests.

The unit tests inject a fake clock and a no-op reconnect so down-duration and
cooldown behavior are deterministic (no real sleeps). The worker-level tests
drive the real wiring: heartbeat/registry failures report into health, the
watchdog reconnects, and a sustained outage surfaces CRITICAL once.
"""

import asyncio
import logging
from datetime import datetime, timezone

import pytest

import scietex.service.valkey.worker as mod
from scietex.service import ValkeyWorker
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig
from scietex.service.valkey.health import TransportHealth

from ._helpers import DummyClient, FakeHandler


class FakeClock:
    """Injectable monotonic clock with a manually advanced offset."""

    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now


def _health(
    clock=None,
    *,
    reconnect=None,
    is_connected=None,
    down_threshold: float = 30.0,
    reconnect_cooldown: float = 1.0,
) -> TransportHealth:
    """Build a TransportHealth for unit tests with a no-op reconnect default."""

    async def noop_reconnect():
        return None

    return TransportHealth(
        reconnect=reconnect if reconnect is not None else noop_reconnect,
        is_connected=is_connected if is_connected is not None else (lambda: False),
        logger=logging.getLogger("test_health"),
        down_threshold=down_threshold,
        reconnect_cooldown=reconnect_cooldown,
        clock=clock if clock is not None else FakeClock(),
    )


def test_report_failure_records_state_and_mark_connected_resets():
    clock = FakeClock()
    health = _health(clock)

    assert health.degraded is False
    assert health.last_error is None
    assert health.failure_count == 0
    assert health.down_duration == 0.0

    health.report_failure(Exception("boom"))
    assert health.degraded is True
    assert health.last_error == "boom"
    assert health.failure_count == 1

    clock.now = 3.0
    assert health.down_duration == 3.0

    health.report_failure(Exception("boom2"))
    assert health.failure_count == 2
    assert health.last_error == "boom2"
    # down_duration is anchored at the first unrecovered failure.
    assert health.down_duration == 3.0

    health.mark_connected()
    assert health.degraded is False
    assert health.last_error is None
    assert health.failure_count == 0
    assert health.down_duration == 0.0
    assert health.connected is True


@pytest.mark.asyncio
async def test_recover_noops_when_healthy():
    calls: list[None] = []

    async def reconnect():
        calls.append(None)

    health = _health(reconnect=reconnect, is_connected=lambda: True)

    await health.recover()

    assert calls == []


@pytest.mark.asyncio
async def test_recover_skips_within_cooldown():
    calls: list[None] = []
    clock = FakeClock()

    async def reconnect():
        calls.append(None)

    health = _health(
        clock,
        reconnect=reconnect,
        is_connected=lambda: False,
        reconnect_cooldown=1.0,
    )
    health.report_failure(Exception("boom"))

    await health.recover()  # first attempt
    assert calls == [None]

    clock.now = 0.5  # still within the 1.0s cooldown
    await health.recover()
    assert calls == [None], "reconnect must be skipped within the cooldown"


@pytest.mark.asyncio
async def test_recover_deduplicates_concurrent_calls():
    calls: list[None] = []

    async def reconnect():
        calls.append(None)
        await asyncio.sleep(0.01)

    health = _health(reconnect=reconnect, is_connected=lambda: True)
    health.report_failure(Exception("boom"))

    await asyncio.gather(health.recover(), health.recover())

    assert calls == [None], "concurrent recover() must reconnect exactly once"


@pytest.mark.asyncio
async def test_recover_keeps_degraded_and_retries_after_cooldown_on_failure():
    calls: list[None] = []
    clock = FakeClock()
    connected = {"value": False}

    async def reconnect():
        calls.append(None)

    health = _health(
        clock,
        reconnect=reconnect,
        is_connected=lambda: connected["value"],
        reconnect_cooldown=1.0,
    )
    health.report_failure(Exception("boom"))

    await health.recover()  # attempt 1: reconnect fails
    assert calls == [None]
    assert health.degraded is True
    assert health.last_error == "boom"

    clock.now = 0.5
    await health.recover()  # within cooldown -> skipped
    assert calls == [None]

    clock.now = 2.0
    connected["value"] = True  # reconnect now succeeds
    await health.recover()  # attempt 2: succeeds
    assert calls == [None, None]
    assert health.degraded is False
    assert health.last_error is None


def test_critical_report_once_per_episode_and_resets_after_recovery():
    clock = FakeClock()
    health = _health(clock, down_threshold=5.0)

    assert health.critical_report() is None  # healthy

    health.report_failure(Exception("boom"))
    clock.now = 1.0
    assert health.critical_report() is None  # below threshold

    clock.now = 6.0
    first = health.critical_report()
    assert first is not None
    assert "boom" in first
    assert health.critical_report() is None, "must not re-report within the same episode"

    health.mark_connected()
    assert health.critical_report() is None  # recovered

    health.report_failure(Exception("boom2"))
    clock.now = 20.0
    second = health.critical_report()
    assert second is not None, "a new outage must report again"
    assert "boom2" in second


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
    worker._BasicWorker__start_time = datetime.now(timezone.utc)
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
    worker._BasicWorker__start_time = datetime.now(timezone.utc)

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
    worker._client = DummyClient(sadd_error=mod.RequestError("sadd failed"))

    await worker._register_instance()

    assert worker.transport_health.degraded is True
    assert worker.transport_health.last_error == "sadd failed"
    assert worker.transport_health.failure_count == 1


@pytest.mark.asyncio
async def test_unregister_instance_failure_reports_into_health():
    worker = ValkeyWorker(ValkeyWorkerConfig(valkey_config=ValkeyConfig()))
    worker._client = DummyClient(srem_error=mod.RequestError("srem failed"))

    await worker._unregister_instance()

    assert worker.transport_health.degraded is True
    assert worker.transport_health.last_error == "srem failed"
    assert worker.transport_health.failure_count == 1
