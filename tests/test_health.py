"""TransportHealth (AR-075, AR-089) unit tests.

The unit tests inject a fake clock and a no-op reconnect so down-duration and
cooldown behavior are deterministic (no real sleeps). These exercise the
transport-agnostic supervisor in core directly, with no Valkey dependency;
the Valkey-worker wiring is covered by ``tests/valkey/test_health.py``.
"""

import asyncio
import logging

import pytest

from scietex.service.health import TransportHealth


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
    transport_name: str = "Transport",
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
        transport_name=transport_name,
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
    assert first.startswith("Transport connection down"), "default label is transport-agnostic"
    assert health.critical_report() is None, "must not re-report within the same episode"

    health.mark_connected()
    assert health.critical_report() is None  # recovered

    health.report_failure(Exception("boom2"))
    clock.now = 20.0
    second = health.critical_report()
    assert second is not None, "a new outage must report again"
    assert "boom2" in second


def test_critical_report_names_the_configured_transport():
    """The CRITICAL message names the failing backend, not a hardcoded one."""
    clock = FakeClock()
    health = _health(clock, transport_name="MQTT", down_threshold=5.0)

    health.report_failure(Exception("broker unreachable"))
    clock.now = 6.0
    report = health.critical_report()

    assert report is not None
    assert report.startswith("MQTT connection down")
    assert "Valkey" not in report
