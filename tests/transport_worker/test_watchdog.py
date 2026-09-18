"""TransportWorker.watchdog tests: call order and CRITICAL surfacing (AR-102a)."""

import logging
import time

import pytest

from ._helpers import build_worker


@pytest.mark.asyncio
async def test_watchdog_call_order(tmp_path, monkeypatch):
    """watchdog drives refresh_leases -> health.recover -> base watchdog ->
    critical_report, in that order."""
    worker = build_worker(tmp_path)
    order = worker.order

    async def recover():
        order.append("recover")

    async def base_watchdog():
        order.append("base_watchdog")

    def critical_report():
        order.append("critical_report")
        return None

    monkeypatch.setattr(worker._health, "recover", recover)
    monkeypatch.setattr(worker._health, "critical_report", critical_report)
    monkeypatch.setattr(worker._executor, "watchdog", base_watchdog)

    await worker.watchdog()

    assert order == ["refresh_leases", "recover", "base_watchdog", "critical_report"]


@pytest.mark.asyncio
async def test_watchdog_logs_critical_report(tmp_path, caplog):
    """A connection down past the threshold surfaces one CRITICAL report."""
    worker = build_worker(tmp_path)
    # Simulate a sustained outage without requesting a reconnect, so recover()
    # no-ops and critical_report() fires (mirrors tests/mqtt/test_worker.py).
    worker._health._down_since = time.monotonic() - 9999
    worker._health._failure_count = 3
    worker._health._last_error = "boom"
    worker._health._degraded = True
    worker._health._reported_critical = False

    with caplog.at_level(logging.CRITICAL):
        await worker.watchdog()

    messages = [r.getMessage() for r in caplog.records]
    assert any("down for" in m for m in messages)
    assert any(m.startswith("Transport connection down") for m in messages)
