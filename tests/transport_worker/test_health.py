"""TransportWorker health-supervisor wiring tests (AR-102a)."""

from scietex.service.config import TaskProcessorConfig
from scietex.service.health import TransportHealth

from ._helpers import build_worker


def test_transport_health_returns_constructed_supervisor(tmp_path):
    """transport_health exposes the TransportHealth built at construction."""
    worker = build_worker(tmp_path)

    assert isinstance(worker.transport_health, TransportHealth)
    assert worker.transport_health is worker._health


def test_health_thresholds_derive_from_intervals(tmp_path):
    """The down threshold and reconnect cooldown derive from the worker's
    heartbeat/watchdog intervals, not from a configured value."""
    config = TaskProcessorConfig(conf_dir=tmp_path, heartbeat_interval=7.0, watchdog_interval=2.0)
    worker = build_worker(tmp_path, config=config)

    assert worker._health._down_threshold == max(3 * 2.0, 7.0)
    assert worker._health._reconnect_cooldown == 2.0


def test_health_default_transport_name(tmp_path):
    """The default ClassVar labels the supervisor with the generic transport."""
    worker = build_worker(tmp_path)

    assert worker._health._transport_name == "Transport"
