"""Configuration tests for the MQTT worker (``MqttWorkerConfig``)."""

import msgspec
import pytest

from scietex.service.mqtt.config import MqttWorkerConfig


def test_worker_config_status_publish_defaults():
    """The six addendum §13.6 fields default to their documented values."""
    cfg = MqttWorkerConfig()
    assert cfg.status_publish_enabled is True
    assert cfg.status_topic_prefix == "scietex/{service}/tasks"
    assert cfg.status_qos == 1
    assert cfg.progress_qos == 0
    assert cfg.progress_min_interval == 1.0
    assert cfg.progress_min_delta == 0.0


def test_worker_config_status_ttl_default():
    """status_ttl defaults to 86400 seconds (one day)."""
    assert MqttWorkerConfig().status_ttl == 86400


def test_worker_config_inbox_ttl_default():
    """inbox_ttl defaults to 86400 seconds (one day)."""
    assert MqttWorkerConfig().inbox_ttl == 86400


def test_worker_config_control_defaults():
    """The four control-plane fields default to their documented values."""
    cfg = MqttWorkerConfig()
    assert cfg.control_topic == "scietex/{service}/control/{instance_id}"
    assert cfg.control_broadcast_topic == "scietex/{service}/control"
    assert cfg.control_qos == 1
    assert cfg.control_inbox_path is None


@pytest.mark.parametrize("status_ttl", [None, 1, 2592000])
def test_worker_config_status_ttl_bounds_accept(status_ttl):
    """status_ttl=None and the [1, 2592000] bounds construct successfully."""
    assert MqttWorkerConfig(status_ttl=status_ttl).status_ttl == status_ttl


@pytest.mark.parametrize("inbox_ttl", [None, 1, 2592000])
def test_worker_config_inbox_ttl_bounds_accept(inbox_ttl):
    """inbox_ttl=None and the [1, 2592000] bounds construct successfully."""
    assert MqttWorkerConfig(inbox_ttl=inbox_ttl).inbox_ttl == inbox_ttl


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("status_qos", 3),
        ("progress_qos", 3),
        ("progress_min_interval", -1),
        ("progress_min_delta", 101),
        ("status_ttl", 0),
        ("status_ttl", 2592001),
        ("inbox_ttl", 0),
        ("inbox_ttl", 2592001),
        ("control_qos", 3),
        ("control_qos", -1),
    ],
)
def test_worker_config_status_fields_out_of_range_raises(field_name, value):
    with pytest.raises(msgspec.ValidationError):
        MqttWorkerConfig(**{field_name: value})
