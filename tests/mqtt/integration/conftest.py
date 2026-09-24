"""Fixtures for the MQTT end-to-end integration tier.

These tests run against a real MQTT broker and are opt-in: set
``SCIETEX_TEST_MQTT_HOST`` (and optionally ``SCIETEX_TEST_MQTT_PORT``) to enable
them. When the host variable is unset the whole directory skips, so the default
suite stays hermetic and CI without a broker stays green.

The mocked unit tests in ``tests/mqtt/`` remain the primary coverage; this tier
proves only what a fake structurally cannot — that the worker drives a real
broker end to end.
"""

import os
from collections.abc import Iterator
from uuid import uuid4

import pytest

from scietex.service.mqtt.config import MqttConfig, MqttWorkerConfig

#: Env var holding the host of the MQTT broker under test.
MQTT_HOST_ENV = "SCIETEX_TEST_MQTT_HOST"

#: Env var holding the port of the MQTT broker under test.
MQTT_PORT_ENV = "SCIETEX_TEST_MQTT_PORT"


@pytest.fixture(scope="session")
def mqtt_address() -> tuple[str, int]:
    """The ``(host, port)`` of the broker under test, or skip when unset."""
    host = os.environ.get(MQTT_HOST_ENV)
    if not host:
        pytest.skip(f"{MQTT_HOST_ENV} not set; skipping MQTT integration tests")
    return host, int(os.environ.get(MQTT_PORT_ENV, "1883"))


@pytest.fixture
def mqtt_config(mqtt_address: tuple[str, int]) -> MqttConfig:
    """An ``MqttConfig`` pointing at the broker under test."""
    host, port = mqtt_address
    return MqttConfig(host=host, port=port)


@pytest.fixture
def service_name() -> Iterator[str]:
    """A per-test service name, so topic names never collide."""
    yield f"itest-{uuid4().hex[:12]}"


@pytest.fixture
def worker_config(service_name: str, mqtt_config: MqttConfig, tmp_path) -> MqttWorkerConfig:
    """An ``MqttWorkerConfig`` with a durable inbox under ``tmp_path``."""
    return MqttWorkerConfig(
        service_name=service_name,
        mqtt_config=mqtt_config,
        inbox_backend="sqlite",
        inbox_path=str(tmp_path / "inbox.sqlite3"),
    )
