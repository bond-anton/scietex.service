"""Tests for the broker monitors, driven by fake clients (no real brokers)."""

import asyncio
import time

import pytest

pytest.importorskip("glide")
pytest.importorskip("aiomqtt")

from examples.textual.broker_snapshot import MqttBrokerSnapshot, ValkeyBrokerSnapshot  # noqa: E402
from examples.textual.mqtt_monitor import MqttBrokerMonitor  # noqa: E402
from examples.textual.valkey_monitor import ValkeyBrokerMonitor, map_info  # noqa: E402

INFO_PAYLOAD = (
    b"# Server\r\n"
    b"valkey_version:8.0.1\r\n"
    b"uptime_in_seconds:3600\r\n"
    b"# Clients\r\n"
    b"connected_clients:3\r\n"
    b"# Memory\r\n"
    b"used_memory:1048576\r\n"
    b"used_memory_peak:2097152\r\n"
    b"# Stats\r\n"
    b"instantaneous_ops_per_sec:42\r\n"
    b"# CPU\r\n"
    b"used_cpu_sys:1.5\r\n"
    b"used_cpu_user:2.25\r\n"
    b"# Keyspace\r\n"
    b"db0:keys=5,expires=1,avg_ttl=0\r\n"
)


class FakeValkeyClient:
    def __init__(self, payload: bytes = INFO_PAYLOAD, lengths: dict[str, int] | None = None) -> None:
        self._payload = payload
        self._lengths = lengths or {}
        self.closed = False

    async def info(self, sections: object) -> bytes:
        return self._payload

    async def xlen(self, key: str) -> int:
        return self._lengths.get(key, 0)

    async def close(self) -> None:
        self.closed = True


class RaisingValkeyClient(FakeValkeyClient):
    async def info(self, sections: object) -> bytes:
        raise RuntimeError("connection refused")


class FakeMqttMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class FakeMqttClient:
    def __init__(self, messages: list[FakeMqttMessage] | None = None) -> None:
        self._messages = messages or []
        self.subscribed: list[str] = []
        self.exited = False

    async def subscribe(self, topic: str, qos: int = 0, *args: object, **kwargs: object) -> object:
        self.subscribed.append(topic)
        return (0,)

    @property
    def messages(self):
        async def _iterate():
            for message in self._messages:
                yield message

        return _iterate()

    async def __aexit__(self, *exc_info: object) -> None:
        self.exited = True


def test_map_info_maps_all_fields():
    from examples.textual.broker_parsing import parse_info

    fields, keyspace = parse_info(INFO_PAYLOAD)
    snapshot = map_info(fields, keyspace)
    assert snapshot.version == "8.0.1"
    assert snapshot.uptime_s == 3600
    assert snapshot.connected_clients == 3
    assert snapshot.used_memory == 1048576
    assert snapshot.used_memory_peak == 2097152
    assert snapshot.ops_per_sec == 42.0
    assert snapshot.used_cpu_sys == 1.5
    assert snapshot.used_cpu_user == 2.25
    assert snapshot.keys_total == 5


def test_map_info_prefers_valkey_version():
    snapshot = map_info({"redis_version": "7.2.0", "valkey_version": "8.0.1"}, {})
    assert snapshot.version == "8.0.1"


def test_map_info_falls_back_to_redis_version():
    snapshot = map_info({"redis_version": "7.2.0"}, {})
    assert snapshot.version == "7.2.0"


def test_map_info_reports_missing_keyspace_as_none():
    snapshot = map_info({}, {})
    assert snapshot.keys_total is None


@pytest.mark.asyncio
async def test_valkey_refresh_maps_info_and_stream_lengths():
    client = FakeValkeyClient(lengths={"scietex:service:tasks": 7, "scietex:service:log": 3})
    monitor = ValkeyBrokerMonitor(client_factory=lambda config: _return(client))

    await monitor.refresh()

    snapshot = monitor.snapshot()
    assert snapshot.connected is True
    assert snapshot.error is None
    assert snapshot.received_at > 0
    assert snapshot.version == "8.0.1"
    assert snapshot.task_stream_len == 7
    assert snapshot.log_stream_len == 3
    assert snapshot.control_stream_len == 0


@pytest.mark.asyncio
async def test_valkey_refresh_reports_disconnected_on_error():
    monitor = ValkeyBrokerMonitor(client_factory=lambda config: _return(RaisingValkeyClient()))

    await monitor.refresh()

    snapshot = monitor.snapshot()
    assert snapshot.connected is False
    assert snapshot.error == "connection refused"
    assert snapshot.received_at == 0.0


@pytest.mark.asyncio
async def test_valkey_refresh_drops_client_after_error():
    client = RaisingValkeyClient()
    monitor = ValkeyBrokerMonitor(client_factory=lambda config: _return(client))

    await monitor.refresh()

    assert client.closed is True


@pytest.mark.asyncio
async def test_valkey_start_and_stop_are_bounded():
    monitor = ValkeyBrokerMonitor(client_factory=lambda config: _return(FakeValkeyClient()), poll_interval=0.01)

    await monitor.start()
    await asyncio.sleep(0.05)
    await asyncio.wait_for(monitor.stop(), timeout=2)

    assert monitor.snapshot().connected is True


@pytest.mark.asyncio
async def test_valkey_stop_is_bounded_when_connect_hangs():
    monitor = ValkeyBrokerMonitor(client_factory=_hanging_factory)

    await monitor.start()
    await asyncio.sleep(0)
    await asyncio.wait_for(monitor.stop(), timeout=2)


def test_mqtt_ingest_maps_sys_topics():
    monitor = MqttBrokerMonitor()

    monitor._ingest("$SYS/broker/clients/connected", b"3")
    monitor._ingest("$SYS/broker/uptime", b"12345 seconds")
    monitor._ingest("$SYS/broker/load/messages/received/1min", b"12.5")

    snapshot = monitor.snapshot()
    assert snapshot.clients_connected == 3
    assert snapshot.uptime_s == 12345
    assert snapshot.load_messages_received_1min == 12.5
    assert snapshot.received_at > 0


def test_mqtt_ingest_ignores_unknown_topic():
    monitor = MqttBrokerMonitor()

    monitor._ingest("$SYS/broker/unknown", b"1")

    assert monitor.snapshot().received_at == 0.0


def test_mqtt_heap_absent_stays_none():
    monitor = MqttBrokerMonitor()

    monitor._ingest("$SYS/broker/clients/connected", b"3")

    assert monitor.snapshot().heap_current is None


@pytest.mark.asyncio
async def test_mqtt_run_subscribes_and_ingests():
    client = FakeMqttClient([FakeMqttMessage("$SYS/broker/clients/connected", b"5")])
    monitor = MqttBrokerMonitor(client_factory=lambda config: _return(client), reconnect_delay=0.01)

    await monitor.start()
    await asyncio.sleep(0.05)
    await asyncio.wait_for(monitor.stop(), timeout=2)

    assert client.subscribed[0] == "$SYS/#"
    assert monitor.snapshot().clients_connected == 5


@pytest.mark.asyncio
async def test_mqtt_stop_is_bounded_when_connect_hangs():
    monitor = MqttBrokerMonitor(client_factory=_hanging_factory)

    await monitor.start()
    await asyncio.sleep(0)
    await asyncio.wait_for(monitor.stop(), timeout=2)


@pytest.mark.asyncio
async def test_mqtt_reports_disconnected_on_connect_error():
    async def failing_factory(config):
        raise RuntimeError("no route to host")

    monitor = MqttBrokerMonitor(client_factory=failing_factory, reconnect_delay=0.01)

    await monitor.start()
    await asyncio.sleep(0.05)
    await asyncio.wait_for(monitor.stop(), timeout=2)

    snapshot = monitor.snapshot()
    assert snapshot.connected is False
    assert snapshot.error == "no route to host"


async def _return(value):
    return value


async def _hanging_factory(config):
    await asyncio.sleep(3600)
    raise AssertionError("unreachable: the factory is cancelled before it returns")


def test_snapshots_default_to_unavailable():
    valkey = ValkeyBrokerSnapshot()
    mqtt = MqttBrokerSnapshot()
    assert valkey.connected is False
    assert valkey.used_memory is None
    assert mqtt.heap_current is None
    assert mqtt.received_at == 0.0


def test_staleness_is_derived_from_monotonic_clock():
    snapshot = MqttBrokerSnapshot(connected=True, received_at=time.monotonic() - 20)
    assert time.monotonic() - snapshot.received_at > 15
