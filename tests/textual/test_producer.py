"""Tests for the task producers: broker routing, interval loop, and lifecycle."""

import asyncio
from typing import cast

import aiomqtt
import pytest

from examples.textual.producer import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_INTERVAL_MS,
    DEFAULT_TIMEOUT_MS,
    MAX_BATCH_SIZE,
    MAX_INTERVAL_MS,
    MIN_BATCH_SIZE,
    MIN_INTERVAL_MS,
    TASK_FIELD,
    TaskProducer,
)
from scietex.service.task_handler.wire import decode_task_envelope
from scietex.service.valkey._glide import GlideClient


class FakeValkeyClient:
    """Records XADD calls and can be made to fail."""

    def __init__(self, fail: bool = False, fail_after: int | None = None) -> None:
        self.calls: list[tuple[str, list]] = []
        self.closed = False
        self._fail = fail
        self._fail_after = fail_after

    async def xadd(self, key, values):
        if self._fail or (self._fail_after is not None and len(self.calls) >= self._fail_after):
            raise ConnectionError("valkey down")
        self.calls.append((key, values))
        return b"1-1"

    async def close(self) -> None:
        self.closed = True


class FakeMqttClient:
    """Records publish calls and can be made to fail."""

    def __init__(self, fail: bool = False) -> None:
        self.calls: list[tuple[str, bytes, int]] = []
        self.exited = False
        self._fail = fail

    async def publish(self, topic, payload, qos=0, **kwargs):
        if self._fail:
            raise ConnectionError("mqtt down")
        self.calls.append((topic, payload, qos))

    async def __aexit__(self, *args) -> None:
        self.exited = True


def _producer(valkey=None, mqtt=None, **kwargs) -> TaskProducer:
    """A producer wired to fake clients, bypassing config-file reads."""
    valkey_client = valkey if valkey is not None else FakeValkeyClient()
    mqtt_client = mqtt if mqtt is not None else FakeMqttClient()

    producer = TaskProducer(
        "fast_task",
        valkey_client_factory=lambda _config: _return(valkey_client),
        mqtt_client_factory=lambda _config: _return(mqtt_client),
        **kwargs,
    )
    producer._valkey_client = cast(GlideClient, valkey_client)
    producer._mqtt_client = cast(aiomqtt.Client, mqtt_client)
    return producer


async def _return(value):
    return value


@pytest.mark.asyncio
async def test_emit_with_no_broker_enabled_is_a_noop():
    producer = _producer()
    assert await producer.emit() == 0
    assert producer.emitted == 0
    assert producer.last_error is None


@pytest.mark.asyncio
async def test_emit_valkey_only():
    valkey = FakeValkeyClient()
    producer = _producer(valkey=valkey)
    producer.set_valkey_enabled(True)

    assert await producer.emit() == 1
    assert producer.emitted == 1
    assert len(valkey.calls) == 1
    key, values = valkey.calls[0]
    assert key == "scietex:service:tasks"
    assert values[0][0] == TASK_FIELD
    task = decode_task_envelope(values[0][1])
    assert task is not None
    assert task.task == "fast_task"


@pytest.mark.asyncio
async def test_emit_mqtt_only():
    mqtt = FakeMqttClient()
    producer = _producer(mqtt=mqtt)
    producer.set_mqtt_enabled(True)

    assert await producer.emit() == 1
    topic, payload, qos = mqtt.calls[0]
    assert topic == "scietex/service/tasks"
    assert qos == 2
    decoded = decode_task_envelope(payload)
    assert decoded is not None
    assert decoded.task == "fast_task"


@pytest.mark.asyncio
async def test_emit_both_brokers():
    valkey = FakeValkeyClient()
    mqtt = FakeMqttClient()
    producer = _producer(valkey=valkey, mqtt=mqtt)
    producer.set_valkey_enabled(True)
    producer.set_mqtt_enabled(True)

    assert await producer.emit() == 2
    assert producer.emitted == 2
    assert len(valkey.calls) == 1
    assert len(mqtt.calls) == 1


@pytest.mark.asyncio
async def test_batch_size_publishes_that_many_tasks_per_broker():
    valkey = FakeValkeyClient()
    mqtt = FakeMqttClient()
    producer = _producer(valkey=valkey, mqtt=mqtt)
    producer.set_valkey_enabled(True)
    producer.set_mqtt_enabled(True)
    producer.set_batch_size(5)

    assert await producer.emit() == 10
    assert producer.emitted == 10
    assert len(valkey.calls) == 5
    assert len(mqtt.calls) == 5


@pytest.mark.asyncio
async def test_batch_tasks_have_distinct_ids():
    valkey = FakeValkeyClient()
    producer = _producer(valkey=valkey)
    producer.set_valkey_enabled(True)
    producer.set_batch_size(3)

    await producer.emit()
    tasks = [decode_task_envelope(call[1][0][1]) for call in valkey.calls]
    ids = {task.task_id for task in tasks if task is not None}
    assert len(ids) == 3


@pytest.mark.asyncio
async def test_batch_failure_keeps_tasks_already_published():
    valkey = FakeValkeyClient(fail_after=2)
    producer = _producer(valkey=valkey)
    producer.set_valkey_enabled(True)
    producer.set_batch_size(5)

    # Two tasks land before the third raises; the count reflects what landed.
    assert await producer.emit() == 2
    assert producer.emitted == 2
    assert producer.last_error is not None


@pytest.mark.asyncio
async def test_timeout_is_applied_to_the_task():
    valkey = FakeValkeyClient()
    producer = _producer(valkey=valkey)
    producer.set_valkey_enabled(True)
    producer.set_timeout_ms(2500)

    await producer.emit()
    task = decode_task_envelope(valkey.calls[0][1][0][1])
    assert task is not None
    assert task.timeout.timeout == 2.5
    assert task.timeout.timeout_action == "requeue"


@pytest.mark.asyncio
async def test_one_broker_failure_does_not_block_the_other():
    valkey = FakeValkeyClient(fail=True)
    mqtt = FakeMqttClient()
    producer = _producer(valkey=valkey, mqtt=mqtt)
    producer.set_valkey_enabled(True)
    producer.set_mqtt_enabled(True)

    assert await producer.emit() == 1
    assert len(mqtt.calls) == 1
    assert producer.last_error is not None
    assert "valkey" in producer.last_error


@pytest.mark.asyncio
async def test_failed_client_is_dropped_for_reconnect():
    valkey = FakeValkeyClient(fail=True)
    producer = _producer(valkey=valkey)
    producer.set_valkey_enabled(True)

    await producer.emit()
    assert producer._valkey_client is None


def test_interval_and_timeout_are_clamped():
    producer = _producer()
    producer.set_interval_ms(0)
    assert producer._interval_ms == MIN_INTERVAL_MS
    producer.set_interval_ms(10_000_000)
    assert producer._interval_ms == MAX_INTERVAL_MS
    producer.set_timeout_ms(0)
    assert producer._timeout_ms == 1


def test_batch_size_is_clamped():
    producer = _producer()
    producer.set_batch_size(0)
    assert producer.batch_size == MIN_BATCH_SIZE
    producer.set_batch_size(10_000_000)
    assert producer.batch_size == MAX_BATCH_SIZE


def test_defaults():
    producer = _producer()
    assert producer._interval_ms == DEFAULT_INTERVAL_MS
    assert producer._timeout_ms == DEFAULT_TIMEOUT_MS
    assert producer.batch_size == DEFAULT_BATCH_SIZE
    assert producer.running is False


@pytest.mark.asyncio
async def test_run_loop_emits_on_the_interval():
    valkey = FakeValkeyClient()
    producer = _producer(valkey=valkey)
    producer.set_valkey_enabled(True)
    producer.set_interval_ms(20)

    producer.start()
    assert producer.running is True
    await asyncio.sleep(0.12)
    await producer.stop()

    assert producer.running is False
    assert producer.emitted >= 2


@pytest.mark.asyncio
async def test_stop_closes_both_clients():
    valkey = FakeValkeyClient()
    mqtt = FakeMqttClient()
    producer = _producer(valkey=valkey, mqtt=mqtt)

    await producer.stop()
    assert valkey.closed is True
    assert mqtt.exited is True


@pytest.mark.asyncio
async def test_stop_is_idempotent():
    producer = _producer()
    await producer.stop()
    await producer.stop()
    assert producer.running is False


@pytest.mark.asyncio
async def test_snapshot_reflects_state():
    producer = _producer()
    producer.set_valkey_enabled(True)
    snapshot = producer.snapshot()
    assert snapshot.valkey_enabled is True
    assert snapshot.mqtt_enabled is False
    assert snapshot.running is False
    assert snapshot.emitted == 0
