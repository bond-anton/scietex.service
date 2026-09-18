"""Tests for ``MqttConfigSource`` and the MqttWorker startup config read.

The source unit tests drive ``MqttConfigSource`` directly against a recording
publish seam; the worker tests exercise ``_handle_message`` topic dispatch and
``initialize()`` through the existing ``FakeClient``/``client_factory=`` fakes
so no live broker is needed.
"""

import asyncio
import logging
from typing import cast
from uuid import uuid4

import pytest

from scietex.service.config import TaskProcessorConfig
from scietex.service.config_reload import (
    ConfigSections,
    ReloadableSettings,
    encode_config_envelope,
    write_local_config,
)
from scietex.service.mqtt.config import MqttConfig, MqttWorkerConfig
from scietex.service.mqtt.config_source import MqttConfigSource
from scietex.service.mqtt.worker import TASK_ID_PROPERTY, MqttWorker
from scietex.service.task_handler.schemas import TaskData
from scietex.service.task_handler.wire import encode_task_envelope

from .test_worker import FakeClient, _FakeMessage, _patch_handler

_CONFIG_TOPIC = "scietex/svc/config"

# A complete, in-bounds snapshot of the eight reloadable core fields.
_CORE_DEFAULTS: dict[str, float | int] = {
    "max_concurrent_tasks": 10,
    "task_manager_sleep_time": 0.1,
    "task_queue_manager_sleep_time": 0.1,
    "task_handler_start_timeout": 10.0,
    "task_handler_stop_timeout": 10.0,
    "task_timeout": 3.0,
    "task_queue_fetch_timeout": 0.5,
    "task_cancellation_timeout": 2.0,
}


def _settings(**overrides) -> ReloadableSettings:
    values = dict(_CORE_DEFAULTS)
    values.update(overrides)
    return ReloadableSettings(**values)


class _RecordingPublish:
    """Recording ``MqttPublish`` seam capturing every publish call."""

    def __init__(self):
        self.calls = []

    async def __call__(self, topic, payload, qos, *, retain=False, properties=None):
        self.calls.append((topic, payload, qos, retain, properties))


def _source(*, qos: int = 1, ttl: int | None = 86400) -> tuple[MqttConfigSource, _RecordingPublish]:
    publish = _RecordingPublish()
    source = MqttConfigSource(
        topic=_CONFIG_TOPIC,
        qos=qos,
        ttl=ttl,
        publish=publish,
        logger=logging.getLogger("test_config_source"),
    )
    return source, publish


def _make_worker(tmp_path, fake, **config_kwargs) -> MqttWorker:
    """Build an MqttWorker rooted at ``tmp_path`` with an injected fake client."""
    cfg_kwargs: dict = {
        "service_name": "svc",
        "remote_config_enabled": True,
        "mqtt_config": MqttConfig(),
        "conf_dir": tmp_path,
        "inbox_backend": "none",
    }
    cfg_kwargs.update(config_kwargs)

    async def factory(cfg):
        return fake

    return MqttWorker(MqttWorkerConfig(**cfg_kwargs), client_factory=factory)


def _plain_worker(tmp_path) -> MqttWorker:
    """Build a worker with a file inbox for direct ``_handle_message`` tests."""
    return MqttWorker(
        MqttWorkerConfig(
            service_name="svc",
            mqtt_config=MqttConfig(),
            inbox_backend="file",
            inbox_path=str(tmp_path / "inbox"),
        )
    )


# --- source unit tests ------------------------------------------------------


@pytest.mark.asyncio
async def test_record_then_load_round_trip():
    """``record`` stores the latest payload; ``load`` returns it."""
    source, _ = _source()

    assert await source.load() is None
    source.record(b"envelope-bytes")
    assert await source.load() == b"envelope-bytes"


@pytest.mark.asyncio
async def test_load_without_snapshot_is_none():
    """``load`` returns ``None`` when no snapshot has arrived yet."""
    source, _ = _source()

    assert await source.load() is None


@pytest.mark.asyncio
async def test_wait_for_snapshot_returns_on_delivery():
    """``wait_for_snapshot`` resolves as soon as ``record`` signals the event."""
    source, _ = _source()

    async def deliver():
        await asyncio.sleep(0.01)
        source.record(b"envelope-bytes")

    task = asyncio.create_task(deliver())
    snapshot = await source.wait_for_snapshot(1.0)
    await task

    assert snapshot == b"envelope-bytes"


@pytest.mark.asyncio
async def test_wait_for_snapshot_times_out_cleanly():
    """A timeout returns ``None`` rather than raising (no retained config)."""
    source, _ = _source()

    assert await source.wait_for_snapshot(0.01) is None


@pytest.mark.asyncio
async def test_store_publishes_retained_with_ttl():
    """``store`` publishes retained at ``qos`` with a message-expiry equal to
    ``ttl`` (mirroring the status_ttl pattern)."""
    source, publish = _source(qos=1, ttl=3600)

    await source.store(b"envelope-bytes")

    topic, payload, qos, retain, properties = publish.calls[0]
    assert topic == _CONFIG_TOPIC
    assert payload == b"envelope-bytes"
    assert qos == 1
    assert retain is True
    assert properties.MessageExpiryInterval == 3600


@pytest.mark.asyncio
async def test_store_without_ttl_has_no_properties():
    """``ttl=None`` publishes retained with no message-expiry properties."""
    source, publish = _source(ttl=None)

    await source.store(b"envelope-bytes")

    _, _, _, retain, properties = publish.calls[0]
    assert retain is True
    assert properties is None


# --- worker _handle_message dispatch tests --------------------------------


@pytest.mark.asyncio
async def test_handle_message_config_topic_records_snapshot(tmp_path, caplog):
    """A config-topic message records the snapshot and is NOT treated as a task
    (no inbox write, no missing-task-id warning)."""
    worker = _plain_worker(tmp_path)
    message = _FakeMessage(b"envelope-bytes", topic=_CONFIG_TOPIC)

    with caplog.at_level(logging.WARNING):
        await worker._handle_message(message)

    assert await worker._mqtt_config_source.load() == b"envelope-bytes"
    assert await worker._inbox.pending() == []
    assert not any("without a scietex-task-id" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_handle_message_task_topic_still_routes(tmp_path):
    """A task-topic message still follows the existing task path (persist to
    inbox) despite the new topic dispatch."""
    worker = _plain_worker(tmp_path)
    task_id = uuid4()
    task_data = TaskData(task="send_email", payload=b'{"to":"a@b.c"}')
    message = _FakeMessage(encode_task_envelope(task_data), [(TASK_ID_PROPERTY, str(task_id))])

    await worker._handle_message(message)

    assert await worker._inbox.pending() == [(task_id, task_data)]


# --- worker startup tests ---------------------------------------------------


@pytest.mark.asyncio
async def test_initialize_applies_remote_config(monkeypatch, tmp_path):
    """A retained config snapshot delivered on the config topic is applied on
    startup with source ``"remote"``."""
    _patch_handler(monkeypatch)
    fake = FakeClient()
    envelope = encode_config_envelope(
        ConfigSections(core=_settings(task_timeout=7.0)),
        revision=5,
    )
    fake.feed(_FakeMessage(envelope, topic=_CONFIG_TOPIC))
    worker = _make_worker(tmp_path, fake)

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_revision == 5
    assert worker.config_source == "remote"
    assert cast(TaskProcessorConfig, worker._config).task_timeout == 7.0

    await worker._stop_message_loop()
    await worker.disconnect()


@pytest.mark.asyncio
async def test_initialize_no_snapshot_times_out_and_succeeds(monkeypatch, tmp_path):
    """No retained snapshot times out cleanly and startup succeeds with the
    default config."""
    _patch_handler(monkeypatch)
    fake = FakeClient()
    worker = _make_worker(tmp_path, fake, config_startup_timeout=0.05)

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_revision == 0
    assert worker.config_source == "default"
    assert cast(TaskProcessorConfig, worker._config).task_timeout is None

    await worker._stop_message_loop()
    await worker.disconnect()


@pytest.mark.asyncio
async def test_initialize_invalid_remote_does_not_fail(monkeypatch, tmp_path):
    """An invalid remote snapshot does not fail startup; the default config is
    left in place (availability-first, design §5)."""
    _patch_handler(monkeypatch)
    fake = FakeClient()
    fake.feed(_FakeMessage(b"not-a-valid-envelope", topic=_CONFIG_TOPIC))
    worker = _make_worker(tmp_path, fake)

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_revision == 0
    assert worker.config_source == "default"
    assert cast(TaskProcessorConfig, worker._config).task_timeout is None

    await worker._stop_message_loop()
    await worker.disconnect()


@pytest.mark.asyncio
async def test_initialize_disabled_ignores_local_config_without_error(monkeypatch, tmp_path, caplog):
    """With remote config disabled (the default), a present ``config.yml`` is
    ignored and must not log an ERROR (a disabled feature is not a failure)."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=_settings(task_timeout=9.0)))
    _patch_handler(monkeypatch)
    fake = FakeClient()
    worker = _make_worker(tmp_path, fake, remote_config_enabled=False, config_startup_timeout=0.05)

    with caplog.at_level(logging.ERROR):
        ok = await worker.initialize()

    assert ok is True
    assert worker.config_source == "default"
    assert worker.config_revision == 0
    assert cast(TaskProcessorConfig, worker._config).task_timeout is None
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    await worker._stop_message_loop()
    await worker.disconnect()
