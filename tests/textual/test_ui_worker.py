"""Tests for the UI worker factory's memory-inbox selection.

``build_ui_worker`` translates the ``memory`` flag into an explicit
``MqttWorkerConfig(inbox_backend="memory")`` for the MQTT worker only; the
Valkey worker ignores it. The tests skip when the ``mqtt`` extra is absent.
"""

import pytest

from examples.textual.ui_worker import build_ui_worker
from scietex.service import MQTT_AVAILABLE, ScietexDark


@pytest.mark.skipif(not MQTT_AVAILABLE, reason="mqtt extra not installed")
def test_memory_true_builds_in_memory_inbox_worker(monkeypatch, tmp_path):
    # Redirect the config dir so the default sqlite path never touches the real
    # ~/.config/scietex even if a durable inbox were built here.
    monkeypatch.setenv("SCIETEX_CONFIG_DIR", str(tmp_path))
    worker = build_ui_worker("mqtt", theme=ScietexDark(show_banner=False), memory=True)
    assert worker._config.inbox_backend == "memory"


@pytest.mark.skipif(not MQTT_AVAILABLE, reason="mqtt extra not installed")
def test_memory_false_builds_default_sqlite_worker(monkeypatch, tmp_path):
    # The default path constructs the durable SQLite inbox, so point the config
    # dir at tmp_path to keep the test hermetic.
    monkeypatch.setenv("SCIETEX_CONFIG_DIR", str(tmp_path))
    worker = build_ui_worker("mqtt", theme=ScietexDark(show_banner=False), memory=False)
    assert worker._config.inbox_backend == "sqlite"
