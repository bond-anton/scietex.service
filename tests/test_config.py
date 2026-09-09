"""Tests for the typed worker configuration objects (AR-046)."""

import logging
from pathlib import Path

import msgspec
import pytest

from scietex.service.config import TaskProcessorConfig, WorkerConfig
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig


def test_worker_config_defaults_are_none():
    """A default WorkerConfig leaves all timing/retry fields None (the worker
    resolves them to the DEFAULT constants at read time)."""
    cfg = WorkerConfig()
    assert cfg.service_name == "service"
    assert cfg.version == "0.0.1"
    assert cfg.conf_dir is None
    assert cfg.logging_level == logging.DEBUG
    assert cfg.heartbeat_interval is None
    assert cfg.watchdog_interval is None
    assert cfg.logger_handler_timeout is None
    assert cfg.manager_shutdown_timeout is None
    assert cfg.manager_max_retries is None
    assert cfg.manager_restart_backoff is None


def test_worker_config_accepts_conf_dir_path():
    """conf_dir accepts a pathlib.Path at construction (msgspec stores it as an
    opaque custom type)."""
    cfg = WorkerConfig(conf_dir=Path("/tmp"))
    assert cfg.conf_dir == Path("/tmp")


def test_worker_config_in_range_values_accepted():
    cfg = WorkerConfig(
        heartbeat_interval=5,
        watchdog_interval=2,
        logger_handler_timeout=3,
        manager_shutdown_timeout=4,
        manager_max_retries=10,
        manager_restart_backoff=0.5,
    )
    assert cfg.heartbeat_interval == 5
    assert cfg.manager_max_retries == 10


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("heartbeat_interval", 0.0),
        ("heartbeat_interval", 601),
        ("watchdog_interval", 0.0),
        ("watchdog_interval", 601),
        ("logger_handler_timeout", 0.5),
        ("logger_handler_timeout", 11),
        ("manager_shutdown_timeout", 0.5),
        ("manager_shutdown_timeout", 11),
        ("manager_max_retries", -1),
        ("manager_max_retries", 101),
        ("manager_restart_backoff", -1),
        ("manager_restart_backoff", 61),
    ],
)
def test_worker_config_out_of_range_raises(field_name, value):
    with pytest.raises(msgspec.ValidationError):
        WorkerConfig(**{field_name: value})


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("max_concurrent_tasks", 0),
        ("task_manager_sleep_time", 0.0001),
        ("task_manager_sleep_time", 2),
        ("task_queue_manager_sleep_time", 0.0001),
        ("task_queue_manager_sleep_time", 2),
        ("task_handler_start_timeout", 0),
        ("task_handler_start_timeout", 61),
        ("task_handler_stop_timeout", 0),
        ("task_handler_stop_timeout", 61),
    ],
)
def test_task_processor_config_out_of_range_raises(field_name, value):
    with pytest.raises(msgspec.ValidationError):
        TaskProcessorConfig(**{field_name: value})


def test_task_processor_config_in_range_values_accepted():
    cfg = TaskProcessorConfig(
        queue_size=10,
        max_concurrent_tasks=5,
        task_manager_sleep_time=0.5,
        task_queue_manager_sleep_time=0.5,
        task_handler_start_timeout=30,
        task_handler_stop_timeout=30,
    )
    assert cfg.queue_size == 10
    assert cfg.max_concurrent_tasks == 5


def test_task_processor_config_timing_fields_default_none():
    """The task-level timing fields default to None (resolved by the worker at
    read time to their DEFAULT_* constants)."""
    cfg = TaskProcessorConfig()
    assert cfg.task_timeout is None
    assert cfg.task_queue_fetch_timeout is None
    assert cfg.task_cancellation_timeout is None


def test_task_processor_config_timing_fields_in_range_accepted():
    cfg = TaskProcessorConfig(
        task_timeout=10,
        task_queue_fetch_timeout=0.5,
        task_cancellation_timeout=2,
    )
    assert cfg.task_timeout == 10
    assert cfg.task_queue_fetch_timeout == 0.5
    assert cfg.task_cancellation_timeout == 2


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        # task_timeout: below the positive min (0.1) but above the unbounded
        # sentinel (<= 0) is invalid; above the max is invalid.
        ("task_timeout", 0.05),
        ("task_timeout", 3601),
        ("task_queue_fetch_timeout", 0.005),
        ("task_queue_fetch_timeout", 61),
        ("task_cancellation_timeout", 0.05),
        ("task_cancellation_timeout", 61),
    ],
)
def test_task_processor_config_timing_fields_out_of_range_raises(field_name, value):
    with pytest.raises(msgspec.ValidationError):
        TaskProcessorConfig(**{field_name: value})


@pytest.mark.parametrize("value", [0, -1, -5])
def test_task_timeout_unbounded_sentinel_accepted(value):
    """task_timeout <= 0 is the unbounded sentinel (watchdog never cancels) and
    must pass validation instead of being rejected by the [0.1, 3600] bound."""
    cfg = TaskProcessorConfig(task_timeout=value)
    assert cfg.task_timeout == value


def test_config_is_immutable():
    cfg = WorkerConfig()
    with pytest.raises(AttributeError):
        setattr(cfg, "heartbeat_interval", 5)


def test_valkey_worker_config_task_fetch_batch_size_raises_below_one():
    with pytest.raises(msgspec.ValidationError):
        ValkeyWorkerConfig(valkey_config=ValkeyConfig(), task_fetch_batch_size=0)


def test_valkey_worker_config_defaults():
    cfg = ValkeyWorkerConfig()
    assert cfg.valkey_config is None
    assert cfg.log_stream_name == "scietex:log"
    assert cfg.task_fetch_batch_size == 10
    assert cfg.claim_min_idle_ms is None


def test_valkey_worker_config_claim_min_idle_ms_in_range_accepted():
    cfg = ValkeyWorkerConfig(valkey_config=ValkeyConfig(), claim_min_idle_ms=5000)
    assert cfg.claim_min_idle_ms == 5000


@pytest.mark.parametrize("value", [0, 3_600_001])
def test_valkey_worker_config_claim_min_idle_ms_out_of_range_raises(value):
    with pytest.raises(msgspec.ValidationError):
        ValkeyWorkerConfig(valkey_config=ValkeyConfig(), claim_min_idle_ms=value)
