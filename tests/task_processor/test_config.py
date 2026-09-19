"""TaskProcessor construction and auto-tune configuration tests."""

import os
from typing import cast

import pytest

from scietex.service.config import (
    DEFAULT_MANAGER_SLEEP_TIME,
    DEFAULT_MAX_CONCURRENT_TASKS,
    DEFAULT_MAX_TIMEOUT_REQUEUES,
    DEFAULT_TASK_CANCELLATION_TIMEOUT,
    DEFAULT_TASK_HANDLER_START_TIMEOUT,
    DEFAULT_TASK_HANDLER_STOP_TIMEOUT,
    DEFAULT_TASK_QUEUE_FETCH_TIMEOUT,
    DEFAULT_TASK_TIMEOUT,
    TaskProcessorConfig,
)
from scietex.service.config_reload import ConfigSections, ReloadableSettings, encode_config_envelope
from scietex.service.task_processor import TaskProcessor

from ._helpers import DemoProcessor


def test_default_config_stored_as_concrete_type():
    """When constructed with ``config=None`` the base must instantiate the
    concrete ``TaskProcessorConfig`` (not a bare ``WorkerConfig``), because the
    processor declares ``_config_type`` (AR-069)."""
    proc = TaskProcessor()
    assert isinstance(proc._config, TaskProcessorConfig)


def test_auto_tune_uses_cpu_count_when_max_concurrent_is_none():
    """auto_tune=True with max_concurrent_tasks=None derives the concurrency
    from the CPU count at startup."""
    proc = DemoProcessor(TaskProcessorConfig(auto_tune=True))
    assert proc.max_concurrent_tasks == max(1, os.cpu_count() or 1)


def test_auto_tune_explicit_max_concurrent_wins():
    """An explicit max_concurrent_tasks always wins over auto_tune."""
    proc = DemoProcessor(TaskProcessorConfig(auto_tune=True, max_concurrent_tasks=3))
    assert proc.max_concurrent_tasks == 3


def test_auto_tune_default_off_uses_static_default():
    """auto_tune=False (default) with max_concurrent_tasks=None resolves to the
    static DEFAULT_MAX_CONCURRENT_TASKS."""
    proc = DemoProcessor(TaskProcessorConfig())
    assert proc.max_concurrent_tasks == DEFAULT_MAX_CONCURRENT_TASKS


def test_effective_resolves_defaults_while_config_stays_raw():
    """A fully-default construction resolves ``_effective`` to the DEFAULT_*
    constants while ``_config`` keeps its declarative ``None`` fields (AR-100)."""
    proc = TaskProcessor()
    assert proc._effective == ReloadableSettings(
        max_concurrent_tasks=DEFAULT_MAX_CONCURRENT_TASKS,
        task_manager_sleep_time=DEFAULT_MANAGER_SLEEP_TIME,
        task_queue_manager_sleep_time=DEFAULT_MANAGER_SLEEP_TIME,
        task_handler_start_timeout=DEFAULT_TASK_HANDLER_START_TIMEOUT,
        task_handler_stop_timeout=DEFAULT_TASK_HANDLER_STOP_TIMEOUT,
        task_timeout=DEFAULT_TASK_TIMEOUT,
        task_queue_fetch_timeout=DEFAULT_TASK_QUEUE_FETCH_TIMEOUT,
        task_cancellation_timeout=DEFAULT_TASK_CANCELLATION_TIMEOUT,
    )
    assert cast(TaskProcessorConfig, proc._config).task_timeout is None


def test_reloadable_fields_have_no_shadow_attributes():
    """The four private reloadable shadows are gone: only ``_effective`` holds
    the resolved values (AR-100)."""
    proc = TaskProcessor()
    assert not hasattr(proc, "_TaskProcessor__max_concurrent_tasks")
    assert not hasattr(proc, "_TaskProcessor__task_timeout")
    assert not hasattr(proc, "_TaskProcessor__task_queue_fetch_timeout")
    assert not hasattr(proc, "_TaskProcessor__task_cancellation_timeout")


def test_default_and_explicit_max_timeout_requeues():
    """The default config resolves the property to DEFAULT_MAX_TIMEOUT_REQUEUES;
    an explicit value is respected (AR-104)."""
    assert TaskProcessor().max_timeout_requeues == DEFAULT_MAX_TIMEOUT_REQUEUES
    assert DemoProcessor(TaskProcessorConfig(max_timeout_requeues=7)).max_timeout_requeues == 7


@pytest.mark.asyncio
async def test_initialize_resets_config_replay_state():
    """A second ``initialize()`` clears the apply bookkeeping left by a previous
    start cycle, so a fresh run starts from revision 0 / ``default`` source and
    the revision-1 local snapshot is not rejected as stale (AR-111)."""
    proc = TaskProcessor(TaskProcessorConfig(remote_config_enabled=True))
    envelope = encode_config_envelope(
        ConfigSections(
            core=ReloadableSettings(
                max_concurrent_tasks=DEFAULT_MAX_CONCURRENT_TASKS,
                task_manager_sleep_time=DEFAULT_MANAGER_SLEEP_TIME,
                task_queue_manager_sleep_time=DEFAULT_MANAGER_SLEEP_TIME,
                task_handler_start_timeout=DEFAULT_TASK_HANDLER_START_TIMEOUT,
                task_handler_stop_timeout=DEFAULT_TASK_HANDLER_STOP_TIMEOUT,
                task_timeout=DEFAULT_TASK_TIMEOUT,
                task_queue_fetch_timeout=DEFAULT_TASK_QUEUE_FETCH_TIMEOUT,
                task_cancellation_timeout=DEFAULT_TASK_CANCELLATION_TIMEOUT,
            )
        ),
        revision=5,
    )

    outcome = await proc._config_manager.apply_envelope(envelope, source="remote")
    assert outcome.applied is True
    assert proc.config_revision == 5
    assert proc.config_source == "remote"

    assert await proc.initialize() is True

    assert proc.config_revision == 0
    assert proc.config_source == "default"
