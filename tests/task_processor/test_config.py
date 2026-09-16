"""TaskProcessor construction and auto-tune configuration tests."""

import os

from scietex.service.config import DEFAULT_MAX_CONCURRENT_TASKS, TaskProcessorConfig
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
