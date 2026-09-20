"""Pin the runtime handle's data model (AR-109)."""

import dataclasses

import msgspec
import pytest

from scietex.service.task_handler.runtime import TaskTracker


def test_task_tracker_is_frozen_dataclass():
    assert dataclasses.is_dataclass(TaskTracker)
    assert [f.name for f in dataclasses.fields(TaskTracker)] == ["worker_task", "data", "started"]


def test_task_tracker_is_frozen():
    tracker = TaskTracker(worker_task=None, data=None, started=0.0)  # type: ignore[arg-type]
    with pytest.raises(dataclasses.FrozenInstanceError):
        tracker.started = 1.0  # type: ignore[misc]


def test_task_tracker_is_not_msgspec_struct():
    assert not issubclass(TaskTracker, msgspec.Struct)
