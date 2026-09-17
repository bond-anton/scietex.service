"""Tests for the ``TaskLifecycle`` per-task state component (AR-088)."""

import asyncio
import time
from typing import cast
from uuid import uuid4

from scietex.service.task_handler.runtime import TaskTracker
from scietex.service.task_handler.schemas import TaskData
from scietex.service.task_lifecycle import TaskLifecycle


def _make_tracker() -> TaskTracker:
    """Build a minimal tracker with a placeholder worker task.

    ``TaskLifecycle`` only stores and returns trackers; it never awaits
    ``worker_task``, so a cast-ed placeholder keeps these tests synchronous
    (no event loop required).
    """
    return TaskTracker(
        worker_task=cast(asyncio.Task, None),
        data=TaskData(task="dummy"),
        started=time.monotonic(),
    )


def test_register_and_get_round_trip():
    """register() then get() returns the same tracker object."""
    lifecycle = TaskLifecycle()
    tracker = _make_tracker()
    task_id = uuid4()

    lifecycle.register(task_id, tracker)

    assert lifecycle.get(task_id) is tracker


def test_get_missing_returns_none():
    """get() on an unknown task id returns None."""
    lifecycle = TaskLifecycle()

    assert lifecycle.get(uuid4()) is None


def test_trackers_returns_snapshot():
    """trackers() is a snapshot, not a live view of the running map."""
    lifecycle = TaskLifecycle()
    task_id = uuid4()
    tracker = _make_tracker()
    lifecycle.register(task_id, tracker)

    snapshot = lifecycle.trackers()
    lifecycle.remove_tracker(task_id)

    assert snapshot == {task_id: tracker}
    assert lifecycle.get(task_id) is None


def test_trackers_reflects_registrations():
    """trackers() reflects all registrations."""
    lifecycle = TaskLifecycle()
    first = _make_tracker()
    second = _make_tracker()
    first_id = uuid4()
    second_id = uuid4()

    lifecycle.register(first_id, first)
    lifecycle.register(second_id, second)

    assert lifecycle.trackers() == {first_id: first, second_id: second}


def test_mark_cancelled_and_take_cancel_reason_round_trip():
    """mark_cancelled() then take_cancel_reason() returns the recorded reason."""
    lifecycle = TaskLifecycle()
    task_id = uuid4()

    lifecycle.mark_cancelled(task_id, "timeout")

    assert lifecycle.take_cancel_reason(task_id) == "timeout"


def test_take_cancel_reason_missing_returns_none():
    """take_cancel_reason() on an unknown task id returns None."""
    lifecycle = TaskLifecycle()

    assert lifecycle.take_cancel_reason(uuid4()) is None


def test_remove_tracker_does_not_consume_cancel_reason():
    """remove_tracker() leaves the cancel reason for the eventual ack."""
    lifecycle = TaskLifecycle()
    task_id = uuid4()
    tracker = _make_tracker()
    lifecycle.register(task_id, tracker)
    lifecycle.mark_cancelled(task_id, "shutdown")

    removed = lifecycle.remove_tracker(task_id)

    assert removed is tracker
    assert lifecycle.take_cancel_reason(task_id) == "shutdown"


def test_remove_tracker_missing_returns_none():
    """remove_tracker() on an unknown task id returns None."""
    lifecycle = TaskLifecycle()

    assert lifecycle.remove_tracker(uuid4()) is None
