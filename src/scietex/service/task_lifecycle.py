"""Per-task lifecycle state: the running tracker and its cancel reason."""

from collections.abc import Mapping
from uuid import UUID

from .task_handler.runtime import TaskTracker
from .task_handler.schemas import CancelReason


class TaskLifecycle:
    """Owns the per-task running record (tracker) and its cancel reason.

    Extracted from ``TaskProcessor`` (AR-088). The tracker map and the cancel
    reason map have joined lifetimes but are popped independently: the watchdog
    removes a tracker for a task whose handler ignored cancellation while
    leaving the reason for the eventual ack, so ``remove_tracker`` and
    ``take_cancel_reason`` are deliberately separate operations.
    """

    def __init__(self) -> None:
        self._trackers: dict[UUID, TaskTracker] = {}
        self._cancel_reasons: dict[UUID, CancelReason] = {}

    def register(self, task_id: UUID, tracker: TaskTracker) -> None:
        """Record a dispatched task's tracker."""
        self._trackers[task_id] = tracker

    def trackers(self) -> Mapping[UUID, TaskTracker]:
        """Return a snapshot of the running trackers.

        A snapshot (not a live view) so callers may iterate while cancelling
        tasks inside the loop.
        """
        return dict(self._trackers)

    def get(self, task_id: UUID) -> TaskTracker | None:
        """Return the tracker for ``task_id``, or ``None`` if not running."""
        return self._trackers.get(task_id)

    def mark_cancelled(self, task_id: UUID, reason: CancelReason) -> None:
        """Record why ``task_id`` is being cancelled.

        Plain overwrite (last writer wins), matching the previous inline
        ``dict[key] = reason`` behaviour.
        """
        self._cancel_reasons[task_id] = reason

    def remove_tracker(self, task_id: UUID) -> TaskTracker | None:
        """Drop the tracker for ``task_id`` without consuming its cancel reason."""
        return self._trackers.pop(task_id, None)

    def take_cancel_reason(self, task_id: UUID) -> CancelReason | None:
        """Consume and return the cancel reason for ``task_id``."""
        return self._cancel_reasons.pop(task_id, None)
