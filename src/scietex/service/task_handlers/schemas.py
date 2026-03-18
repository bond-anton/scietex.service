"""Defines types used in task handlers.

This module exposes a lightweight `TaskType` helper (existing) and
TypedDict definitions for task payloads and results so handlers and
processors can use consistent typing for `task_data` and returned
results.
"""

from typing import TypeVar, Literal
from enum import Enum
from datetime import datetime, timezone

import msgspec

TaskType = TypeVar("TaskType", bound=Enum)


class TaskTimeout(msgspec.Struct, frozen=True):
    timeout: float | None = None
    timeout_action: Literal["requeue", "drop", "discard"] = "requeue"


class TaskData(msgspec.Struct, frozen=True):
    """Minimal required fields for task data."""

    # The task identifier/type string used to select a handler.
    task: str
    timeout: TaskTimeout = TaskTimeout(timeout=None, timeout_action="requeue")
    payload: bytes = b""


class TaskResult(msgspec.Struct, frozen=True):
    """Standardized result structure returned from handlers.

    Fields are optional to allow handlers to return custom payloads,
    but `status` and `error` are common enough to standardize.
    """

    status: Literal["success", "error"]
    error: str
    processed_at: datetime = datetime.now(timezone.utc)
    payload: bytes = b""
