"""Task handler subsystem for ``scietex.service``.

Provides the abstract ``TaskHandler`` base class, typed schemas
(``TaskData``, ``TaskResult``, ``TaskTimeout``, ``TaskTracker``), and
the ``task_type`` TypeVar used for handler registration.
"""

from .basic import TaskHandler
from .schemas import TaskData, TaskResult, TaskTimeout, TaskTracker, task_type

__all__ = ["TaskHandler", "task_type", "TaskTimeout", "TaskData", "TaskResult", "TaskTracker"]
