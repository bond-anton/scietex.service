"""Task handler subsystem for ``scietex.service``.

Provides the abstract ``TaskHandler`` base class and typed schemas
(``TaskData``, ``TaskResult``, ``TaskTimeout``, ``TaskTracker``)
that define the contract for processing async tasks in the service.
"""

from .basic import TaskHandler
from .context import TaskHandlerContext
from .schemas import TaskData, TaskResult, TaskTimeout, TaskTracker

__all__ = [
    "TaskData",
    "TaskHandler",
    "TaskHandlerContext",
    "TaskResult",
    "TaskTimeout",
    "TaskTracker",
]
