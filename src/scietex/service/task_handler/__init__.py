"""Task handler subsystem for ``scietex.service``.

Provides the abstract ``TaskHandler`` base class, typed schemas
(``TaskData``, ``TaskResult``, ``TaskTimeout``, ``TaskTracker``,
``TaskEnvelope``) that define the contract for processing async tasks in the
service, and the versioned transport wire helpers
(``encode_task_envelope``/``decode_task_envelope``).
"""

from .basic import TaskHandler
from .context import TaskHandlerContext
from .schemas import TaskData, TaskEnvelope, TaskResult, TaskTimeout, TaskTracker
from .wire import decode_task_envelope, encode_task_envelope

__all__ = [
    "TaskData",
    "TaskEnvelope",
    "TaskHandler",
    "TaskHandlerContext",
    "TaskResult",
    "TaskTimeout",
    "TaskTracker",
    "decode_task_envelope",
    "encode_task_envelope",
]
