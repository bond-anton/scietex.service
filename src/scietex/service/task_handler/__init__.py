"""Task handler subsystem for ``scietex.service``.

Provides the abstract ``TaskHandler`` base class, typed schemas
(``TaskData``, ``TaskResult``, ``TaskTimeout``, ``TaskStatus``,
``TaskEnvelope``) that define the contract for processing async tasks in the
service, an in-memory ``TaskTracker`` runtime handle, the built-in
``CancelTaskHandler`` for the ``cancel_task`` task type, and the versioned
transport wire helpers
(``encode_task_envelope``/``decode_task_envelope``).
"""

from .basic import TaskHandler
from .cancel import (
    CancelCallback,
    CancelOutcome,
    CancelTaskHandler,
    CancelTaskRequest,
    CancelTaskResponse,
)
from .capabilities import TaskCapabilities
from .config import (
    ConfigApplyCallback,
    ConfigApplyHandler,
    ConfigApplyRequest,
    ConfigApplyResponse,
    ConfigShowCallback,
    ConfigShowHandler,
    ConfigShowRequest,
    ConfigShowResponse,
    ConfigSourceLabel,
    ConfigStoreCallback,
    ConfigStoreHandler,
    ConfigStoreRequest,
    ConfigStoreResponse,
)
from .context import TaskHandlerContext
from .runtime import TaskTracker
from .schemas import (
    CANCEL_TASK_TYPE,
    CONFIG_APPLY_TASK_TYPE,
    CONFIG_SHOW_TASK_TYPE,
    CONFIG_STORE_TASK_TYPE,
    CONTROL_TASK_TYPES,
    CancelReason,
    TaskData,
    TaskEnvelope,
    TaskProgress,
    TaskResult,
    TaskStatus,
    TaskTimeout,
)
from .wire import decode_task_envelope, decode_task_envelope_version, encode_task_envelope

__all__ = [
    "CANCEL_TASK_TYPE",
    "CONFIG_APPLY_TASK_TYPE",
    "CONFIG_SHOW_TASK_TYPE",
    "CONFIG_STORE_TASK_TYPE",
    "CONTROL_TASK_TYPES",
    "CancelCallback",
    "CancelOutcome",
    "CancelReason",
    "CancelTaskHandler",
    "CancelTaskRequest",
    "CancelTaskResponse",
    "ConfigApplyCallback",
    "ConfigApplyHandler",
    "ConfigApplyRequest",
    "ConfigApplyResponse",
    "ConfigShowCallback",
    "ConfigShowHandler",
    "ConfigShowRequest",
    "ConfigShowResponse",
    "ConfigSourceLabel",
    "ConfigStoreCallback",
    "ConfigStoreHandler",
    "ConfigStoreRequest",
    "ConfigStoreResponse",
    "TaskCapabilities",
    "TaskData",
    "TaskEnvelope",
    "TaskHandler",
    "TaskHandlerContext",
    "TaskProgress",
    "TaskResult",
    "TaskStatus",
    "TaskTimeout",
    "TaskTracker",
    "decode_task_envelope",
    "decode_task_envelope_version",
    "encode_task_envelope",
]
