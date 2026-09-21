"""Task handler subsystem for ``scietex.service``.

Provides the abstract ``TaskHandler`` base class, typed schemas
(``TaskData``, ``TaskResult``, ``TaskTimeout``, ``TaskStatus``,
``TaskEnvelope``) that define the contract for processing async tasks in the
service, an in-memory ``TaskTracker`` runtime handle, the built-in
``CancelTaskHandler`` for the ``task:cancel`` task name, the built-in
``WorkerControlHandler`` for the ``worker:*`` task names, and the versioned
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
    CANCEL_TASK_NAME,
    CONFIG_APPLY_TASK_NAME,
    CONFIG_SHOW_TASK_NAME,
    CONFIG_STORE_TASK_NAME,
    CONTROL_TASK_NAMES,
    WORKER_EXIT_TASK_NAME,
    WORKER_RESTART_TASK_NAME,
    WORKER_START_TASK_NAME,
    WORKER_STOP_TASK_NAME,
    CancelReason,
    TaskData,
    TaskEnvelope,
    TaskProgress,
    TaskResult,
    TaskStatus,
    TaskTimeout,
)
from .wire import decode_task_envelope, decode_task_envelope_version, encode_task_envelope
from .worker import (
    WorkerAction,
    WorkerActionCallback,
    WorkerControlHandler,
    WorkerControlRequest,
    WorkerControlResponse,
)

__all__ = [
    "CANCEL_TASK_NAME",
    "CONFIG_APPLY_TASK_NAME",
    "CONFIG_SHOW_TASK_NAME",
    "CONFIG_STORE_TASK_NAME",
    "CONTROL_TASK_NAMES",
    "WORKER_EXIT_TASK_NAME",
    "WORKER_RESTART_TASK_NAME",
    "WORKER_START_TASK_NAME",
    "WORKER_STOP_TASK_NAME",
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
    "WorkerAction",
    "WorkerActionCallback",
    "WorkerControlHandler",
    "WorkerControlRequest",
    "WorkerControlResponse",
    "decode_task_envelope",
    "decode_task_envelope_version",
    "encode_task_envelope",
]
