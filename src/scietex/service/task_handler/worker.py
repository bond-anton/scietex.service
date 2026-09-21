"""Built-in handler for the ``worker:*`` control task names."""

from collections.abc import Awaitable, Callable
from typing import ClassVar, Literal

import msgspec

from .basic import TaskHandler
from .capabilities import TaskCapabilities
from .context import TaskHandlerContext
from .schemas import (
    WORKER_EXIT_TASK_NAME,
    WORKER_RESTART_TASK_NAME,
    WORKER_START_TASK_NAME,
    WORKER_STOP_TASK_NAME,
    TaskData,
    TaskResult,
)

#: Lifecycle action a ``worker:*`` command requests.
WorkerAction = Literal["start", "stop", "restart", "exit"]

#: Async callback injected by the processor to perform a lifecycle action.
#: The callback schedules the transition and returns without awaiting it.
WorkerActionCallback = Callable[[], Awaitable[None]]


class WorkerControlRequest(msgspec.Struct, frozen=True):
    """Payload of a ``worker:*`` task.

    Args:
        reason: Optional operator note, for logging/audit only.
    """

    reason: str = ""


class WorkerControlResponse(msgspec.Struct, frozen=True):
    """Payload returned by a successful ``worker:*`` task.

    Args:
        action: The :data:`WorkerAction` that was accepted.
        accepted: Always ``True`` on a success result; the transition runs in
            the background and is not awaited by the handler.
    """

    action: str
    accepted: bool = True


class WorkerControlHandler(TaskHandler):
    """Handler for the built-in ``worker:*`` control task names.

    Decodes a :class:`WorkerControlRequest` and calls the injected callback for
    the requested action. A stop, restart, or exit targets the very worker
    executing the handler, so the callbacks schedule the transition as a
    background task and return immediately: the handler acks the command with a
    ``TaskResult`` reporting *acceptance*, not completion, before shutdown
    begins. A malformed payload yields a non-retryable error result rather
    than raising, so a bad request never crashes the task loop.
    """

    #: Worker lifecycle commands are control-plane: they always arrive on a
    #: control channel and are served from the control registry, never the data
    #: registry.
    control: ClassVar[bool] = True

    def __init__(
        self,
        name: str,
        context: TaskHandlerContext,
        *,
        start: WorkerActionCallback,
        stop: WorkerActionCallback,
        restart: WorkerActionCallback,
        exit: WorkerActionCallback,
    ) -> None:
        """Initialize the handler.

        Args:
            name: Human-readable name for this handler instance.
            context: Narrow context provided by the owning processor.
            start: Callback that starts the worker.
            stop: Callback that stops the worker.
            restart: Callback that stops and then starts the worker.
            exit: Callback that requests worker exit.
        """
        super().__init__(name, context)
        self._actions: dict[str, WorkerActionCallback] = {
            WORKER_START_TASK_NAME: start,
            WORKER_STOP_TASK_NAME: stop,
            WORKER_RESTART_TASK_NAME: restart,
            WORKER_EXIT_TASK_NAME: exit,
        }

    @property
    def supported_tasks(self) -> list[str]:
        """Task names handled by this handler."""
        return list(self._actions)

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        """Perform the lifecycle action named by the task.

        Args:
            task_data: Task data whose ``task`` selects the action and whose
                ``payload`` is a msgpack-encoded :class:`WorkerControlRequest`.
            capabilities: Keyword-only per-call capabilities (unused here; the
                handler only needs the injected action callbacks).

        Returns:
            A ``TaskResult``: ``success`` with a msgpack-encoded
            :class:`WorkerControlResponse` when the action was accepted, or a
            non-retryable ``error`` for an unknown action or malformed payload.
        """
        action = self._actions.get(task_data.task)
        if action is None:
            return TaskResult(
                status="error",
                error=f"unsupported worker action: {task_data.task}",
                error_code="UNKNOWN_WORKER_ACTION",
                retryable=False,
            )

        try:
            msgspec.msgpack.decode(task_data.payload, type=WorkerControlRequest)
        except msgspec.DecodeError as exc:
            return TaskResult(
                status="error",
                error=f"invalid {task_data.task} payload: {exc}",
                error_code="INVALID_WORKER_PAYLOAD",
                retryable=False,
            )

        await action()
        return TaskResult(
            status="success",
            payload=msgspec.msgpack.encode(WorkerControlResponse(action=task_data.task)),
        )
