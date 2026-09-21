"""Built-in handler for the ``task:cancel`` task name.

The handler is transport-agnostic: it decodes a :class:`CancelTaskRequest`
from the task payload and delegates the actual cancellation to an async
callback injected by the owning processor. The processor owns the running
tasks and the queue, so the handler never reaches into processor internals.
"""

from collections.abc import Awaitable, Callable
from typing import ClassVar, Literal
from uuid import UUID

import msgspec

from .basic import TaskHandler
from .capabilities import TaskCapabilities
from .context import TaskHandlerContext
from .schemas import TASK_CANCEL_TASK_NAME, TaskData, TaskResult

#: Result of a cancellation attempt.
#:
#: - ``"cancelled"``: the target was running and stopped, or was queued and
#:   removed before it started.
#: - ``"not_running"``: the target is not running or queued (already finished,
#:   never seen, or the request targeted the cancelling task itself).
#: - ``"ignored"``: the target is running but did not stop within the
#:   cancellation timeout; it stays tracked and will finish on its own.
#: - ``"not_found"``: reserved for a target that cannot be resolved.
CancelOutcome = Literal["cancelled", "not_running", "ignored", "not_found"]

#: Async callback injected by the processor to cancel a target task.
CancelCallback = Callable[[UUID], Awaitable[CancelOutcome]]


class CancelTaskRequest(msgspec.Struct, frozen=True):
    """Payload of a ``task:cancel`` task.

    Args:
        target_task_id: UUID (as a string) of the task to cancel.
        reason: Optional operator note, for logging/audit only.
    """

    target_task_id: str
    reason: str = ""


class CancelTaskResponse(msgspec.Struct, frozen=True):
    """Payload returned by a successful ``task:cancel`` task.

    Args:
        target_task_id: UUID (as a string) of the task that was cancelled.
        outcome: The :data:`CancelOutcome` value.
    """

    target_task_id: str
    outcome: str


class CancelTaskHandler(TaskHandler):
    """Handler for the built-in ``task:cancel`` task name.

    Decodes a :class:`CancelTaskRequest` and calls the injected ``cancel``
    callback. A malformed payload yields a non-retryable error result rather
    than raising, so a bad request never crashes the task loop.
    """

    #: The cancel command is control-plane: it always arrives on a control
    #: channel and is served from the control registry, never the data registry.
    control: ClassVar[bool] = True

    def __init__(
        self,
        name: str,
        context: TaskHandlerContext,
        *,
        cancel: CancelCallback,
    ) -> None:
        """Initialize the handler.

        Args:
            name: Human-readable name for this handler instance.
            context: Narrow context provided by the owning processor.
            cancel: Async callback that performs the cancellation and returns
                the outcome.
        """
        super().__init__(name, context)
        self._cancel: CancelCallback = cancel

    @property
    def supported_tasks(self) -> list[str]:
        """Task names handled by this handler."""
        return [TASK_CANCEL_TASK_NAME]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        """Cancel the target task named in the payload.

        Args:
            task_data: Task data whose ``payload`` is a msgpack-encoded
                :class:`CancelTaskRequest`.
            capabilities: Keyword-only per-call capabilities (unused here; the
                handler only needs the injected ``cancel`` callback).

        Returns:
            A ``TaskResult``: ``success`` with a msgpack-encoded
            :class:`CancelTaskResponse` when the target was cancelled, or a
            non-retryable ``error`` otherwise.
        """
        try:
            request = msgspec.msgpack.decode(task_data.payload, type=CancelTaskRequest)
            target_id = UUID(request.target_task_id)
        except (msgspec.DecodeError, ValueError) as exc:
            return TaskResult(
                status="error",
                error=f"invalid task:cancel payload: {exc}",
                error_code="INVALID_CANCEL_PAYLOAD",
                retryable=False,
            )

        outcome = await self._cancel(target_id)
        if outcome == "cancelled":
            return TaskResult(
                status="success",
                payload=msgspec.msgpack.encode(CancelTaskResponse(target_task_id=str(target_id), outcome=outcome)),
            )
        if outcome == "ignored":
            return TaskResult(
                status="error",
                error=f"target {target_id} ignored cancellation",
                error_code="CANCEL_IGNORED",
                retryable=False,
            )
        return TaskResult(
            status="error",
            error=f"target {target_id} is not running",
            error_code="TASK_NOT_RUNNING",
            retryable=False,
        )
