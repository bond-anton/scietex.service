"""Per-call task capabilities passed to a handler's ``handle`` method."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class TaskCapabilities:
    """Capabilities a handler has for one task invocation.

    Replaces the processor's ``ContextVar``-based ``report_progress``: the task
    id is explicit, so a handler reports progress on its own task without
    relying on ambient context. ``report_progress`` clamps ``value`` to
    ``[0.0, 100.0]`` and forwards it to the transport's ``on_progress`` hook.
    """

    task_id: UUID
    _write_progress: Callable[[UUID, float], Awaitable[None]]

    async def report_progress(self, value: float) -> None:
        """Report granular progress for this task, clamped to ``[0.0, 100.0]``."""
        clamped = min(max(value, 0.0), 100.0)
        await self._write_progress(self.task_id, clamped)
