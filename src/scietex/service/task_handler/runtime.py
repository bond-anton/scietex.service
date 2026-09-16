"""In-memory runtime handles for the task handler subsystem.

Unlike :mod:`~scietex.service.task_handler.schemas`, the types defined here
are live process-local objects (holding an ``asyncio.Task``) and are never
serialized to the transport.
"""

from asyncio import Task

import msgspec

from .schemas import TaskData


class TaskTracker(msgspec.Struct, frozen=True):
    """Tracks a running task's asyncio.Task, data, and start time.

    Used by ``TaskProcessor`` to monitor task progress, enforce
    timeouts, and manage cleanup on shutdown.

    Args:
        worker_task: The ``asyncio.Task`` executing this task.
        data: The ``TaskData`` associated with the task.
        started: Monotonic timestamp when the task was created.
    """

    worker_task: Task
    data: TaskData
    started: int | float
