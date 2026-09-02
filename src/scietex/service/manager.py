"""Manager decorator and lifecycle utilities for ``scietex.service``.

Provides the ``@Manager`` class decorator that wraps async methods
into managed loops with automatic restart on error, and
``ManagerStatus`` for tracking manager lifecycle states.
"""

from collections.abc import Callable, Coroutine
from enum import Enum
from typing import Any

DEFAULT_MAX_OUTPUT_QUEUE_SIZE = 100


class ManagerStatus(Enum):
    """Lifecycle status of a ``Manager``-decorated loop."""

    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"
    STOPPED = "Stopped"


class Manager:
    """Class-based decorator that wraps an async method into a managed loop.

    When applied to an async method, the method becomes a manager loop
    that runs indefinitely until cancelled. The parent ``BasicAsyncWorker``
    iterates over all ``Manager`` instances in the class MRO and executes
    them as ``asyncio.Task`` objects with automatic restart on error.

    Args:
        name: Human-readable name for the manager. Defaults to the
            decorated method name if ``None``.
        cleanup: Optional async callable that runs when the manager
            stops. Receives the worker instance as its argument.
    """

    def __init__(
        self,
        name: str | None = None,
        cleanup: Callable[[Any], Coroutine[None, None, None]] | None = None,
    ):
        """
        Initialize the Manager decorator.

        Args:
            name: Human-readable name for the manager. Defaults to the
                decorated method name if ``None``.
            cleanup: Optional async callable that runs when the manager
                stops. Receives the worker instance as its argument.
        """
        self.name: str | None = name
        self.cleanup: Callable[[Any], Coroutine[None, None, None]] | None = cleanup
        self.method: Callable[[Any], Coroutine[None, None, None]] | None = None

    def __call__(self, method: Callable[[Any], Coroutine[None, None, None]]) -> Callable:
        """Apply the decorator to an async method.

        Stores the method reference and returns self so the decorated
        method can be used as a ``Manager`` instance by
        ``BasicAsyncWorker._iter_manager_definitions()``.

        Args:
            method: The async method to wrap as a manager loop.

        Returns:
            ``self``, which can be inspected by the worker to discover
            and execute the manager.
        """
        self.method = method
        return self
