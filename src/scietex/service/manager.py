"""Service managers utilities."""

from collections.abc import Callable, Coroutine
from enum import Enum
from typing import Any

DEFAULT_MAX_OUTPUT_QUEUE_SIZE = 100


class ManagerStatus(Enum):
    """Manager Status."""

    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"
    STOPPED = "Stopped"


class Manager:
    """Class-based decorator to define a manager."""

    def __init__(
        self,
        name: str | None = None,
        cleanup: Callable[[Any], Coroutine[None, None, None]] | None = None,
    ):
        self.name: str | None = name
        self.cleanup: Callable[[Any], Coroutine[None, None, None]] | None = cleanup
        self.method: Callable[[Any], Coroutine[None, None, None]] | None = None

    def __call__(self, method: Callable[[Any], Coroutine[None, None, None]]) -> Callable:
        self.method = method
        return self
