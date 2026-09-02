"""Abstract task handler base class for ``scietex.service``."""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from .schemas import TaskData, TaskResult

if TYPE_CHECKING:
    from ..basic_async_worker import BasicAsyncWorker


class TaskHandler(ABC):
    """
    Abstract base class for all task handlers.
    Defines the contract that all concrete handlers must implement.
    """

    def __init__(self, name: str, worker: "BasicAsyncWorker") -> None:
        """Initialize the task handler with a reference to the parent worker.

        Args:
            worker: Reference to the parent ``BasicAsyncWorker`` instance,
                allowing handlers to access shared resources and utilities.
        """
        self.name: str = name
        self.worker: BasicAsyncWorker = worker
        self.logger: logging.Logger = self.worker.logger
        self._is_initialized: bool = False

    @property
    @abstractmethod
    def supported_tasks(self) -> list[str]:
        """List of supported task names."""
        pass

    @abstractmethod
    async def handle(self, task_data: TaskData) -> TaskResult:
        """Process the given task and return a ``TaskResult``.

        Args:
            task_data: Typed task data structure containing the task type,
                payload, timeout, and cancellation behavior.

        Returns:
            A ``TaskResult`` with status, optional error message, and payload.

        Raises:
            Exception: Any error that occurs during task processing.
        """
        pass

    def supports(self, task_type: str) -> bool:
        """Check whether this handler can process the given task type.

        Args:
            task_type: The task type string to check against.

        Returns:
            ``True`` if this handler supports the given task type,
            ``False`` otherwise.
        """
        return task_type in self.supported_tasks

    async def initialize(self) -> bool:
        """
        Optional asynchronous initialization method.
        Can be overridden by handlers that require setup before processing tasks.
        """
        return True

    async def cleanup(self) -> None:
        """
        Optional cleanup method.
        Can be used to close connections and perform other cleanup tasks.
        """
        pass

    async def start(self) -> None:
        """Used to start the handler."""
        self._is_initialized = await self.initialize()
        self.logger.log(
            logging.INFO,
            "Started handler: %s: %s",
            self.name,
            self.supported_tasks,
        )

    async def stop(self) -> None:
        """Used to stop the handler."""
        await self.cleanup()
        self._is_initialized = False
        self.logger.log(logging.INFO, "Stopped handler: %s", self.name)

    @property
    def is_ready(self) -> bool:
        """Checks if the handler is ready for use (initialized)."""
        return self._is_initialized
