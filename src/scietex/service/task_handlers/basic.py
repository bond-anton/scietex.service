"""Module: scietex.service.task_handlers.basic"""

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..basic_async_worker import BasicAsyncWorker


class TaskHandler(ABC):
    """
    Abstract base class for all task handlers.
    Defines the contract that all concrete handlers must implement.
    """

    def __init__(self, worker: "BasicAsyncWorker") -> None:
        """
        Initialize the handler.


        Args:
            worker: Link to the main BasicAsyncWorker,
                allowing handlers to access shared resources and utilities.
        """
        self.worker = worker
        self._is_initialized = False

    async def initialize(self) -> None:
        """
        Optional asynchronous initialization method.
        Can be overridden by handlers that require setup before processing tasks.
        """
        self._is_initialized = True

    @abstractmethod
    async def handle(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """
        Process the given task data and return the results.

        Args:
            task_data: dictionary with task data

        Returns:
            dictionary with processing results

        Raises:
            Exception: when errors occur while processing the task
        """
        pass

    @abstractmethod
    def supports(self, task_type: str) -> bool:
        """
        Checks if the handler supports the specified task type.

        Args:
            task_type: the type of task to check

        Returns:
            True if the handler supports the specified task type. False otherwise.
        """
        pass

    async def cleanup(self) -> None:
        """
        Optional cleanup method.
        Can be used to close connections and perform other cleanup tasks.
        """
        pass

    @property
    def is_ready(self) -> bool:
        """Checks if the handler is ready for use (initialized)."""
        return self._is_initialized
