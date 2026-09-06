"""Abstract task handler base class for ``scietex.service``."""

import logging
from abc import ABC, abstractmethod

from .context import TaskHandlerContext
from .schemas import TaskData, TaskResult


class TaskHandler(ABC):
    """Abstract base class for all task handlers.

    Defines the lifecycle contract (``start``/``stop``), the handler
    selection mechanism (``supports``), and the task processing method
    (``handle``) that concrete implementations must provide.

    Subclasses must implement the :attr:`supported_tasks` property and
    the :meth:`handle` method.
    """

    def __init__(self, name: str, context: TaskHandlerContext) -> None:
        """Initialize the task handler.

        Args:
            name: Human-readable name for this handler instance.
            context: Narrow context (service name, worker id, logger)
                provided by the owning processor.
        """
        self.name: str = name
        self.context: TaskHandlerContext = context
        self.logger: logging.Logger = context.logger
        self._is_initialized: bool = False

    @property
    @abstractmethod
    def supported_tasks(self) -> list[str]:
        """List of task type strings this handler can process.

        Must be implemented by subclasses to declare which task types
        they support.
        """
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
        """Asynchronous initialization called by :meth:`start`.

        Override this method to perform any setup required before
        processing tasks (e.g., opening connections, loading data).

        Returns:
            ``True`` if initialization succeeded, ``False`` otherwise.
        """
        return True

    async def cleanup(self) -> None:
        """Asynchronous cleanup called by :meth:`stop`.

        Override this method to release resources (e.g., close database
        connections, flush buffers) before the handler is shut down.
        """
        pass

    async def start(self) -> None:
        """Start the handler.

        Calls :meth:`initialize` and sets :attr:`is_ready` to ``True``.
        Logs the handler name and supported task types on success.
        """
        self._is_initialized = await self.initialize()
        self.logger.log(
            logging.INFO,
            "Started handler: %s: %s",
            self.name,
            self.supported_tasks,
        )

    async def stop(self) -> None:
        """Stop the handler.

        Calls :meth:`cleanup` and resets :attr:`is_ready` to ``False``.
        Logs the handler name on shutdown.
        """
        await self.cleanup()
        self._is_initialized = False
        self.logger.log(logging.INFO, "Stopped handler: %s", self.name)

    @property
    def is_ready(self) -> bool:
        """Whether the handler has been initialized and is ready to process tasks."""
        return self._is_initialized
