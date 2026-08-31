"""Service managers utilities."""

import asyncio
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import TYPE_CHECKING, Any

from .helpers import get_anything_name
from .status import ManagerStatus

if TYPE_CHECKING:
    from ..basic_async_worker import BasicAsyncWorker

DEFAULT_MAX_RESULTS_QUEUE_SIZE = 100
DEFAULT_ACTION_TIMEOUT = 2


class RegisterManager:
    """Class-based decorator to register manager."""

    def __init__(
        self,
        name: str | None = None,
        max_results_queue_size: int = DEFAULT_MAX_RESULTS_QUEUE_SIZE,
        sliding_queue: bool = True,
        queue_timeout: float = 1,
        no_consumer: bool = False,
        cleanup: Callable[[Any], Coroutine[None, None, None]] | None = None,
    ):
        self.name = name
        self.max_results_queue_size: int = max_results_queue_size
        self.sliding_queue: bool = sliding_queue
        self.queue_timeout: float = queue_timeout
        self.no_consumer: bool = no_consumer
        if self.no_consumer:
            self.max_results_queue_size = 1
        self.cleanup: Callable[[Any], Coroutine[None, None, None]] | None = cleanup
        self._is_manager = True  # Attribute is properly defined

    def __call__(self, method: Callable[[Any], Coroutine[None, None, Any]]) -> Callable:
        @wraps(method)
        def wrapper(self_wrp: "BasicAsyncWorker", *args, **kwargs) -> Callable:
            manager_name = get_anything_name(method)
            self.name: str = self.name or manager_name
            self_wrp._managers_statuses[self.name] = ManagerStatus.STOPPED
            self_wrp._managers_errors[self.name] = None
            self_wrp._managers_results[self.name] = asyncio.Queue(self.max_results_queue_size)

            async def manager(service: "BasicAsyncWorker", *args, **kwargs):
                service._managers_statuses[self.name] = ManagerStatus.RUNNING
                service._managers_errors[self.name] = None

                async def stop_manager():
                    service._managers_statuses[self.name] = ManagerStatus.STOPPING
                    service.logger.info("🟡 Manager %s stopping", self.name)
                    if self.cleanup:
                        await self.cleanup(service)
                    service._managers_statuses[self.name] = ManagerStatus.STOPPED
                    service.logger.info("🔴 Manager %s stopped", self.name)

                async def put_result(result: Any):
                    if self.no_consumer:
                        return
                    if self.sliding_queue:
                        if service._managers_results[self.name].full():
                            service.logger.warning(
                                "⚠️ Manager %s Result Queue is full. Dropping oldest item."
                            )
                            _ = await service._managers_results[self.name].get()
                            service._managers_results[self.name].task_done()
                    try:
                        await asyncio.wait_for(
                            service._managers_results[self.name].put(result),
                            timeout=self.queue_timeout,
                        )
                    except asyncio.TimeoutError:
                        service.logger.error(
                            "❌ Manager %s failed to put result to the Result Queue", self.name
                        )

                service.logger.info("🟢 Manager %s started", self.name)

                while not service.events["stop"].is_set():
                    try:
                        result = await method(service, *args, **kwargs)
                        await put_result(result)
                    except asyncio.CancelledError:
                        await stop_manager()
                        raise

                    except Exception as e:
                        service._managers_errors[self.name] = e
                        service.logger.error("❌ Manager %s error %s", self.name, e)
                        await stop_manager()
                        raise
                await stop_manager()

            self_wrp._managers[self.name] = manager
            self_wrp.logger.info("Manager %s successfully activated", self.name)
            return manager

        setattr(wrapper, "_is_manager", True)
        return wrapper


def timeout_action(delay: int = DEFAULT_ACTION_TIMEOUT):
    """Adds timeout before function call."""

    def decorator(
        method: Callable[[Any], Coroutine[None, None, Any]],
    ) -> Callable[[Any], Coroutine[None, None, Any]]:

        @wraps(method)
        async def wrapper(self: "BasicAsyncWorker", *args, **kwargs):
            """Run after delay."""
            try:
                await asyncio.wait_for(
                    self.events["stop"].wait(),
                    timeout=delay,
                )
            except asyncio.TimeoutError:
                return await method(self, *args, **kwargs)

        return wrapper

    return decorator
