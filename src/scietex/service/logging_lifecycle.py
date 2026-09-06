"""Logging lifecycle component for ``scietex.service``.

Provides ``LoggingLifecycle``, which owns async logging-handler
start/stop and status bookkeeping used by ``BasicAsyncWorker``.
"""

import asyncio
from typing import TYPE_CHECKING

from scietex.logging import AsyncBaseHandler

from .logging import LoggerStatus

if TYPE_CHECKING:
    from .basic_async_worker import BasicAsyncWorker


class LoggingLifecycle:
    """Owns async logging-handler start/stop and status bookkeeping.

    Extracted from ``BasicAsyncWorker`` (AR-003) so the worker delegates its
    logging-handler lifecycle here while keeping its public API and subclass
    hooks stable. Config is read off the worker's public properties, which
    remain the single source of truth for clamped values.
    """

    def __init__(self, worker: "BasicAsyncWorker") -> None:
        """Initialize the logging lifecycle with a back-reference to its worker.

        Args:
            worker: The owning ``BasicAsyncWorker`` instance, providing the
                logger and clamped config values used by handler start/stop.
        """
        self.worker: BasicAsyncWorker = worker
        self.statuses: dict[str, LoggerStatus] = {}

    def register_logger_handler(
        self,
        handler: AsyncBaseHandler,
        name: str | None = None,
    ) -> None:
        """
        Attach an async logging handler to the logger.

        The handler is restartable in place (``start_logging``/``stop_logging``
        may be called repeatedly on the same event loop), so a single instance
        is registered once and reused across start/stop cycles.

        Args:
            handler: The ``AsyncBaseHandler`` (or subclass) to attach.
            name: Optional handler name, accepted for compatibility with
                subclasses that pass an explicit name.
        """
        handler.setLevel(self.worker.logging_level)
        self.worker.logger.addHandler(handler)

    async def start_handlers(self) -> None:
        """
        Start all async logging handlers that are not already running.

        Iterates over the logger's handlers and calls start_logging() on each
        AsyncBaseHandler whose recorded status is not RUNNING. Handlers are
        restartable in place, so no replacement is needed. Handles timeouts and
        errors gracefully, falling back to print statements if the logger is in
        an unrecoverable state.
        """
        for handler in list(self.worker.logger.handlers):
            handler_name = handler.name or handler.__class__.__name__
            if handler_name not in self.statuses or self.statuses[handler_name] == LoggerStatus.STOPPED:
                if isinstance(handler, AsyncBaseHandler):
                    try:
                        await asyncio.wait_for(handler.start_logging(), timeout=self.worker.logger_handler_timeout)
                    except asyncio.TimeoutError:
                        try:
                            self.worker.logger.warning(
                                "Timeout starting logging handler %s (%s)", handler_name, handler
                            )
                        except Exception:
                            # logger itself may be in a bad state; fallback to print
                            print(f"Timeout starting logging handler {handler_name} ({handler})")
                    except Exception as e:
                        try:
                            self.worker.logger.error(
                                "Failed to start logging handler %s (%s): %s",
                                handler_name,
                                handler,
                                e,
                            )
                        except Exception:
                            print(f"Failed to start logging handler {handler_name} ({handler}): {e}")
                self.statuses[handler_name] = LoggerStatus.RUNNING

    async def shut_down_handlers(self) -> None:
        """Cleanly shut down all async logging handlers.

        This will attempt to stop each `AsyncBaseHandler` with a per-handler
        timeout to avoid hanging shutdowns if a handler blocks. `stop_logging`
        is idempotent in scietex.logging >= 1.0, so it is safe to call on every
        handler regardless of its current state.
        """
        for handler in self.worker.logger.handlers:
            handler_name = handler.name or handler.__class__.__name__
            if isinstance(handler, AsyncBaseHandler):
                try:
                    await asyncio.wait_for(handler.stop_logging(), timeout=self.worker.logger_handler_timeout)
                except asyncio.TimeoutError:
                    try:
                        self.worker.logger.warning("Timeout stopping logging handler %s (%s)", handler_name, handler)
                    except Exception:
                        # logger itself may be in a bad state; fallback to print
                        print(f"Timeout stopping logging handler {handler_name} ({handler})")
                except Exception as e:
                    try:
                        self.worker.logger.error(
                            "Failed to shut down logging handler %s (%s): %s",
                            handler_name,
                            handler,
                            e,
                        )
                    except Exception:
                        print(f"Failed to shut down logging handler {handler_name} ({handler}): {e}")
            self.statuses[handler_name] = LoggerStatus.STOPPED
