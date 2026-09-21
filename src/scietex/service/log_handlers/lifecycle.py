"""Logging lifecycle component for ``scietex.service``.

Provides ``LoggingLifecycle``, which owns async logging-handler
start/stop and status bookkeeping used by ``BasicWorker``.
"""

import asyncio
from typing import TYPE_CHECKING

from scietex.logging import AsyncLoggingHandler

from . import LoggerStatus

if TYPE_CHECKING:
    from ..basic_worker import BasicWorker


class LoggingLifecycle:
    """Owns async logging-handler start/stop and status bookkeeping.

    Extracted from ``BasicWorker`` (AR-003) so the worker delegates its
    logging-handler lifecycle here while keeping its public API and subclass
    hooks stable. Config is read off the worker's public properties, which
    remain the single source of truth for clamped values.
    """

    def __init__(self, worker: "BasicWorker") -> None:
        """Initialize the logging lifecycle with a back-reference to its worker.

        Args:
            worker: The owning ``BasicWorker`` instance, providing the
                logger and clamped config values used by handler start/stop.
        """
        self.worker: BasicWorker = worker
        # Keyed by handler identity, not name/class name: two unnamed handlers of
        # the same class would otherwise share one status entry (AR-119). Identity
        # is stable across cycles because a handler instance is reused on restart.
        self.statuses: dict[AsyncLoggingHandler, LoggerStatus] = {}

    def register_logger_handler(self, handler: AsyncLoggingHandler) -> None:
        """
        Attach an async logging handler to the logger.

        The handler is restartable in place (``start_logging``/``stop_logging``
        may be called repeatedly on the same event loop), so a single instance
        is registered once and reused across start/stop cycles.

        Args:
            handler: The ``AsyncLoggingHandler`` (or subclass) to attach.
        """
        handler.setLevel(self.worker.logging_level)
        self.worker.logger.addHandler(handler)

    async def start_handlers(self) -> None:
        """
        Start all async logging handlers that are not already running.

        Iterates over the logger's handlers and calls ``start_logging()`` on
        each ``AsyncLoggingHandler`` whose recorded status is not ``RUNNING``.
        Statuses are keyed by handler identity, not name/class name, so two
        unnamed handlers of the same class each get their own entry (AR-119).
        Non-async handlers have no start/stop lifecycle and are not tracked.
        Handlers are restartable in place, so no replacement is needed. A
        handler that fails to start (timeout or exception) is recorded as
        ``FAILED`` so it is retried on the next ``start_handlers`` call.
        Handles timeouts and errors gracefully, falling back to print
        statements if the logger is in an unrecoverable state.
        """
        for handler in list(self.worker.logger.handlers):
            if not isinstance(handler, AsyncLoggingHandler):
                # Non-async handlers have no start/stop lifecycle to track.
                continue
            if self.statuses.get(handler) == LoggerStatus.RUNNING:
                continue
            label = handler.name or handler.__class__.__name__
            try:
                await asyncio.wait_for(handler.start_logging(), timeout=self.worker.logger_handler_timeout)
            except asyncio.TimeoutError:
                self.statuses[handler] = LoggerStatus.FAILED
                try:
                    self.worker.logger.warning("Timeout starting logging handler %s (%s)", label, handler)
                except Exception:
                    # logger itself may be in a bad state; fallback to print
                    print(f"Timeout starting logging handler {label} ({handler})")
            except Exception as e:
                self.statuses[handler] = LoggerStatus.FAILED
                try:
                    self.worker.logger.error(
                        "Failed to start logging handler %s (%s): %s",
                        label,
                        handler,
                        e,
                    )
                except Exception:
                    print(f"Failed to start logging handler {label} ({handler}): {e}")
            else:
                self.statuses[handler] = LoggerStatus.RUNNING

    async def shut_down_handlers(self) -> None:
        """Cleanly shut down all async logging handlers.

        Attempts to stop each ``AsyncLoggingHandler`` with a per-handler
        timeout to avoid hanging shutdowns if a handler blocks.
        ``stop_logging`` is idempotent in scietex.logging >= 1.0, so it is
        safe to call on every handler regardless of its current state.
        Statuses are keyed by handler identity, not name/class name, so two
        unnamed handlers of the same class each get their own entry (AR-119).
        Non-async handlers have no start/stop lifecycle and are not tracked.
        """
        for handler in self.worker.logger.handlers:
            if not isinstance(handler, AsyncLoggingHandler):
                # Never started by this lifecycle; leave it out of the status map.
                continue
            label = handler.name or handler.__class__.__name__
            try:
                await asyncio.wait_for(handler.stop_logging(), timeout=self.worker.logger_handler_timeout)
            except asyncio.TimeoutError:
                try:
                    self.worker.logger.warning("Timeout stopping logging handler %s (%s)", label, handler)
                except Exception:
                    # logger itself may be in a bad state; fallback to print
                    print(f"Timeout stopping logging handler {label} ({handler})")
            except Exception as e:
                try:
                    self.worker.logger.error(
                        "Failed to shut down logging handler %s (%s): %s",
                        label,
                        handler,
                        e,
                    )
                except Exception:
                    print(f"Failed to shut down logging handler {label} ({handler}): {e}")
            self.statuses[handler] = LoggerStatus.STOPPED
