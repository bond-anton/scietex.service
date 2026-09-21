"""Signal-handler lifecycle component for ``scietex.service``.

Provides ``SignalHandler``, which owns SIGINT/SIGTERM registration and
removal for graceful shutdown, used by ``BasicWorker``.
"""

import asyncio
import logging
import signal
import weakref
from asyncio import AbstractEventLoop
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .basic_worker import BasicWorker

#: Last-worker-wins ownership registry. Keyed by the running loop so two
#: workers on one loop cooperate: only the most recent ``setup()`` owner may
#: remove the shared SIGINT/SIGTERM handlers. Weak keys mean a collected loop
#: drops its entry with no explicit cleanup.
_signal_owner: weakref.WeakKeyDictionary[AbstractEventLoop, "SignalHandler"] = weakref.WeakKeyDictionary()


class SignalHandler:
    """Owns SIGINT/SIGTERM registration and removal for graceful shutdown.

    Extracted from ``BasicWorker`` (AR-087) so the worker delegates its
    signal-handler lifecycle here while keeping its public API and subclass
    hooks stable. Registering and removing handlers on the same event loop
    from multiple workers is coordinated through a last-worker-wins
    ownership registry, so one worker's shutdown never unregisters another
    worker's still-active handlers.
    """

    def __init__(self, worker: "BasicWorker") -> None:
        """Initialize the signal handler with a back-reference to its worker.

        Args:
            worker: The owning ``BasicWorker`` instance, providing the
                logger and the ``_request_exit`` callback invoked on signal.
        """
        self.worker: BasicWorker = worker
        # True once ``setup()`` has registered handlers; reset only when this
        # handler removes its own handlers as the loop's current owner.
        self._registered = False

    def setup(self) -> None:
        """
        Set up signal handlers for graceful shutdown.

        Registers handlers for SIGINT and SIGTERM that trigger a graceful
        shutdown of the worker, and records this handler as the loop's
        current owner. A later ``setup()`` from another worker on the same
        loop becomes the new owner (last-worker-wins); this handler's
        ``remove()`` then no-ops.

        No-ops on platforms without ``loop.add_signal_handler`` support
        (e.g. Windows).
        """
        loop = asyncio.get_running_loop()
        if not hasattr(loop, "add_signal_handler"):
            return
        # Record ownership before registering so ``remove()`` sees this
        # handler as the owner for the duration of the loop's handlers.
        _signal_owner[loop] = self
        # The callback is bound to the worker's exit-request path. After the
        # step-3 wiring this becomes ``self.worker._lifecycle.request_exit()``;
        # calling ``self.worker._request_exit()`` works both before and after.
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.worker._request_exit)
        self._registered = True
        self.worker.logger.log(logging.DEBUG, "Signal handlers are all setup")

    def remove(self) -> None:
        """
        Remove the signal handlers registered for graceful shutdown.

        Ownership-guarded: a no-op unless this handler is the current owner
        of the loop's SIGINT/SIGTERM handlers. This prevents a worker whose
        handlers were superseded by a later ``setup()`` on the same loop from
        unregistering the new owner's handlers.

        Mirrors ``setup()`` and no-ops on platforms without
        ``loop.remove_signal_handler`` support (e.g. Windows).
        """
        loop = asyncio.get_running_loop()
        if not hasattr(loop, "remove_signal_handler"):
            return
        if _signal_owner.get(loop) is not self:
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        del _signal_owner[loop]
        self._registered = False
        self.worker.logger.log(logging.DEBUG, "Signal handlers removed")
