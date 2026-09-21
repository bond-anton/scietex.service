"""Tests for ``SignalHandler`` registration/removal and loop ownership (AR-087)."""

import asyncio
import logging
import signal
from typing import cast

import pytest

from scietex.service.basic_worker import BasicWorker
from scietex.service.signal_handler import SignalHandler


class _StubWorker:
    """Minimal stand-in for ``BasicWorker`` used by ``SignalHandler``.

    Provides only the attributes ``SignalHandler`` reads: the logger and the
    ``_request_exit`` callback registered for SIGINT/SIGTERM.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(f"test_signal_handler_{id(self)}")
        self.logger.setLevel(logging.DEBUG)
        self.exit_requests = 0

    def _request_exit(self) -> None:
        self.exit_requests += 1


def _registered_callback(loop: asyncio.AbstractEventLoop, sig: int):
    """Return the callback asyncio registered for ``sig`` on ``loop``.

    asyncio exposes no public getter, so read the loop's private
    signal -> ``Handle`` registry and unwrap its ``_callback``.
    """
    return loop._signal_handlers[sig]._callback


@pytest.mark.asyncio
async def test_setup_registers_signal_handlers():
    """setup() registers SIGINT and SIGTERM on the loop and flags itself registered."""
    worker = _StubWorker()
    handler = SignalHandler(cast(BasicWorker, worker))
    loop = asyncio.get_running_loop()

    handler.setup()

    # asyncio exposes no public getter, so inspect the loop's private
    # signal -> callback registry to assert on registration.
    assert signal.SIGINT in loop._signal_handlers
    assert signal.SIGTERM in loop._signal_handlers
    assert handler._registered is True

    handler.remove()


@pytest.mark.asyncio
async def test_remove_unregisters_when_owner():
    """remove() unregisters SIGINT and SIGTERM when this handler is the owner."""
    worker = _StubWorker()
    handler = SignalHandler(cast(BasicWorker, worker))
    loop = asyncio.get_running_loop()

    handler.setup()
    handler.remove()

    assert signal.SIGINT not in loop._signal_handlers
    assert signal.SIGTERM not in loop._signal_handlers
    assert handler._registered is False


@pytest.mark.asyncio
async def test_remove_noop_when_not_owner():
    """remove() from a handler that never owned the loop leaves the owner's handlers intact."""
    owner_worker = _StubWorker()
    owner = SignalHandler(cast(BasicWorker, owner_worker))
    stranger = SignalHandler(cast(BasicWorker, _StubWorker()))
    loop = asyncio.get_running_loop()

    owner.setup()
    stranger.remove()

    assert signal.SIGINT in loop._signal_handlers
    assert signal.SIGTERM in loop._signal_handlers
    assert _registered_callback(loop, signal.SIGINT).__self__ is owner_worker

    owner.remove()


@pytest.mark.asyncio
async def test_last_worker_wins():
    """A superseded worker's remove() must not unregister the new owner's handlers (AR-087)."""
    worker_a = _StubWorker()
    worker_b = _StubWorker()
    handler_a = SignalHandler(cast(BasicWorker, worker_a))
    handler_b = SignalHandler(cast(BasicWorker, worker_b))
    loop = asyncio.get_running_loop()

    handler_a.setup()
    handler_b.setup()

    handler_a.remove()  # A is no longer the owner; must no-op

    assert signal.SIGINT in loop._signal_handlers
    assert signal.SIGTERM in loop._signal_handlers
    assert _registered_callback(loop, signal.SIGINT).__self__ is worker_b

    handler_b.remove()

    assert signal.SIGINT not in loop._signal_handlers
    assert signal.SIGTERM not in loop._signal_handlers


@pytest.mark.asyncio
async def test_signal_callback_requests_exit():
    """The registered callback must invoke the worker's exit-request path."""
    worker = _StubWorker()
    handler = SignalHandler(cast(BasicWorker, worker))
    loop = asyncio.get_running_loop()

    handler.setup()
    _registered_callback(loop, signal.SIGINT)()

    assert worker.exit_requests == 1

    handler.remove()
