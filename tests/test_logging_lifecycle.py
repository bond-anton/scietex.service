"""Tests for ``LoggingLifecycle`` handler status bookkeeping (AR-020)."""

import itertools
import logging
from typing import cast

import pytest
from scietex.logging import AsyncLoggingHandler

from scietex.service.basic_worker import BasicWorker
from scietex.service.log_handlers import LoggerStatus
from scietex.service.log_handlers.lifecycle import LoggingLifecycle

# ``id(self)`` is reused after a worker is garbage-collected between tests, so a
# name derived from it can collide with a still-cached logger from a prior test.
# A module-level counter keeps each worker's logger unique for the whole run.
_logger_ids = itertools.count()


class _StubWorker:
    """Minimal stand-in for ``BasicWorker`` used by ``LoggingLifecycle``.

    Provides only the attributes ``LoggingLifecycle`` reads: the logger, the
    handler logging level, and the per-handler start/stop timeout.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(f"test_logging_lifecycle_{next(_logger_ids)}")
        self.logger.setLevel(logging.DEBUG)
        self.logging_level = logging.DEBUG
        self.logger_handler_timeout = 2.0


class _FlakyHandler(AsyncLoggingHandler):
    """Handler whose ``start_logging`` raises until told to succeed."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_next_start = True

    async def start_logging(self) -> None:
        if self.fail_next_start:
            raise RuntimeError("start failure")
        await super().start_logging()


@pytest.mark.asyncio
async def test_start_failure_recorded_as_failed_then_retried():
    """A handler whose start_logging raises is FAILED, then retried to RUNNING."""
    worker = _StubWorker()
    lifecycle = LoggingLifecycle(cast(BasicWorker, worker))
    handler = _FlakyHandler()
    lifecycle.register_logger_handler(handler)

    await lifecycle.start_handlers()
    assert lifecycle.statuses[handler] == LoggerStatus.FAILED

    handler.fail_next_start = False
    await lifecycle.start_handlers()
    assert lifecycle.statuses[handler] == LoggerStatus.RUNNING


@pytest.mark.asyncio
async def test_same_class_handlers_have_independent_statuses():
    """Two instances of one handler class get separate status entries (AR-119)."""
    worker = _StubWorker()
    lifecycle = LoggingLifecycle(cast(BasicWorker, worker))
    failing = _FlakyHandler()
    running = _FlakyHandler()
    running.fail_next_start = False
    lifecycle.register_logger_handler(failing)
    lifecycle.register_logger_handler(running)

    await lifecycle.start_handlers()

    assert lifecycle.statuses[failing] is LoggerStatus.FAILED
    assert lifecycle.statuses[running] is LoggerStatus.RUNNING


@pytest.mark.asyncio
async def test_non_async_handler_not_tracked():
    """A plain (non-async) handler is skipped and never enters the status map."""
    worker = _StubWorker()
    lifecycle = LoggingLifecycle(cast(BasicWorker, worker))
    plain = logging.StreamHandler()
    worker.logger.addHandler(plain)

    await lifecycle.start_handlers()
    await lifecycle.shut_down_handlers()

    assert plain not in lifecycle.statuses
    assert not lifecycle.statuses


@pytest.mark.asyncio
async def test_shutdown_without_start_marks_async_stopped():
    """Shutting down before start marks a registered handler STOPPED, idempotently."""
    worker = _StubWorker()
    lifecycle = LoggingLifecycle(cast(BasicWorker, worker))
    handler = _FlakyHandler()
    lifecycle.register_logger_handler(handler)

    await lifecycle.shut_down_handlers()

    assert lifecycle.statuses[handler] is LoggerStatus.STOPPED
