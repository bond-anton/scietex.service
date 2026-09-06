"""Tests for ``LoggingLifecycle`` handler status bookkeeping (AR-020)."""

import logging
from typing import cast

import pytest
from scietex.logging import AsyncBaseHandler

from scietex.service.basic_async_worker import BasicAsyncWorker
from scietex.service.logging import LoggerStatus
from scietex.service.logging_lifecycle import LoggingLifecycle


class _StubWorker:
    """Minimal stand-in for ``BasicAsyncWorker`` used by ``LoggingLifecycle``.

    Provides only the attributes ``LoggingLifecycle`` reads: the logger, the
    handler logging level, and the per-handler start/stop timeout.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(f"test_logging_lifecycle_{id(self)}")
        self.logger.setLevel(logging.DEBUG)
        self.logging_level = logging.DEBUG
        self.logger_handler_timeout = 2.0


class _FlakyHandler(AsyncBaseHandler):
    """Handler whose ``start_logging`` raises until told to succeed."""

    def __init__(self) -> None:
        super().__init__(service_name="test", worker_id=1, stdout_enable=False)
        self.fail_next_start = True

    async def start_logging(self) -> None:
        if self.fail_next_start:
            raise RuntimeError("start failure")
        await super().start_logging()


@pytest.mark.asyncio
async def test_start_failure_recorded_as_failed_then_retried():
    """A handler whose start_logging raises is FAILED, then retried to RUNNING."""
    worker = _StubWorker()
    lifecycle = LoggingLifecycle(cast(BasicAsyncWorker, worker))
    handler = _FlakyHandler()
    lifecycle.register_logger_handler(handler)

    await lifecycle.start_handlers()
    assert lifecycle.statuses[type(handler).__name__] == LoggerStatus.FAILED

    handler.fail_next_start = False
    await lifecycle.start_handlers()
    assert lifecycle.statuses[type(handler).__name__] == LoggerStatus.RUNNING
