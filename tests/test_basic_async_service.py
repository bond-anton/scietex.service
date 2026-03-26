"""Tests for BasicAsyncWorker: ensure logging drain and graceful stop."""

import asyncio
import logging

import pytest

from scietex.service.basic_async_worker import BasicAsyncWorker


@pytest.fixture(scope="module")
def test_event_loop():
    """Fixture to create asyncio event loop for testing."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_graceful_shutdown_and_log_drain(test_event_loop):
    """Start the worker, enqueue some logs, then stop and ensure drain."""
    worker = BasicAsyncWorker(service_name="test_service", version="1.0.0")

    # Start worker (initializes logging handlers and managers)
    await worker.start()

    # Put some log messages into the queue
    await worker.log("first message", level=logging.INFO)
    await worker.log("second message", level=logging.WARNING)

    # Ensure stop works and drains logs
    await worker.stop()

    assert worker._stop_event.is_set()
    assert worker.log_queue.empty()
    assert worker._completion_event.is_set()
