"""Test Basic AsyncWorker"""

import asyncio
import os
import signal

import pytest

try:
    from src.scietex.service.basic_async_worker import BasicAsyncWorker
except ModuleNotFoundError:
    from scietex.service.basic_async_worker import BasicAsyncWorker


@pytest.fixture(scope="module")
def test_event_loop():
    """
    Fixture to create asyncio event loop for testing.
    """
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# pylint: disable=redefined-outer-name, protected-access
@pytest.mark.asyncio
async def test_graceful_shutdown(test_event_loop):
    """
    Test that the worker shuts down gracefully after receiving a SIGINT/SIGTERM signal.
    """
    worker = BasicAsyncWorker(service_name="test_service", version="1.0.0")
    test_event_loop.create_task(worker.run())
    await asyncio.sleep(1)

    assert not worker._stop_event.is_set()

    # Simulate a SIGINT signal
    worker.setup_signal_handlers()
    os.kill(os.getpid(), signal.SIGINT)
    await asyncio.sleep(0.1)  # Allow some time for the signal handler to react

    # Verify that the stop event was triggered
    assert worker._stop_event.is_set()

    # Await stop procedure
    await worker.stop()

    # Assert that cleanup has been performed
    assert worker.task_queue.empty()
    assert worker.results_queue.empty()
    assert worker.log_queue.empty()


# pylint: disable=redefined-outer-name
@pytest.mark.asyncio
async def test_task_processing(test_event_loop):
    """
    Test that tasks are correctly added to the task queue and processed.
    """
    worker = BasicAsyncWorker(service_name="test_service", version="1.0.0")
    await worker.initialize()

    # Add a task to the queue
    task_id = 1
    task_data = {"task_type": "example"}
    await worker.task_queue.put((task_id, task_data))
    test_event_loop.create_task(worker.run())
    await asyncio.sleep(1)
    await worker.stop()
    print("Task gathered")
    assert worker.log_queue.empty()
    # Check that the task was removed from the queue
    assert worker.task_queue.empty()
    assert worker.results_queue.empty()
