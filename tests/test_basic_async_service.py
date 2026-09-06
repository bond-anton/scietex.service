"""Tests for BasicAsyncWorker: ensure logging drain and graceful stop."""

import asyncio
import logging

import pytest

from scietex.service.basic_async_worker import BasicAsyncWorker, ServiceStatus
from scietex.service.logging import LoggerStatus
from scietex.service.manager import Manager


@pytest.mark.asyncio
async def test_graceful_shutdown():
    """Start the worker, enqueue some logs, then stop and ensure drain."""
    worker = BasicAsyncWorker(service_name="test_service", version="1.0.0")

    # Start worker (initializes logging handlers and managers)
    await worker.start()
    for _ in range(50):
        if worker.state == ServiceStatus.RUNNING:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.RUNNING

    # Put some log messages into the queue
    worker.logger.log(logging.INFO, "first message")
    worker.logger.log(logging.WARNING, "second message")

    # Ensure stop works
    await worker.stop()
    for _ in range(50):
        if worker.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.STOPPED

    await worker.exit()
    assert worker.events["exit"].is_set()


@pytest.mark.asyncio
async def test_logging_handlers_restartable_after_shutdown():
    """After a shutdown, logging handlers must be marked STOPPED and be
    restarted in place (same instance) on the next start (scietex.logging >= 1.0)."""
    worker = BasicAsyncWorker(service_name="test_service", version="1.0.0")

    await worker.start()
    for _ in range(50):
        if worker.state == ServiceStatus.RUNNING:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.RUNNING

    statuses = worker._logging_lifecycle.statuses
    assert statuses.get("AsyncBaseHandler") == LoggerStatus.RUNNING
    first_handler = next(h for h in worker.logger.handlers if h.__class__.__name__ == "AsyncBaseHandler")

    await worker.stop()
    for _ in range(50):
        if worker.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.STOPPED
    # Handlers must be recorded as STOPPED after shutdown.
    assert statuses.get("AsyncBaseHandler") == LoggerStatus.STOPPED

    # Restart: the same handler instance is reused and restarted in place.
    await worker.start()
    for _ in range(50):
        if worker.state == ServiceStatus.RUNNING:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.RUNNING
    assert statuses.get("AsyncBaseHandler") == LoggerStatus.RUNNING
    second_handler = next(h for h in worker.logger.handlers if h.__class__.__name__ == "AsyncBaseHandler")
    assert second_handler is first_handler, "handler should be restarted in place, not replaced"

    await worker.stop()
    for _ in range(50):
        if worker.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.STOPPED


@pytest.mark.asyncio
async def test_initialize_runs_before_managers_start():
    """initialize() must complete before any @Manager-decorated method runs.

    Managers may depend on resources created in initialize() (e.g. a Valkey
    client), so a manager must never race a not-yet-initialized worker.
    """

    class OrderingWorker(BasicAsyncWorker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.order: list[str] = []

        async def initialize(self) -> bool:
            # Yield once so that, if managers were already started (the old,
            # buggy ordering), their first iteration would run before this
            # method records its event.
            await asyncio.sleep(0)
            self.order.append("initialize")
            return True

        @Manager(name="Ordering")
        async def _ordering_manager(self):
            self.order.append("manager")
            # Block so the manager records its event only once per iteration.
            await asyncio.sleep(3600)

    worker = OrderingWorker(service_name="test_service", version="1.0.0")

    await worker.start()
    for _ in range(100):
        if "manager" in worker.order:
            break
        await asyncio.sleep(0.05)

    assert worker.order.index("initialize") < worker.order.index("manager")

    await worker.stop()
    await worker.exit()


@pytest.mark.asyncio
async def test_shutdown_cancellation_forces_stopped():
    """A cancelled shutdown must force STOPPED and allow a clean restart.

    Regression test for AR-017: if the _shutdown task is cancelled mid-shutdown,
    the worker must reach STOPPED (not remain STOPPING) so a later start() is
    not blocked.
    """

    class SlowCleanupWorker(BasicAsyncWorker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.cleanup_started = asyncio.Event()
            self.block_cleanup = True

        async def cleanup(self):
            self.cleanup_started.set()
            if self.block_cleanup:
                await asyncio.sleep(3600)

    worker = SlowCleanupWorker(service_name="test_service", version="1.0.0")

    await worker.start()
    for _ in range(50):
        if worker.state == ServiceStatus.RUNNING:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.RUNNING

    await worker.stop()
    await worker.cleanup_started.wait()
    assert worker.state == ServiceStatus.STOPPING

    stop_task = next(t for t in asyncio.all_tasks() if t.get_name() == "Stop")
    stop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stop_task

    assert worker.state == ServiceStatus.STOPPED

    # A cancelled shutdown must not block a subsequent start.
    worker.block_cleanup = False
    await worker.start()
    for _ in range(50):
        if worker.state == ServiceStatus.RUNNING:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.RUNNING

    await worker.stop()
    for _ in range(50):
        if worker.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.STOPPED


@pytest.mark.asyncio
async def test_signal_handler_no_reentry():
    """Firing the exit handler twice must run only one shutdown (AR-033)."""

    class CountingWorker(BasicAsyncWorker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.shutdown_calls = 0

        async def _shutdown(self):
            self.shutdown_calls += 1
            await super()._shutdown()

    worker = CountingWorker(service_name="test_service", version="1.0.0")

    await worker.start()
    for _ in range(50):
        if worker.state == ServiceStatus.RUNNING:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.RUNNING

    worker._request_exit()
    worker._request_exit()

    for _ in range(50):
        if worker.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.05)

    assert worker.state == ServiceStatus.STOPPED
    assert worker.shutdown_calls == 1


@pytest.mark.asyncio
async def test_first_heartbeat_fires_promptly():
    """The first heartbeat must fire promptly, not after a full interval (AR-040)."""

    class HeartbeatWorker(BasicAsyncWorker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.heartbeat_count = 0

        async def heartbeat(self):
            self.heartbeat_count += 1

    worker = HeartbeatWorker(
        service_name="test_service",
        version="1.0.0",
        heartbeat_interval=5,
    )

    await worker.start()
    # The first beat must fire well before the 5s interval elapses.
    for _ in range(50):
        if worker.heartbeat_count >= 1:
            break
        await asyncio.sleep(0.05)

    assert worker.heartbeat_count >= 1

    await worker.stop()
    for _ in range(50):
        if worker.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.STOPPED
