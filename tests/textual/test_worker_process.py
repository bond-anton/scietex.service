"""Tests for the worker subprocess: snapshot, identity, metrics, log shipping, and lifecycle.

These spawn real child processes, so they are slower than the in-process tests.
They cover the IPC contract the app depends on: the child constructs the worker
but never starts it, so no broker is required, every read comes from a snapshot
the child pushed, and the child is reaped on ``exit``.

``WorkerProcess.stop`` only stops the worker inside the already-alive child, so
reaping happens through ``exit``, which is the teardown call in every test's
``finally``.
"""

import asyncio

import pytest

from examples.textual.worker_process import LogRecordData, WorkerProcess, WorkerSnapshot
from scietex.service.basic_worker import ServiceStatus
from scietex.service.task_metrics import TaskMetricsSnapshot

#: A spawned child needs a moment to import the example package, build the
#: worker, and push its first snapshot before it can answer a read.
STARTUP_DELAY = 1.5


async def _started(kind: str = "valkey") -> WorkerProcess:
    """A worker whose child is spawned and has pushed its first snapshot."""
    process = WorkerProcess(kind)
    process.start_process()
    await asyncio.sleep(STARTUP_DELAY)
    return process


@pytest.mark.asyncio
async def test_child_starts_and_reports_a_snapshot():
    process = await _started()
    try:
        assert process._process is not None
        assert process._process.is_alive()
        snapshot = process.snapshot()
        assert isinstance(snapshot, WorkerSnapshot)
        # The child builds the worker but never calls start(), so it reports
        # the terminal STOPPED state rather than RUNNING.
        assert snapshot.state == ServiceStatus.STOPPED.value
        assert snapshot.exited is False
    finally:
        await process.exit()


@pytest.mark.asyncio
async def test_start_process_is_idempotent():
    process = await _started()
    try:
        first = process._process
        process.start_process()
        assert process._process is first
    finally:
        await process.exit()


@pytest.mark.asyncio
@pytest.mark.parametrize(("kind", "label"), [("valkey", "Valkey"), ("mqtt", "MQTT")])
async def test_identity_reports_the_kind_label(kind, label):
    process = await _started(kind)
    try:
        # The identity is captured from the child's first snapshot, so drain it.
        process.snapshot()
        assert process.identity.kind_label == label
    finally:
        await process.exit()


@pytest.mark.asyncio
async def test_instance_id_is_populated():
    process = await _started()
    try:
        # The instance id rides the child's first snapshot, so drain it first.
        process.snapshot()
        assert process.instance_id != ""
        assert process.identity.instance_id == process.instance_id
    finally:
        await process.exit()


@pytest.mark.asyncio
async def test_metrics_surface_is_readable():
    process = await _started()
    try:
        # Metrics are rebuilt from the latest snapshot, so drain it first.
        process.snapshot()
        metrics = process.task_metrics()
        assert isinstance(metrics, TaskMetricsSnapshot)
        assert isinstance(process.max_concurrent_tasks, int)
        assert process.max_concurrent_tasks > 0
        assert isinstance(process.queue_size, int)
        assert process.queue_size > 0
        assert isinstance(process.failed_managers, tuple)
    finally:
        await process.exit()


@pytest.mark.asyncio
async def test_transport_health_is_reported():
    process = await _started("valkey")
    try:
        process.snapshot()
        health = process.transport_health
        # A transport-backed worker always has a supervisor. Before start() no
        # connection has been established, so assert the object exists and its
        # fields are booleans, not that it is connected.
        assert health is not None
        assert isinstance(health.connected, bool)
        assert isinstance(health.degraded, bool)
    finally:
        await process.exit()


@pytest.mark.asyncio
async def test_exit_reaps_the_child():
    process = await _started()
    child = process._process
    await process.exit()
    assert child is not None
    assert not child.is_alive()
    assert process._process is None
    # The child pushes a terminal exited=True snapshot before it leaves; drain
    # it so the exited flag surfaces.
    process.snapshot()
    assert process.exited is True


@pytest.mark.asyncio
async def test_stop_is_idempotent():
    process = await _started()
    try:
        # stop() asks the child to stop its worker but keeps the process alive,
        # so the handle survives and a second stop is a no-op that must not raise.
        await process.stop()
        assert process._process is not None
        await process.stop()
        assert process._process is not None
    finally:
        await process.exit()


@pytest.mark.asyncio
async def test_commands_after_stop_are_ignored():
    process = await _started()
    # Reaping the child is the WorkerProcess analog of the producer's stop:
    # once it is gone, start()/stop() hit the liveness guard in _send and
    # return without sending a command, so they must not raise or block. Using
    # stop() here would keep the child alive and make start() connect to a
    # broker, which these tests avoid.
    await process.exit()
    await process.start()
    await process.stop()
    assert process._process is None


@pytest.mark.asyncio
async def test_log_records_cross_the_boundary():
    process = await _started()
    try:
        # The child runs its logger at INFO, so trigger an INFO record: start()
        # logs the startup sequence even when the broker is unreachable, which
        # keeps this test broker-free.
        await process.start()
        deadline = asyncio.get_running_loop().time() + 2.0
        records = process.drain_logs()
        while not records and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
            records = process.drain_logs()
        assert records
        for record in records:
            assert isinstance(record, LogRecordData)
            assert isinstance(record.message, str)
    finally:
        await process.exit()


class _FakeSpawnedProcess:
    """Stand-in for a spawned child: ``start`` is a no-op, so nothing runs."""

    def start(self) -> None:
        return None


def test_memory_flag_is_stored_and_forwarded(monkeypatch):
    process = WorkerProcess("mqtt", memory=True)
    assert process._memory is True
    captured: list[tuple] = []

    def _fake_process(*, target, args, daemon):
        captured.append(args)
        return _FakeSpawnedProcess()

    # Intercept the spawn so the args tuple is inspected before a real child
    # launches; a spawned ``SpawnProcess`` drops ``_args`` once it starts, so it
    # cannot be read after ``start_process`` returns.
    monkeypatch.setattr(process._ctx, "Process", _fake_process)
    process.start_process()
    # ``memory`` rides right after ``log_level`` in the child's positional args.
    assert captured[0][2] is True


def test_memory_flag_defaults_to_false():
    assert WorkerProcess("valkey")._memory is False


def test_broker_logging_flag_is_stored_and_forwarded(monkeypatch):
    process = WorkerProcess("mqtt", broker_logging=True)
    assert process._broker_logging is True
    captured: list[tuple] = []

    def _fake_process(*, target, args, daemon):
        captured.append(args)
        return _FakeSpawnedProcess()

    # Intercept the spawn so the args tuple is inspected before a real child
    # launches; a spawned ``SpawnProcess`` drops ``_args`` once it starts, so it
    # cannot be read after ``start_process`` returns.
    monkeypatch.setattr(process._ctx, "Process", _fake_process)
    process.start_process()
    # ``broker_logging`` rides right after ``memory`` in the child's positional args.
    assert captured[0][3] is True


def test_broker_logging_flag_defaults_to_false():
    assert WorkerProcess("valkey")._broker_logging is False
