"""Tests for the producer subprocess: command round-trips and lifecycle.

These spawn real child processes, so they are slower than the in-process
producer tests. They cover the IPC contract the app depends on: a command is
applied in the child and its effect is visible in the parent's snapshot, and the
child is reaped on stop.
"""

import asyncio

import pytest

from examples.textual.producer_process import ProducerProcess

#: A spawned child needs a moment to import the example package and start its
#: loop before it can answer a command.
STARTUP_DELAY = 1.5


async def _started(task_name: str = "fast_task") -> ProducerProcess:
    """A producer whose child is spawned and ready to answer commands."""
    producer = ProducerProcess(task_name)
    producer.start_process()
    await asyncio.sleep(STARTUP_DELAY)
    return producer


@pytest.mark.asyncio
async def test_child_starts_and_reports_a_snapshot():
    producer = await _started()
    try:
        assert producer._process is not None
        assert producer._process.is_alive()
        snapshot = producer.snapshot()
        assert snapshot.emitted == 0
        assert snapshot.running is False
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_start_process_is_idempotent():
    producer = await _started()
    try:
        first = producer._process
        producer.start_process()
        assert producer._process is first
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_broker_switch_round_trips():
    producer = await _started()
    try:
        producer.set_valkey_enabled(True)
        assert producer.valkey_enabled is True
        assert producer.snapshot().valkey_enabled is True
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_interval_and_batch_round_trip():
    producer = await _started()
    try:
        producer.set_interval_ms(250)
        producer.set_batch_size(7)
        assert producer._interval_ms == 250
        assert producer.batch_size == 7
        assert producer.snapshot().batch_size == 7
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_emit_publishes_a_batch():
    producer = await _started()
    try:
        producer.set_valkey_enabled(True)
        producer.set_batch_size(5)
        await producer.emit()
        assert producer.snapshot().emitted == 5
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_emit_with_no_broker_is_a_noop():
    producer = await _started()
    try:
        await producer.emit()
        assert producer.snapshot().emitted == 0
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_interval_loop_advances_the_counter():
    producer = await _started()
    try:
        producer.set_valkey_enabled(True)
        producer.set_interval_ms(20)
        producer.set_batch_size(2)
        producer.start()
        await asyncio.sleep(0.5)
        assert producer.snapshot().running is True
        assert producer.snapshot().emitted > 0
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_stop_loop_keeps_the_process_alive():
    producer = await _started()
    try:
        producer.set_valkey_enabled(True)
        producer.start()
        await asyncio.sleep(0.3)
        await producer.stop_loop()
        assert producer.snapshot().running is False
        assert producer._process is not None
        assert producer._process.is_alive()
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_stop_reaps_the_child():
    producer = await _started()
    process = producer._process
    await producer.stop()
    assert process is not None
    assert not process.is_alive()
    assert producer._process is None


@pytest.mark.asyncio
async def test_stop_is_idempotent():
    producer = await _started()
    await producer.stop()
    await producer.stop()
    assert producer._process is None


@pytest.mark.asyncio
async def test_commands_after_stop_are_ignored():
    producer = await _started()
    await producer.stop()
    # No child to serve the command; the handle must not raise or block.
    producer.set_valkey_enabled(True)
    await producer.emit()
    assert producer.snapshot().emitted == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("task_name", ["fast_task", "slow_task"])
async def test_task_name_is_preserved(task_name):
    producer = await _started(task_name)
    try:
        assert producer.task_name == task_name
    finally:
        await producer.stop()
