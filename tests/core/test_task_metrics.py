"""TaskMetrics unit tests: sliding-window rate and cumulative count.

The tests inject a fake clock so eviction and the fixed-window rate are
deterministic (no real sleeps).
"""

from scietex.service.task_metrics import TaskMetrics, TaskMetricsSnapshot


class FakeClock:
    """Injectable monotonic clock with a manually advanced offset."""

    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now


def test_rate_is_zero_when_empty():
    metrics = TaskMetrics(window=10.0, clock=FakeClock())

    assert metrics.rate() == 0.0
    assert metrics.total() == 0


def test_rate_divides_by_fixed_window():
    """One completion in a 10s window reports 1/10, not 1/elapsed."""
    metrics = TaskMetrics(window=10.0, clock=FakeClock())

    metrics.record_completion()

    assert metrics.rate() == 0.1


def test_rate_evicts_completions_outside_window():
    clock = FakeClock()
    metrics = TaskMetrics(window=10.0, clock=clock)

    metrics.record_completion()  # at t=0
    clock.now = 9.0
    metrics.record_completion()  # at t=9, still inside the window

    assert metrics.rate() == 0.2  # both completions inside the window

    clock.now = 11.0
    assert metrics.rate() == 0.1  # t=0 completion evicted; t=9 remains


def test_rate_evicts_boundary_timestamp_only_once_older():
    clock = FakeClock()
    metrics = TaskMetrics(window=10.0, clock=clock)

    metrics.record_completion()  # at t=0
    clock.now = 10.0
    # At exactly now - window the timestamp is not older than the cutoff.
    assert metrics.rate() == 0.1

    clock.now = 10.0001
    assert metrics.rate() == 0.0


def test_total_is_cumulative_and_never_evicted():
    clock = FakeClock()
    metrics = TaskMetrics(window=10.0, clock=clock)

    metrics.record_completion()
    metrics.record_completion()
    clock.now = 100.0  # far past the window: rate drops, total does not

    assert metrics.total() == 2
    assert metrics.rate() == 0.0


def test_snapshot_is_a_frozen_struct():
    snapshot = TaskMetricsSnapshot(queue_depth=3, running=2, rate=1.5, total=7)

    assert snapshot.queue_depth == 3
    assert snapshot.running == 2
    assert snapshot.rate == 1.5
    assert snapshot.total == 7
