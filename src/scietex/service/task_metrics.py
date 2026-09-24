"""Task-processing throughput metrics: completion rate over a sliding window."""

import time
from collections import deque
from collections.abc import Callable

import msgspec


class TaskMetrics:
    """Counts task completions and derives a short sliding-window rate.

    Every terminal task (success, error, or cancellation) calls
    :meth:`record_completion`, which appends the current clock reading and
    evicts readings older than ``window`` seconds. :meth:`rate` reports
    ``completions_in_window / window`` — dividing by the fixed window rather
    than the elapsed span keeps the figure meaningful immediately after start
    and self-correcting as the window fills. :meth:`total` is the cumulative
    completion count since construction and is never evicted.
    """

    def __init__(self, window: float = 2.0, clock: Callable[[], float] = time.monotonic) -> None:
        self._window = window
        self._clock = clock
        self._completions: deque[float] = deque()
        self._total = 0

    def record_completion(self) -> None:
        """Record one terminal task at the current clock reading."""
        now = self._clock()
        self._completions.append(now)
        self._total += 1
        self._evict(now)

    def rate(self) -> float:
        """Completions per second over the window; ``0.0`` while empty."""
        now = self._clock()
        self._evict(now)
        if not self._completions:
            return 0.0
        return len(self._completions) / self._window

    def total(self) -> int:
        """Cumulative completions since construction (never evicted)."""
        return self._total

    def _evict(self, now: float) -> None:
        """Drop timestamps older than ``now - window`` from the left of the deque."""
        cutoff = now - self._window
        while self._completions and self._completions[0] < cutoff:
            self._completions.popleft()


class TaskMetricsSnapshot(msgspec.Struct, frozen=True):
    """Immutable snapshot of a processor's task-processing metrics.

    Args:
        queue_depth: Tasks waiting in the data-plane queue.
        running: Tasks currently being processed.
        rate: Completions per second over the sliding window.
        total: Cumulative completions since the processor was built.
    """

    queue_depth: int
    running: int
    rate: float
    total: int
