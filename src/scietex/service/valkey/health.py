"""Connection-health supervision for the Valkey transport (AR-075).

Every glide failure site reports through :meth:`TransportHealth.report_failure`
(synchronous, non-blocking), which marks the connection degraded and requests a
reconnect. :meth:`TransportHealth.recover` is the single place a reconnect is
actually attempted: ``ValkeyWorker.watchdog`` and ``ValkeyTransport.fetch``
both funnel into it, so a burst of concurrent failures still produces exactly
one reconnect attempt, gated by a cooldown to prevent storms (e.g.
``lease.refresh`` looping over N tasks).

This module deliberately imports no ``glide`` types: a failure is unclassified
and every reported error requests a reconnect, with the cooldown — not error
classification — as the storm guard.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

DEFAULT_TRANSPORT_DOWN_THRESHOLD_SECONDS: float = 30.0


class TransportHealth:
    """Tracks connection health and owns the single reconnect path.

    ``report_failure`` only records state and sets a ``reconnect_requested``
    flag (it creates no background task), preserving the swallow-and-continue
    semantics of the hot paths and staying deterministic in tests. ``recover``
    is awaited explicitly by the watchdog and by ``fetch``'s error path, and
    deduplicates concurrent triggers behind an ``asyncio.Lock``.
    """

    def __init__(
        self,
        *,
        reconnect: Callable[[], Awaitable[None]],
        is_connected: Callable[[], bool],
        logger: logging.Logger,
        down_threshold: float = DEFAULT_TRANSPORT_DOWN_THRESHOLD_SECONDS,
        reconnect_cooldown: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._reconnect = reconnect
        self._is_connected = is_connected
        self._logger = logger
        self._down_threshold = down_threshold
        self._reconnect_cooldown = reconnect_cooldown
        self._clock = clock

        self._connected: bool = False
        self._degraded: bool = False
        self._last_error: str | None = None
        self._failure_count: int = 0
        # First unrecovered-failure timestamp (monotonic); None while healthy.
        self._down_since: float | None = None

        self._reconnect_requested: bool = False
        self._reconnect_in_flight: bool = False
        self._last_attempt: float | None = None
        self._reconnect_lock: asyncio.Lock = asyncio.Lock()
        self._reported_critical: bool = False

    @property
    def connected(self) -> bool:
        """Whether the transport believes it is currently connected."""
        return self._connected

    @property
    def degraded(self) -> bool:
        """Whether an unrecovered failure is still outstanding."""
        return self._degraded

    @property
    def last_error(self) -> str | None:
        """The most recent reported failure, or ``None`` when healthy."""
        return self._last_error

    @property
    def failure_count(self) -> int:
        """Number of failures reported since the last successful reconnect."""
        return self._failure_count

    @property
    def down_duration(self) -> float:
        """Seconds since the first unrecovered failure; ``0.0`` when healthy."""
        if self._down_since is None:
            return 0.0
        return self._clock() - self._down_since

    def mark_connected(self) -> None:
        """Clear degraded state: the transport is back up."""
        self._connected = True
        self._degraded = False
        self._last_error = None
        self._failure_count = 0
        self._down_since = None
        self._reconnect_requested = False
        self._reported_critical = False

    def mark_disconnected(self) -> None:
        """Record an intentional teardown; does not request a reconnect."""
        self._connected = False

    def report_failure(self, exc: BaseException) -> None:
        """Record a failure and request a reconnect (synchronous, non-blocking).

        Does not attempt the reconnect itself: hot paths (heartbeat, lease
        refresh over N tasks) stay swallow-and-continue, and the reconnect is
        performed once by :meth:`recover`.
        """
        self._degraded = True
        self._last_error = str(exc)
        self._failure_count += 1
        if self._down_since is None:
            self._down_since = self._clock()
        self._reconnect_requested = True

    async def recover(self) -> None:
        """The single reconnect owner.

        No-ops unless a reconnect is requested. Serializes callers behind an
        ``asyncio.Lock`` so concurrent triggers produce exactly one reconnect —
        a second caller waits on the lock, then sees the request cleared and
        returns. The cooldown skips a re-attempt within
        ``reconnect_cooldown`` seconds of the last attempt. On success the
        degraded state is cleared; on failure ``reconnect_requested``/``degraded``
        stay set so the next watchdog tick retries after the cooldown.
        """
        if not self._reconnect_requested:
            return
        async with self._reconnect_lock:
            # A concurrent recover() may have completed while we awaited the
            # lock; re-check the flag the lock was acquired to protect.
            if not self._reconnect_requested:
                return
            now = self._clock()
            if self._last_attempt is not None and now - self._last_attempt < self._reconnect_cooldown:
                return
            self._last_attempt = now
            self._reconnect_in_flight = True
            try:
                await self._reconnect()
            finally:
                self._reconnect_in_flight = False
            if not self._is_connected():
                # Reconnect failed: degraded and reconnect_requested remain set,
                # so the next watchdog tick retries after the cooldown.
                return
            self.mark_connected()

    def critical_report(self) -> str | None:
        """Return a CRITICAL message once per down episode, once past the threshold.

        A sustained outage therefore logs CRITICAL once (not every tick), and a
        new outage after a recovery logs again. Returns ``None`` while healthy,
        below threshold, or already reported for the current episode.
        """
        if self._reported_critical or self._down_since is None:
            return None
        duration = self.down_duration
        if duration < self._down_threshold:
            return None
        self._reported_critical = True
        return (
            f"Valkey connection down for {duration:.1f}s "
            f"({self._failure_count} failures, last error: {self._last_error})"
        )
