"""Per-entry lease management for the Valkey transport (AR-073).

Extracts the lease subsystem that ``ValkeyWorker`` previously inlined: the
server-side-TTL key marking "a live worker owns this entry", its atomic
``SET ... NX`` acquisition, refresh, delete, and the derived TTL (AR-060). All
glide errors are logged and swallowed so a lease failure never breaks task
processing.
"""

import logging
from collections.abc import Callable, Iterable
from uuid import UUID

from ._glide import (
    ClientProvider,
    ConditionalChange,
    ExpirySet,
    ExpiryType,
    GlideConnectionError,
    GlideTimeoutError,
    RequestError,
)

# Per-entry lease TTL derivation (AR-060): the lease must outlive its refresh
# cadence (the watchdog tick) with margin, and must be at least as long as the
# status-key TTL rationale (2 x heartbeat_interval) so a slow-but-alive worker
# keeps its entry lease alive.
LEASE_TTL_HEARTBEAT_MULTIPLIER: int = 2
LEASE_TTL_WATCHDOG_MULTIPLIER: int = 3
MIN_TASK_LEASE_TTL_SECONDS: int = 1


def derive_task_lease_ttl(heartbeat_interval: float, watchdog_interval: float) -> int:
    """max(1, int(max(2*heartbeat_interval, 3*watchdog_interval))) (AR-060)."""
    return max(
        MIN_TASK_LEASE_TTL_SECONDS,
        int(
            max(
                LEASE_TTL_HEARTBEAT_MULTIPLIER * heartbeat_interval,
                LEASE_TTL_WATCHDOG_MULTIPLIER * watchdog_interval,
            )
        ),
    )


class TaskLeaseManager:
    """Owns the per-entry lease keys for one worker.

    The lease is a server-side-TTL key whose presence means "a live worker owns
    this entry". Recovery consults it (via :meth:`acquire`) before reclaiming a
    pending entry; the worker refreshes it over its ownership map and deletes it
    when an entry is acknowledged.
    """

    def __init__(
        self,
        *,
        service_name: str,
        consumer_name: str,
        lease_ttl: int,
        client_provider: ClientProvider,
        logger: logging.Logger,
        report_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._service_name = service_name
        self._consumer_name = consumer_name
        self._lease_ttl = lease_ttl
        self._client_provider = client_provider
        self._logger = logger
        self._report_failure = report_failure

    def key(self, task_id: UUID) -> str:
        """Return the Valkey key holding the per-entry lease for ``task_id``."""
        return f"scietex:{self._service_name}:lease:{task_id}"

    async def write(self, task_id: UUID) -> None:
        """Write/refresh the per-entry lease for ``task_id``.

        The lease is a server-side-TTL key whose presence means "a live worker
        owns this entry". Recovery consults it before reclaiming a pending entry.
        No-op when the client is not connected; glide errors are logged and
        swallowed so a lease failure never breaks task processing.
        """
        client = self._client_provider()
        if client is None:
            return
        try:
            await client.set(
                self.key(task_id),
                value=self._consumer_name.encode("utf-8"),
                expiry=ExpirySet(ExpiryType.SEC, self._lease_ttl),
            )
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self._logger.log(logging.WARNING, "Failed to write lease for task %s: %s", task_id, exc)
            if self._report_failure is not None:
                self._report_failure(exc)

    async def acquire(self, task_id: UUID) -> bool:
        """Atomically claim the lease for ``task_id``.

        Uses ``SET ... NX`` so that when two replicas run startup recovery
        concurrently, exactly one wins the claim and the other defers. Returns
        ``True`` when this worker claimed the lease, ``False`` when the key
        already exists — whether another holder owns it or this worker already
        holds it.
        """
        client = self._client_provider()
        if client is None:
            return True
        try:
            result = await client.set(
                self.key(task_id),
                value=self._consumer_name.encode("utf-8"),
                expiry=ExpirySet(ExpiryType.SEC, self._lease_ttl),
                conditional_set=ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
            )
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self._logger.log(logging.WARNING, "Failed to acquire lease for task %s: %s", task_id, exc)
            if self._report_failure is not None:
                self._report_failure(exc)
            return True
        return result is not None

    async def delete(self, task_id: UUID) -> None:
        """Delete the per-entry lease for ``task_id`` (no-op if absent)."""
        client = self._client_provider()
        if client is None:
            return
        try:
            await client.delete([self.key(task_id)])
        except (GlideConnectionError, RequestError, GlideTimeoutError) as exc:
            self._logger.log(logging.WARNING, "Failed to delete lease for task %s: %s", task_id, exc)
            if self._report_failure is not None:
                self._report_failure(exc)

    async def refresh(self, task_ids: Iterable[UUID]) -> None:
        """Renew the lease for every task id, over a snapshot of the iterable."""
        for task_id in list(task_ids):
            await self.write(task_id)
