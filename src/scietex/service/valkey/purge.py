"""Operational utility to purge a Valkey task stream.

Provides :func:`purge_task_stream` — a standalone consumer/operator utility
that reads, acknowledges, and deletes every entry in a task stream. It is
independent of :class:`~scietex.service.valkey.worker.ValkeyWorker`
so an operator can clear a stream without running a worker.

Requires the optional ``valkey-glide`` dependency.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._glide import GlideClient

__all__ = ["purge_task_stream", "PurgeResult"]


@dataclass(frozen=True)
class PurgeResult:
    """Outcome of :func:`purge_task_stream`.

    Args:
        entries_purged: Number of stream entries deleted via ``XDEL``.
        errors: Descriptions of failures encountered. Empty means the purge
            completed without error.
    """

    entries_purged: int = 0
    errors: tuple[str, ...] = ()


async def purge_task_stream(
    client: GlideClient,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    logger: logging.Logger | None = None,
) -> PurgeResult:
    """Purge all pending and unacknowledged entries from a Valkey task stream.

    Reads and acknowledges every entry in the stream via ``XREADGROUP`` (both
    pending and unclaimed), then deletes them with ``XDEL``. Also purges any
    remaining entries via ``XREAD``.

    Args:
        client: An open ``GlideClient`` connected to the Valkey server.
        stream_name: The task stream name (e.g. ``scietex:{service}:tasks``).
        group_name: The consumer group name (e.g. ``scietex:{service}:task_group``).
        consumer_name: The consumer name used for ``XREADGROUP`` reads.
        logger: Optional logger for progress/error messages. If ``None``, a
            module-level logger is used.

    Returns:
        A :class:`PurgeResult` reporting how many entries were deleted and any
        error descriptions. ``errors`` is empty on a full success. Failures are
        also logged (never raised), so callers that ignore the return value
        still see the log output.
    """
    log = logger or logging.getLogger(__name__)
    entries_purged = 0
    try:
        # Entries already delivered to the group (pending + delivered).
        entries_purged += await _purge_group_entries(client, stream_name, group_name, consumer_name, "0-0")
        # Entries not yet delivered to the group.
        entries_purged += await _purge_group_entries(client, stream_name, group_name, consumer_name, ">")
        # Entries in the stream the group never saw.
        entries_purged += await _purge_stream_entries(client, stream_name)
        log.log(logging.INFO, "Purged %d task entries from Valkey", entries_purged)
    except Exception as exc:
        log.log(logging.ERROR, "Failed to purge tasks from Valkey: %s", exc)
        return PurgeResult(entries_purged=entries_purged, errors=(str(exc),))
    return PurgeResult(entries_purged=entries_purged)


async def _purge_group_entries(
    client: GlideClient,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    start: str,
) -> int:
    """Read, acknowledge, and delete group entries from a task stream.

    Reads entries via ``XREADGROUP`` from ``start``, acknowledges them with
    ``XACK`` so they leave the pending list, then deletes them with ``XDEL``.
    Loops until ``XREADGROUP`` returns no more entries.
    """
    purged = 0
    while True:
        res = await client.xreadgroup({stream_name: start}, group_name, consumer_name)
        entry_ids = _stream_entry_ids(res, stream_name)
        if not entry_ids:
            return purged
        await client.xack(stream_name, group_name, entry_ids)
        await client.xdel(stream_name, entry_ids)
        purged += len(entry_ids)


async def _purge_stream_entries(client: GlideClient, stream_name: str) -> int:
    """Delete every remaining entry in a task stream.

    Reads all stream entries via ``XREAD`` (independent of the consumer group)
    and deletes them with ``XDEL``. Loops until ``XREAD`` returns no more
    entries.
    """
    purged = 0
    while True:
        res = await client.xread({stream_name: "0-0"})
        entry_ids = _stream_entry_ids(res, stream_name)
        if not entry_ids:
            return purged
        await client.xdel(stream_name, entry_ids)
        purged += len(entry_ids)


def _stream_entry_ids(res, stream_name: str) -> list[str | bytes | bytearray | memoryview]:
    """Extract stream entry ids from an XREADGROUP/XREAD result mapping."""
    if not res:
        return []
    entries = res[stream_name.encode("utf-8")]
    return list(entries.keys()) if entries else []
