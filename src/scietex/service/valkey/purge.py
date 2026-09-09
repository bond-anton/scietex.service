"""Operational utility to purge a Valkey task stream.

Provides :func:`purge_task_stream` — a standalone consumer/operator utility
that reads, acknowledges, and deletes every entry in a task stream. It is
independent of :class:`~scietex.service.valkey.worker.ValkeyWorker`
so an operator can clear a stream without running a worker.

Requires the optional ``valkey-glide`` dependency.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._glide import GlideClient

__all__ = ["purge_task_stream"]


async def purge_task_stream(
    client: GlideClient,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    logger: logging.Logger | None = None,
) -> None:
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
        None. Logs a confirmation message on success or an error description
        on failure.
    """
    log = logger or logging.getLogger(__name__)
    try:
        # Entries already delivered to the group (pending + delivered).
        await _purge_group_entries(client, stream_name, group_name, consumer_name, "0-0")
        # Entries not yet delivered to the group.
        await _purge_group_entries(client, stream_name, group_name, consumer_name, ">")
        # Entries in the stream the group never saw.
        await _purge_stream_entries(client, stream_name)
        log.log(logging.INFO, "All pending tasks purged from Valkey")
    except Exception as exc:
        log.log(logging.ERROR, "Failed to purge tasks from Valkey: %s", exc)


async def _purge_group_entries(
    client: GlideClient,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    start: str,
) -> None:
    """Read, acknowledge, and delete group entries from a task stream.

    Reads entries via ``XREADGROUP`` from ``start``, acknowledges them with
    ``XACK`` so they leave the pending list, then deletes them with ``XDEL``.
    Loops until ``XREADGROUP`` returns no more entries.
    """
    while True:
        res = await client.xreadgroup({stream_name: start}, group_name, consumer_name)
        entry_ids = _stream_entry_ids(res, stream_name)
        if not entry_ids:
            return
        await client.xack(stream_name, group_name, entry_ids)
        await client.xdel(stream_name, entry_ids)


async def _purge_stream_entries(client: GlideClient, stream_name: str) -> None:
    """Delete every remaining entry in a task stream.

    Reads all stream entries via ``XREAD`` (independent of the consumer group)
    and deletes them with ``XDEL``. Loops until ``XREAD`` returns no more entries.
    """
    while True:
        res = await client.xread({stream_name: "0-0"})
        entry_ids = _stream_entry_ids(res, stream_name)
        if not entry_ids:
            return
        await client.xdel(stream_name, entry_ids)


def _stream_entry_ids(res, stream_name: str) -> list[str | bytes | bytearray | memoryview]:
    """Extract stream entry ids from an XREADGROUP/XREAD result mapping."""
    if not res:
        return []
    entries = res[stream_name.encode("utf-8")]
    return list(entries.keys()) if entries else []
