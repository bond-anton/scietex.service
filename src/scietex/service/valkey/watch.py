"""Valkey polling backend for the worker watcher.

Enumerates worker heartbeats by ``SCAN``-ing the status keys
(``scietex:{service}:*:status``) and reading each one. This is the polling
implementation of :class:`~scietex.service.client.watcher.WatchBackend`; a
keyspace-notification backend can replace it later without changing the
watcher's contract.

Requires the optional ``valkey-glide`` dependency.
"""

from __future__ import annotations

from typing import cast

from ..client.backends import decode_heartbeat
from ..client.watcher import WatchBackend
from ..heartbeat import Heartbeat
from ._glide import GlideClient

__all__ = ["PollingBackend"]

#: SCAN page size hint. The server may return more or fewer; this only bounds
#: how much work one round trip does.
_SCAN_COUNT: int = 100


class PollingBackend(WatchBackend):
    """Feeds a watcher by SCAN-ing the Valkey status keys.

    Args:
        client: A connected ``GlideClient``.
        service_name: The service whose workers to watch; scopes the SCAN
            pattern to ``scietex:{service}:*:status``.
    """

    def __init__(self, client: GlideClient, service_name: str) -> None:
        self._client = client
        self._pattern = f"scietex:{service_name}:*:status"

    async def poll(self) -> list[Heartbeat]:
        """SCAN the status keys and return every decodable heartbeat.

        A key that expires between the SCAN and the ``MGET`` is simply absent
        from the result, so a worker that died mid-poll is skipped rather than
        raising. Connection errors propagate to the watcher's caller, which
        owns the retry policy.

        Returns:
            The heartbeats currently stored, undecodable entries skipped.
        """
        keys = await self._scan_keys()
        if not keys:
            return []
        # glide's mget stub takes an invariant ``List[TEncodable]``; a
        # ``list[bytes]`` is valid at runtime but not assignable to it.
        values = await self._client.mget(cast("list[str | bytes | bytearray | memoryview]", keys))
        heartbeats = [decode_heartbeat(value) for value in values]
        return [heartbeat for heartbeat in heartbeats if heartbeat is not None]

    async def _scan_keys(self) -> list[bytes]:
        """Collect every key matching the status pattern via a full SCAN."""
        keys: list[bytes] = []
        cursor: bytes = b"0"
        while True:
            # glide types scan's result as a flat union list, but the wire shape
            # is ``[cursor, keys]``; unpack positionally and narrow each part.
            result = await self._client.scan(cursor, match=self._pattern, count=_SCAN_COUNT)
            next_cursor = cast(bytes, result[0])
            page = cast(list[bytes], result[1])
            keys.extend(page)
            if next_cursor == b"0":
                return keys
            cursor = next_cursor

    async def close(self) -> None:
        """No-op: the backend does not own the client it reads through."""
