"""Watch backends: the delivery mechanisms feeding a :class:`WorkerWatcher`.

Each backend implements :class:`~scietex.service.client.watcher.WatchBackend`
and is imported lazily by its transport package, so the core client stays
importable without the optional extras:

- :class:`PollingBackend` — Valkey ``SCAN`` over the status keys. Requires
  ``valkey-glide``.
- :class:`SubscribeBackend` — MQTT wildcard subscription to the retained
  heartbeat topics. Requires ``aiomqtt``.

Both decode the shared :class:`~scietex.service.heartbeat.Heartbeat` payload, so
a client sees the same record regardless of which transport produced it.
"""

from __future__ import annotations

import msgspec

from ..heartbeat import Heartbeat

__all__ = ["decode_heartbeat"]


def decode_heartbeat(payload: bytes | None) -> Heartbeat | None:
    """Decode a heartbeat payload, returning ``None`` for anything unusable.

    A retained MQTT message is cleared with an empty payload, and a broker may
    hold a payload written by an older schema. Both are skipped rather than
    raising, so one bad entry cannot break a poll cycle.

    Args:
        payload: The raw msgpack payload, or ``None``.

    Returns:
        The decoded heartbeat, or ``None`` if it is absent or undecodable.
    """
    if not payload:
        return None
    try:
        return msgspec.msgpack.decode(payload, type=Heartbeat)
    except msgspec.ValidationError:
        # A pre-v5 heartbeat (no ``ttl``) or a foreign payload: skip it. The
        # producer's own TTL still bounds the broker-side entry.
        return None
