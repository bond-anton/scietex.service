"""Valkey durable-key implementation of the core ``ConfigSource`` protocol.

The durable key ``scietex:{service}:config`` is the source of truth for remote
config, not PubSub. Valkey PubSub is at-most-once and not persisted — it is a
"something changed" notice, never a store — so it cannot answer "what is the
desired state now?" on startup or reconnect. The key is durable and
``GET``-able on demand, which is exactly what the
:class:`~scietex.service.config_reload.ConfigReloader` needs: ``load`` reads
the desired-state envelope, ``store`` writes the effective config back.
"""

import logging

from ._glide import GlideClient


class ValkeyConfigSource:
    """``ConfigSource`` over a durable Valkey key: GET/SET the desired state.

    ``load`` performs a live ``GET`` of the key and returns ``None`` when the
    key is absent, so the reloader falls back to the local/default config.
    Connection errors are not swallowed: they propagate to the reloader, which
    maps them to ``CONFIG_SOURCE_UNAVAILABLE``. ``store`` writes an envelope
    back with ``SET`` (used by ``config:store`` targeting ``remote``).
    """

    def __init__(self, *, client: GlideClient, key: str, logger: logging.Logger) -> None:
        self._client = client
        self._key = key
        self._logger = logger

    async def load(self) -> bytes | None:
        """Read the desired-state envelope, or ``None`` when the key is absent."""
        return await self._client.get(self._key)

    async def store(self, envelope: bytes) -> None:
        """Write the desired-state envelope back to the durable key."""
        await self._client.set(self._key, value=envelope)
