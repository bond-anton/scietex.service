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

from ._glide import ClientProvider


class ValkeyConfigSource:
    """``ConfigSource`` over a durable Valkey key: GET/SET the desired state.

    ``load`` performs a live ``GET`` of the key and returns ``None`` when the
    key is absent, so the reloader falls back to the local/default config.
    Connection errors are not swallowed: they propagate to the reloader, which
    maps them to ``CONFIG_SOURCE_UNAVAILABLE``. ``store`` writes an envelope
    back with ``SET`` (used by ``config:store`` targeting ``remote``).

    The client is late-bound through ``client_provider`` so a reconnect that
    swaps the underlying ``GlideClient`` is picked up on the next call (AR-103).
    """

    def __init__(self, *, client_provider: ClientProvider, key: str, logger: logging.Logger) -> None:
        self._client_provider = client_provider
        self._key = key
        self._logger = logger

    async def load(self) -> bytes | None:
        """Read the desired-state envelope, or ``None`` when the key is absent."""
        client = self._client_provider()
        if client is None:
            return None
        return await client.get(self._key)

    async def store(self, envelope: bytes) -> None:
        """Write the desired-state envelope back to the durable key."""
        client = self._client_provider()
        if client is None:
            raise RuntimeError("Valkey client is not connected; cannot store config")
        await client.set(self._key, value=envelope)
