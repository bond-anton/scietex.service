"""Centralized guarded import of the optional ``glide`` client.

All ``scietex.service.valkey`` modules import their ``glide`` names from here
so the ``try/except ImportError`` guard (and its install hint) lives in exactly
one place (AR-048). Importing this module raises ``ImportError`` with an
install hint when ``valkey-glide`` is absent.
"""

from collections.abc import Callable

try:
    from glide import (
        AdvancedGlideClientConfiguration,
        BackoffStrategy,
        ConditionalChange,
        ConfigurationError,
        ExpirySet,
        ExpiryType,
        GlideClient,
        GlideClientConfiguration,
        NodeAddress,
        ProtocolVersion,
        PubSubMsg,
        ReadFrom,
        RequestError,
        ServerCredentials,
        StreamAddOptions,
        StreamGroupOptions,
        StreamReadGroupOptions,
        StreamReadOptions,
        TlsAdvancedConfiguration,
        TrimByMaxLen,
    )
    from glide import (
        ConnectionError as GlideConnectionError,
    )
    from glide import (
        TimeoutError as GlideTimeoutError,
    )
except ImportError as e:
    raise ImportError(
        "The 'valkey-glide' module is required to use this feature. "
        "Please install it by running:\n\n    pip install scietex.service[valkey]\n"
    ) from e

#: Local alias, not a glide type: lets collaborators depend on a late-bound
#: client getter instead of the concrete ``GlideClient``.
ClientProvider = Callable[[], GlideClient | None]

__all__ = [
    "AdvancedGlideClientConfiguration",
    "BackoffStrategy",
    "ClientProvider",
    "ConditionalChange",
    "ConfigurationError",
    "GlideConnectionError",
    "ExpirySet",
    "ExpiryType",
    "GlideClient",
    "GlideClientConfiguration",
    "NodeAddress",
    "ProtocolVersion",
    "PubSubMsg",
    "ReadFrom",
    "RequestError",
    "ServerCredentials",
    "StreamAddOptions",
    "StreamGroupOptions",
    "StreamReadGroupOptions",
    "StreamReadOptions",
    "GlideTimeoutError",
    "TlsAdvancedConfiguration",
    "TrimByMaxLen",
]
