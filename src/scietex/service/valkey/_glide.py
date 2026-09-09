"""Centralized guarded import of the optional ``glide`` client.

All ``scietex.service.valkey`` modules import their ``glide`` names from here
so the ``try/except ImportError`` guard (and its install hint) lives in exactly
one place (AR-048). Importing this module raises ``ImportError`` with an
install hint when ``valkey-glide`` is absent.
"""

try:
    from glide import (
        AdvancedGlideClientConfiguration,
        BackoffStrategy,
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
        StreamGroupOptions,
        StreamReadGroupOptions,
        TlsAdvancedConfiguration,
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

__all__ = [
    "AdvancedGlideClientConfiguration",
    "BackoffStrategy",
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
    "StreamGroupOptions",
    "StreamReadGroupOptions",
    "GlideTimeoutError",
    "TlsAdvancedConfiguration",
]
