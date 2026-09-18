"""Centralized guarded import of the optional ``aiomqtt`` client.

All ``scietex.service.mqtt`` modules import their ``aiomqtt`` names from here
so the ``try/except ImportError`` guard (and its install hint) lives in exactly
one place (AR-048). Importing this module raises ``ImportError`` with an
install hint when ``aiomqtt`` is absent.
"""

try:
    import aiomqtt
    from aiomqtt import (
        Client,
        Message,
        MqttError,
        ProtocolVersion,
        TLSParameters,
        Topic,
    )
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties
except ImportError as e:
    raise ImportError(
        "The 'aiomqtt' module is required to use this feature. "
        "Please install it by running:\n\n    pip install scietex.service[mqtt]\n"
    ) from e

__all__ = [
    "Client",
    "Message",
    "MqttError",
    "PacketTypes",
    "Properties",
    "ProtocolVersion",
    "TLSParameters",
    "Topic",
    "aiomqtt",
]
