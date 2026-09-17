"""MQTT logging-handler configuration translator.

Translates a typed :class:`~scietex.service.mqtt.config.MqttConfig` into the
plain scalar ``dict`` the external
:class:`~scietex.logging.handler.mqtt.AsyncMqttHandler` expects via its
``mqtt_config=`` keyword.
"""

from .config import MqttConfig


def logging_handler_config(mqtt_config: MqttConfig) -> dict:
    """Translate a typed ``MqttConfig`` into the logging handler's config dict.

    The external :class:`~scietex.logging.handler.mqtt.AsyncMqttHandler` builds
    its own connection from a plain ``dict`` of scalar ``aiomqtt`` options
    passed via ``mqtt_config=``. That dict schema uses the MQTT 3.1.1-style
    ``clean_session`` field, while :class:`MqttConfig` models the MQTT 5
    ``clean_start``/``session_expiry_interval`` pair. Those two session fields
    are deliberately omitted: the log handler's connection is independent and
    uses the handler's own session defaults, while the worker's task connection
    uses the MQTT 5 session fields.
    """
    return {
        "host": mqtt_config.host,
        "port": mqtt_config.port,
        "username": mqtt_config.username,
        "password": mqtt_config.password,
        "identifier": mqtt_config.identifier,
        "keepalive": mqtt_config.keepalive,
        "transport": mqtt_config.transport,
        "timeout": mqtt_config.timeout,
        "tls_insecure": mqtt_config.tls_insecure,
    }


__all__ = ["logging_handler_config"]
