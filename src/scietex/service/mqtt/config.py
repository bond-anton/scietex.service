"""MQTT configuration schemas and YAML loader for ``scietex.service``."""

import ssl
from pathlib import Path
from typing import Literal

import msgspec

from .._validation import validate_range
from ..config import TaskProcessorConfig

MIN_MQTT_PORT: int = 1
MAX_MQTT_PORT: int = 65535
MIN_MQTT_KEEPALIVE: int = 0
MAX_MQTT_KEEPALIVE: int = 65535
MIN_SESSION_EXPIRY_INTERVAL: int = 0
MAX_SESSION_EXPIRY_INTERVAL: int = 4294967295
MIN_TASK_QOS: int = 0
MAX_TASK_QOS: int = 2
MIN_LOG_QOS: int = 0
MAX_LOG_QOS: int = 2
MIN_INBOX_TTL: int = 1
MAX_INBOX_TTL: int = 30 * 24 * 3600
DEFAULT_INBOX_TTL: int = 24 * 3600
MIN_STATUS_QOS: int = 0
MAX_STATUS_QOS: int = 2
MIN_STATUS_TTL: int = 1
MAX_STATUS_TTL: int = 30 * 24 * 3600
MIN_PROGRESS_QOS: int = 0
MAX_PROGRESS_QOS: int = 2
MIN_PROGRESS_MIN_INTERVAL: float = 0.0
MAX_PROGRESS_MIN_INTERVAL: float = 3600.0
MIN_PROGRESS_MIN_DELTA: float = 0.0
MAX_PROGRESS_MIN_DELTA: float = 100.0
MIN_CONFIG_QOS: int = 0
MAX_CONFIG_QOS: int = 2
MIN_CONFIG_TTL: int = 1
MAX_CONFIG_TTL: int = 30 * 24 * 3600
MIN_CONTROL_QOS: int = 0
MAX_CONTROL_QOS: int = 2


class MqttConfig(msgspec.Struct, frozen=True):
    """Immutable MQTT connection configuration.

    Maps to the ``aiomqtt`` v2.5.1 ``Client`` scalar options, in the same
    spirit as :class:`~scietex.service.valkey.config.ValkeyConfig`. MQTT 5 is
    the target protocol, so the session fields are the MQTT-5 ones
    (``clean_start``, ``session_expiry_interval``) rather than the 3.1.1
    ``clean_session``.

    Args:
        host: Hostname or IP address of the MQTT broker.
        port: Port number the broker listens on (``[1, 65535]``).
        username: Optional username for broker authentication.
        password: Optional password for broker authentication.
        identifier: Optional MQTT client identifier. ``None`` lets the broker
            assign one.
        keepalive: Keep-alive interval in seconds (``[0, 65535]``).
        clean_start: If ``True``, the broker discards any previous session
            state (MQTT 5 semantics).
        session_expiry_interval: Session expiry interval in seconds
            (``[0, 4294967295]``). ``0`` ends the session when the connection
            closes.
        transport: Underlying transport (``"tcp"``, ``"websockets"``, or
            ``"unix"``).
        timeout: Optional socket timeout in seconds.
        tls_insecure: If ``True``, skip certificate verification (not
            recommended for production).
        tls_context: Optional preconfigured :class:`ssl.SSLContext`.
            Runtime-only: it cannot be expressed in ``mqtt.yml`` and serializes
            as ``null``.
    """

    host: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    identifier: str | None = None
    keepalive: int = 60
    clean_start: bool = False
    session_expiry_interval: int = 0
    transport: Literal["tcp", "websockets", "unix"] = "tcp"
    timeout: float | None = None
    tls_insecure: bool | None = None
    tls_context: ssl.SSLContext | None = None


class MqttWorkerConfig(TaskProcessorConfig, frozen=True):
    """Immutable configuration for a :class:`~scietex.service.mqtt.worker.MqttWorker`.

    Extends :class:`~scietex.service.config.TaskProcessorConfig` with the
    MQTT-specific fields. Its ``mqtt_config`` field references the optional
    :class:`MqttConfig` type, which is why this struct lives here rather than
    in the always-imported core :mod:`scietex.service.config` module.

    Args:
        mqtt_config: A :class:`MqttConfig` schema. ``None`` means the worker
            reads ``mqtt.yml`` from its config directory.
        task_topic: MQTT topic tasks are consumed from. ``{service}`` is
            replaced with the service name.
        task_qos: QoS level for task messages (``[0, 2]``).
        inbox_backend: Durable inbox backend. ``"file"`` persists entries to
            disk (at-least-once); ``"memory"`` and its alias ``"none"`` buffer
            entries in process only (at-most-once).
            ``"none"`` is the explicit at-most-once opt-out.
        inbox_path: Optional path to the inbox store. ``None`` derives it from
            the config directory.
        inbox_ttl: TTL in seconds for inbox entries and tombstones
            (``[1, 2592000]``), defaulting to one day. ``None`` disables expiry
            (an explicit unbounded-growth opt-out).
        log_topic: MQTT topic worker logs are published to. ``{service}`` is
            replaced with the service name.
        log_qos: QoS level for log messages (``[0, 2]``).
        log_retain: If ``True``, log messages are published with the retained
            flag.
        status_publish_enabled: Master switch for all status/progress
            publishing. ``False`` restores the pre-addendum no-op behavior.
        status_topic_prefix: Prefix for the per-task status/progress topics.
            ``{service}`` is substituted at construction.
        status_qos: QoS level for ``TaskStatus`` publishes (``[0, 2]``).
        status_ttl: MQTT 5 message-expiry interval in seconds applied to every
            retained ``TaskStatus`` publish (``[1, 2592000]``). ``None``
            disables expiry.
        progress_qos: QoS level for ``TaskProgress`` publishes (``[0, 2]``).
        progress_min_interval: Minimum seconds between progress publishes
            (``[0.0, 3600.0]``). ``0`` disables the interval threshold.
        progress_min_delta: Minimum absolute progress change that forces a
            publish (``[0.0, 100.0]``). ``0`` disables the delta threshold.
        config_topic: Retained desired-state topic for remote config.
            ``{service}`` is replaced with the service name.
        config_qos: QoS level for config-topic publishes and the subscription
            (``[0, 2]``).
        config_ttl: MQTT 5 message-expiry interval in seconds applied to the
            retained config publish (``[1, 2592000]``). ``None`` disables
            expiry.
        control_topic: Per-worker directed control topic. Both ``{service}`` and
            ``{instance_id}`` are replaced.
        control_broadcast_topic: Service-scoped broadcast control topic.
            ``{service}`` is replaced.
        control_qos: QoS level for control-topic publishes and subscriptions
            (``[0, 2]``).
        control_inbox_path: Optional path to the control inbox store. ``None``
            derives it from the config directory (a sibling of the data inbox).
    """

    mqtt_config: "MqttConfig | None" = None
    task_topic: str = "scietex/{service}/tasks"
    task_qos: int = 2
    inbox_backend: Literal["file", "memory", "none"] = "file"
    inbox_path: str | None = None
    inbox_ttl: int | None = DEFAULT_INBOX_TTL
    log_topic: str = "scietex/{service}/log"
    log_qos: int = 0
    log_retain: bool = False
    status_publish_enabled: bool = True
    status_topic_prefix: str = "scietex/{service}/tasks"
    status_qos: int = 1
    status_ttl: int | None = 86400
    progress_qos: int = 0
    progress_min_interval: float = 1.0
    progress_min_delta: float = 0.0
    config_topic: str = "scietex/{service}/config"
    config_qos: int = 1
    config_ttl: int | None = 86400
    control_topic: str = "scietex/{service}/workers/{instance_id}/control"
    control_broadcast_topic: str = "scietex/{service}/control"
    control_qos: int = 1
    control_inbox_path: str | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        validate_range(self.task_qos, "task_qos", minimum=MIN_TASK_QOS, maximum=MAX_TASK_QOS)
        validate_range(self.log_qos, "log_qos", minimum=MIN_LOG_QOS, maximum=MAX_LOG_QOS)
        validate_range(self.inbox_ttl, "inbox_ttl", minimum=MIN_INBOX_TTL, maximum=MAX_INBOX_TTL)
        validate_range(self.status_qos, "status_qos", minimum=MIN_STATUS_QOS, maximum=MAX_STATUS_QOS)
        validate_range(self.status_ttl, "status_ttl", minimum=MIN_STATUS_TTL, maximum=MAX_STATUS_TTL)
        validate_range(self.progress_qos, "progress_qos", minimum=MIN_PROGRESS_QOS, maximum=MAX_PROGRESS_QOS)
        validate_range(
            self.progress_min_interval,
            "progress_min_interval",
            minimum=MIN_PROGRESS_MIN_INTERVAL,
            maximum=MAX_PROGRESS_MIN_INTERVAL,
        )
        validate_range(
            self.progress_min_delta,
            "progress_min_delta",
            minimum=MIN_PROGRESS_MIN_DELTA,
            maximum=MAX_PROGRESS_MIN_DELTA,
        )
        validate_range(self.config_qos, "config_qos", minimum=MIN_CONFIG_QOS, maximum=MAX_CONFIG_QOS)
        validate_range(self.config_ttl, "config_ttl", minimum=MIN_CONFIG_TTL, maximum=MAX_CONFIG_TTL)
        validate_range(self.control_qos, "control_qos", minimum=MIN_CONTROL_QOS, maximum=MAX_CONTROL_QOS)
        if self.mqtt_config is not None:
            validate_range(
                self.mqtt_config.port,
                "mqtt_config.port",
                minimum=MIN_MQTT_PORT,
                maximum=MAX_MQTT_PORT,
            )
            validate_range(
                self.mqtt_config.keepalive,
                "mqtt_config.keepalive",
                minimum=MIN_MQTT_KEEPALIVE,
                maximum=MAX_MQTT_KEEPALIVE,
            )
            validate_range(
                self.mqtt_config.session_expiry_interval,
                "mqtt_config.session_expiry_interval",
                minimum=MIN_SESSION_EXPIRY_INTERVAL,
                maximum=MAX_SESSION_EXPIRY_INTERVAL,
            )


def _encode_mqtt_config_value(obj: object) -> object:
    """``msgspec.yaml.encode`` hook: map runtime-only values to ``null``.

    ``MqttConfig.tls_context`` is an :class:`ssl.SSLContext` (and a callable,
    like a future parse callback, would be too) that has no ``mqtt.yml``
    representation, so encoding it would raise ``TypeError``. This hook drops
    both so a config with a populated context still round-trips through YAML
    (the context is expected to be lost on decode).
    """
    if callable(obj):
        return None
    if isinstance(obj, ssl.SSLContext):
        return None
    raise TypeError(f"Cannot encode {type(obj).__name__} to YAML")


def read_mqtt_config(conf_dir: Path | None, *, create_default: bool = True) -> MqttConfig:
    """Read MQTT configuration from a YAML file in the given config directory.

    The ``mqtt.yml`` file (and, when missing, its config directory) is only
    created when ``create_default=True`` (the default), which is the deliberate
    write-capable bootstrap path used on worker first-run. When
    ``create_default=False`` the read is write-free: a missing file or directory
    raises a ``RuntimeError`` instead of writing defaults. If the file exists but
    cannot be parsed, a ``RuntimeError`` is raised and the file is left untouched
    regardless of ``create_default``.

    Args:
        conf_dir: Path to the configuration directory.
        create_default: Whether to create the config directory and write a
            default ``mqtt.yml`` when missing. Default ``True``.

    Returns:
        A ``MqttConfig`` instance loaded from ``mqtt.yml`` or with
        default values if the file was missing (only when ``create_default=True``).

    Raises:
        RuntimeError: If ``conf_dir`` is ``None``, not a directory, if the
            ``mqtt.yml`` file is present but invalid, or if a missing file or
            directory is encountered while ``create_default=False``.
    """
    if not isinstance(conf_dir, Path):
        raise RuntimeError("Configuration dir was not set!")
    if not conf_dir.exists():
        if create_default:
            try:
                conf_dir.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                raise RuntimeError(f"Failed to create configuration directory {conf_dir}!") from exc
        else:
            raise RuntimeError(
                f"Configuration directory {conf_dir} does not exist and create_default=False (no default generated)."
            )
    elif not conf_dir.is_dir():
        raise RuntimeError(f"Provided configuration directory path {conf_dir} is not a directory!")
    mqtt_yml = conf_dir.joinpath("mqtt.yml")
    if not mqtt_yml.exists():
        if create_default:
            mqtt_config = MqttConfig()
            with open(mqtt_yml, "wb") as f:
                f.write(msgspec.yaml.encode(mqtt_config, enc_hook=_encode_mqtt_config_value))
            return mqtt_config
        raise RuntimeError(
            f"MQTT configuration file {mqtt_yml} does not exist and create_default=False "
            "(pass create_default=True to generate defaults)."
        )
    try:
        with open(mqtt_yml, "rb") as f:
            return msgspec.yaml.decode(f.read(), type=MqttConfig, strict=True)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to parse MQTT configuration file {mqtt_yml}. Fix the file or remove it to regenerate defaults."
        ) from exc


__all__ = ["MqttConfig", "MqttWorkerConfig", "read_mqtt_config"]
