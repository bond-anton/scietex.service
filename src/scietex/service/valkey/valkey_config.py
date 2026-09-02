"""Valkey configuration schemas and YAML loader for ``scietex.service``.

Defines typed configuration structures (``ValkeyConfig``, ``ValkeyBaseConfig``,
``ValkeyAdvancedConfig``, etc.) that map to ``glide`` client configuration
objects. Includes ``read_valkey_config()`` for YAML file loading and
``generate_glide_config()`` for converting schemas to ``GlideClientConfiguration``.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec
from msgspec import field

try:
    from glide import (
        AdvancedGlideClientConfiguration,
        BackoffStrategy,
        ConfigurationError,
        GlideClientConfiguration,
        NodeAddress,
        ProtocolVersion,
        PubSubMsg,
        ReadFrom,
        ServerCredentials,
        TlsAdvancedConfiguration,
    )
except ImportError as e:
    raise ImportError(
        "The 'valkey-glide' module is required to use this feature. "
        "Please install it by running:\n\n    pip install scietex.service[valkey]\n"
    ) from e


class ValkeyNode(msgspec.Struct, frozen=True):
    """Represents a single Valkey server node.

    Args:
        host: Hostname or IP address of the node.
        port: Port number the node listens on.
    """

    host: str = "localhost"
    port: int = 6379


class ValkeyUserCredentials(msgspec.Struct, frozen=True):
    """Authentication credentials for a Valkey server.

    Args:
        username: User name for authentication.
        password: Password for authentication.
    """

    username: str
    password: str


class ValkeyBackoffStrategy(msgspec.Struct, frozen=True):
    """Exponential backoff strategy for Valkey reconnection attempts.

    Args:
        num_of_retries: Maximum number of reconnection attempts.
        factor: Multiplicative factor for backoff calculation.
        exponent_base: Base for the exponential backoff function.
        jitter_percent: Optional jitter percentage to avoid thundering herd.
    """

    num_of_retries: int
    factor: int
    exponent_base: int
    jitter_percent: int | None = None

    @property
    def reconnect_strategy(self) -> BackoffStrategy:
        """Create a :class:`~glide.BackoffStrategy` from these settings.

        Returns:
            A :class:`~glide.BackoffStrategy` instance configured with
            the same retry count, factor, exponent base, and jitter.
        """
        return BackoffStrategy(
            num_of_retries=self.num_of_retries,
            factor=self.factor,
            exponent_base=self.exponent_base,
            jitter_percent=self.jitter_percent,
        )


class ValkeyTlsAdvancedConfiguration(msgspec.Struct, frozen=True):
    """TLS configuration for Valkey connections.

    Args:
        use_insecure_tls: Skip certificate verification (not recommended
            for production).
        root_pem_cacerts: PEM-encoded CA certificates for custom trust
            store.
    """

    use_insecure_tls: bool = False
    root_pem_cacerts: str | None = None

    def to_tls_advanced_config(self) -> TlsAdvancedConfiguration:
        """Convert to a :class:`~glide.TlsAdvancedConfiguration` instance.

        Encodes ``root_pem_cacerts`` to bytes if provided.

        Returns:
            A :class:`~glide.TlsAdvancedConfiguration` with matching
            ``use_insecure_tls`` and ``root_pem_cacerts`` values.
        """
        return TlsAdvancedConfiguration(
            use_insecure_tls=self.use_insecure_tls,
            root_pem_cacerts=self.root_pem_cacerts.encode() if self.root_pem_cacerts else None,
        )


class ValkeyAdvancedConfig(msgspec.Struct, frozen=True):
    """Advanced connection settings for the Valkey client.

    Args:
        connection_timeout: Connection timeout in milliseconds.
        tcp_nodelay: Disable Nagle's algorithm for lower latency.
        tls_config: TLS configuration for encrypted connections.
    """

    connection_timeout: int | None = 10_000
    tcp_nodelay: bool | None = None
    tls_config: ValkeyTlsAdvancedConfiguration = ValkeyTlsAdvancedConfiguration()

    def to_advanced_config(self) -> AdvancedGlideClientConfiguration:
        """Convert to an :class:`~glide.AdvancedGlideClientConfiguration` instance.

        Recursively converts ``tls_config`` if present.

        Returns:
            An :class:`~glide.AdvancedGlideClientConfiguration` with
            matching ``connection_timeout``, ``tcp_nodelay``, and
            ``tls_config`` values.
        """
        return AdvancedGlideClientConfiguration(
            connection_timeout=self.connection_timeout,
            tcp_nodelay=self.tcp_nodelay,
            tls_config=self.tls_config.to_tls_advanced_config() if self.tls_config else None,
        )


class ValkeyBaseConfig(msgspec.Struct, frozen=True):
    """Basic Valkey connection configuration.

    Maps to ``glide`` client settings including addresses, credentials,
    TLS, read preferences, and reconnection behavior.

    Args:
        nodes: List of server node addresses.
        user_credentials: Authentication credentials.
        use_tls: Enable TLS encryption.
        request_timeout: Request timeout in milliseconds.
        database_id: Logical database index.
        client_name: Client identifier sent to the server.
        inflight_requests_limit: Maximum concurrent unacknowledged requests.
        client_az: Availability zone for cloud deployments.
        lazy_connect: Defer connection until first command.
        read_from: Read preference (``"PRIMARY"``, ``"PRIMARY_PREFERRED"``, etc.).
        backoff_strategy: Reconnection backoff configuration.
        protocol: Protocol version (``"RESP2"`` or ``"RESP3"``).
    """

    nodes: list[ValkeyNode] = field(default_factory=lambda: [ValkeyNode(host="localhost", port=6379)])
    user_credentials: ValkeyUserCredentials | None = None
    use_tls: bool = False
    request_timeout: int | None = 5_000
    database_id: int | None = None
    client_name: str | None = None
    inflight_requests_limit: int | None = None
    client_az: str | None = None
    lazy_connect: bool | None = None
    read_from: str = "PRIMARY"
    backoff_strategy: ValkeyBackoffStrategy | None = None
    protocol: str = "RESP3"

    @property
    def addresses(self) -> list[NodeAddress]:
        """Create :class:`~glide.NodeAddress` instances for all configured nodes.

        Returns:
            A list of :class:`~glide.NodeAddress` objects, one per node
            in ``self.nodes``.
        """
        return [NodeAddress(node.host, node.port) for node in self.nodes]

    @property
    def credentials(self) -> ServerCredentials | None:
        """Create :class:`~glide.ServerCredentials` from stored user credentials.

        Returns ``None`` if ``user_credentials`` is not set or if
        credential construction raises a :class:`~glide.ConfigurationError`.

        Returns:
            A :class:`~glide.ServerCredentials` instance, or ``None``.
        """
        if self.user_credentials:
            try:
                return ServerCredentials(
                    password=self.user_credentials.password,
                    username=self.user_credentials.username,
                )
            except ConfigurationError:
                return None
        return None

    @property
    def reconnect_strategy(self) -> BackoffStrategy | None:
        """Create a :class:`~glide.BackoffStrategy` from the configured backoff settings.

        Returns ``None`` if ``backoff_strategy`` is not set.

        Returns:
            A :class:`~glide.BackoffStrategy` instance, or ``None``.
        """
        if self.backoff_strategy:
            return self.backoff_strategy.reconnect_strategy
        return None


class ValkeyConfig(msgspec.Struct, frozen=True):
    """Top-level Valkey configuration combining base and advanced settings.

    Args:
        base_config: Basic connection parameters.
        advanced_config: Advanced connection settings.
    """

    base_config: ValkeyBaseConfig = ValkeyBaseConfig()
    advanced_config: ValkeyAdvancedConfig = ValkeyAdvancedConfig()


def read_valkey_config(conf_dir: Path | None) -> ValkeyConfig:
    """Read Valkey configuration from a YAML file in the given config directory.

    If the YAML file does not exist, creates it with default values.
    If parsing fails, returns a ``ValkeyConfig`` with defaults and writes
    the defaults to the file.

    Args:
        conf_dir: Path to the configuration directory.

    Returns:
        A ``ValkeyConfig`` instance loaded from ``valkey.yml`` or with
        default values if the file was missing or invalid.

    Raises:
        RuntimeError: If ``conf_dir`` is ``None`` or not a directory.
    """
    if isinstance(conf_dir, Path):
        if not conf_dir.exists():
            try:
                conf_dir.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                raise RuntimeError(f"Failed to create configuration directory {conf_dir}!") from exc
        elif not conf_dir.is_dir():
            raise RuntimeError(f"Provided configuration directory path {conf_dir} is not a directory!")
        valkey_yml = conf_dir.joinpath("valkey.yml")
    else:
        raise RuntimeError("Configuration dir was not set!")
    try:
        with open(valkey_yml, "rb") as f:
            valkey_config = msgspec.yaml.decode(f.read(), type=ValkeyConfig, strict=True)
    except Exception:
        valkey_config = ValkeyConfig()
        with open(valkey_yml, "wb") as f:
            f.write(msgspec.yaml.encode(valkey_config))
    return valkey_config


def generate_glide_config(
    valkey_config: ValkeyConfig,
    service_name: str,
    worker_id: str | int,
    listening: bool = False,
    parse_control_message: Callable[[PubSubMsg, Any], None] | None = None,
) -> GlideClientConfiguration:
    """Convert a ``ValkeyConfig`` schema into a ``GlideClientConfiguration``.

    Maps the typed configuration to ``glide`` client settings including
    addresses, credentials, TLS, read preferences, and optional PubSub
    subscriptions for broadcast messages.

    Args:
        valkey_config: The typed configuration schema.
        service_name: Service name used for PubSub channel names.
        worker_id: Worker identifier used for PubSub channel names.
        listening: If ``True``, subscribes to service-specific and
            broadcast channels.
        parse_control_message: Optional callback for PubSub messages.

    Returns:
        A fully configured ``GlideClientConfiguration`` instance.

    Raises:
        ValueError: If ``read_from`` or ``protocol`` contain invalid values.
    """
    pubsub_subscriptions = None
    if listening:
        pubsub_subscriptions = GlideClientConfiguration.PubSubSubscriptions(
            channels_and_patterns={
                GlideClientConfiguration.PubSubChannelModes.Exact: {
                    f"scietex:{service_name}:{worker_id}",
                    "scietex:broadcast",
                },
            },
            callback=parse_control_message,
            context=None,
        )
    try:
        read_from = ReadFrom[valkey_config.base_config.read_from]
    except KeyError as exc:
        raise ValueError(f"""
            Invalid read_from value in Valkey Config: {valkey_config.base_config.read_from}.
            Supported values are: {[e.name for e in ReadFrom]}.

            """) from exc
    try:
        protocol = ProtocolVersion[valkey_config.base_config.protocol]
    except KeyError as exc:
        raise ValueError(f"""
            Invalid protocol value in Valkey Config: {valkey_config.base_config.protocol}.
            Supported values are: {[e.name for e in ProtocolVersion]}.
            """) from exc
    client_config = GlideClientConfiguration(
        addresses=valkey_config.base_config.addresses,
        credentials=valkey_config.base_config.credentials,
        use_tls=valkey_config.base_config.use_tls,
        read_from=read_from,
        request_timeout=valkey_config.base_config.request_timeout,
        reconnect_strategy=valkey_config.base_config.reconnect_strategy,
        database_id=valkey_config.base_config.database_id,
        client_name=valkey_config.base_config.client_name,
        protocol=protocol,
        inflight_requests_limit=valkey_config.base_config.inflight_requests_limit,
        client_az=valkey_config.base_config.client_az,
        lazy_connect=valkey_config.base_config.lazy_connect,
        advanced_config=valkey_config.advanced_config.to_advanced_config() if valkey_config.advanced_config else None,
        pubsub_subscriptions=pubsub_subscriptions,
    )
    return client_config
