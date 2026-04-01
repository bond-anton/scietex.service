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
    """Valkey Node Schema."""

    host: str = "localhost"
    port: int = 6379


class ValkeyUserCredentials(msgspec.Struct, frozen=True):
    """Valkey Credentials Schema."""

    username: str
    password: str


class ValkeyBackoffStrategy(msgspec.Struct, frozen=True):
    """Valkey Backoff Strategy Schema."""

    num_of_retries: int
    factor: int
    exponent_base: int
    jitter_percent: int | None = None

    @property
    def reconnect_strategy(self) -> BackoffStrategy:
        """BackoffStrategy object generation."""
        return BackoffStrategy(
            num_of_retries=self.num_of_retries,
            factor=self.factor,
            exponent_base=self.exponent_base,
            jitter_percent=self.jitter_percent,
        )


class ValkeyTlsAdvancedConfiguration(msgspec.Struct, frozen=True):
    """Valkey TLS Advanced Configuration Schema."""

    use_insecure_tls: bool = False
    root_pem_cacerts: str | None = None

    def to_tls_advanced_config(self) -> TlsAdvancedConfiguration:
        """TlsAdvancedConfiguration object generation."""
        return TlsAdvancedConfiguration(
            use_insecure_tls=self.use_insecure_tls,
            root_pem_cacerts=self.root_pem_cacerts.encode() if self.root_pem_cacerts else None,
        )


class ValkeyAdvancedConfig(msgspec.Struct, frozen=True):
    """Valkey Advanced Configuration Schema."""

    connection_timeout: int | None = 10_000
    tcp_nodelay: bool | None = None
    tls_config: ValkeyTlsAdvancedConfiguration | None = None

    def to_advanced_config(self) -> AdvancedGlideClientConfiguration:
        """AdvancedGlideClientConfiguration object generation."""
        return AdvancedGlideClientConfiguration(
            connection_timeout=self.connection_timeout,
            tcp_nodelay=self.tcp_nodelay,
            tls_config=self.tls_config.to_tls_advanced_config() if self.tls_config else None,
        )


class ValkeyBaseConfig(msgspec.Struct, frozen=True):
    """Valkey Basic Configuration Schema."""

    nodes: list[ValkeyNode] = field(
        default_factory=lambda: [ValkeyNode(host="localhost", port=6379)]
    )
    user_credentials: ValkeyUserCredentials | None = None
    use_tls: bool = False
    request_timeout: int | None = 10_000
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
        """NodeAddresses objects generation."""
        return [NodeAddress(node.host, node.port) for node in self.nodes]

    @property
    def credentials(self) -> ServerCredentials | None:
        """ServerCredentials object generation."""
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
        """BackoffStrategy object generation."""
        if self.backoff_strategy:
            return self.backoff_strategy.reconnect_strategy
        return None


class ValkeyConfig(msgspec.Struct, frozen=True):
    """Valkey Configuration Schema."""

    base_config: ValkeyBaseConfig = ValkeyBaseConfig()
    advanced_config: ValkeyAdvancedConfig | None = None


def read_valkey_config(conf_dir: Path | None) -> ValkeyConfig:
    """Read valkey config from YML file"""
    if isinstance(conf_dir, Path):
        if not conf_dir.exists():
            try:
                conf_dir.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                raise RuntimeError(f"Failed to create configuration directory {conf_dir}!") from exc
        elif not conf_dir.is_dir():
            raise RuntimeError(
                f"Provided configuration directory path {conf_dir} is not a directory!"
            )
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
    """Generate Glide configuration from the Valkey Config Schema."""
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
        advanced_config=valkey_config.advanced_config.to_advanced_config()
        if valkey_config.advanced_config
        else None,
        pubsub_subscriptions=pubsub_subscriptions,
    )
    return client_config
