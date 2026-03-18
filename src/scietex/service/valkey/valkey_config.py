from typing import Callable, Any
from pathlib import Path
import msgspec
from msgspec import field

try:
    from glide import (
        ConfigurationError,
        GlideClientConfiguration,
        BackoffStrategy,
        ProtocolVersion,
        ReadFrom,
        ServerCredentials,
        NodeAddress,
        PubSubMsg,
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


class ValkeyBaseConfig(msgspec.Struct, frozen=True):
    """Valkey Basic Configuration Schema."""

    nodes: list[ValkeyNode] = field(
        default_factory=lambda: [ValkeyNode(host="localhost", port=6379)]
    )
    user_credentials: ValkeyUserCredentials | None = None
    use_tls: bool = False
    request_timeout: int | None = None
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


def read_valkey_config(conf_dir: Path | None) -> ValkeyBaseConfig:
    """Read valkey config from YML file"""
    if isinstance(conf_dir, Path):
        valkey_yml = conf_dir.joinpath("valkey.yml")
    else:
        raise RuntimeError("Configuration dir was not set!")
    try:
        with open(valkey_yml, "rb") as f:
            valkey_config = msgspec.yaml.decode(
                f.read(), type=ValkeyBaseConfig, strict=True
            )
    except Exception:  # pylint: disable=broad-exception-caught
        valkey_config = ValkeyBaseConfig()
        with open(valkey_yml, "wb") as f:
            f.write(msgspec.yaml.encode(valkey_config))
    return valkey_config


def generate_glide_config(
    valkey_config: ValkeyBaseConfig,
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
        read_from = ReadFrom[valkey_config.read_from]
    except KeyError as exc:
        raise ValueError(f"""
            Invalid read_from value in Valkey Config: {valkey_config.read_from}.
            Supported values are: {[e.name for e in ReadFrom]}.

            """) from exc
    try:
        protocol = ProtocolVersion[valkey_config.protocol]
    except KeyError as exc:
        raise ValueError(f"""
            Invalid protocol value in Valkey Config: {valkey_config.protocol}.
            Supported values are: {[e.name for e in ProtocolVersion]}.
            """) from exc
    client_config = GlideClientConfiguration(
        addresses=valkey_config.addresses,
        credentials=valkey_config.credentials,
        use_tls=valkey_config.use_tls,
        read_from=read_from,
        request_timeout=valkey_config.request_timeout,
        reconnect_strategy=valkey_config.reconnect_strategy,
        database_id=valkey_config.database_id,
        client_name=valkey_config.client_name,
        protocol=protocol,
        inflight_requests_limit=valkey_config.inflight_requests_limit,
        client_az=valkey_config.client_az,
        lazy_connect=valkey_config.lazy_connect,
        advanced_config=None,
        pubsub_subscriptions=pubsub_subscriptions,
    )
    return client_config
