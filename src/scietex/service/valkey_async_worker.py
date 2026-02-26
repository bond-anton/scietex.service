"""
Module providing asynchronous worker, which communicates with the Valkey server using glide client.
Worker provides handling connections, disconnections, initialization, cleanups, and logging.
"""

import asyncio
from typing import Any
import logging
import json
from pathlib import Path
import msgspec
from msgspec import field

try:
    from glide import (
        ConfigurationError,
        GlideClient,
        GlideClientConfiguration,
        BackoffStrategy,
        ProtocolVersion,
        ReadFrom,
        ServerCredentials,
        NodeAddress,
        PubSubMsg,
        ConnectionError as GlideConnectionError,
        TimeoutError as GlideTimeoutError,
    )
except ImportError as e:
    raise ImportError(
        "The 'valkey-glide' module is required to use this feature. "
        "Please install it by running:\n\n    pip install scietex.service[valkey]\n"
    ) from e

from scietex.logging import AsyncValkeyHandler
from .basic_async_worker import BasicAsyncWorker

# pylint: disable=duplicate-code, too-few-public-methods


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
    read_from: ReadFrom = ReadFrom.PRIMARY
    backoff_strategy: ValkeyBackoffStrategy | None = None
    protocol: ProtocolVersion = ProtocolVersion.RESP3

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


class ValkeyWorker(BasicAsyncWorker):
    """
    An asynchronous worker class designed to interact with Valkey services via its glide client.

    Inherits from BasicAsyncWorker and extends its capabilities by adding support for
    Valkey-specific operations like connection management and logging.

    Attributes:
        client (Optional[GlideClient]): Instance of the Valkey client initialized during runtime.
    """

    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        valkey_config: ValkeyBaseConfig | None = None,
        **kwargs,
    ):
        """
        Constructor method initializing the ValkeyWorker.

        Args:
            service_name (str): Name of the service (default: "service").
            version (str): Version string associated with the service (default: "0.0.1").
            valkey_config (ValkeyBaseConfigSchema, optional): Custom configuration for
                the Valkey client. If omitted, tries to read config from the yaml file,
                if fail defaults to minimal settings.
            kwargs: Additional keyword arguments passed through to parent constructor.
        """
        super().__init__(service_name=service_name, version=version, **kwargs)
        if valkey_config is None:
            valkey_config = self.read_valkey_config()
        self._client_config: GlideClientConfiguration = self.generate_glide_config(
            valkey_config
        )
        self._listening_client_config: GlideClientConfiguration = (
            self.generate_glide_config(valkey_config, listening=True)
        )
        self.client: GlideClient | None = None
        self.listening_client: GlideClient | None = None
        self.control_msg_queue: asyncio.Queue[dict[str, str | dict]] = asyncio.Queue()

    def read_valkey_config(self) -> ValkeyBaseConfig:
        """Read valkey config from YML file"""
        if isinstance(self.conf_dir, Path):
            valkey_yml = self.conf_dir.joinpath("valkey.yml")
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
        self, valkey_config: ValkeyBaseConfig, listening: bool = False
    ) -> GlideClientConfiguration:
        """Generate Glide configuration from the Valkey Config Schema."""
        pubsub_subscriptions = None
        if listening:
            pubsub_subscriptions = GlideClientConfiguration.PubSubSubscriptions(
                channels_and_patterns={
                    GlideClientConfiguration.PubSubChannelModes.Exact: {
                        f"scietex:{self.service_name}:{self.worker_id}",
                        "scietex:broadcast",
                    },
                },
                callback=self.parse_message,
                context=None,
            )
        client_config = GlideClientConfiguration(
            addresses=valkey_config.addresses,
            credentials=valkey_config.credentials,
            use_tls=valkey_config.use_tls,
            read_from=valkey_config.read_from,
            request_timeout=valkey_config.request_timeout,
            reconnect_strategy=valkey_config.reconnect_strategy,
            database_id=valkey_config.database_id,
            client_name=valkey_config.client_name,
            protocol=valkey_config.protocol,
            inflight_requests_limit=valkey_config.inflight_requests_limit,
            client_az=valkey_config.client_az,
            lazy_connect=valkey_config.lazy_connect,
            advanced_config=None,
            pubsub_subscriptions=pubsub_subscriptions,
        )
        return client_config

    def parse_message(self, message: PubSubMsg, context: Any) -> None:
        """Parse message from the Valkey client and put it to processing queue."""
        try:
            if isinstance(message.channel, bytes):
                channel = message.channel.decode(encoding="utf-8")
            else:
                channel = message.channel
            if isinstance(message.message, bytes):
                data = json.loads(message.message.decode(encoding="utf-8"))
            else:
                data = json.loads(str(message.message))
            if channel == f"scietex:{self.service_name}:{self.worker_id}":
                self.logger.debug("Received message: %s, context: %s", data, context)
            else:
                self.logger.debug(
                    "Received broadcast message: %s, context: %s", data, context
                )
            self.control_msg_queue.put_nowait({"channel": channel, "data": data})
        except (AttributeError, json.decoder.JSONDecodeError) as ex:
            self.logger.error("Message decode error: %s", ex)

    async def listen_for_control_messages(self):
        """Process control messages from the Valkey client."""
        while not self._stop_event.is_set():
            try:
                message = await asyncio.wait_for(
                    self.control_msg_queue.get(), timeout=1
                )
                message_data = None
                try:
                    message_data = message["data"]
                except KeyError:
                    await self.log(
                        f"Message should have data payload, got {message}",
                        logging.ERROR,
                    )
                if message_data is not None:
                    if (
                        message["channel"]
                        == f"scietex:{self.service_name}:{self.worker_id}"
                    ):
                        await self.process_control_message(message_data)
                    else:
                        await self.process_broadcast_message(message_data)
                self.control_msg_queue.task_done()
            except TimeoutError:
                pass

    async def process_control_message(
        self, message: str | dict[str, str | dict]
    ) -> None:
        """
        Process control messages from the Valkey client.
        Args:
            message (dict[str, Union[str, dict]]): Message data received from the Valkey client.

        This method should be overridden by subclasses to implement
        the actual message processing logic.
        """
        self.logger.debug("Processing control message: %s", message)

    async def process_broadcast_message(
        self, message: str | dict[str, str | dict]
    ) -> None:
        """
        Process control messages from the Valkey client.
        Args:
            message (dict[str, Union[str, dict]]): Message data received from the Valkey client.

        This method should be overridden by subclasses to implement
        the actual message processing logic.
        """
        self.logger.debug("Processing broadcast message: %s", message)

    async def connect(self) -> bool:
        """
        Establishes an asynchronous connection to Valkey.

        Attempts to initialize the Valkey client using the specified configuration.
        Logs successful or unsuccessful connection attempt based on results.

        Returns:
            bool: True if successfully connected, otherwise False.
        """
        if self.client is None:
            try:
                self.client = await GlideClient.create(self._client_config)
                self.listening_client = await GlideClient.create(
                    self._listening_client_config
                )
                if await self.client.ping():
                    await self.log("Connected to Valkey", logging.INFO)
                    return True
                print("Error pinging Valkey")
                return False
            except (GlideConnectionError, GlideTimeoutError):
                print("Error connecting to Valkey")
                return False
        return True

    async def disconnect(self):
        """
        Gracefully closes the connection to Valkey.
        Closes the current Valkey client session and removes references to it.
        """
        if self.client is not None:
            await self.client.close()
            if self.listening_client:
                await self.listening_client.close()
            self.logger.info("Valkey client disconnected")
            self.client = None
            self.listening_client = None

    async def initialize(self) -> bool:
        """
        Performs basic initialization steps along with establishing a connection to Valkey.

        Calls the base class's initialize method first, then connects to Valkey.

        Returns:
            bool: True if both initialization steps succeed, otherwise False.
        """
        if not await super().initialize():
            return False
        return await self.connect()

    async def cleanup(self):
        """
        Handles cleanup tasks upon termination, including closing any open connections.
        """
        await self.disconnect()

    async def logger_add_custom_handlers(self) -> None:
        """
        Adds a custom logging handler specific to Valkey.

        Configures an AsyncValkeyHandler that forwards log messages to Valkey.
        Disables standard output logging (stdout_enable=False).
        """
        valkey_handler = AsyncValkeyHandler(
            stream_name="log",
            service_name=self.service_name,
            worker_id=self.worker_id,
            valkey_config=self._client_config,
            stdout_enable=False,
        )
        valkey_handler.setLevel(self.logging_level)
        self.logger.addHandler(valkey_handler)
