"""
Module providing asynchronous worker, which communicates with the Valkey server using glide client.
Worker provides handling connections, disconnections, initialization, cleanups, and logging.
"""

import asyncio
from typing import Optional, Union, Any, Mapping
import logging
import json
from pathlib import Path
import yaml

try:
    from glide import (
        GlideClient,
        GlideClientConfiguration,
        ServerCredentials,
        NodeAddress,
        CoreCommands,
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

# pylint: disable=duplicate-code


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
        valkey_config: Optional[GlideClientConfiguration] = None,
        **kwargs,
    ):
        """
        Constructor method initializing the ValkeyWorker.

        Args:
            service_name (str): Name of the service (default: "service").
            version (str): Version string associated with the service (default: "0.0.1").
            valkey_config (Optional[GlideClientConfiguration], optional): Custom configuration for
                the Valkey client. If omitted, defaults to minimal settings.
            kwargs: Additional keyword arguments passed through to parent constructor.
        """
        super().__init__(service_name=service_name, version=version, **kwargs)
        self._client_config: GlideClientConfiguration = (
            valkey_config or self.read_valkey_config()
        )
        self._listening_client_config: GlideClientConfiguration = (
            GlideClientConfiguration(
                addresses=self._client_config.addresses,
                use_tls=self._client_config.use_tls,
                credentials=self._client_config.credentials,
                read_from=self._client_config.read_from,
                request_timeout=self._client_config.request_timeout,
                reconnect_strategy=self._client_config.reconnect_strategy,
                database_id=self._client_config.database_id,
                client_name=self._client_config.client_name,
                protocol=self._client_config.protocol,
                inflight_requests_limit=self._client_config.inflight_requests_limit,
                client_az=self._client_config.client_az,
                # advanced_config=self._client_config.advanced_config,
                lazy_connect=self._client_config.lazy_connect,
                pubsub_subscriptions=GlideClientConfiguration.PubSubSubscriptions(
                    channels_and_patterns={
                        GlideClientConfiguration.PubSubChannelModes.Exact: {
                            self.service_name,
                            "broadcast",
                        },
                    },
                    callback=self.parse_message,
                    context=None,
                ),
            )
        )
        self.client: Optional[GlideClient] = None
        self.listening_client: Optional[GlideClient] = None
        self.control_msg_queue: asyncio.Queue[Mapping[str, Union[str, dict]]] = (
            asyncio.Queue()
        )

    def read_valkey_config(self) -> GlideClientConfiguration:
        """Read valkey config from YML file"""
        if isinstance(self.conf_dir, Path):
            valkey_yml = self.conf_dir.joinpath("valkey.yml")
        else:
            raise RuntimeError("Configuration dir was not set!")
        try:
            valkey_config = yaml.safe_load(valkey_yml.read_text(encoding="utf-8"))
        except (yaml.YAMLError, FileNotFoundError):
            valkey_config = {
                "nodes": [{"host": "localhost", "port": 6379}],
                "database_id": 0,
                "use_tls": False,
            }
            with open(valkey_yml, "w", encoding="utf-8") as f:
                yaml.dump(valkey_config, f, default_flow_style=False, sort_keys=False)
        addresses = [NodeAddress(**node) for node in valkey_config["nodes"]]
        credentials = None
        if "credentials" in valkey_config:
            try:
                credentials = ServerCredentials(**valkey_config["credentials"])
            except TypeError:
                pass
        valkey_safe_conf = {
            k: v for k, v in valkey_config.items() if k not in ("nodes", "credentials")
        }
        client_config = GlideClientConfiguration(
            addresses=addresses, credentials=credentials, **valkey_safe_conf
        )

        return client_config

    def parse_message(self, message: CoreCommands.PubSubMsg, context: Any) -> None:
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
            if channel == self.service_name:
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
                if message["channel"] == self.service_name:
                    await self.process_control_message(message)
                else:
                    await self.process_broadcast_message(message)
                self.control_msg_queue.task_done()
            except TimeoutError:
                pass

    async def process_control_message(
        self, message: Mapping[str, Union[str, dict]]
    ) -> None:
        """
        Process control messages from the Valkey client.
        Args:
            message (Mapping[str, Union[str, dict]]): Message received from the Valkey client.

        This method should be overridden by subclasses to implement
        the actual message processing logic.
        """
        self.logger.debug("Processing control message: %s", message["data"])

    async def process_broadcast_message(
        self, message: Mapping[str, Union[str, dict]]
    ) -> None:
        """
        Process control messages from the Valkey client.
        Args:
            message (Mapping[str, Union[str, dict]]): Message received from the Valkey client.

        This method should be overridden by subclasses to implement
        the actual message processing logic.
        """
        self.logger.debug("Processing broadcast message: %s", message["data"])

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
