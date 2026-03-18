"""
Module providing asynchronous worker, which communicates with the Valkey server using glide client.
Worker provides handling connections, disconnections, initialization, cleanups, and logging.
"""

from typing import Any
import logging
import json

try:
    from glide import (
        GlideClient,
        GlideClientConfiguration,
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
from ..async_tasks_processor import AsyncTaskProcessor

from .valkey_config import (
    ValkeyBaseConfig,
    read_valkey_config,
    generate_glide_config,
)


class ValkeyWorker(AsyncTaskProcessor):
    """
    An asynchronous worker class designed to interact with Valkey services via its glide client.

    Inherits from AsyncTaskProcessor and extends its capabilities by adding support for
    Valkey-specific operations like connection management and logging.

    Attributes:
        client (Optional[GlideClient]): Instance of the Valkey client initialized during runtime.
    """

    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        queue_size: int | None = None,
        max_concurrent_tasks: int | None = None,
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
        super().__init__(
            service_name=service_name,
            version=version,
            queue_size=queue_size,
            max_concurrent_tasks=max_concurrent_tasks,
            **kwargs,
        )
        if valkey_config is None:
            valkey_config = read_valkey_config(self.conf_dir)
        self._valkey_config = valkey_config
        self._client_config: GlideClientConfiguration = generate_glide_config(
            valkey_config,
            service_name=self.service_name,
            worker_id=self.worker_id,
            listening=False,
        )
        self._client: GlideClient | None = None

    @property
    def valkey_config(self) -> ValkeyBaseConfig:
        """Valkey configuration property."""
        return self._valkey_config

    @property
    def client(self) -> GlideClient | None:
        """Valkey client property."""
        return self._client

    def _parse_control_message(self, message: PubSubMsg, context: Any) -> None:
        """Parse control message from the Valkey client."""
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
            # self.control_msg_queue.put_nowait({"channel": channel, "data": data})
        except (AttributeError, json.decoder.JSONDecodeError) as ex:
            self.logger.error("Message decode error: %s", ex)

    async def connect(self) -> bool:
        """
        Establishes an asynchronous connection to Valkey.

        Attempts to initialize the Valkey client using the specified configuration.
        Logs successful or unsuccessful connection attempt based on results.

        Returns:
            bool: True if successfully connected, otherwise False.
        """
        if self._client is None:
            try:
                self._client = await GlideClient.create(self._client_config)

                if await self._client.ping():
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
        if self._client is not None:
            await self._client.close()
            self.logger.info("Valkey client disconnected")
            self._client = None

    async def initialize(self) -> bool:
        """
        Performs basic initialization steps along with establishing a connection to Valkey.

        Calls the base class's initialize method first, then connects to Valkey.

        Returns:
            bool: True if both initialization steps succeed, otherwise False.
        """
        if not await super().initialize():
            return False
        if not self.initialized:
            return await self.connect()
        else:
            await self.log("Already initialized", level=logging.DEBUG)
            return True

    async def cleanup(self):
        """
        Handles cleanup tasks upon termination, including closing any open connections.
        """
        await super().cleanup()
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
