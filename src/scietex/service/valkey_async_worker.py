"""
Module providing asynchronous worker, which communicates with the Valkey server using glide client.
Worker provides handling connections, disconnections, initialization, cleanups, and logging.
"""

from typing import Optional
import logging

try:
    from glide import (
        GlideClient,
        GlideClientConfiguration,
        NodeAddress,
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
            valkey_config or GlideClientConfiguration([NodeAddress()])
        )
        self.client: Optional[GlideClient] = None

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
            self.logger.info("Valkey client disconnected")
            self.client = None

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
