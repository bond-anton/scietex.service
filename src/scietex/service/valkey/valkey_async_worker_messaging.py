"""
Module providing asynchronous worker, which communicates with the Valkey server using glide client.
Worker provides handling connections, disconnections, initialization, cleanups, and logging.
"""

import asyncio
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


from .valkey_config import ValkeyBaseConfig, generate_glide_config
from .valkey_async_worker import ValkeyWorker


class ValkeyMessagingWorker(ValkeyWorker):
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
            valkey_config=valkey_config,
            **kwargs,
        )

        self._listening_client_config: GlideClientConfiguration = generate_glide_config(
            self.valkey_config,
            service_name=self.service_name,
            worker_id=self.worker_id,
            listening=True,
            parse_control_message=self._parse_control_message,
        )
        self._listening_client: GlideClient | None = None
        self.control_msg_queue: asyncio.Queue[dict[str, str | dict]] = asyncio.Queue()

    @property
    def listening_client(self) -> GlideClient | None:
        """Client used for listening to pub/sub messages."""
        return self._listening_client

    def _parse_control_message(self, message: PubSubMsg, context: Any) -> None:
        """Parse control message from the Valkey client and put it to messages processing queue."""
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

    async def control_messages_manager(self):
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
            except asyncio.TimeoutError:
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
        if not await super().connect():
            return False
        if self._listening_client is None:
            try:
                self._listening_client = await GlideClient.create(
                    self._listening_client_config
                )
                if await self._listening_client.ping():
                    await self.log("Connected Listening client to Valkey", logging.INFO)
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
        await super().disconnect()
        if self._listening_client is not None:
            await self._listening_client.close()
            self.logger.info("Valkey Listening client disconnected")
            self._listening_client = None

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
            # Start managers
            self.managers_tasks += [
                asyncio.create_task(self.control_messages_manager()),
            ]
            await self.log("Control messages manager started", level=logging.DEBUG)
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
