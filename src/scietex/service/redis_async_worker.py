"""
Module providing asynchronous worker, which can communicate with the Redis server.
Worker provides handling connections, disconnections, initialization, cleanups, and logging.
"""

from typing import Optional, Union, Mapping
import asyncio
import logging
import json

try:
    import redis.asyncio as redis
except ImportError as e:
    raise ImportError(
        "The 'redis' module is required to use this feature. "
        "Please install it by running:\n\n    pip install scietex.service[redis]\n"
    ) from e


from scietex.logging import AsyncRedisHandler
from .basic_async_worker import BasicAsyncWorker

# pylint: disable=duplicate-code


class RedisWorker(BasicAsyncWorker):
    """
    An asynchronous worker class designed to interact with Redis server.

    Inherits from BasicAsyncWorker and extends its capabilities by adding support for Redis-specific
    operations like connection management and logging.

    Attributes:
        client (Optional[redis.Redis]): Instance of the Redis client initialized during runtime.
    """

    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        redis_config: Optional[dict] = None,
        **kwargs,
    ):
        """
        Constructor method initializing the RedisWorker.

        Args:
            service_name (str): Name of the service (default: "service").
            version (str): Version string associated with the service (default: "0.0.1").
            redis_config (Optional[dict], optional): Custom configuration for
                the Redis client. If omitted, defaults to minimal settings.
            kwargs: Additional keyword arguments passed through to parent constructor.
        """
        super().__init__(service_name=service_name, version=version, **kwargs)
        self._client_config: dict = redis_config or {
            "host": "localhost",
            "port": 6379,
            "db": 0,
        }
        self.client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None

    async def connect(self) -> bool:
        """
        Connect to Redis asynchronously.

        Initializes the Redis client connection using the provided Redis configuration.
        Sets `decode_responses=True` for handling Redis data in string format.

        Returns:
            bool: True if successfully connected, otherwise False.
        """
        if self.client is None:
            try:
                self.client = await redis.Redis(
                    **self._client_config, decode_responses=True
                )
                self.pubsub = self.client.pubsub()
                if await self.client.ping():
                    await self.log("Connected to Redis", logging.INFO)
                    return True
                print("Error pinging Redis")
                return False
            except (redis.ConnectionError, redis.TimeoutError):
                print("Error connecting to Redis")
                return False
        return True

    async def disconnect(self):
        """
        Disconnect from Redis asynchronously.
        Closes the current Redis client session and removes references to it.
        """
        if self.client is not None:
            await self.client.aclose()
            await self.pubsub.close()
            self.logger.info("Redis client disconnected")
            self.client = None
            self.pubsub = None

    async def listen_for_control_messages(self):
        """
        Listen for messages from the Redis client.
        """
        await self.pubsub.subscribe(self.service_name)
        await self.pubsub.subscribe("broadcast")

        try:
            async for message in self.pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        if message["channel"] == self.service_name:
                            await self.process_control_message(
                                {"channel": message["channel"], "data": data}
                            )
                        else:
                            await self.process_broadcast_message(
                                {"channel": message["channel"], "data": data}
                            )
                    except json.JSONDecodeError as ex:
                        self.logger.error("Message decode error: %s", ex)
        except asyncio.CancelledError:
            self.logger.debug("Redis message handler stopped.")

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

    async def initialize(self) -> bool:
        """
        Performs basic initialization steps along with establishing a connection to Redis.

        Calls the base class's initialize method first, then connects to Redis.

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
        Adds a custom logging handler specific to Redis.

        Configures an AsyncRedisHandler that forwards log messages to Redis.
        Disables standard output logging (stdout_enable=False).
        """
        redis_handler = AsyncRedisHandler(
            stream_name="log",
            service_name=self.service_name,
            worker_id=self.worker_id,
            redis_config=self._client_config,
            stdout_enable=False,
        )
        redis_handler.setLevel(self.logging_level)
        self.logger.addHandler(redis_handler)
