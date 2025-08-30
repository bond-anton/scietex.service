from typing import Optional
import logging

import redis.asyncio as redis

from scietex.logging import RedisConfig, AsyncRedisHandler
from .basic_async_worker import BasicAsyncWorker


class RedisWorker(BasicAsyncWorker):

    def __init__(
            self,
            redis_config: RedisConfig,
            service_name: str = "service",
            version: str = "0.0.1",
            delay: float = 5,
            **kwargs,
    ):
        super().__init__(service_name=service_name, version=version, delay=delay, **kwargs)
        self.redis_config = redis_config
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> bool:
        """
        Connect to Redis asynchronously.

        Initializes the Redis client connection using the provided Redis configuration.
        Sets `decode_responses=True` for handling Redis data in string format.

        Returns:
            None
        """
        if self.client is None:
            try:
                self.client = await redis.Redis(**self.redis_config, decode_responses=True)
                if await self.client.ping():
                    await self.log("Connected to Redis", logging.INFO)
                    return True
                else:
                    print("Error pinging Redis")
                    return False
            except (redis.ConnectionError, redis.TimeoutError):
                print("Error connecting to Redis")
                return False
        return True

    async def disconnect(self):
        """
        Disconnect from Redis asynchronously.
        """
        if self.client is not None:
            await self.client.aclose()
            self.logger.info("Redis client disconnected")
            self.client = None

    async def initialize(self) -> bool:
        if not await super().initialize():
            return False
        return await self.connect()

    async def cleanup(self):
        await self.disconnect()

    async def logger_add_custom_handlers(self) -> None:
        redis_handler = AsyncRedisHandler(
            stream_name="log",
            service_name=self.service_name,
            worker_id=self.worker_id,
            redis_config=self.redis_config,
            stdout_enable=False
        )
        redis_handler.setLevel(self.logging_level)
        self.logger.addHandler(redis_handler)
