from typing import Union
import sys
import logging
import asyncio

try:
    import redis.asyncio as redis
except ImportError as e:
    raise ImportError(
        "The 'redis' module is required to use this feature. "
        "Please install it by running:\n\n    pip install mypackage[redis]\n"
    ) from e

from nts.service.logging.formatter import NTSFormatter


class AsyncBaseHandler(logging.Handler):

    def __init__(
        self, service_name: Union[str, None] = None, worker_id: Union[int, None] = None
    ):
        super().__init__()
        if worker_id is None:
            worker_id = 1
        if service_name is None:
            service_name = "Service"
        self.formatter = NTSFormatter(service_name=service_name, worker_id=worker_id)
        self.queues = {"console": asyncio.Queue()}  # Queue for console logs
        self.accept_log_records_event = (
            asyncio.Event()
        )  # Indicates if logging is running
        self.logging_running_event = asyncio.Event()  # Indicates if logging is running
        self.workers = [
            self._console_worker()
        ]  # Initialize list with the console worker
        self.worker_tasks = []
        self.queue_put_tasks = []

    async def start_logging(self):
        """Start the logging workers."""
        self.accept_log_records_event.set()  # Set the event to indicate logs are accepted
        self.logging_running_event.set()  # Set the event to indicate logging is active
        self.worker_tasks = [asyncio.create_task(worker) for worker in self.workers]

    def emit(self, record):
        # Format the log record message
        if self.accept_log_records_event.is_set():
            # Schedule an asynchronous task to put the message in each queue
            for queue_name, queue in self.queues.items():
                try:
                    # Use asyncio.create_task to handle each put asynchronously
                    queue_put_task = asyncio.create_task(queue.put(record))
                    self.queue_put_tasks.append(queue_put_task)  # Track the put task
                    # Cleanup completed tasks from the list to prevent memory buildup
                    self.queue_put_tasks = [
                        task for task in self.queue_put_tasks if not task.done()
                    ]
                except Exception as e:
                    pass
        else:
            pass

    async def stop_logging(self, timeout=5.0):
        """Stop the logging workers and wait for all queues to be processed with a timeout."""
        self.accept_log_records_event.clear()
        # Wait for all pending put tasks to complete
        if self.queue_put_tasks:
            await asyncio.gather(*self.queue_put_tasks)
            self.queue_put_tasks = [
                task for task in self.queue_put_tasks if not task.done()
            ]

        self.logging_running_event.clear()  # Signal workers to stop processing

        # Process each worker's queue except the console queue
        for name, queue in self.queues.items():
            if name == "console":
                continue  # Skip the console queue for now
            try:
                # Attempt to wait for the queue to join with a timeout
                await asyncio.wait_for(queue.join(), timeout=timeout)
                # Create an INFO LogRecord for successful completion
                log_record = logging.LogRecord(
                    name=f"{name.capitalize()}Worker",
                    level=logging.INFO,
                    pathname=__file__,
                    lineno=0,
                    msg=f"{name.capitalize()} logging worker has completed processing its queue.",
                    args=None,
                    exc_info=None,
                )
                await self.queues["console"].put(log_record)
            except asyncio.TimeoutError:
                # Create an ERROR LogRecord for timeout
                log_record = logging.LogRecord(
                    name=f"{name.capitalize()}Worker",
                    level=logging.ERROR,
                    pathname=__file__,
                    lineno=0,
                    msg=f"Timeout while waiting for {name} logging worker to complete its queue.",
                    args=None,
                    exc_info=None,
                )
                await self.queues["console"].put(log_record)
            except Exception as e:
                # Create an ERROR LogRecord for other exceptions
                log_record = logging.LogRecord(
                    name=f"{name.capitalize()}Worker",
                    level=logging.ERROR,
                    pathname=__file__,
                    lineno=0,
                    msg=f"Error while waiting for {name} logging worker: {e}",
                    args=None,
                    exc_info=None,
                )
                await self.queues["console"].put(log_record)

        # Process the console queue last
        try:
            await asyncio.wait_for(self.queues["console"].join(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            pass
        # Wait for all worker tasks to complete

        await asyncio.gather(*self.worker_tasks)
        self.close()

    async def _console_worker(self):
        """Worker to handle logging to the console."""
        while self.logging_running_event.is_set() or not self.queues["console"].empty():
            try:
                record = await asyncio.wait_for(self.queues["console"].get(), 1)
                sys.stdout.write(self.formatter.format(record) + "\n")
                sys.stdout.flush()
                self.queues["console"].task_done()
            except asyncio.TimeoutError:
                pass


class AsyncRedisHandler(AsyncBaseHandler):
    def __init__(self, stream_name: str, redis_url: str = "redis://localhost:6379"):
        super().__init__()
        self.stream_name = stream_name
        self.redis_url = redis_url
        self.redis_client = None
        self.queues["redis"] = asyncio.Queue()  # Add queue for Redis logs
        self.workers.append(self._redis_worker())  # Add Redis worker to the list

    async def connect_redis(self):
        """Connect to Redis asynchronously."""
        self.redis_client = await redis.from_url(self.redis_url, decode_responses=True)

    async def _redis_worker(self):
        """Worker to handle logging to Redis."""
        await self.connect_redis()  # Establish Redis connection in the worker
        while self.logging_running_event.is_set() or not self.queues["redis"].empty():
            try:
                record = await asyncio.wait_for(self.queues["redis"].get(), 1)
                try:
                    logger_name = record.worker_name
                except AttributeError:
                    logger_name = record.name
                log_entry = {
                    "level": record.levelname,
                    "message": record.getMessage(),
                    "name": logger_name,
                    "time": self.formatter.formatTime(record),
                }
                print(log_entry)
                await self.redis_client.xadd(self.stream_name, log_entry)
                self.queues["redis"].task_done()
            except asyncio.TimeoutError:
                pass


# Example setup and usage
async def main():
    logger = logging.getLogger("AsyncMultiLogger")
    logger.setLevel(logging.DEBUG)

    # async_handler = AsyncBaseHandler()
    async_handler = AsyncRedisHandler(stream_name="async_log_stream")
    logger.addHandler(async_handler)

    # Start the async logging tasks
    await async_handler.start_logging()  # Call this after setting up the handler

    # Log some messages
    logger.info("This is an info message logged to Redis and console.")
    logger.error("This is an error message logged to Redis and console.")

    # Stop the logging and cleanup
    await async_handler.stop_logging()


# Run the asynchronous logger example
asyncio.run(main())
