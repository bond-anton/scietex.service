import asyncio
import aioredis
import signal
import time  # For tracking task start times

STREAM_NAME = "task_stream"
RESULTS_STREAM_NAME = "results_stream"
GROUP_NAME = "worker_group"
DEFAULT_MAX_CONCURRENT_TASKS = 5
TASK_TIMEOUT = 10  # Timeout in seconds for task completion


class AsyncWorker:
    def __init__(self, worker_id, **kwargs):
        self.worker_id = worker_id
        self.consumer_name = f"worker_{worker_id}"
        self.running_tasks = {}  # Track running tasks and their start times
        self.max_queue_size = DEFAULT_MAX_CONCURRENT_TASKS
        self.task_queue = asyncio.Queue(maxsize=self.max_queue_size)
        self.results_queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self.tasks = []
        self.pubsub_channel = f"worker:{worker_id}"

        # Redis connection parameters
        self.__redis_conf = {
            "host": kwargs.get("redis_host", "localhost"),
            "port": kwargs.get("redis_port", 6379),
            "db": kwargs.get("redis_db", 0),
        }
        self.redis = None

    async def start(self):
        self.redis = await aioredis.Redis(**self.__redis_conf)
        await self.create_consumer_group()
        self.setup_signal_handlers()

        # Start the pub/sub listener
        asyncio.create_task(self.listen_for_control_messages())

        # Main tasks
        self.tasks = [
            asyncio.create_task(self.task_manager()),
            asyncio.create_task(self.worker()),
            asyncio.create_task(self.results_manager()),
            asyncio.create_task(self.watchdog()),  # Start the watchdog
        ]

    async def stop(self):
        print("Stopping worker gracefully...")
        self._stop_event.set()

        # Return tasks in queue to Redis
        while not self.task_queue.empty():
            task_id, task_data = await self.task_queue.get()
            await self.redis.xadd(STREAM_NAME, task_data)
            self.task_queue.task_done()

        # Cancel and requeue running tasks
        for task_id, (task, task_data, _) in list(self.running_tasks.items()):
            if not task.done():
                task.cancel()
                await self.redis.xadd(STREAM_NAME, task_data)
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Process remaining results
        while not self.results_queue.empty():
            task_id, result = await self.results_queue.get()
            await self.redis.xadd(RESULTS_STREAM_NAME, {f"task_{task_id}": result})
            self.results_queue.task_done()

        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.redis.close()
        print("Worker stopped.")

    def setup_signal_handlers(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

    async def create_consumer_group(self):
        try:
            await self.redis.xgroup_create(
                STREAM_NAME, GROUP_NAME, id="0", mkstream=True
            )
            print(f"Consumer group '{GROUP_NAME}' created for stream '{STREAM_NAME}'")
        except aioredis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"Consumer group '{GROUP_NAME}' already exists.")
            else:
                raise

    async def process_task(self, task_id, task_data):
        print(f"Processing task {task_id}: {task_data}")
        await asyncio.sleep(1)
        result = f"Result of {task_id}"
        print(f"Task {task_id} completed with result: {result}")
        return result

    async def task_manager(self):
        while not self._stop_event.is_set():
            task_id, task_data = await self.task_queue.get()

            async def handle_task(task_id, task_data):
                try:
                    result = await self.process_task(task_id, task_data)
                    await self.results_queue.put((task_id, result))
                finally:
                    self.task_queue.task_done()

            task = asyncio.create_task(handle_task(task_id, task_data))
            self.running_tasks[task_id] = (
                task,
                task_data,
                time.time(),
            )  # Track start time
            task.add_done_callback(lambda t: self.running_tasks.pop(task_id, None))

    async def worker(self):
        while not self._stop_event.is_set():
            count_to_fetch = max(0, self.max_queue_size - self.task_queue.qsize())
            if count_to_fetch > 0:
                try:
                    tasks = await self.redis.xreadgroup(
                        groupname=GROUP_NAME,
                        consumername=self.consumer_name,
                        streams={STREAM_NAME: ">"},
                        count=count_to_fetch,
                        block=5000,
                    )
                    for stream, messages in tasks:
                        for message_id, message_data in messages:
                            await self.task_queue.put((message_id, message_data))
                            await self.redis.xack(STREAM_NAME, GROUP_NAME, message_id)
                except Exception as e:
                    print(f"Error fetching tasks: {e}")
            await asyncio.sleep(0.1)

    async def results_manager(self):
        while not self._stop_event.is_set():
            task_id, result = await self.results_queue.get()
            if "Result" in result:
                await self.redis.xadd(RESULTS_STREAM_NAME, {f"task_{task_id}": result})
            self.results_queue.task_done()

    async def listen_for_control_messages(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.pubsub_channel)
        async for message in pubsub.listen():
            if message["type"] == "message":
                msg_data = message["data"].decode()
                if msg_data == "stop":
                    await self.stop()
                elif msg_data.startswith("set_queue_size:"):
                    try:
                        new_size = int(msg_data.split(":")[1])
                        if 1 <= new_size <= 100:
                            print(f"Updating queue size to {new_size}")
                            self.max_queue_size = new_size
                            self.task_queue = asyncio.Queue(maxsize=self.max_queue_size)
                        else:
                            print("Queue size out of bounds (1-100)")
                    except ValueError:
                        print("Invalid queue size value received")

    async def watchdog(self):
        """Periodically check for tasks that exceed the timeout."""
        while not self._stop_event.is_set():
            now = time.time()
            for task_id, (task, task_data, start_time) in list(
                self.running_tasks.items()
            ):
                if not task.done() and (now - start_time) > TASK_TIMEOUT:
                    print(f"Task {task_id} exceeded timeout. Cancelling and requeuing.")
                    task.cancel()
                    await self.redis.xadd(STREAM_NAME, task_data)
                    self.running_tasks.pop(task_id, None)
                    try:
                        await task  # Wait for cancellation to complete
                    except asyncio.CancelledError:
                        pass
            await asyncio.sleep(5)  # Check for timeouts every 5 seconds

    async def run(self):
        await self.start()
        await asyncio.Event().wait()
