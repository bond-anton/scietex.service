import asyncio
import argparse
from sdnotify import SystemdNotifier
from async_worker import AsyncWorker  # Assuming AsyncWorker class is in async_worker.py


class WorkerManager:
    def __init__(self, num_workers, redis_host, redis_port, redis_db):
        self.num_workers = num_workers
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.notifier = SystemdNotifier()
        self.tasks = []  # List of tasks for each worker

    async def start_workers(self):
        """Start workers and notify systemd when all are running."""
        print("Starting workers...")
        for worker_id in range(1, self.num_workers + 1):
            worker = AsyncWorker(
                worker_id, self.redis_host, self.redis_port, self.redis_db
            )
            task = asyncio.create_task(worker.run())
            self.tasks.append((worker_id, task))

        # Notify systemd that workers are ready
        self.notifier.notify("READY=1")
        print("All workers are up and running.")

        # Start monitoring tasks to restart if any fail
        await self.monitor_workers()

    async def monitor_workers(self):
        """Monitor workers and restart any that have failed."""
        while True:
            for i, (worker_id, task) in enumerate(self.tasks):
                if task.done():
                    # If the task has failed, restart it
                    if task.exception():
                        print(
                            f"Worker {worker_id} failed with exception: {task.exception()}"
                        )
                    else:
                        print(f"Worker {worker_id} has stopped unexpectedly.")

                    # Restart the failed worker
                    new_worker = AsyncWorker(
                        worker_id, self.redis_host, self.redis_port, self.redis_db
                    )
                    new_task = asyncio.create_task(new_worker.run())
                    self.tasks[i] = (worker_id, new_task)
                    print(f"Worker {worker_id} restarted.")

            await asyncio.sleep(5)  # Check every 5 seconds for dead workers


def main():
    parser = argparse.ArgumentParser(
        description="Launch multiple AsyncWorker instances with monitoring."
    )
    parser.add_argument(
        "--num-workers", type=int, default=1, help="Number of worker instances to start"
    )
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis database number")
    args = parser.parse_args()

    manager = WorkerManager(
        args.num_workers, args.redis_host, args.redis_port, args.redis_db
    )
    asyncio.run(manager.start_workers())


if __name__ == "__main__":
    main()
