# AsyncTaskProcessor

The `AsyncTaskProcessor` is a concurrent task processing framework built
on `BasicAsyncWorker`. It adds a task queue, handler dispatch, concurrent
task execution, timeout monitoring via watchdog, and graceful shutdown
with task re-queueing.

## Overview

```python
from scietex.service import AsyncTaskProcessor
```

`AsyncTaskProcessor` extends `BasicAsyncWorker` with two additional
`@Manager`-decorated loops:

| Manager | Method | Description |
|---|---|---|
| `TaskQueueManager` | `task_queue_manager` | Periodically calls `fetch_tasks()` to pull work into the queue |
| `TaskManager` | `task_manager` | Pulls tasks from the queue and processes them concurrently |

Plus a built-in `watchdog()` override that monitors running tasks for
timeouts.

## Architecture

```
  ┌─────────────────────────────────────────────────────────────┐
  │                    AsyncTaskProcessor                        │
  │                                                              │
  │  ┌─────────────────┐    ┌──────────────────────────────┐    │
  │  │ TaskQueueManager │───►│  task_queue (asyncio.Queue)  │    │
  │  │ (fetch loop)     │    │                              │    │
  │  └─────────────────┘    └──────────┬───────────────────┘    │
  │                                    │                         │
  │  ┌─────────────────┐    ┌──────────▼───────────────────┐    │
  │  │   TaskManager   │───►│  process_task()              │    │
  │  │ (process loop)  │    │  ┌─────────────────────────┐ │    │
  │  └─────────────────┘    │  │ _find_task_handler()    │ │    │
  │                          │  └────────┬───────────────┘ │    │
  │                          │           │                   │    │
  │                          │  ┌────────▼───────────────┐ │    │
  │                          │  │ handler.handle(task)    │ │    │
  │                          │  └────────┬───────────────┘ │    │
  │                          └───────────┼─────────────────┘    │
  │                                      │                       │
  │  ┌─────────────────┐    ┌────────────▼───────────────┐     │
  │  │    Watchdog     │───►│  running_tasks dict        │     │
  │  │ (timeout monitor)│   │  TaskTracker per task      │     │
  │  └─────────────────┘    └────────────────────────────┘     │
  └─────────────────────────────────────────────────────────────┘
```

## Constants

| Constant | Default | Min | Max | Description |
|---|---|---|---|---|
| `DEFAULT_MAX_TASKS_QUEUE_SIZE` | `2` | — | — | Default max queue size |
| `DEFAULT_MAX_CONCURRENT_TASKS` | `2` | `1` | — | Default max concurrent tasks |
| `DEFAULT_TASK_TIMEOUT` | `3` | — | — | Default task timeout in seconds |
| `TASK_QUEUE_FETCH_TIMEOUT` | `1` | — | — | Timeout waiting for task from queue |
| `DEFAULT_MANAGER_SLEEP_TIME` | `0.01` | `0.001` | `1` | Default manager loop sleep |
| `WORKER_TASK_CANCELLATION_TIMEOUT` | `5` | — | — | Timeout waiting for task cancellation |
| `DEFAULT_TASK_HANDLER_START_TIMEOUT` | `5` | `1` | `60` | Timeout for starting a handler |
| `DEFAULT_TASK_HANDLER_STOP_TIMEOUT` | `5` | `1` | `60` | Timeout for stopping a handler |

## Lifecycle

```
  [STOPPED] ──► [STARTING] ──► [RUNNING] ──► [STOPPING] ──► [STOPPED]
                   │              │               │
            _startup()      initialize()     _shutdown()
                   │              │               │
                   ▼              ▼               ▼
            print logo    start all         stop managers
            start loggers  task handlers     empty queue
            start managers start managers    cancel running tasks
                                  start       requeue tasks
                                  managers    stop handlers
```

The `initialize()` method starts all registered task handlers before
the worker enters `RUNNING` state.

## Properties

### Queue & Concurrency

| Property | Type | Default | Description |
|---|---|---|---|
| `queue_size` | `int` | `2` | Maximum size of the internal task queue |
| `max_concurrent_tasks` | `int` | `2` | Maximum tasks processed in parallel |
| `task_queue` | `asyncio.Queue` | — | The internal async queue holding `(UUID, TaskData)` tuples |

### Timing

| Property | Type | Default | Description |
|---|---|---|---|
| `task_manager_sleep_time` | `float` | `0.01` | Sleep between task manager loop iterations |
| `task_queue_manager_sleep_time` | `float` | `0.01` | Sleep between queue manager loop iterations |
| `task_handler_start_timeout` | `float` | `5` | Timeout for starting a task handler |
| `task_handler_stop_timeout` | `float` | `5` | Timeout for stopping a task handler |

### Task State

| Property | Type | Description |
|---|---|---|
| `task_handlers` | `dict[str, TaskHandler]` | Currently active (started) handlers |
| `running_tasks` | `dict[UUID, TaskTracker]` | Currently running tasks and their trackers |

All timing properties are clamped to their min/max bounds. Setters
accept `None` to reset to the default value.

## Constructor

```python
AsyncTaskProcessor(
    service_name: str = "service",
    version: str = "0.0.1",
    worker_id: int = 1,
    conf_dir: str | Path | None = None,
    logging_level: int | str = logging.DEBUG,
    heartbeat_interval: float | None = None,
    watchdog_interval: float | None = None,
    queue_size: int | None = None,
    max_concurrent_tasks: int | None = None,
    **kwargs,
)
```

Extra parameters (in addition to `BasicAsyncWorker`):

| Parameter | Default | Description |
|---|---|---|
| `queue_size` | `None` (uses `DEFAULT_MAX_TASKS_QUEUE_SIZE`) | Max queue size |
| `max_concurrent_tasks` | `None` (uses `DEFAULT_MAX_CONCURRENT_TASKS`) | Max concurrent tasks |

**kwargs** supports additional keys:

| Key | Default | Description |
|---|---|---|
| `task_manager_sleep_time` | `0.01` | Sleep between task manager iterations |
| `task_queue_manager_sleep_time` | `0.01` | Sleep between queue manager iterations |
| `task_handler_start_timeout` | `5` | Timeout for starting handlers |
| `task_handler_stop_timeout` | `5` | Timeout for stopping handlers |

Plus all `BasicAsyncWorker` kwargs (`logger_handler_timeout`,
`manager_shutdown_timeout`).

## Task Handler Registration

### Adding Handlers

```python
processor = AsyncTaskProcessor(service_name="task_worker", version="1.0.0")

# Register handler classes (processor creates instances)
processor.add_task_handler("email", EmailHandler)
processor.add_task_handler("report", ReportHandler)
```

Handlers can be registered before or after `start()`. If the worker is
already running, the handler is started asynchronously.

### Removing Handlers

```python
processor.remove_task_handler("email")
```

If the handler is currently active, it is stopped asynchronously. The
class mapping is removed immediately.

### Runtime Handler Discovery

When a task arrives, the processor iterates over all active handlers
and calls `handler.supports(task_type)`. The first handler returning
`True` receives the task:

```python
handler = processor._find_task_handler("email")
# Returns the EmailHandler instance, or None
```

## Task Processing

### Flow

```
  fetch_tasks()          task_manager()          process_task()
        │                       │                        │
        ▼                       ▼                        ▼
   [task_id, TaskData] ──► get from queue ──► _find_task_handler()
        │                       │                        │
        │                  create asyncio.Task         handler.handle()
        │                       │                        │
        │                  track in running_tasks      TaskResult
        │                       │                        │
        └──── Watchdog monitors ────────────────────────┘
            for timeouts and cancellation
```

### Concurrent Execution

The `task_manager` respects `max_concurrent_tasks`. When the limit is
reached, it sleeps for `task_manager_sleep_time` before the next
iteration:

```python
# If max_concurrent_tasks=4 and 4 tasks are running:
#   task_manager sleeps, does NOT fetch more tasks
# When a task completes, running_tasks shrinks and a new task is fetched
```

### Task Timeout Monitoring

The `watchdog()` method (overridden from `BasicAsyncWorker`) checks all
running tasks for timeouts:

```python
async def watchdog(self) -> None:
    now = time.time()
    for task_id, tracker in self.running_tasks.items():
        timeout = tracker.data.timeout.timeout or DEFAULT_TASK_TIMEOUT
        if 0 < timeout < (now - tracker.started):
            # Task exceeded its timeout
            tracker.worker_task.cancel()
            if tracker.data.timeout.timeout_action == "requeue":
                await self.return_task_to_queue(task_id, tracker.data)
```

Timeout behavior is controlled by `TaskTimeout`:

| `timeout` | `timeout_action` | Behavior |
|---|---|---|
| `None` | — | Uses `DEFAULT_TASK_TIMEOUT` (3s) |
| `> 0` | `"requeue"` | Cancel task and return to external queue |
| `> 0` | `"discard"` | Cancel task, do not requeue |

## Overriding Methods

### fetch_tasks()

Override to retrieve tasks from an external source and enqueue them:

```python
class MyWorker(AsyncTaskProcessor):
    async def fetch_tasks(self) -> None:
        """Pull tasks from a message queue."""
        while not self.task_queue.full():
            try:
                raw = await self.message_queue.get(timeout=0.1)
                task_id = uuid4()
                task_data = TaskData(
                    task=raw["type"],
                    payload=raw["payload"].encode(),
                    timeout=TaskTimeout(timeout=raw.get("timeout")),
                )
                self.task_queue.put_nowait((task_id, task_data))
            except Empty:
                break
```

### return_task_to_queue()

Override to implement custom re-queueing logic for timed-out or
cancelled tasks:

```python
class MyWorker(AsyncTaskProcessor):
    async def return_task_to_queue(self, task_id: UUID, task_data: TaskData) -> None:
        """Send timed-out tasks back to the external queue."""
        raw = {
            "task_id": str(task_id),
            "task": task_data.task,
            "payload": task_data.payload.decode(),
        }
        await self.message_queue.put(json.dumps(raw).encode())
```

### cleanup()

Override to add custom cleanup logic. The base implementation already:

1. Calls `super().cleanup()` (stops managers, shuts down loggers)
2. Returns pending tasks from the internal queue
3. Cancels and requeues running tasks
4. Stops all task handlers

```python
async def cleanup(self) -> None:
    # Custom cleanup before base cleanup runs
    await self.flush_local_cache()

    # Call base cleanup (handles queue, running tasks, handlers)
    await super().cleanup()

    # Custom cleanup after
    await self.notify_shutdown_complete()
```

### initialize()

Override to add custom initialization. The base implementation starts
all registered task handlers:

```python
async def initialize(self) -> bool:
    # Custom initialization
    self.external_client = await connect_external_service()

    # Start task handlers (base behavior)
    result = await super().initialize()

    return result
```

### watchdog()

Override to extend the default timeout monitoring:

```python
async def watchdog(self) -> None:
    # Run default timeout monitoring
    await super().watchdog()

    # Additional watchdog logic
    if await self.is_memory_high():
        self.logger.warning("Memory usage high, pausing task fetch")
```

## Example

```python
import asyncio
import json
import uuid
from uuid import uuid4

from scietex.service import AsyncTaskProcessor
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult


class EmailHandler(TaskHandler):
    """Handles email-sending tasks."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["send_email"]

    async def handle(self, task_data: TaskData) -> TaskResult:
        payload = json.loads(task_data.payload)
        # await self.smtp_client.send(payload["to"], payload["subject"], payload["body"])
        return TaskResult(
            status="success",
            payload=json.dumps({"sent_to": payload["to"]}).encode(),
        )


class MyTaskWorker(AsyncTaskProcessor):
    """A task processor that fetches from a simulated queue."""

    def __init__(self, **kwargs):
        super().__init__(service_name="task_worker", version="1.0.0", **kwargs)
        self._external_queue: list[dict] = []

    async def initialize(self) -> bool:
        """Register handlers and connect to external services."""
        self.add_task_handler("email", EmailHandler)
        return await super().initialize()

    async def fetch_tasks(self) -> None:
        """Pull tasks from the simulated external queue."""
        while not self.task_queue.full():
            if not self._external_queue:
                break
            item = self._external_queue.pop(0)
            task_id = uuid4()
            task_data = TaskData(
                task=item["type"],
                payload=json.dumps(item["payload"]).encode(),
                timeout=TaskTimeout(timeout=item.get("timeout")),
            )
            self.task_queue.put_nowait((task_id, task_data))

    async def return_task_to_queue(self, task_id: uuid.UUID, task_data: TaskData) -> None:
        """Re-queue timed-out tasks."""
        self._external_queue.append(
            {
                "type": task_data.task,
                "payload": json.loads(task_data.payload),
            }
        )
        self.logger.info("Re-queued task %s", task_id)

    async def cleanup(self) -> None:
        """Flush any remaining tasks back to the external queue."""
        while not self.task_queue.empty():
            task_id, task_data = await self.task_queue.get()
            await self.return_task_to_queue(task_id, task_data)
            self.task_queue.task_done()
        await super().cleanup()


async def main():
    worker = MyTaskWorker(
        queue_size=10,
        max_concurrent_tasks=4,
        watchdog_interval=2,
    )

    # Simulate incoming tasks
    worker._external_queue.extend(
        [
            {"type": "send_email", "payload": {"to": "user@example.com", "subject": "Hello"}},
            {"type": "send_email", "payload": {"to": "admin@example.com", "subject": "Alert"}},
        ]
    )

    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

## Best Practices

### Queue Size vs Concurrency

Size `queue_size` based on your expected burst load. Set
`max_concurrent_tasks` based on your resource constraints (CPU, memory,
external service rate limits):

```python
worker = MyWorker(
    queue_size=100,  # Buffer for bursts
    max_concurrent_tasks=8,  # Respect API rate limits
)
```

### Handler Registration Timing

Register handlers early. If registered after `start()`, they are started
asynchronously and there is a brief window where tasks may arrive before
the handler is ready:

```python
# Best: register before start
processor.add_task_handler("email", EmailHandler)
await processor.start()

# Acceptable: register immediately after start
await processor.start()
processor.add_task_handler("email", EmailHandler)
```

### Timeout Configuration

Set per-task timeouts based on expected processing time. Use
`timeout_action="discard"` for idempotent operations where retry is
unnecessary:

```python
task = TaskData(
    task="send_notification",
    payload=b'{"user_id": 123}',
    timeout=TaskTimeout(timeout=2.0, timeout_action="discard"),
)

task = TaskData(
    task="generate_report",
    payload=b'{"report_id": 42}',
    timeout=TaskTimeout(timeout=30.0, timeout_action="requeue"),
)
```

### Error Handling in Handlers

Always return `TaskResult` with explicit status. The processor wraps
unexpected exceptions automatically, but explicit error handling
provides better context:

```python
async def handle(self, task_data: TaskData) -> TaskResult:
    try:
        result = await self._do_work(task_data)
        return TaskResult(status="success", payload=result)
    except ValidationError as exc:
        # Client error — result is recorded but not retried
        return TaskResult(status="error", error=str(exc))
    except ConnectionError as exc:
        # Retryable — task may be requeued by watchdog
        return TaskResult(status="error", error=str(exc))
```

### Sleep Time Tuning

Lower sleep times reduce latency but increase CPU usage. Higher sleep
times reduce CPU but increase task fetch delay:

```python
worker = MyWorker(
    task_manager_sleep_time=0.001,  # Low latency, higher CPU
    task_queue_manager_sleep_time=0.1,  # Lower CPU for fetch loop
)
```
