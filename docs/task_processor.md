# TaskProcessor

The `TaskProcessor` is a concurrent task processing framework built
on `BasicWorker`. It adds a task queue, handler dispatch, concurrent
task execution, timeout monitoring via watchdog, and graceful shutdown
with task re-queueing.

## Overview

```python
from scietex.service import TaskProcessor
```

`TaskProcessor` extends `BasicWorker` with two additional
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
  │                    TaskProcessor                        │
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
| `DEFAULT_MAX_TASKS_QUEUE_SIZE` | `100` | — | — | Default max queue size |
| `DEFAULT_MAX_CONCURRENT_TASKS` | `10` | `1` | — | Default max concurrent tasks |
| `DEFAULT_MANAGER_SLEEP_TIME` | `0.01` | `0.001` | `1` | Default manager loop sleep |
| `DEFAULT_TASK_HANDLER_START_TIMEOUT` | `5` | `1` | `60` | Timeout for starting a handler |
| `DEFAULT_TASK_HANDLER_STOP_TIMEOUT` | `5` | `1` | `60` | Timeout for stopping a handler |

The task-level timing defaults (`task_timeout`, `task_queue_fetch_timeout`,
`task_cancellation_timeout`) are config fields on `TaskProcessorConfig` (AR-062);
see the [Constructor](#constructor) field table for their defaults and bounds.

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
| `queue_size` | `int` | `100` | Maximum size of the internal task queue |
| `max_concurrent_tasks` | `int` | `10` | Maximum tasks processed in parallel |

The internal queue is private. Access it through these non-blocking
methods:

| Method | Returns | Description |
|---|---|---|
| `enqueue_task(task_id, task_data)` | `bool` | Non-blocking `put_nowait`; returns `False` if the queue is full |
| `dequeue_task()` | `tuple[UUID, TaskData] \| None` | Non-blocking `get_nowait`; returns `None` if empty |
| `task_queue_empty()` | `bool` | `True` if the queue has no pending tasks |
| `task_queue_full()` | `bool` | `True` if the queue has reached its maximum size |

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
| `task_handlers` | `Mapping[str, TaskHandler]` | Currently active (started) handlers, as a read-only `MappingProxyType` view |
| `running_tasks` | `Mapping[UUID, TaskTracker]` | Currently running tasks and their trackers, as a read-only `MappingProxyType` view |

All timing properties are read-only and derive from the immutable
`TaskProcessorConfig`; a `None` field resolves to its `DEFAULT_*` constant,
and an out-of-range value raises `msgspec.ValidationError` at construction
(no runtime clamping or setters).

## Constructor

`TaskProcessor` takes a single immutable configuration object
(`TaskProcessorConfig`, from `scietex.service.config`, which extends
`WorkerConfig`), or `None` to use the struct defaults:

```python
import logging

from scietex.service import TaskProcessor, TaskProcessorConfig

processor = TaskProcessor(
    TaskProcessorConfig(
        service_name="service",
        version="0.0.1",
        conf_dir=None,
        logging_level=logging.DEBUG,
        heartbeat_interval=None,
        watchdog_interval=None,
        queue_size=None,
        max_concurrent_tasks=None,
        auto_tune=False,
        task_manager_sleep_time=None,
        task_queue_manager_sleep_time=None,
        task_handler_start_timeout=None,
        task_handler_stop_timeout=None,
        task_timeout=None,
        task_queue_fetch_timeout=None,
        task_cancellation_timeout=None,
    )
)
```

Fields added by `TaskProcessorConfig` (in addition to `WorkerConfig`):

| Field | Default | Description |
|---|---|---|
| `queue_size` | `None` (uses `DEFAULT_MAX_TASKS_QUEUE_SIZE`, `100`) | Max queue size |
| `max_concurrent_tasks` | `None` (uses `DEFAULT_MAX_CONCURRENT_TASKS`, `10`) | Max concurrent tasks |
| `auto_tune` | `False` | If `True` and `max_concurrent_tasks` is `None`, derive the concurrency from `os.cpu_count()` at startup |
| `task_manager_sleep_time` | `None` (uses `DEFAULT_MANAGER_SLEEP_TIME`, `0.01`) | Sleep between task manager iterations |
| `task_queue_manager_sleep_time` | `None` (uses `DEFAULT_MANAGER_SLEEP_TIME`, `0.01`) | Sleep between queue manager iterations |
| `task_handler_start_timeout` | `None` (uses `DEFAULT_TASK_HANDLER_START_TIMEOUT`, `5`) | Timeout for starting handlers |
| `task_handler_stop_timeout` | `None` (uses `DEFAULT_TASK_HANDLER_STOP_TIMEOUT`, `5`) | Timeout for stopping handlers |
| `task_timeout` | `None` (default `3`) | Global per-task timeout used when a task's `TaskTimeout.timeout` is `None`; `<= 0` means no timeout (unbounded) |
| `task_queue_fetch_timeout` | `None` (default `1`) | Timeout waiting to dequeue the next task |
| `task_cancellation_timeout` | `None` (default `5`) | Timeout waiting for a cancelled task to actually stop |

All `WorkerConfig` fields (`logger_handler_timeout`,
`manager_shutdown_timeout`, `manager_max_retries`,
`manager_restart_backoff`, etc.) are inherited. Configuration is
immutable: values are fixed at construction, and out-of-range values raise
`msgspec.ValidationError`.

## Task Handler Registration

### Adding Handlers

```python
processor = TaskProcessor(
    TaskProcessorConfig(service_name="task_worker", version="1.0.0")
)

# Register handler classes (processor creates instances)
processor.add_task_handler(EmailHandler)
processor.add_task_handler(ReportHandler)
```

`add_task_handler()` takes the handler class and an optional keyword-only
`name`. The lifecycle key is the resolved name: `name` if given, otherwise the
class name (`handler_class.__name__`). By default a single instance per class
is created on start, and registering the same resolved key twice raises a
`ValueError`. The optional `name` lets multiple instances of one class coexist
under distinct keys. Dispatch selects handlers by their `supported_tasks`
membership, not by the registration key, so a subclass can derive its task set
from `self.name` and split one class across several non-overlapping instances:

```python
class SlicedHandler(TaskHandler):
    _TASKS_BY_NAME = {"alpha": ["alpha_task"], "beta": ["beta_task"]}

    @property
    def supported_tasks(self) -> list[str]:
        return self._TASKS_BY_NAME[self.name]


processor.add_task_handler(SlicedHandler, name="alpha")
processor.add_task_handler(SlicedHandler, name="beta")
```

Because `_find_task_handler()` returns the first active instance whose
`supports(task)` is `True`, name-derived task sets must NOT overlap — if two
instances of the same class both claim the same task type, dispatch is
ambiguous (the first-registered wins). Choosing non-overlapping names is the
caller's responsibility.

`add_task_handler()` also accepts arbitrary keyword-only `**handler_kwargs`
that are forwarded to the handler constructor on **every** instantiation. This
is what makes a handler stateful: inject a shared mutable object (a counter, a
cache, a connection pool) once at registration, and each start cycle hands the
same object back to the handler, so its state survives stop/start. Because
`TaskHandler` subclasses do not accept arbitrary kwargs, a misspelled kwarg
raises a loud `TypeError` at construction rather than being silently ignored:

```python
class CountingHandler(TaskHandler):
    def __init__(self, name, context, *, counter: dict):
        super().__init__(name, context)
        self.counter = counter

    @property
    def supported_tasks(self) -> list[str]:
        return ["count"]

    async def handle(self, task_data: TaskData) -> TaskResult:
        self.counter["n"] = self.counter.get("n", 0) + 1
        return TaskResult(status="success")


shared_counter: dict = {}
processor.add_task_handler(CountingHandler, counter=shared_counter)
# each (re)start constructs CountingHandler(name, context, counter=shared_counter)
```

A runnable version of this pattern lives in
[`examples/stateful_handler.py`](../examples/stateful_handler.py).

Handlers can be registered before or after `start()`. If the worker is
already running, the handler is started asynchronously.

### Removing Handlers

```python
processor.remove_task_handler("EmailHandler")
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

The `watchdog()` method (overridden from `BasicWorker`) checks all
running tasks for timeouts:

```python
async def watchdog(self) -> None:
    now = time.monotonic()
    for task_id, tracker in list(self.running_tasks.items()):
        timeout = tracker.data.timeout.timeout
        if timeout is None:
            timeout = self._task_timeout  # resolved from config task_timeout (default 3 s)
        if 0 < timeout < (now - tracker.started) and not tracker.worker_task.done():
            # Task exceeded its timeout and is still running
            tracker.worker_task.cancel()
            await asyncio.wait(
                [tracker.worker_task],
                timeout=self._task_cancellation_timeout,  # config task_cancellation_timeout (default 5 s)
            )
            if tracker.worker_task.done():
                # Handler actually stopped; requeue a fresh delivery only now
                if tracker.data.timeout.timeout_action == "requeue":
                    await self.return_task_to_queue(task_id, tracker.data)
            # else: handler ignored cancellation — requeueing would run it twice
```

Timeout behavior is controlled by `TaskTimeout`:

| `timeout` | `timeout_action` | Behavior |
|---|---|---|
| `None` | — | Uses `task_timeout` (default 3s) |
| `> 0` | `"requeue"` | Cancel task and return to external queue |
| `> 0` | `"discard"` | Cancel task, do not requeue |
| `<= 0` | — | No timeout (unbounded) — the watchdog never cancels the task |

## Overriding Methods

### fetch_tasks()

Override to retrieve tasks from an external source and enqueue them:

```python
class MyWorker(TaskProcessor):
    async def fetch_tasks(self) -> bool:
        """Pull tasks from a message queue."""
        while not self.task_queue_full():
            try:
                raw = await self.message_queue.get(timeout=0.1)
                task_id = uuid4()
                task_data = TaskData(
                    task=raw["type"],
                    payload=raw["payload"].encode(),
                    timeout=TaskTimeout(timeout=raw.get("timeout")),
                )
                self.enqueue_task(task_id, task_data)
            except Empty:
                break
        # Return True when at least one task was enqueued so the
        # task_queue_manager skips its idle backoff and drains back-to-back.
        return True
```

### return_task_to_queue()

Override to implement custom re-queueing logic for timed-out or
cancelled tasks:

```python
class MyWorker(TaskProcessor):
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

1. Calls `super().cleanup()` (a no-op on `BasicWorker`)
2. Drains the internal queue by dropping items — they stay pending in the
   transport and are redelivered on restart (subclasses whose transport
   does not keep items pending must override to requeue drained items)
3. Cancels running tasks; a task is requeued via `return_task_to_queue()`
   only after its handler actually stops, honoring `canceled_action`
4. Stops all task handlers

It does not stop managers or shut down loggers — `BasicWorker`
handles those during shutdown.

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

from scietex.service import TaskProcessor, TaskProcessorConfig
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult, TaskTimeout


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


class MyTaskWorker(TaskProcessor):
    """A task processor that fetches from a simulated queue."""

    def __init__(self, config: TaskProcessorConfig | None = None):
        super().__init__(config)
        self._external_queue: list[dict] = []

    async def initialize(self) -> bool:
        """Register handlers and connect to external services."""
        self.add_task_handler(EmailHandler)
        return await super().initialize()

    async def fetch_tasks(self) -> bool:
        """Pull tasks from the simulated external queue."""
        enqueued = False
        while not self.task_queue_full():
            if not self._external_queue:
                break
            item = self._external_queue.pop(0)
            task_id = uuid4()
            task_data = TaskData(
                task=item["type"],
                payload=json.dumps(item["payload"]).encode(),
                timeout=TaskTimeout(timeout=item.get("timeout")),
            )
            self.enqueue_task(task_id, task_data)
            enqueued = True
        return enqueued

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
        while True:
            task = self.dequeue_task()
            if task is None:
                break
            task_id, task_data = task
            await self.return_task_to_queue(task_id, task_data)
        await super().cleanup()


async def main():
    worker = MyTaskWorker(
        TaskProcessorConfig(
            service_name="task_worker",
            version="1.0.0",
            queue_size=10,
            max_concurrent_tasks=4,
            watchdog_interval=2,
        )
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
    TaskProcessorConfig(
        queue_size=100,  # Buffer for bursts
        max_concurrent_tasks=8,  # Respect API rate limits
    )
)
```

### Handler Registration Timing

Register handlers early. If registered after `start()`, they are started
asynchronously and there is a brief window where tasks may arrive before
the handler is ready:

```python
# Best: register before start
processor.add_task_handler(EmailHandler)
await processor.start()

# Acceptable: register immediately after start
await processor.start()
processor.add_task_handler(EmailHandler)
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

`process_task()` applies an error taxonomy to the `TaskResult` it
produces:

- A handler that **raises** is treated as **permanent**
  (`retryable=False`). An unhandled exception is unclassified, so it
  must not create an infinite requeue loop under retry-once.
- A handler that **returns** its own `TaskResult` controls `retryable`
  (which defaults to `False`). Set `retryable=True` on transient errors
  to trigger the framework's single retry.
- Framework failures (empty `task` field, no matching handler) are
  permanent and leave `retryable=False`.

```python
async def handle(self, task_data: TaskData) -> TaskResult:
    try:
        result = await self._do_work(task_data)
        return TaskResult(status="success", payload=result)
    except ValidationError as exc:
        # Permanent client error — not retryable
        return TaskResult(status="error", error=str(exc))
    except ConnectionError as exc:
        # Transient error — mark retryable to trigger the single retry
        return TaskResult(status="error", error=str(exc), retryable=True)
```

### Sleep Time Tuning

Lower sleep times reduce latency but increase CPU usage. Higher sleep
times reduce CPU but increase task fetch delay:

```python
worker = MyWorker(
    TaskProcessorConfig(
        task_manager_sleep_time=0.001,  # Low latency, higher CPU
        task_queue_manager_sleep_time=0.1,  # Lower CPU for fetch loop
    )
)
```
