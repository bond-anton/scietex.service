# Task Handler System

The task handler subsystem provides a pluggable architecture for processing
different types of async tasks within `scietex.service` workers. It defines
the contract between the task processor and concrete handler implementations,
along with typed schemas for task data and results.

## Overview

```python
from scietex.service.task_handler import TaskHandler, TaskData, TaskResult
```

The system consists of:

- **`TaskHandler`** — Abstract base class that all handlers must extend
- **`TaskData`** — Immutable task payload passed to handlers
- **`TaskResult`** — Standardized result returned by handlers
- **`TaskTimeout`** — Configuration for task timeout behavior
- **`TaskTracker`** — Internal structure for monitoring running tasks

## Handler Lifecycle

Each handler goes through a well-defined lifecycle managed by
`TaskProcessor`:

```
  [created] ──► [start()] ──► [is_ready=initialize()] ──► [stop()] ──► [is_ready=False]
                    │                    │                    │
                    │              initialize()            cleanup()
                    │                    │                    │
                    ▼                    ▼                    ▼
              logging          setup resources       release resources
              handler name     handler state set      handler state reset
```

1. **Registration** — `processor.add_task_handler(HandlerClass)`
2. **Start** — `handler.start()` calls `handler.initialize()` and sets
   `is_ready` to the value `initialize()` returns. An `initialize()`
   returning `False` leaves `is_ready == False`, and the processor then
   fails the handler (removes it from active handlers)
3. **Processing** — Tasks are dispatched to `handler.handle(task_data)`
   only when `handler.is_ready` is `True`
4. **Stop** — `handler.stop()` calls `handler.cleanup()` and sets
   `is_ready = False`

## TaskHandler Base Class

The `TaskHandler` abstract base class defines the contract that all
concrete handlers must implement.

### Required Overrides

| Member | Type | Description |
|---|---|---|
| `supported_tasks` | `property` | List of task type strings this handler supports |
| `handle()` | `async def` | Process a task and return a `TaskResult` |

### Optional Overrides

| Member | Type | Description |
|---|---|---|
| `initialize()` | `async def` | Setup resources before processing |
| `cleanup()` | `async def` | Release resources on shutdown |

### Available Attributes

| Attribute | Type | Description |
|---|---|---|
| `name` | `str` | Handler name (set at construction) |
| `context` | `TaskHandlerContext` | Narrow context exposing `service_name`, `instance_id`, `logger` |
| `logger` | `logging.Logger` | Logger instance from the parent worker |
| `is_ready` | `bool` | Whether the handler is initialized and ready |

The `worker` attribute no longer exists — handlers receive only the narrow
`TaskHandlerContext`, which cannot reach processor internals.

### Example Handler

```python
import json
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult


class EmailHandler(TaskHandler):
    """Sends emails based on task payload."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["send_email", "send_bulk_email"]

    async def initialize(self) -> bool:
        """Connect to the email service API."""
        self.logger.info("Connecting to email service…")
        # self.email_client = EmailClient(...)
        return True

    async def handle(self, task_data: TaskData) -> TaskResult:
        """Process an email task."""
        try:
            payload = json.loads(task_data.payload)
            recipient = payload["to"]
            subject = payload["subject"]
            # await self.email_client.send(recipient, subject, payload["body"])
            return TaskResult(status="success", payload=json.dumps({"sent": True}).encode())
        except Exception as exc:
            return TaskResult(status="error", error=str(exc))

    async def cleanup(self) -> None:
        """Close the email service connection."""
        self.logger.info("Closing email service connection")
        # await self.email_client.close()
```

### Task Type Selection

When a task arrives, the processor iterates over all registered handlers
and calls `handler.supports(task_type)`. The first handler returning
`True` receives the task:

```python
handler = processor._find_task_handler("send_email")
# Returns the EmailHandler instance above
```

## Schemas

All schemas are frozen `msgspec.Struct` instances, making them immutable
and hashable.

### TaskData

Immutable task payload passed to handlers.

```python
class TaskData(msgspec.Struct, frozen=True):
    task: str  # Task type string
    timeout: TaskTimeout = TaskTimeout()  # Timeout configuration
    canceled_action: Literal["requeue", "discard"] = "requeue"
    payload: bytes = b""  # Raw task data
```

| Field | Type | Default | Description |
|---|---|---|---|
| `task` | `str` | *(required)* | Task type used to select a handler |
| `timeout` | `TaskTimeout` | `TaskTimeout()` | Timeout configuration |
| `canceled_action` | `"requeue"` or `"discard"` | `"requeue"` | Action when task is canceled |
| `payload` | `bytes` | `b""` | Raw bytes payload |

### TaskResult

Standardized result returned from task handlers.

```python
class TaskResult(msgspec.Struct, frozen=True):
    status: Literal["success", "error"]
    error: str = ""
    processed_at: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
    payload: bytes = b""
    error_code: str = ""
    retryable: bool = False
    partial: bool = False
```

| Field | Type | Default | Description |
|---|---|---|---|
| `status` | `"success"` or `"error"` | *(required)* | Processing outcome |
| `error` | `str` | `""` | Error message (empty on success) |
| `processed_at` | `datetime` | current UTC | Timestamp when result was created |
| `payload` | `bytes` | `b""` | Optional result payload |
| `error_code` | `str` | `""` | Structured error taxonomy code (e.g. `"PERMANENT"` or `"TRANSIENT"`, or a domain-specific code). Empty means unset |
| `retryable` | `bool` | `False` | The single retry signal: whether the failure is transient and may succeed on retry. `True` triggers the framework's one retry |
| `partial` | `bool` | `False` | Whether partial progress was made before the error |

The error-taxonomy fields (`error_code`, `retryable`, `partial`) all
default to "no extra information", so handlers that only set `status`
and `error` keep working unchanged.

### TaskTimeout

Configuration for task timeout behavior.

```python
class TaskTimeout(msgspec.Struct, frozen=True):
    timeout: float | None = None
    timeout_action: Literal["requeue", "discard"] = "requeue"
```

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout` | `float` or `None` | `None` | Max seconds for completion. `None` uses the default (3s) |
| `timeout_action` | `"requeue"` or `"discard"` | `"requeue"` | Action when timeout is exceeded |

### TaskTracker

Internal structure used by `TaskProcessor` to monitor running tasks.

```python
class TaskTracker(msgspec.Struct, frozen=True):
    worker_task: asyncio.Task
    data: TaskData
    started: int | float  # Monotonic timestamp
```

| Field | Type | Description |
|---|---|---|
| `worker_task` | `asyncio.Task` | The async task executing this work |
| `data` | `TaskData` | Associated task data |
| `started` | `int` or `float` | Monotonic timestamp when created |

## Integration with TaskProcessor

The `TaskProcessor` manages task handler registration and dispatch.

### Registration

```python
processor = TaskProcessor(service_name="my_service", version="1.0.0")

# Register a handler class (not an instance — processor creates instances)
processor.add_task_handler(EmailHandler)
processor.add_task_handler(DataHandler)
```

`add_task_handler()` takes only the handler class. The lifecycle key is the
class name (`handler_class.__name__`), and a single instance per class is
created on start. Dispatch selects handlers by their `supported_tasks`
membership, so registering the same class twice raises a `ValueError`.

A handler can support multiple task types by returning them all from
`supported_tasks`. The processor matches incoming tasks by calling
`handler.supports(task_type)`.

### Task Dispatch Flow

```
  fetch_tasks()          process_task()          handler.handle()
       │                       │                        │
       ▼                       ▼                        ▼
  [task_id, TaskData] ──► _find_task_handler() ──► TaskResult
       │                       │
       │                  first handler where
       │                  supports(task_type) == True
       │
       └──► timeout watchdog monitors
           TaskTracker for each running task
```

### Overriding Task Re-queueing

Subclasses can override `return_task_to_queue` to implement custom
re-queueing logic (e.g., writing timed-out tasks back to a message queue):

```python
class MyWorker(TaskProcessor):
    async def return_task_to_queue(self, task_id: UUID, task_data: TaskData) -> None:
        await self.valkey_client.rpush("retry_queue", msgspec.msgpack.encode(task_data))
```

## Best Practices

### Handler Registration

Register each handler class once. A single handler class can serve
multiple task types by declaring them all in `supported_tasks`:

```python
# One registration per class covers all its supported task types
processor.add_task_handler(ImageHandler)


# Handler declares all supported types
class ImageHandler(TaskHandler):
    @property
    def supported_tasks(self) -> list[str]:
        return ["resize_image", "compress_image", "convert_image"]
```

### Error Handling

The processor distinguishes failure outcomes via `process_task()`:

- A handler that **raises** is treated as **permanent**
  (`retryable=False`). An unhandled exception is unclassified, so it
  must not create an infinite requeue loop under retry-once.
- A handler that **returns** its own `TaskResult` controls `retryable`
  (which defaults to `False`). Set `retryable=True` on transient errors
  that may succeed on retry.
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

### Resource Management

Use `initialize()` for setup and `cleanup()` for teardown. Both are
called by `start()` and `stop()` respectively:

```python
async def initialize(self) -> bool:
    self.db_pool = await create_pool(self.connection_string)
    self.logger.info("Database pool created")
    return True


async def cleanup(self) -> None:
    await self.db_pool.close()
    self.logger.info("Database pool closed")
```

### Timeout Configuration

Use `TaskTimeout` to control per-task timeout behavior:

```python
# Long-running report generation (10 second timeout)
task = TaskData(
    task="generate_report",
    payload=b'{"report_id": 42}',
    timeout=TaskTimeout(timeout=10.0, timeout_action="requeue"),
)

# Fast operation with discard-on-timeout
task = TaskData(
    task="send_notification",
    payload=b'{"user_id": 123}',
    timeout=TaskTimeout(timeout=1.0, timeout_action="discard"),
)
```
