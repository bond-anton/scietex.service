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
- **`CancelTaskHandler`** — Built-in handler for the `cancel_task` task type (in `task_handler/cancel.py`)
- **`TaskData`** — Immutable task payload passed to handlers
- **`TaskResult`** — Standardized result returned by handlers
- **`TaskTimeout`** — Configuration for task timeout behavior
- **`TaskStatus`** — Per-task tracking record published to the transport
- **`TaskProgress`** — Granular progress payload embedded in `TaskStatus.progress`
- **`TaskTracker`** — In-memory runtime handle (in `task_handler/runtime.py`) for monitoring running tasks
- **`TaskEnvelope`** — Versioned transport envelope for the durable wire format

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
3. **Processing** — Tasks are dispatched to
   `handler.handle(task_data, capabilities=...)` only when `handler.is_ready`
   is `True`
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
from scietex.service.task_handler import TaskCapabilities, TaskData, TaskHandler, TaskResult


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

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
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

## Task Cancellation

`TaskProcessor` auto-registers a built-in `CancelTaskHandler` for the
`cancel_task` task type. It is transport-agnostic: the processor injects an
async callback (its own `_cancel_task`) at registration, so the handler never
reaches into processor internals.

A cancellation is submitted like any other task — a `cancel_task` `TaskData`
wrapped in the usual `TaskEnvelope` via `encode_task_envelope`:

```python
import msgspec
from scietex.service.task_handler import CancelTaskRequest, TaskData

task_data = TaskData(
    task_id="<uuid>",
    task="cancel_task",
    payload=msgspec.msgpack.encode(
        CancelTaskRequest(target_task_id="<uuid>", reason="operator request")
    ),
)
```

The worker cancels a running target using the same pattern as the watchdog
(`cancel()` plus a bounded `asyncio.wait`), or removes a queued-but-not-yet-
dispatched target. The outcome is one of `CancelOutcome`:

| Outcome | Meaning |
|---|---|
| `cancelled` | The target was running and stopped, or was queued and removed before it started |
| `not_running` | The target is not running or queued (already finished, never seen, or the request targeted the cancelling task itself) |
| `ignored` | The target is running but did not stop within the cancellation timeout; it stays tracked and is not requeued |
| `not_found` | Reserved for a target that cannot be resolved |

The cancel task's own result is:

- **Success** with a msgpack-encoded `CancelTaskResponse` payload when the
  target was cancelled.
- **Non-retryable error** (`retryable=False`) otherwise, with `error_code`
  one of `TASK_NOT_RUNNING`, `CANCEL_IGNORED`, or `INVALID_CANCEL_PAYLOAD`.

A deliberate cancel is never requeued automatically. The transport (e.g.
`ValkeyWorker`) writes a terminal `TaskStatus` with `status="cancelled"` and the
original `TaskData` embedded in `data`; an external process can read that
record, modify the payload, and resubmit under a **new** task id. Timeout and
shutdown cancellations keep the existing `failed`/`"canceled"` status.

Two constraints apply:

- Reliable cancellation needs `max_concurrent_tasks >= 2`: with a single slot
  the cancel task queues behind its target and cannot run.
- Tasks still unread in the stream are not cancellable and yield
  `not_running`.

### CancelTaskRequest

Payload of a `cancel_task` task (in `task_handler/cancel.py`).

```python
class CancelTaskRequest(msgspec.Struct, frozen=True):
    target_task_id: str  # UUID (as a string) of the task to cancel
    reason: str = ""  # Optional operator note, for logging/audit only
```

| Field | Type | Default | Description |
|---|---|---|---|
| `target_task_id` | `str` | *(required)* | UUID (as a string) of the task to cancel |
| `reason` | `str` | `""` | Optional operator note, for logging/audit only |

### CancelTaskResponse

Payload returned by a successful `cancel_task` task.

```python
class CancelTaskResponse(msgspec.Struct, frozen=True):
    target_task_id: str  # UUID (as a string) of the cancelled task
    outcome: str  # The CancelOutcome value
```

| Field | Type | Default | Description |
|---|---|---|---|
| `target_task_id` | `str` | *(required)* | UUID (as a string) of the task that was cancelled |
| `outcome` | `str` | *(required)* | The `CancelOutcome` value |

## Schemas

All schemas are frozen `msgspec.Struct` instances, making them immutable
and hashable.

### TaskData

Immutable task payload passed to handlers.

```python
class TaskData(msgspec.Struct, frozen=True):
    task_id: str  # Unique task id (string UUID)
    task: str  # Task type string
    timeout: TaskTimeout = TaskTimeout()  # Timeout configuration
    canceled_action: Literal["requeue", "discard"] = "requeue"
    payload: bytes = b""  # Raw task data
```

| Field | Type | Default | Description |
|---|---|---|---|
| `task_id` | `str` | *(required)* | Unique task id (a string UUID); added as a required first field in v5.0.0 |
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
| `retryable` | `bool` | `False` | The single retry signal: whether the failure is transient and may succeed on retry. `True` triggers the framework's one retry; a second consecutive retryable failure is acked terminal |
| `partial` | `bool` | `False` | Whether partial progress was made before the error |

The error-taxonomy fields (`error_code`, `retryable`, `partial`) all
default to "no extra information", so handlers that only set `status`
and `error` keep working unchanged.

### TaskStatus

Per-task tracking record published to the transport by transports that
implement tracking (e.g. `ValkeyWorker`).

```python
class TaskStatus(msgspec.Struct, frozen=True):
    task_id: str
    service: str
    task: str
    status: Literal["queued", "running", "completed", "failed", "cancelled"]
    progress: TaskProgress = TaskProgress()
    result: bytes | None = None
    data: TaskData | None = None
    error: str = ""
    error_code: str = ""
    created_at: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = msgspec.field(default_factory=lambda: datetime.now(timezone.utc))
```

| Field | Type | Default | Description |
|---|---|---|---|
| `task_id` | `str` | *(required)* | Task identifier |
| `service` | `str` | *(required)* | Service name that owns the task |
| `task` | `str` | *(required)* | Task type string |
| `status` | `"queued"`, `"running"`, `"completed"`, `"failed"`, or `"cancelled"` | *(required)* | Tracking state; `"cancelled"` is written only for a deliberate `cancel_task` |
| `progress` | `TaskProgress` | `TaskProgress()` | Granular progress reported by the handler |
| `result` | `bytes` or `None` | `None` | Handler result payload on success |
| `data` | `TaskData` or `None` | `None` | Original task data, embedded only on a deliberate cancel so an external process can modify and resubmit it |
| `error` | `str` | `""` | Error message; `"canceled"` on cancellation |
| `error_code` | `str` | `""` | Structured error code |
| `created_at` | `datetime` | current UTC | When the record was created |
| `updated_at` | `datetime` | current UTC | When the record was last updated |

> **Split ownership of `"queued"`.** Under the Valkey transport, `"queued"` is a
> submitter-side write that the library does not perform. Under the MQTT
> transport, the worker itself publishes `"queued"` when a task is accepted,
> recovered from the inbox, or requeued (see `MqttTransport`), so it does appear
> in worker output there. In both transports the worker writes `"running"`,
> `"completed"`, `"failed"`, and (for a deliberate cancel) `"cancelled"`.

### TaskProgress

Granular progress reported by a task handler, embedded in
`TaskStatus.progress`. `value` is only meaningful when `progress` is `True`.

```python
class TaskProgress(msgspec.Struct, frozen=True):
    progress: bool = False
    value: float = 0.0
```

| Field | Type | Default | Description |
|---|---|---|---|
| `progress` | `bool` | `False` | Whether the handler reports granular progress |
| `value` | `float` | `0.0` | Progress value; only meaningful when `progress` is `True` |

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

Internal structure used by `TaskProcessor` to monitor running tasks. It is a
runtime handle (defined in `task_handler/runtime.py`), not a wire schema — it
holds a live `asyncio.Task` and is never serialized. It is a frozen dataclass,
not a `msgspec.Struct`, so it cannot be mistaken for a serializable type.

```python
@dataclass(frozen=True, slots=True)
class TaskTracker:
    worker_task: asyncio.Task
    data: TaskData
    started: int | float  # Monotonic timestamp
```

| Field | Type | Description |
|---|---|---|
| `worker_task` | `asyncio.Task` | The async task executing this work |
| `data` | `TaskData` | Associated task data |
| `started` | `int` or `float` | Monotonic timestamp when created |

### TaskEnvelope

Versioned transport envelope (AR-064). The durable on-the-wire format for a
task is this envelope, not a bare `TaskData`, so the transport format can
evolve independently of the handler contract:

```python
class TaskEnvelope(msgspec.Struct, frozen=True):
    version: int = 1  # Wire-format version
    data: bytes = b""  # Serialized task payload (version 1: msgpack TaskData)
```

| Field | Type | Default | Description |
|---|---|---|
| `version` | `int` | `1` | Wire-format version |
| `data` | `bytes` | `b""` | Serialized task payload; version 1 wraps a msgpack-encoded `TaskData` |

### Wire Format

The durable stream value is
`msgpack(TaskEnvelope(version=1, data=msgpack(TaskData)))`. Two
transport-agnostic helpers centralize encoding/decoding
(`scietex.service.task_handler.wire`, exported from
`scietex.service.task_handler`):

- `encode_task_envelope(task_data: TaskData) -> bytes` — wraps a `TaskData`
  into the versioned envelope and msgpack-encodes it.
- `decode_task_envelope(payload: bytes) -> TaskData | None` — decodes an
  envelope back to a `TaskData`; returns `None` on an invalid payload or an
  unknown version so callers skip the entry without crashing intake.

Handlers never see the envelope — they receive the decoded `TaskData` and
their contract is unchanged. See
[ValkeyWorker — Wire Format](valkey_worker.md#wire-format) for the transport
contract.

## Integration with TaskProcessor

The `TaskProcessor` manages task handler registration and dispatch.

### Registration

```python
from scietex.service import TaskProcessor, TaskProcessorConfig

processor = TaskProcessor(TaskProcessorConfig(service_name="my_service", version="1.0.0"))

# Register a handler class (not an instance — processor creates instances)
processor.add_task_handler(EmailHandler)
processor.add_task_handler(DataHandler)
```

`add_task_handler()` takes the handler class plus an optional keyword-only
`name`. The lifecycle key is the resolved name: `name` if given, otherwise
the class name (`handler_class.__name__`). A single instance per resolved
key is created on start. Dispatch selects handlers by their `supported_tasks`
membership, so registering the same resolved key twice raises a
`ValueError`. The optional `name` lets multiple instances of one class
coexist under distinct keys (e.g. to split one class's task types across
instances via name-derived `supported_tasks`).

Any additional keyword arguments are forwarded to the handler constructor on
each instantiation (`**handler_kwargs`), which is how a handler becomes
stateful — inject a shared mutable object at registration and every start
cycle receives the same object. A misspelled kwarg raises a loud `TypeError`
at construction, since `TaskHandler` subclasses do not accept arbitrary
kwargs.

A runnable stateful-handler example is in
[`examples/stateful_handler.py`](../examples/stateful_handler.py).

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

### Overriding Task Re-queueing (compatibility shim)

Prefer `TaskTransport.requeue`; this hook is a compatibility shim.

Subclasses can override `return_task_to_queue` to implement custom
re-queueing logic (e.g., writing timed-out tasks back to a message queue):

```python
class MyWorker(TaskProcessor):
    async def return_task_to_queue(self, task_data: TaskData) -> None:
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
- The framework grants **at most one error-path retry per task id**. A
  second consecutive `retryable=True` failure is acked as **terminal**
  with `retryable=False` (and a warning logged) instead of being
  requeued again.
- Framework failures (empty `task` field, no matching handler) are
  permanent and leave `retryable=False`.

```python
async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
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
    task_id="<uuid>",
    task="generate_report",
    payload=b'{"report_id": 42}',
    timeout=TaskTimeout(timeout=10.0, timeout_action="requeue"),
)

# Fast operation with discard-on-timeout
task = TaskData(
    task_id="<uuid>",
    task="send_notification",
    payload=b'{"user_id": 123}',
    timeout=TaskTimeout(timeout=1.0, timeout_action="discard"),
)
```
