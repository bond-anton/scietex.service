# ValkeyWorker

The `ValkeyWorker` is a Valkey-backed async task processor that extends
`AsyncTaskProcessor` with Valkey stream-based task distribution, heartbeat
publishing, and async logging. It uses the `glide` client for all Valkey
operations.

## Overview

```python
from scietex.service.valkey import ValkeyWorker
```

`ValkeyWorker` adds Valkey-specific operations on top of
`AsyncTaskProcessor`:

| Feature | Description |
|---|---|
| Stream-based tasks | Tasks are stored in a Valkey stream with consumer group support |
| Heartbeat publishing | Worker status is published to a key with automatic TTL |
| Async logging | Log entries are written to a Valkey stream via `AsyncValkeyHandler` |
| Auto-reconnect | Connection errors trigger automatic disconnect/reconnect cycles |

**Requires the optional `valkey-glide` dependency:**

```bash
pip install scietex.service[valkey]
```

## Architecture

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                      ValkeyWorker                               │
  │                                                                 │
  │  ┌─────────────────────┐    ┌──────────────────────────────┐    │
  │  │  TaskQueueManager    │───►│  task_queue (asyncio.Queue)  │    │
  │  │  (fetch loop)        │    │                              │    │
  │  └─────────────────────┘    └──────────┬───────────────────┘    │
  │                                        │                         │
  │  ┌─────────────────────┐    ┌──────────▼───────────────────┐    │
  │  │     TaskManager      │───►│  process_task()              │    │
  │  │  (process loop)      │    │  ┌─────────────────────────┐ │    │
  │  └─────────────────────┘    │  │ handler.handle(task)     │ │    │
  │                              │  └────────┬───────────────┘ │    │
  │                              └───────────┼─────────────────┘    │
  │                                          │                       │
  │  ┌─────────────────────┐    ┌────────────▼───────────────┐     │
  │  │      Watchdog        │───►│  running_tasks dict        │     │
  │  │  (timeout monitor)   │    │  TaskTracker per task      │     │
  │  └─────────────────────┘    └────────────────────────────┘     │
  │                                                                 │
  │  ┌─────────────────────┐    ┌────────────────────────────┐     │
  │  │    Heartbeat         │───►│  scietex:{svc}:{id}:status │     │
  │  │    (periodic)        │    │  msgpack, TTL = 2*interval │     │
  │  └─────────────────────┘    └────────────────────────────┘     │
  │                                                                 │
  │  ┌─────────────────────┐    ┌────────────────────────────┐     │
  │  │  AsyncValkeyHandler  │───►│  scietex:log stream        │     │
  │  │  (log entries)       │    │  msgpack-encoded entries   │     │
  │  └─────────────────────┘    └────────────────────────────┘     │
  └─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
                 ┌─────────────────┐
                 │   Valkey Server  │
                 │                  │
                 │  Stream:         │
                 │  scietex:{svc}   │
                 │  :{id}:tasks     │
                 │                  │
                 │  Group:          │
                 │  scietex:{svc}   │
                 │  :{id}:task_group│
                 └─────────────────┘
```

## Key Names

All Valkey resources follow a naming convention based on `service_name`
and `worker_id`:

| Resource | Key Pattern |
|---|---|
| Task stream | `scietex:{service_name}:{worker_id}:tasks` |
| Consumer group | `scietex:{service_name}:{worker_id}:task_group` |
| Consumer name | `scietex:{service_name}:{worker_id}` |
| Heartbeat key | `scietex:{service_name}:{worker_id}:status` |
| Log stream | `scietex:log` (configurable via `log_stream_name`) |

## Constants

| Constant | Default | Description |
|---|---|---|
| `DEFAULT_MAX_TASKS_QUEUE_SIZE` | `2` | Default max queue size (inherited) |
| `DEFAULT_MAX_CONCURRENT_TASKS` | `2` | Default max concurrent tasks (inherited) |
| `DEFAULT_TASK_TIMEOUT` | `3` | Default task timeout in seconds (inherited) |
| `DEFAULT_HEARTBEAT_INTERVAL` | `10` | Default heartbeat interval in seconds |
| `DEFAULT_WATCHDOG_INTERVAL` | `1` | Default watchdog check interval in seconds |

## Lifecycle

```
  [STOPPED] ──► [STARTING] ──► [RUNNING] ──► [STOPPING] ──► [STOPPED]
                   │              │               │
            _startup()      initialize()     _shutdown()
                   │              │               │
                   ▼              ▼               ▼
            print logo    connect Valkey     disconnect
            start loggers  create stream      stop managers
            start managers start handlers     empty queue
                                 start        cancel running tasks
                                 managers      stop handlers
```

The `initialize()` method starts all registered task handlers, connects
to Valkey, and creates the consumer group for the task stream (with
`make_stream=True`).

## Properties

### Valkey-Specific

| Property | Type | Default | Description |
|---|---|---|---|
| `valkey_config` | `ValkeyConfig \| GlideClientConfiguration` | — | The Valkey configuration used by this worker |
| `client` | `GlideClient \| None` | `None` | The active Valkey client (``None`` until connected) |
| `logging_connected` | `bool` | `False` | Whether the logging handler has a live Valkey client (``True`` only when a registered `AsyncValkeyHandler` exists and its `client` is not ``None``) |

### Inherited from AsyncTaskProcessor

| Property | Type | Default | Description |
|---|---|---|---|
| `queue_size` | `int` | `2` | Maximum size of the internal task queue |
| `max_concurrent_tasks` | `int` | `2` | Maximum tasks processed in parallel |
| `task_handlers` | `Mapping[str, TaskHandler]` | — | Currently active (started) handlers, as a read-only `MappingProxyType` view |
| `running_tasks` | `Mapping[UUID, TaskTracker]` | — | Currently running tasks and their trackers, as a read-only `MappingProxyType` view |

## Constructor

```python
ValkeyWorker(
    service_name: str = "service",
    version: str = "0.0.1",
    worker_id: int = 1,
    conf_dir: str | Path | None = None,
    logging_level: int | str = logging.DEBUG,
    heartbeat_interval: float | None = None,
    watchdog_interval: float | None = None,
    queue_size: int | None = None,
    max_concurrent_tasks: int | None = None,
    valkey_config: ValkeyConfig | GlideClientConfiguration | None = None,
    log_stream_name: str = "scietex:log",
    share_glide_client: bool = False,
    **kwargs,
)
```

| Parameter | Default | Description |
|---|---|---|
| `service_name` | `"service"` | Name of the service, used for key naming and logging |
| `version` | `"0.0.1"` | Version string of the service |
| `worker_id` | `1` | Unique identifier for this worker instance |
| `conf_dir` | `None` | Directory to use for configuration files |
| `logging_level` | `logging.DEBUG` | Logging level as string or integer |
| `heartbeat_interval` | `None` (uses `DEFAULT_HEARTBEAT_INTERVAL`) | Heartbeat interval in seconds |
| `watchdog_interval` | `None` (uses `DEFAULT_WATCHDOG_INTERVAL`) | Watchdog check interval in seconds |
| `queue_size` | `None` (uses `DEFAULT_MAX_TASKS_QUEUE_SIZE`) | Max queue size |
| `max_concurrent_tasks` | `None` (uses `DEFAULT_MAX_CONCURRENT_TASKS`) | Max concurrent tasks |
| `valkey_config` | `None` | Custom Valkey configuration. If ``None``, reads
``valkey.yml`` from the config directory |
| `log_stream_name` | `"scietex:log"` | Name of the Valkey stream used for log entries |
| `share_glide_client` | `False` | Reserved feature flag for a single shared `GlideClient` across the task client and the logging handler. The external `scietex.logging` handler does not yet accept an injected client, so `True` logs a warning and falls back to the handler owning its own client |
| `**kwargs` | — | Additional kwargs passed to `AsyncTaskProcessor` |

## Methods

### connect()

Establish an asynchronous connection to the Valkey server.

```python
async def connect(self) -> bool:
    """Create a GlideClient and verify connectivity with PING."""
```

Returns `True` if the connection is established and `PING` succeeds;
`False` on connection failure or timeout.

### disconnect()

Gracefully close the connection to the Valkey server.

```python
async def disconnect(self):
    """Close the client, log the disconnection, set _client to None."""
```

### heartbeat()

Publish a heartbeat entry to the Valkey status key.

```python
async def heartbeat(self) -> None:
    """Encode Heartbeat struct, write to status key with TTL = 2 * interval."""
```

The heartbeat is serialized as msgpack and stored at
`scietex:{service_name}:{worker_id}:status` with a TTL set to twice the
heartbeat interval.

### initialize()

Initialize the worker and prepare the Valkey task stream.

```python
async def initialize(self) -> bool:
    """Start handlers, connect to Valkey, create consumer group."""
```

Returns `True` if the parent initialization and Valkey connection
succeed, and the consumer group is ready. `False` if the parent
initialization fails or the client is unavailable.

### cleanup()

Perform cleanup on shutdown.

```python
async def cleanup(self):
    """Drain queue, cancel tasks, close Valkey connection."""
```

Drains the internal task queue and cancels running tasks via the parent
`AsyncTaskProcessor.cleanup()`, then closes the Valkey connection.

### purge_tasks()

Purge all pending and unacknowledged tasks from the Valkey task stream.

```python
async def purge_tasks(self):
    """Read+ack+delete all entries, then purge the stream itself."""
```

Reads and acknowledges every entry in the task stream via `XREADGROUP`
(both pending and unclaimed), then deletes them with `XDEL`. Also purges
any remaining entries via `XREAD`.

### return_task_to_queue()

Re-queue a task by appending it to the Valkey task stream.

```python
async def return_task_to_queue(self, task_id: UUID, task_data: TaskData) -> None:
    """Encode TaskData with msgpack, append to task stream."""
```

Encodes `task_data` with msgpack and appends a new entry to the stream.
The entry key is the string representation of `task_id`.

### fetch_tasks()

Fetch a single task from the Valkey task stream and enqueue it.

```python
async def fetch_tasks(self):
    """XREADGROUP with block_ms=1000, decode msgpack, enqueue (non-blocking)."""
```

On the first call, recovers entries left pending by a previous run (see
[At-Least-Once Delivery](#at-least-once-delivery)). Then reads one entry
from the task stream using `XREADGROUP` with `block_ms=1000` and the
configured consumer group, decodes the msgpack payload into a `TaskData`
struct, and enqueues it via the non-blocking `enqueue_task()` as a
`(UUID, TaskData)` tuple. The stream entry is NOT acknowledged here — it
stays in the consumer group's pending list until `on_task_completed()`
acks it after the handler finishes. If the queue is full, the entry is
left pending (deferred) and is never blocking. On read errors,
disconnects and attempts to reconnect to Valkey.

### on_task_completed()

Acknowledge the stream entry for a completed task.

```python
async def on_task_completed(self, task_id, task_data, task_result):
    """XACK + XDEL the entry recorded in _task_entry_ids for task_id."""
```

Called by the base `AsyncTaskProcessor` when a task's processing
terminates (success, error, or cancellation). Looks up the stream entry
id recorded at fetch time and `XACK`s + `XDEL`s it, so the entry leaves
the consumer group's pending list only after the handler's work on it is
done. `task_result` is `None` when the task was cancelled before
producing a result.

## At-Least-Once Delivery

`ValkeyWorker` delivers each task at least once: a stream entry is
acknowledged and deleted only after its handler finishes, so a crash
mid-processing redelivers the task on restart.

- `_recover_pending_tasks()` — On the first `fetch_tasks()`, uses
  `XAUTOCLAIM` to claim every entry in the consumer group's pending list
  and re-enqueue it, redelivering tasks that were read but never
  acknowledged before a crash.
- `_task_entry_ids` — A `dict[UUID, str | bytes]` mapping each enqueued
  task's UUID to the stream entry id it was read from, recorded at fetch
  time so it can be acknowledged later.
- `on_task_completed()` — Called when a task's processing terminates.
  Looks up the recorded entry id and `XACK`s + `XDEL`s it, removing the
  entry from the pending list only after the handler's work is done.

If the queue is full when fetching (or during recovery), the entry is
left pending and redelivered on a later poll rather than dropped.

## Example

```python
import asyncio
import json
import uuid
from uuid import uuid4

from scietex.service.valkey import ValkeyWorker
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult
from scietex.service.task_handler import TaskTimeout


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


class MyValkeyWorker(ValkeyWorker):
    """A Valkey-backed task processor."""

    def __init__(self, **kwargs):
        super().__init__(
            service_name="email_service",
            version="1.0.0",
            worker_id=1,
            **kwargs,
        )

    async def initialize(self) -> bool:
        """Register handlers and prepare Valkey resources."""
        self.add_task_handler("email", EmailHandler)
        return await super().initialize()


async def main():
    worker = MyValkeyWorker(
        queue_size=10,
        max_concurrent_tasks=4,
        heartbeat_interval=10,
        watchdog_interval=2,
    )

    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration

### YAML Configuration File

`ValkeyWorker` reads configuration from `valkey.yml` in the config
directory. The file is created automatically with default values if it
does not exist. If the file exists but is invalid (unparseable), a
`RuntimeError` is raised and the file is left untouched.

```yaml
base_config:
  nodes:
    - host: localhost
      port: 6379
  user_credentials: null
  use_tls: false
  request_timeout: 5000
  database_id: null
  client_name: null
  inflight_requests_limit: null
  client_az: null
  lazy_connect: null
  read_from: PRIMARY
  backoff_strategy: null
  protocol: RESP3
advanced_config:
  connection_timeout: 10000
  tcp_nodelay: null
  tls_config:
    use_insecure_tls: false
    root_pem_cacerts: null
```

See the [Configuration Reference](#configuration-reference) below for
full details on all configuration options.

### Programmatic Configuration

You can also pass a `ValkeyConfig` or raw `GlideClientConfiguration`
directly:

```python
from scietex.service.valkey import ValkeyConfig, ValkeyNode, ValkeyBackoffStrategy

config = ValkeyConfig(
    base_config=ValkeyBaseConfig(
        nodes=[ValkeyNode(host="redis.example.com", port=6380)],
        user_credentials=ValkeyUserCredentials(
            username="myuser",
            password="secret",
        ),
        use_tls=True,
        backoff_strategy=ValkeyBackoffStrategy(
            num_of_retries=5,
            factor=2,
            exponent_base=1000,
            jitter_percent=25,
        ),
    ),
)

worker = MyValkeyWorker(valkey_config=config)
```

### Async Logging Credentials

The worker resolves the generated `GlideClientConfiguration.credentials`
and passes them — along with the node addresses, TLS setting, request
timeout, database id, client name, and related connection settings — to
the `AsyncValkeyHandler` used for async log entries. Logging therefore
uses the same Valkey connectivity (including authentication) as the task
client.

## Configuration Reference

### ValkeyConfig

Top-level configuration combining base and advanced settings.

| Field | Type | Default | Description |
|---|---|---|---|
| `base_config` | `ValkeyBaseConfig` | `ValkeyBaseConfig()` | Basic connection parameters |
| `advanced_config` | `ValkeyAdvancedConfig` | `ValkeyAdvancedConfig()` | Advanced connection settings |

### ValkeyBaseConfig

Basic Valkey connection configuration.

| Field | Type | Default | Description |
|---|---|---|---|
| `nodes` | `list[ValkeyNode]` | `[ValkeyNode(host="localhost", port=6379)]` | List of server node addresses |
| `user_credentials` | `ValkeyUserCredentials \| None` | `None` | Authentication credentials |
| `use_tls` | `bool` | `False` | Enable TLS encryption |
| `request_timeout` | `int \| None` | `5000` | Request timeout in milliseconds |
| `database_id` | `int \| None` | `None` | Logical database index |
| `client_name` | `str \| None` | `None` | Client identifier sent to the server |
| `inflight_requests_limit` | `int \| None` | `None` | Maximum concurrent unacknowledged requests |
| `client_az` | `str \| None` | `None` | Availability zone for cloud deployments |
| `lazy_connect` | `bool \| None` | `None` | Defer connection until first command |
| `read_from` | `str` | `"PRIMARY"` | Read preference |
| `backoff_strategy` | `ValkeyBackoffStrategy \| None` | `None` | Reconnection backoff configuration |
| `protocol` | `str` | `"RESP3"` | Protocol version |

**`read_from` values:**

| Value | Description |
|---|---|
| `"PRIMARY"` | Always read from the primary node |
| `"PRIMARY_PREFERRED"` | Read from replicas if available, otherwise primary |
| `"SECONDARY"` | Always read from a replica |
| `"SECONDARY_PREFERRED"` | Read from replicas if available, otherwise primary |
| `"RANDOM"` | Read from any node randomly |

**`protocol` values:**

| Value | Description |
|---|---|
| `"RESP2"` | RESP2 protocol version |
| `"RESP3"` | RESP3 protocol version |

### ValkeyAdvancedConfig

Advanced connection settings.

| Field | Type | Default | Description |
|---|---|---|---|
| `connection_timeout` | `int \| None` | `10000` | Connection timeout in milliseconds |
| `tcp_nodelay` | `bool \| None` | `None` | Disable Nagle's algorithm for lower latency |
| `tls_config` | `ValkeyTlsAdvancedConfiguration` | `ValkeyTlsAdvancedConfiguration()` | TLS configuration |

### ValkeyNode

Single Valkey server node.

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | `str` | `"localhost"` | Hostname or IP address of the node |
| `port` | `int` | `6379` | Port number the node listens on |

### ValkeyUserCredentials

Authentication credentials.

| Field | Type | Default | Description |
|---|---|---|---|
| `username` | `str` | *(required)* | User name for authentication |
| `password` | `str` | *(required)* | Password for authentication |

### ValkeyBackoffStrategy

Exponential backoff strategy for reconnection attempts.

| Field | Type | Default | Description |
|---|---|---|---|
| `num_of_retries` | `int` | *(required)* | Maximum number of reconnection attempts |
| `factor` | `int` | *(required)* | Multiplicative factor for backoff calculation |
| `exponent_base` | `int` | *(required)* | Base for the exponential backoff function |
| `jitter_percent` | `int \| None` | `None` | Optional jitter percentage to avoid thundering herd |

The backoff formula is: `factor * (exponent_base ^ attempt) + jitter`

### ValkeyTlsAdvancedConfiguration

TLS configuration for encrypted connections.

| Field | Type | Default | Description |
|---|---|---|---|
| `use_insecure_tls` | `bool` | `False` | Skip certificate verification (not recommended for production) |
| `root_pem_cacerts` | `str \| None` | `None` | PEM-encoded CA certificates for custom trust store |

## Data Schemas

### Heartbeat

Heartbeat data published by `ValkeyWorker` to track worker status.
Serialized as msgpack and stored at
`scietex:{service_name}:{worker_id}:status` with a TTL set to twice the
heartbeat interval.

| Field | Type | Default | Description |
|---|---|---|---|
| `service` | `str` | *(required)* | Name of the publishing service |
| `worker_id` | `int` | *(required)* | Unique identifier of the worker instance |
| `status` | `Literal["active", "inactive"]` | *(required)* | Current worker status |
| `heartbeat_interval` | `float` | *(required)* | Interval in seconds between heartbeats |
| `start_time` | `datetime` | *(required)* | UTC timestamp when the worker started |
| `timestamp` | `datetime` | `datetime.now(timezone.utc)` | UTC timestamp of this heartbeat entry |

## PubSub Broadcasting

> **Not implemented.** `ValkeyWorker` always creates its client with
> `listening=False` (`generate_glide_config(..., listening=False)`), so the
> PubSub path described below is not active. Inter-worker PubSub
> communication is reserved for future work and is documented here only
> as a design note.

The `generate_glide_config()` helper accepts a `listening` parameter.
Were `listening=True` passed, the client would subscribe to:

| Channel | Pattern | Description |
|---|---|---|
| `scietex:{service_name}:{worker_id}` | Exact | Service-specific channel for this worker |
| `scietex:broadcast` | Exact | Broadcast channel for all workers in the service |

A `parse_control_message` callback could be provided to handle incoming
PubSub messages.
