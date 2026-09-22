# scietex.service

Async worker framework for building background daemon services in Python.

Provides a hierarchy of workers — from basic signal-handling daemons to
concurrent task processors with Valkey- or MQTT-backed distributed queues.

**Python ≥ 3.10** · **License: MIT**

## Documentation

- [Overview](docs/index.md) — Core components and architecture
- [BasicWorker](docs/basic_worker.md) — Signal handling, logging, heartbeat & watchdog managers
- [TaskProcessor](docs/task_processor.md) — Concurrent task processing, handler dispatch, timeout monitoring
- [ValkeyWorker](docs/valkey_worker.md) — Valkey stream-based task distribution
- [MqttWorker](docs/mqtt_worker.md) — MQTT 5 topic-based task distribution
- [Task Handler](docs/task_handler.md) — Pluggable handler architecture, typed schemas

## Installation

```bash
# Core package (no Valkey)
pip install scietex.service

# With Valkey (Redis-compatible) support
pip install "scietex.service[valkey]"

# With MQTT 5 support
pip install "scietex.service[mqtt]"
```

**Dependencies:** `msgspec>=0.20.0`, `pyyaml>=6.0`, `scietex.logging>=2.1.0`

## Quick Start

### Basic Async Worker

A minimal daemon with signal handling, heartbeat, and watchdog. See the [full BasicWorker docs](docs/basic_worker.md) for lifecycle, manager system, and configuration details.

```python
import asyncio
import logging
from scietex.service import BasicWorker, WorkerConfig


class MyWorker(BasicWorker):
    async def heartbeat(self) -> None:
        self.logger.info("Worker is alive")

    async def watchdog(self) -> None:
        self.logger.debug("Running watchdog checks")

    async def cleanup(self) -> None:
        self.logger.info("Shutting down gracefully")


async def main() -> None:
    worker = MyWorker(
        WorkerConfig(
            service_name="my_service",
            version="1.0.0",
            logging_level=logging.DEBUG,
            heartbeat_interval=10,
            watchdog_interval=1,
        )
    )
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Send `SIGINT` (Ctrl+C) or `SIGTERM` to trigger graceful shutdown.

### Task Processor

Register handlers for different task types and process them concurrently. See the [full TaskProcessor docs](docs/task_processor.md) for architecture, task processing flow, and best practices.

```python
import asyncio
import logging
from scietex.service import TaskProcessor, TaskProcessorConfig
from scietex.service.task_handler import TaskCapabilities, TaskData, TaskHandler, TaskResult


class EmailHandler(TaskHandler):
    @property
    def supported_tasks(self) -> list[str]:
        return ["send_email"]

    async def initialize(self) -> bool:
        # Connect to email service, etc.
        self.logger.info("Email handler initialized")
        return True

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        try:
            # Process task_data.payload
            self.logger.info("Sending email…")
            return TaskResult(status="success", error="")
        except Exception as exc:
            return TaskResult(status="error", error=str(exc))


class MyProcessor(TaskProcessor):
    async def fetch_tasks(self) -> bool:
        # NOTE: fetch_tasks is a compatibility shim; prefer a TaskTransport via transport=.
        # Pull tasks from your source (DB, API, queue, etc.)
        # and enqueue them for processing:
        #     self.enqueue_task(task_data)
        # Return True when at least one task was enqueued so the
        # task_queue_manager drains a backlog back-to-back.
        return False


async def main() -> None:
    processor = MyProcessor(
        TaskProcessorConfig(
            service_name="email_worker",
            version="1.0.0",
            queue_size=100,
            max_concurrent_tasks=5,
        )
    )
    processor.add_task_handler(EmailHandler)
    await processor.start()
    await processor.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

### Valkey Worker

Distributed task processing backed by a Valkey (Redis-compatible) stream. See the [full ValkeyWorker docs](docs/valkey_worker.md) for architecture, key naming, and configuration reference.

```python
import asyncio
import logging
from scietex.service import (
    ValkeyAdvancedConfig,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyWorker,
    ValkeyWorkerConfig,
)


async def main() -> None:
    config = ValkeyConfig(
        base_config=ValkeyBaseConfig(
            nodes=[ValkeyNode(host="localhost", port=6379)],
            request_timeout=10_000,
        ),
        advanced_config=ValkeyAdvancedConfig(
            connection_timeout=10_000,
            tcp_nodelay=True,
        ),
    )
    worker = ValkeyWorker(
        ValkeyWorkerConfig(
            service_name="distributed_worker",
            version="1.0.0",
            logging_level=logging.DEBUG,
            heartbeat_interval=10,
            valkey_config=config,
            queue_size=100,
            max_concurrent_tasks=10,
        )
    )
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Tasks are stored in a Valkey stream named
`scietex:{service_name}:tasks` and consumed via a consumer
group `scietex:{service_name}:task_group`. Log entries are written to a
per-service stream `scietex:{service}:log` (override with the
`log_stream_name` config field).

`ValkeyWorker` also exposes:

- `client_factory=` (keyword-only) — an async callable
  `(GlideClientConfiguration) -> Awaitable[GlideClient]` used by `connect()`;
  defaults to `GlideClient.create`. Inject a fake to test without a server.
- `transport_health` — a `TransportHealth` supervisor aggregating connection
  failures, owning the single reconnect path, and logging one CRITICAL per
  sustained outage.
- `task_lease_ttl` (config field) — lease lifetime in seconds; `None` derives
  `max(1, int(max(2*heartbeat_interval, 3*watchdog_interval)))`.

### MQTT Worker

Distributed task processing backed by MQTT 5 topics. See the [full MqttWorker docs](docs/mqtt_worker.md) for architecture, topic naming, delivery semantics, and configuration reference.

```python
import asyncio
import logging
from scietex.service import MqttConfig, MqttWorker, MqttWorkerConfig


async def main() -> None:
    worker = MqttWorker(
        MqttWorkerConfig(
            service_name="mqtt_worker",
            version="1.0.0",
            logging_level=logging.DEBUG,
            heartbeat_interval=10,
            mqtt_config=MqttConfig(host="localhost", port=1883),
            queue_size=100,
            max_concurrent_tasks=10,
        )
    )
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Tasks are published to the topic `scietex/{service_name}/tasks`; the task id
travels inside the encoded `TaskData` (its required `task_id` field), not as a
separate user property. Because aiomqtt v2.5.1
auto-acks at the broker when a message is received, the worker persists every
message to a durable file-backed inbox before processing it, restoring
at-least-once delivery. Set `inbox_backend="memory"` (or its alias `"none"`)
to opt into at-most-once.

`MqttTransport` also publishes each task's lifecycle as fire-and-forget
observability: a retained `TaskStatus` to
`scietex/{service}/tasks/{task_id}/status` (default QoS 1, covering
`queued`/`running`/`completed`/`failed`/`cancelled`) and a throttled,
non-retained `TaskProgress` to `scietex/{service}/tasks/{task_id}/progress`
(default QoS 0). This is a status publisher, not a store — there is no
read-back API.
See [Task Status and Progress
Publishing](docs/mqtt_worker.md#task-status-and-progress-publishing).

`MqttWorker` also exposes:

- `client_factory=` (keyword-only) — an async callable
  `(MqttConfig) -> Awaitable[Client]` used by `connect()`; defaults to
  `_create_client`, which builds an `aiomqtt.Client` (MQTT 5) and enters its
  async context. Inject a fake to test without a broker.
- `transport_health` — a `TransportHealth` supervisor aggregating connection
  failures, owning the single reconnect path, and logging one CRITICAL per
  sustained outage.

## Architecture

### Worker Hierarchy

<!-- markdown-link-check-disable -->
See [BasicWorker](docs/basic_worker.md), [TaskProcessor](docs/task_processor.md), [ValkeyWorker](docs/valkey_worker.md), and [MqttWorker](docs/mqtt_worker.md) for detailed architecture diagrams.
<!-- markdown-link-check-enable -->

```
BasicWorker          — Signal handling, async logging, heartbeat &
                            watchdog managers, graceful shutdown
    └── TaskProcessor — Task queue, concurrent processing, handler
                            dispatch, timeout watchdog
        ├── ValkeyWorker  — Valkey stream integration, connection
        │                    management, stream-based task fetching
        └── MqttWorker    — MQTT 5 topic integration, durable file
                             inbox, retained registry/heartbeat
```

### Transport Layer

Task delivery is abstracted behind the `TaskTransport` protocol
(`fetch`/`requeue`/`on_started`/`ack`/`on_progress`/`on_drain`/`refresh_leases`/`recover_pending_tasks`).
`TaskProcessor` composes a transport rather than inheriting delivery hooks:

- **`InMemoryTransport`** is the default: a deque-backed in-process transport.
  Feed it with `transport.submit(task_data)`; `fetch` drains it into
  the processor's queue. A bare `TaskProcessor` therefore works with no
  external backend.
- **`ValkeyTransport`** (in `scietex.service.valkey`) implements the same
  protocol over a Valkey stream; `ValkeyWorker` injects it automatically.
- **`MqttTransport`** (in `scietex.service.mqtt`) implements the same protocol
  over MQTT 5 topics, draining a durable file-backed inbox; `MqttWorker`
  injects it automatically.

Pass a custom transport with the keyword-only `transport=` argument:

```python
from scietex.service import InMemoryTransport, TaskProcessor, TaskProcessorConfig

transport = InMemoryTransport(logger=logging.getLogger("transport"))
processor = TaskProcessor(TaskProcessorConfig(service_name="svc"), transport=transport)
transport.submit(task_data)
```

The legacy template-method hooks (`fetch_tasks`, `return_task_to_queue`,
`on_task_started`, `on_task_completed`, `_write_task_progress`,
`_on_queue_drain_task_processing`) are retained on `TaskProcessor` as thin
delegators to the transport, so existing subclasses keep working. New code
should implement a `TaskTransport` and pass it via `transport=` instead; the
delegators exist only for back-compat.

### Manager Lifecycle

Managers are async methods decorated with `@Manager(name=...)` — `name` is
required and is the manager's stable identity. The worker records them in a
per-class registry (`__manager_registry__`) and discovers them by walking the
class MRO, running each as an `asyncio.Task`:

1. **Start** — Manager loop runs the decorated method in a `while True`
   loop until cancelled.
2. **Error** — On any exception (except `CancelledError`), the error is
   recorded and the manager is automatically restarted, up to
   `manager_max_retries` consecutive failures (default 5), after which
   the manager gives up and ends in the terminal `FAILED` state
   (observable via `worker.failed_managers`; the watchdog logs CRITICAL
   but does not auto-shutdown).
3. **Stop** — On shutdown, managers are cancelled and their optional
   `cleanup` callbacks are invoked.

### Task Handler System

See the [Task Handler docs](docs/task_handler.md) for the full handler lifecycle, schema details, and best practices.

1. **Register**: `processor.add_task_handler(HandlerClass)` —
   Registers a handler class under its class name. The processor
   creates a single handler instance on start. Dispatch is driven by
   the handler's `supported_tasks` declaration, not by a registration
   key. An optional keyword-only `name`
   (`processor.add_task_handler(HandlerClass, name="...")`) lets
   multiple instances of one class coexist under distinct keys.
   Arbitrary `**handler_kwargs` are also forwarded to the handler
   constructor on every instantiation, enabling stateful handlers —
   see `examples/stateful_handler.py`.
2. **Declare support**: `Handler.supported_tasks` property must return
   a list of task type strings this handler can process.
3. **Dispatch**: When a task arrives, the processor calls
   `handler.supports(task_type)` on each registered handler. The first
   handler returning `True` receives the task.
4. **Initialize**: `handler.start()` calls `handler.initialize()` and
   sets `handler.is_ready` to the returned value, so it is `True` only
   if `initialize()` returned `True`.
5. **Handle**: `await handler.handle(task_data, capabilities=...)` returns a
   `TaskResult` with `status` ("success"/"error"), optional `error` message,
   and optional `payload`.
6. **Timeout**: Tasks exceeding their `timeout` (default 3s) are
   canceled and either re-queued or discarded per
   `TaskTimeout.timeout_action`.
7. **Cancel**: A built-in `task:cancel` handler cancels a running or
   queued task by id. A deliberate cancel writes `status="cancelled"`
   with the original `TaskData` embedded, so the caller can modify and
   resubmit it under a new task id.
8. **Worker control**: Built-in `worker:start`, `worker:stop`,
   `worker:restart`, and `worker:exit` handlers steer the worker's own
   lifecycle. Each is acknowledged before the transition begins, so the
   response reports acceptance, not completion.

### Task Schemas

All schemas are frozen `msgspec.Struct` instances (immutable).

| Type | Description |
|---|---|
| `TaskData` | Immutable task payload: `task` (type string), `payload` (bytes), `timeout` (`TaskTimeout`), `canceled_action` ("requeue"/"discard") |
| `TaskResult` | Handler result: `status` ("success"/"error"), `error` (message), `processed_at` (UTC datetime), `payload` (bytes), plus error-taxonomy fields `error_code`, `retryable`, `partial` |
| `TaskTimeout` | Timeout config: `timeout` (seconds, `None` for default 3s), `timeout_action` ("requeue"/"discard") |
| `TaskStatus` | Per-task tracking record: `task_id`, `service`, `task`, `status` ("queued"/"running"/"completed"/"failed"/"cancelled"), `progress`, `result`, `data` (original `TaskData` embedded on a deliberate cancel), `error`, `error_code`, timestamps |
| `TaskTracker` | Internal runtime handle (in `task_handler/runtime.py`): tracks running `asyncio.Task`, associated `TaskData`, and monotonic start time |
| `TaskEnvelope` | Versioned transport envelope: `version` (int, `1`) wrapping `data` (serialized `TaskData` bytes) — the durable on-the-wire format |

## Configuration

### Config Directory Precedence

The worker searches for a config directory in this order:

1. `conf_dir` argument (if provided and is a directory)
2. `SCIETEX_CONFIG_DIR` environment variable
3. `$XDG_CONFIG_HOME/scietex/`
4. `~/.config/scietex/`
5. `/etc/scietex/`
6. `/usr/local/etc/scietex/`
7. `./config/` (current working directory)
8. `~/.config/scietex/` — created if none of the above exist

The first existing directory is used. If none exist, `~/.config/scietex/`
is created.

### Valkey Configuration

`ValkeyWorker` reads `valkey.yml` from the config directory:

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

If the file is missing, it is created with default values. If the file is
present but invalid, a ``RuntimeError`` is raised and the file is left
untouched. The read (and the default-file write) is deferred to the first
`connect()`/`initialize()` call — constructing `ValkeyWorker()` with no
explicit `valkey_config` does not touch the filesystem (AR-066).

### MQTT Configuration

`MqttWorker` reads `mqtt.yml` from the config directory:

```yaml
host: localhost
port: 1883
username: null
password: null
identifier: null
keepalive: 60
clean_start: false
session_expiry_interval: 0
transport: tcp
timeout: null
tls_insecure: null
tls_context: null
```

If the file is missing, it is created with default values. If the file is
present but invalid, a ``RuntimeError`` is raised and the file is left
untouched. As with Valkey, the read is deferred to the first
`connect()`/`initialize()` call (AR-066).

### Status and Progress Publishing

These `MqttWorkerConfig` fields control status/progress publishing. They are
worker configuration, not `mqtt.yml` entries:

| Field | Default | Description |
|---|---|---|
| `status_publish_enabled` | `True` | Master switch for all status/progress publishing; `False` restores the no-op behavior |
| `status_topic_prefix` | `"scietex/{service}/tasks"` | Prefix for the per-task status/progress topics; `{service}` is substituted at construction |
| `status_qos` | `1` | QoS for `TaskStatus` publishes; range `[0, 2]` |
| `status_ttl` | `86400` | MQTT 5 message-expiry interval in seconds for retained `TaskStatus` publishes; range `[1, 2592000]`; `None` disables expiry |
| `progress_qos` | `0` | QoS for `TaskProgress` publishes; range `[0, 2]` |
| `progress_min_interval` | `1.0` | Minimum seconds between progress publishes; range `[0.0, 3600.0]`; `0` disables the interval threshold |
| `progress_min_delta` | `0.0` | Minimum absolute progress change that forces a publish; range `[0.0, 100.0]`; `0` disables the delta threshold |

### Remote Configuration

Remote configuration delivers a reloadable-behaviour snapshot to a running
worker over the transport it already uses — a durable Valkey key or an MQTT
retained topic — plus three `config:apply` / `config:store` / `config:show`
commands delivered as tasks. Opt in with `remote_config_enabled=True`.

| Field | Config class | Default | Meaning |
|---|---|---|---|
| `remote_config_enabled` | `TaskProcessorConfig` | `False` | Opt-in master switch; `False` ⇒ commands return `REMOTE_CONFIG_DISABLED`, no startup read |
| `config_file` | `TaskProcessorConfig` | `"config.yml"` | Local reloadable-snapshot filename, resolved under `conf_dir` |
| `config_signing_key` | `TaskProcessorConfig` | `None` | HMAC key for envelope authenticity; `None` disables signature enforcement |
| `config_startup_timeout` | `TaskProcessorConfig` | `None` (→ `2.0`) | Bounded MQTT wait for the retained snapshot at startup; range `[0.0, 60.0]` |
| `config_key` | `ValkeyWorkerConfig` | `"scietex:{service}:config"` | Durable desired-state key; `{service}` substituted at construction |
| `config_topic` | `MqttWorkerConfig` | `"scietex/{service}/config"` | Retained desired-state topic; `{service}` substituted at construction |
| `config_qos` | `MqttWorkerConfig` | `1` | QoS for the config-topic subscription and publish; range `[0, 2]` |
| `config_ttl` | `MqttWorkerConfig` | `86400` | MQTT 5 message-expiry for the retained config; range `[1, 2592000]`; `None` disables expiry |

Only the eight core fields (`max_concurrent_tasks`,
`task_manager_sleep_time`, `task_queue_manager_sleep_time`,
`task_handler_start_timeout`, `task_handler_stop_timeout`, `task_timeout`,
`task_queue_fetch_timeout`, `task_cancellation_timeout`) are hot-reloadable;
everything else is restart-required and cannot be expressed remotely. A custom
service extends the surface with
`worker.register_config_settings(name, struct_type, apply=...)`. Startup
precedence is `constructor config < config.yml < remote source`, and an invalid
remote config never fails startup. See the [Remote Configuration
guide](docs/remote_config.md) and `examples/remote_config.py`.

## API Reference

### Exported from `scietex.service`

| Symbol | Description |
|---|---|
| `BasicWorker` | Base async daemon worker |
| `TaskProcessor` | Concurrent task processor |
| `Manager` | Decorator for creating managed async loop methods |
| `register_manager` | Explicit post-creation manager registration (`register_manager(owner, method, *, name, ...)`); compatibility shim with no in-tree production caller |
| `TaskTransport` | Protocol for the task-delivery backend (`fetch`/`requeue`/`on_started`/`ack`/`on_progress`/`on_drain`/`refresh_leases`/`recover_pending_tasks`) |
| `TaskSink` | Protocol for the enqueue surface a transport delivers into (`task_queue_full`/`enqueue_task`) |
| `InMemoryTransport` | Default in-process transport (deque-backed; `submit()` feeds it) |
| `ValkeyWorker` | Valkey-backed distributed worker |
| `MqttWorker` | MQTT 5-backed distributed worker |
| `MqttTransport` | MQTT 5 transport (drains a durable file-backed inbox) |
| `WorkerConfig` | Immutable `msgspec.Struct` configuration for `BasicWorker` |
| `TaskProcessorConfig` | Immutable configuration for `TaskProcessor` (extends `WorkerConfig`) |
| `ValkeyWorkerConfig` | Immutable configuration for `ValkeyWorker` (extends `TaskProcessorConfig`) |
| `MqttWorkerConfig` | Immutable configuration for `MqttWorker` (extends `TaskProcessorConfig`) |
| `MqttConfig` | Immutable MQTT connection configuration |
| `read_mqtt_config` | Read (or create) `mqtt.yml` from the config directory |
| `__version__` | Package version string |

The Valkey configuration classes (`ValkeyConfig`, `ValkeyNode`,
`ValkeyUserCredentials`, `ValkeyBackoffStrategy`, `ValkeyBaseConfig`,
`ValkeyAdvancedConfig`, `ValkeyPubSubConfig`, `ValkeyTlsAdvancedConfiguration`,
`ValkeyWorkerConfig`)
are top-level
re-exports: they are importable directly from `scietex.service` (as in the
Valkey quick-start above), not only from `scietex.service.valkey`.

### Exported from `scietex.service.task_handler`

| Symbol | Description |
|---|---|
| `TaskHandler` | Abstract base class for task handlers |
| `TaskHandlerContext` | Narrow read-only context passed to handlers (service name, instance id, logger) |
| `TaskCapabilities` | Per-call capabilities passed to `handle` (task id + `report_progress(value)`) |
| `CancelTaskHandler` | Built-in handler for the `task:cancel` task name |
| `CancelTaskRequest` | Payload schema for a `task:cancel` task (`target_task_id`, `reason`) |
| `CancelTaskResponse` | Success payload schema for a `task:cancel` task (`target_task_id`, `outcome`) |
| `CancelOutcome` | Cancellation outcome literal (`cancelled`/`not_running`/`ignored`/`not_found`) |
| `CancelReason` | Why a task was cancelled (`deliberate`/`timeout`/`shutdown`) |
| `TASK_CANCEL_TASK_NAME` | Task name that selects the built-in cancel handler (`"task:cancel"`) |
| `WorkerControlHandler` | Built-in handler for the `worker:*` control task names |
| `WorkerControlRequest` | Payload schema for a `worker:*` task (`reason`) |
| `WorkerControlResponse` | Success payload schema for a `worker:*` task (`action`, `accepted`) |
| `WORKER_START_TASK_NAME` | Task name for `worker:start` (`"worker:start"`) |
| `WORKER_STOP_TASK_NAME` | Task name for `worker:stop` (`"worker:stop"`) |
| `WORKER_RESTART_TASK_NAME` | Task name for `worker:restart` (`"worker:restart"`) |
| `WORKER_EXIT_TASK_NAME` | Task name for `worker:exit` (`"worker:exit"`) |
| `CONTROL_TASK_NAMES` | Canonical enumeration of built-in control task names (a catalogue, not a routing table) |
| `CONFIG_APPLY_TASK_NAME` | Task name for `config:apply` (`"config:apply"`) |
| `CONFIG_STORE_TASK_NAME` | Task name for `config:store` (`"config:store"`) |
| `CONFIG_SHOW_TASK_NAME` | Task name for `config:show` (`"config:show"`) |
| `ConfigApplyHandler` | Built-in handler for `config:apply` |
| `ConfigStoreHandler` | Built-in handler for `config:store` |
| `ConfigShowHandler` | Built-in handler for `config:show` |
| `ConfigApplyRequest` | Payload schema for `config:apply` (`payload`, `persist`) |
| `ConfigApplyResponse` | Success payload schema for `config:apply` (`applied`, `revision`, `hash`, `changed`, `restart_required`, `error`) |
| `ConfigStoreRequest` | Payload schema for `config:store` (`target`: `"disk"`/`"remote"`/`"both"`) |
| `ConfigStoreResponse` | Success payload schema for `config:store` (`stored`, `target`, `path`, `revision`, `hash`, `error`) |
| `ConfigShowRequest` | Payload schema for `config:show` (`include_restart_required`) |
| `ConfigShowResponse` | Success payload schema for `config:show` (`settings`, `revision`, `hash`, `source`, `restart_required_fields`, `error`, `error_code`) |
| `ConfigSourceLabel` | Source literal for the effective config (`default`/`file`/`remote`/`inline`) |
| `TaskData` | Task payload schema |
| `TaskResult` | Task result schema |
| `TaskTimeout` | Timeout configuration schema |
| `TaskStatus` | Per-task tracking record schema |
| `TaskProgress` | Granular progress payload embedded in `TaskStatus.progress` (`progress`, `value`) |
| `TaskTracker` | Internal runtime handle for running tasks (not a wire schema) |
| `TaskEnvelope` | Versioned transport envelope (version + serialized payload bytes) |
| `encode_task_envelope` | Wrap a `TaskData` in a versioned envelope and msgpack-encode it |
| `decode_task_envelope` | Decode an envelope back to a `TaskData` (returns `None` on invalid/unknown version) |
| `decode_task_envelope_version` | Return an envelope's wire-format version, or `None` if malformed |

### Exported from `scietex.service.config_reload`

| Symbol | Description |
|---|---|
| `ConfigReloader` | Transport-agnostic owner of the remote-config apply/reload/store/show pipeline (validate-before-swap, serialized behind an `asyncio.Lock`, replay protection) |
| `ConfigSource` | Core Protocol (`load`/`store`) both transports implement to deliver the desired-state envelope |
| `ConfigEnvelope` | Versioned transport envelope (`version`/`revision`/`hash`/`signature`/`settings`/`created_at`) |
| `ConfigSections` | Named-section payload (`core: ReloadableSettings`, `services: dict[str, bytes]`) |
| `ReloadableSettings` | Complete snapshot of the eight hot-reloadable core fields (all required) |
| `ConfigApplyOutcome` | Result of an envelope apply attempt (`applied`/`revision`/`hash`/`changed`/`restart_required`/`error`/`error_code`) |
| `ConfigStoreOutcome` | Result of a config store attempt (`stored`/`target`/`path`/`revision`/`hash`/`error`/`error_code`) |
| `RELOADABLE_FIELDS` | The eight-field hot-reload allowlist |
| `encode_config_envelope` | Encode a `ConfigSections` snapshot into a hashed, optionally HMAC-signed envelope |
| `decode_config_envelope` | Decode an envelope back to a `ConfigEnvelope` (returns `None` on invalid input) |
| `read_local_config` | Read the local `config.yml` snapshot (write-free; `None` on missing/invalid) |
| `write_local_config` | Atomically write a `ConfigSections` snapshot as YAML |

### Exported from `scietex.service.valkey`

| Symbol | Description |
|---|---|
| `ValkeyConfig` | Top-level Valkey configuration |
| `ValkeyBaseConfig` | Basic connection settings |
| `ValkeyAdvancedConfig` | Advanced connection settings |
| `ValkeyPubSubConfig` | PubSub control-channel settings (`listening`, `parse_control_message`) |
| `ValkeyNode` | Server node address |
| `ValkeyUserCredentials` | Authentication credentials |
| `ValkeyBackoffStrategy` | Reconnection backoff config |
| `ValkeyTlsAdvancedConfiguration` | TLS settings |
| `ValkeyWorkerConfig` | Immutable configuration for `ValkeyWorker` (extends `TaskProcessorConfig`) |
| `ValkeyConfigSource` | Durable-key `ConfigSource` for remote config (`load` does a live `GET`, `store` does `SET`) |
| `purge_task_stream` | Standalone operational utility to purge a task stream (returns a `PurgeResult` with counts and errors) |
| `PurgeResult` | Frozen result of `purge_task_stream` (`entries_purged`, `errors`) |

### Exported from `scietex.service.mqtt`

| Symbol | Description |
|---|---|
| `MqttConfig` | Immutable MQTT connection configuration |
| `MqttWorkerConfig` | Immutable configuration for `MqttWorker` (extends `TaskProcessorConfig`) |
| `MqttWorker` | MQTT 5-backed distributed worker |
| `MqttTransport` | MQTT 5 transport implementing the `TaskTransport` protocol |
| `MqttConfigSource` | Retained-topic `ConfigSource` for remote config (snapshot + retained publish with an optional `config_ttl` message-expiry) |
| `read_mqtt_config` | Read (or create) `mqtt.yml` from the config directory |
| `logging_handler_config` | Translate an `MqttConfig` into `AsyncMqttHandler` keyword arguments |

## Development

### Setup

```bash
# Clone the repository and install all dependencies
uv sync --all-extras

# Or install specific extras
uv sync --extra dev --extra test --extra lint
```

### Commands

| Command | Description |
|---|---|
| `uv run ruff check src/` | Lint (auto-fix: `ruff check --fix`) |
| `uv run ty check src/` | Type check |
| `uv run ruff format src/` | Format code |
| `uv run pytest tests/` | Run tests |
| `tox` | Run tests with coverage |

### Running Examples

The `examples/` directory contains runnable blueprints; see
[`examples/README.md`](examples/README.md) for what each demonstrates.

```bash
python -m examples.basic_worker
python -m examples.manager_cleanup
python -m examples.manager_collision
python -m examples.task_processor
python -m examples.named_task_handlers
python -m examples.stateful_handler
python -m examples.valkey_async_service      # requires valkey-glide
python -m examples.valkey_pubsub_worker      # requires valkey-glide
python -m examples.valkey_perf               # requires valkey-glide
python -m examples.progress_and_cancel       # requires valkey-glide
python -m examples.mqtt_worker               # requires aiomqtt
python -m examples.mqtt_perf                 # requires aiomqtt
python -m examples.remote_config             # requires aiomqtt
```

## License

MIT
