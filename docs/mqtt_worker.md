# MqttWorker

The `MqttWorker` is an MQTT-backed async task processor that extends
`TaskProcessor` with MQTT 5 topic-based task distribution, heartbeat
publishing, and async logging. It uses the `aiomqtt` client for all broker
operations.

## Overview

```python
from scietex.service.mqtt import MqttWorker
```

`MqttWorker` adds MQTT-specific operations on top of `TaskProcessor`:

| Feature | Description |
|---|---|
| Topic-based tasks | Tasks are published to an MQTT 5 topic and consumed by every subscribed worker |
| Durable inbox | Every received message is persisted before processing, restoring at-least-once delivery (see [Delivery Semantics](#delivery-semantics)) |
| Heartbeat publishing | A retained liveness marker is published to a per-instance registry topic |
| Async logging | Log entries are published to an MQTT topic via `AsyncMqttHandler` |
| Task status publishing | Retained `TaskStatus` and throttled `TaskProgress` messages are published to per-task topics (see [Task Status and Progress Publishing](#task-status-and-progress-publishing)) |
| Auto-reconnect | Connection errors trigger automatic disconnect/reconnect cycles |

**Requires the optional `aiomqtt` dependency:**

```bash
pip install scietex.service[mqtt]
```

The extra pins `aiomqtt~=2.5.0` and `scietex.logging[mqtt]`.

## Architecture

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                         MqttWorker                              │
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
  │  │    Heartbeat         │───►│  scietex/{svc}/workers/    │     │
  │  │    (periodic)        │    │  {instance_id}  (retained) │     │
  │  └─────────────────────┘    └────────────────────────────┘     │
  │                                                                 │
  │  ┌─────────────────────┐    ┌────────────────────────────┐     │
  │  │  AsyncMqttHandler    │───►│  scietex/{svc}/log         │     │
  │  │  (log entries)       │    │  (own connection)          │     │
  │  └─────────────────────┘    └────────────────────────────┘     │
  │                                                                 │
  │  ┌─────────────────────┐    ┌────────────────────────────┐     │
  │  │   MqttTransport      │───►│  MqttInbox                 │     │
  │  │  (fetch/ack/drain)   │    │  sqlite: inbox.sqlite3     │     │
  │  └─────────────────────┘    └────────────────────────────┘     │
  └─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
                 ┌─────────────────┐
                 │   MQTT Broker    │
                 │   (MQTT 5 only)  │
                 │                  │
                 │  Topic:          │
                 │  scietex/{svc}/  │
                 │  tasks           │
                 └─────────────────┘
```

Unlike the Valkey stream, an MQTT topic is a broadcast channel: every worker
subscribed to `scietex/{service}/tasks` receives every task. The durable inbox
is the shared SQLite store: multiple workers open one database and a
cross-process claim/lease ensures each task id is processed once, so
at-least-once delivery is a property of the shared persisted state, not of a
consumer group.

## Topic Names

The MQTT topic space is split into service-scoped and worker-scoped topics.
Service-scoped topics are shared across all replicas of a service;
worker-scoped topics embed the auto-generated `instance_id`:

| Resource | Topic Pattern | Scope |
|---|---|---|
| Task topic | `scietex/{service_name}/tasks` | service-scoped |
| Registry / heartbeat | `scietex/{service_name}/workers/{instance_id}` | worker-scoped |
| Log topic | `scietex/{service_name}/log` | service-scoped |
| Task status | `scietex/{service_name}/tasks/{task_id}/status` | per-task, retained |
| Task progress | `scietex/{service_name}/tasks/{task_id}/progress` | per-task, not retained |

`{service}` in `task_topic` and `log_topic` is replaced with the service name
at construction. The registry topic is always built as
`scietex/{service_name}/workers/{instance_id}`. The status and progress topics
are derived from `status_topic_prefix` (default `scietex/{service}/tasks`) with
the same `{service}` substitution; see
[Task Status and Progress Publishing](#task-status-and-progress-publishing).

## Constants

| Constant | Default | Description |
|---|---|---|
| `_REGISTRY_QOS` (`worker.py`) | `1` | QoS for retained registry/heartbeat messages |
| `MIN_TASK_QOS` / `MAX_TASK_QOS` | `0` / `2` | Bounds of `MqttWorkerConfig.task_qos` |
| `MIN_LOG_QOS` / `MAX_LOG_QOS` | `0` / `2` | Bounds of `MqttWorkerConfig.log_qos` |
| `MIN_MQTT_PORT` / `MAX_MQTT_PORT` | `1` / `65535` | Bounds of `MqttConfig.port` |
| `MIN_MQTT_KEEPALIVE` / `MAX_MQTT_KEEPALIVE` | `0` / `65535` | Bounds of `MqttConfig.keepalive` |
| `MIN_SESSION_EXPIRY_INTERVAL` / `MAX_SESSION_EXPIRY_INTERVAL` | `0` / `4294967295` | Bounds of `MqttConfig.session_expiry_interval` |
| `MIN_INBOX_TTL` / `MAX_INBOX_TTL` | `1` / `2592000` | Bounds of `MqttWorkerConfig.inbox_ttl` (30 days) |
| `DEFAULT_INBOX_TTL` | `86400` | Default `MqttWorkerConfig.inbox_ttl` (one day) |
| `MIN_INBOX_LEASE_TTL` / `MAX_INBOX_LEASE_TTL` | `1` / `86400` | Bounds of `MqttWorkerConfig.inbox_lease_ttl` (24 hours) |
| `MIN_INBOX_PRUNE_INTERVAL` / `MAX_INBOX_PRUNE_INTERVAL` | `1.0` / `3600.0` | Bounds of `MqttWorkerConfig.inbox_prune_interval` |
| `DEFAULT_INBOX_PRUNE_INTERVAL` | `60.0` | Default `MqttWorkerConfig.inbox_prune_interval` (one minute) |
| `MIN_INBOX_PRUNE_JITTER` / `MAX_INBOX_PRUNE_JITTER` | `0.0` / `1.0` | Bounds of `MqttWorkerConfig.inbox_prune_jitter` |
| `DEFAULT_INBOX_PRUNE_JITTER` | `0.25` | Default `MqttWorkerConfig.inbox_prune_jitter` (plus or minus 25 percent) |
| `MIN_STATUS_QOS` / `MAX_STATUS_QOS` | `0` / `2` | Bounds of `MqttWorkerConfig.status_qos` |
| `MIN_STATUS_TTL` / `MAX_STATUS_TTL` | `1` / `2592000` | Bounds of `MqttWorkerConfig.status_ttl` (30 days) |
| `MIN_PROGRESS_QOS` / `MAX_PROGRESS_QOS` | `0` / `2` | Bounds of `MqttWorkerConfig.progress_qos` |
| `MIN_PROGRESS_MIN_INTERVAL` / `MAX_PROGRESS_MIN_INTERVAL` | `0.0` / `3600.0` | Bounds of `MqttWorkerConfig.progress_min_interval` |
| `MIN_PROGRESS_MIN_DELTA` / `MAX_PROGRESS_MIN_DELTA` | `0.0` / `100.0` | Bounds of `MqttWorkerConfig.progress_min_delta` |
| `DEFAULT_MAX_TASKS_QUEUE_SIZE` | `100` | Default max queue size (inherited) |
| `DEFAULT_MAX_CONCURRENT_TASKS` | `10` | Default max concurrent tasks (inherited) |
| `task_timeout` (`TaskProcessorConfig`) | `3` | Default task timeout in seconds (inherited) |
| `DEFAULT_HEARTBEAT_INTERVAL` | `10` | Default heartbeat interval in seconds |
| `DEFAULT_WATCHDOG_INTERVAL` | `1` | Default watchdog check interval in seconds |

## Protocol and Wire Format

**MQTT 5 is the only supported protocol.** `connect()` always builds the
client with `protocol=ProtocolVersion.V5`, so MQTT 5 user properties are
available. There is no 3.1.1 code path.

The task id travels inside the `TaskData` itself — `TaskData.task_id` is a
required `str` field (v5.0.0) — so the message payload is the same
transport-agnostic `msgpack(TaskEnvelope(version=1,
data=msgpack(TaskData)))` used by every transport (see
[Wire Format](#wire-format) below). There is no separate user property: MQTT
needs no out-of-band id channel because the id is part of the encoded payload.

## Lifecycle

```
  [STOPPED] ──► [STARTING] ──► [RUNNING] ──► [STOPPING] ──► [STOPPED]
                   │              │               │
            _startup()      initialize()     _shutdown()
                   │              │               │
                   ▼              ▼               ▼
            print logo    connect MQTT       disconnect
            start loggers  subscribe topic    stop message loop
            start managers start message loop  stop managers
                           replay inbox        empty queue
                                               cancel running tasks
                                               stop handlers
```

`initialize()` runs the at-least-once guard **first** — it refuses to start
rather than silently lose durability when the durable **data** inbox was
expected but could not be built (see [Inbox Backend](#inbox-backend)). It then
starts all registered task handlers (`super().initialize()`), connects to the
broker (subscribing to the task topic and starting the background message loop),
applies the local `config.yml` snapshot followed by the retained remote
snapshot, and finally replays any non-terminal data inbox entries left by a
previous run. The in-memory control lane holds nothing across a restart, so it
has nothing to replay.

## Properties

### MQTT-Specific

| Property | Type | Default | Description |
|---|---|---|---|
| `mqtt_config` | `MqttConfig \| None` | `None` | The MQTT configuration used by this worker. When no explicit config was given at construction, it is loaded lazily from disk at first connect, so it is `None` until then (AR-066) |
| `client` | `Client \| None` | `None` | The active `aiomqtt.Client` (`None` until `initialize` completes) |
| `transport_health` | `TransportHealth` | — | Connection-health supervisor: aggregates transport failures, owns the single reconnect path, and surfaces `connected`/`degraded`/`last_error`/`failure_count`/`down_duration` |

### Inherited from TaskProcessor

| Property | Type | Default | Description |
|---|---|---|---|
| `queue_size` | `int` | `100` | Maximum size of the internal task queue |
| `max_concurrent_tasks` | `int` | `10` | Maximum tasks processed in parallel |
| `task_handlers` | `Mapping[str, TaskHandler]` | — | Currently active (started) handlers, as a read-only `MappingProxyType` view |
| `running_tasks` | `Mapping[UUID, TaskTracker]` | — | Snapshot of currently running tasks and their trackers |

## Constructor

`MqttWorker` takes a single immutable configuration object
(`MqttWorkerConfig`, from `scietex.service.mqtt.config`, which extends
`TaskProcessorConfig`), or `None` to use the struct defaults. It also accepts
an optional keyword-only `client_factory` — an async callable used by
`connect()` to build the `aiomqtt.Client`:

```python
import logging

from scietex.service.mqtt import MqttWorker, MqttWorkerConfig

worker = MqttWorker(
    MqttWorkerConfig(
        service_name="service",
        version="0.0.1",
        conf_dir=None,
        logging_level=logging.DEBUG,
        heartbeat_interval=None,
        watchdog_interval=None,
        queue_size=None,
        max_concurrent_tasks=None,
        mqtt_config=None,
        task_topic="scietex/{service}/tasks",
        task_qos=2,
        inbox_backend="sqlite",
        inbox_path=None,
        inbox_ttl=86400,
        inbox_lease_ttl=None,
        inbox_prune_interval=60.0,
        inbox_prune_jitter=0.25,
        log_topic="scietex/{service}/{instance_id}/log",
        log_qos=0,
        log_retain=False,
        log_message_expiry=86400,
        status_publish_enabled=True,
        status_topic_prefix="scietex/{service}/tasks",
        status_qos=1,
        status_ttl=86400,
        progress_qos=0,
        progress_min_interval=1.0,
        progress_min_delta=0.0,
    )
)
```

### Client factory

`MqttWorker.__init__(config=None, *, client_factory: ClientFactory | None = None)`
where `ClientFactory = Callable[[MqttConfig], Awaitable[Client]]`. When
omitted, the factory defaults to `_create_client`, which maps the scalar
`MqttConfig` fields to aiomqtt v2.5.1 `Client` kwargs, enters the client's
async context, and returns a connected client. Supplying one lets embedders
and tests inject a fake or externally-built client without a live broker:

```python
async def my_factory(mqtt_config):
    return await _create_client(mqtt_config)

worker = MqttWorker(MqttWorkerConfig(service_name="svc"), client_factory=my_factory)
```

The factory runs inside `connect()`, so the full connect contract (client
assignment, health marking, logging-handler setup, subscription) is
preserved.

`MqttWorkerConfig` adds these fields on top of `TaskProcessorConfig`:

| Field | Default | Description |
|---|---|---|
| `mqtt_config` | `None` | A `MqttConfig` schema. If `None`, `mqtt.yml` is read lazily from the config directory at first connect (not at construction) |
| `task_topic` | `"scietex/{service}/tasks"` | Topic tasks are consumed from; `{service}` is replaced with the service name |
| `task_qos` | `2` | QoS for task messages; valid range `[0, 2]` |
| `inbox_backend` | `"sqlite"` | Durable inbox backend (`"sqlite"`, `"memory"`, or `"none"`); `"sqlite"` is the shared multi-process store, `"memory"`/`"none"` the explicit at-most-once opt-out |
| `inbox_path` | `None` | Path to the inbox store; `None` derives `<conf_dir>/inbox.sqlite3` |
| `inbox_ttl` | `86400` | TTL in seconds for inbox entries and tombstones (one day); valid range `[1, 2592000]`; `None` disables expiry (the explicit unbounded-growth opt-out) |
| `inbox_lease_ttl` | `None` | Claim-lease lifetime in seconds for the SQLite backend; valid range `[1, 86400]`; `None` derives `max(1, int(max(2*heartbeat_interval, 3*watchdog_interval)))` |
| `inbox_prune_interval` | `60.0` | Base seconds between inbox maintenance passes; valid range `[1.0, 3600.0]` |
| `inbox_prune_jitter` | `0.25` | Fractional jitter applied to `inbox_prune_interval` (plus or minus this fraction); valid range `[0.0, 1.0]`; `0.0` disables jitter |
| `log_topic` | `"scietex/{service}/{instance_id}/log"` | Topic worker logs are published to; both `{service}` and `{instance_id}` are replaced, so each worker logs to its own topic |
| `log_qos` | `0` | QoS for log messages; valid range `[0, 2]` |
| `log_retain` | `False` | If `True`, log messages are published with the retained flag |
| `log_message_expiry` | `86400` | MQTT 5 message-expiry interval in seconds applied to every log publish; valid range `[1, 2592000]`; `None` disables expiry |
| `status_publish_enabled` | `True` | Master switch for all status/progress publishing; `False` restores the no-op behavior |
| `status_topic_prefix` | `"scietex/{service}/tasks"` | Prefix for the per-task status/progress topics; `{service}` is substituted at construction |
| `status_qos` | `1` | QoS for `TaskStatus` publishes; valid range `[0, 2]` |
| `status_ttl` | `86400` | MQTT 5 message-expiry interval in seconds applied to every retained `TaskStatus` publish; valid range `[1, 2592000]`; `None` disables expiry |
| `progress_qos` | `0` | QoS for `TaskProgress` publishes; valid range `[0, 2]` |
| `progress_min_interval` | `1.0` | Minimum seconds between progress publishes; valid range `[0.0, 3600.0]`; `0` disables the interval threshold |
| `progress_min_delta` | `0.0` | Minimum absolute progress change that forces a publish; valid range `[0.0, 100.0]`; `0` disables the delta threshold |

All `TaskProcessorConfig` and `WorkerConfig` fields are inherited.
Configuration is immutable: values are fixed at construction, and
out-of-range values raise `msgspec.ValidationError`.

## Methods

### connect()

Establish an asynchronous connection to the MQTT broker.

```python
async def connect(self) -> bool:
    """Build a connected aiomqtt.Client, subscribe, and start the message loop."""
```

Serialized behind an `asyncio.Lock` so a concurrent `disconnect()` cannot race
the create → assign sequence. Builds a connected client by awaiting the
configured `client_factory` with the resolved `MqttConfig`; `_client` is
assigned only on success, so a failed build leaves `_client` as `None` and
`connect()` returns `False`. On success it marks the connection healthy,
ensures the logging handler is built and started, subscribes to the task
topic at `task_qos`, and starts the background message loop. A reconnect
reaches the same path and restores intake. Returns `True` when connected and
subscribed; `False` on connection or subscription failure.

### disconnect()

Gracefully close the connection to the MQTT broker.

```python
async def disconnect(self):
    """Exit the client context, log the disconnection, set _client to None."""
```

Serialized behind the same lock as `connect()`. The logging handler owns its
own client, so it is left untouched here; only the worker's operational
client is closed. Best-effort: an `aiomqtt.MqttError` raised by the broker
during close is swallowed because the client is unusable either way.

### heartbeat()

Publish a retained heartbeat message to the registry topic.

```python
async def heartbeat(self) -> None:
    """Publish a retained msgpack payload to scietex/{service}/workers/{instance_id}."""
```

The retained marker on `scietex/{service}/workers/{instance_id}` is the
instance's liveness signal; each beat refreshes it at QoS 1. The payload
carries `service`, `instance_id`, `status`, `start_time`, and `timestamp`.
The write is guarded by `self.client and self.start_time`, so the first beat
fires promptly after startup. A publish failure is reported to
`TransportHealth`, which drives the reconnect.

### initialize()

Initialize the worker, connect, and replay the inbox.

```python
async def initialize(self) -> bool:
    """Start handlers, run the at-least-once guard, connect, replay the inbox."""
```

Returns `True` if the parent initialization, connection (including
subscription and message-loop start), and inbox replay succeed. Returns
`False` if the parent initialization fails, the at-least-once guard trips
(see [Inbox Backend](#inbox-backend)), or the connection/subscription fails.

### cleanup()

Perform cleanup on shutdown.

```python
async def cleanup(self):
    """Drain queue, cancel tasks, stop message loop, stop log handler, disconnect."""
```

Drains the internal task queue and cancels running tasks via the parent
`TaskProcessor.cleanup()`, then stops the background message loop, stops the
MQTT logging handler so its worker drains remaining records, and closes the
MQTT connection. The data inbox is then closed: the SQLite inbox closes its
database connection. The in-memory control lane holds no resources to release.

### fetch_tasks() / fetch()

Fetch tasks from the durable inbox and enqueue them.

```python
async def fetch_tasks(self) -> bool:
    """Drain the inbox into the processor's queue; recover on the first call."""
```

`TaskProcessor.fetch_tasks()` is a thin delegator to
`MqttTransport.fetch(sink)`. On the first call, every non-terminal inbox
entry is replayed (`recover_pending_tasks`) before draining, so tasks
persisted by a previous run are redelivered exactly once. The drain walks the
inbox's non-terminal snapshot, skipping task ids already handed over this run
and stopping on backpressure: a rejected task is left pending in the inbox,
so it is redelivered, never lost. For the SQLite backend, each entry is
**claimed** before it is enqueued; a lost claim (another worker won it) is
skipped without blocking the drain, and a rejected entry is released back to
the pool. Returns `True` if at least one task was enqueued, `False` otherwise.

Prefer `MqttTransport`; these hooks are back-compat shims.

The MQTT message loop itself only **persists** messages to the inbox
(`_handle_message`); `fetch` is the single intake path that drains the inbox
into the processor queue. Persist-before-enqueue therefore always holds.

### on_task_completed()

Mark the inbox entry terminal for a completed task.

```python
async def on_task_completed(self, task_data, task_result, *, cancel_reason=None):
    """Mark the inbox entry terminal and drop the in-process claim."""
```

Called by the base `TaskProcessor` when a task's processing terminates
(success, error, or cancellation). Marking terminal writes a tombstone that
dedupes any re-delivered copy of the task id, then releases the in-process
enqueued marker. `task_result` is `None` when the task was cancelled before
producing a result.

A **retryable error is the exception**: when `task_result.status == "error"`
and `task_result.retryable` is `True`, the entry is left non-terminal and
only the in-process marker is dropped. `requeue` has already re-published the
task with the **same task id**, and tombstoning it here would make
`inbox.put` suppress that retry copy, silently losing the retry (the AR-077b
mirror). See [Retry Semantics](#retry-semantics).

### _write_task_progress()

Publish a throttled progress tick.

```python
async def _write_task_progress(self, task_id: UUID, value: float) -> None:
    """Delegate to MqttTransport.on_progress, which publishes TaskProgress."""
```

`TaskProcessor._write_task_progress` delegates to `MqttTransport.on_progress`,
which publishes a non-retained `TaskProgress` (at `progress_qos`, default 0) to
the task's progress topic, subject to the
[throttling policy](#progress-throttling). Progress is observability: a publish
failure is logged at DEBUG and reported to `TransportHealth`, never raised into
the task path. Granular progress also remains available in-process via
`TaskCapabilities.report_progress(value)`, which clamps to `[0.0, 100.0]`. With
`status_publish_enabled=False`, `on_progress` is a no-op and progress stays
in-process only.

Prefer `MqttTransport`; these hooks are back-compat shims.

### watchdog()

Refresh inbox leases, supervise the connection, then run the inherited
watchdog.

```python
async def watchdog(self) -> None:
    """Refresh leases, health.recover(), super().watchdog(), critical_report()."""
```

`refresh_leases()` renews the cross-process claims on this worker's enqueued
data tasks for the SQLite backend; it is a no-op for the memory backend
(kept for parity with `ValkeyWorker`). When the SQLite backend is in use the
watchdog also drives the inbox prune: every worker runs
`prune_expired()` independently on its own jittered schedule, so redundant
maintenance across workers sharing one store is safe and self-healing (see
[Inbox Backend](#inbox-backend)). `inbox_prune_interval` (default `60.0`) is
the base seconds between passes and `inbox_prune_jitter` (default `0.25`)
spreads workers apart. `health.recover()` is the single
reconnect owner for every failure reported by the message loop, heartbeat, and
publish sites. After the base watchdog runs, a degraded connection past its
down threshold surfaces one CRITICAL message per down episode; when healthy it
emits nothing.

### Task cancellation and external requeue

`MqttWorker` inherits the built-in `task:cancel` handler from
`TaskProcessor` (auto-registered in `TaskProcessor.__init__`). A `task:cancel`
message is published to the task topic like any other task, carrying a
msgpack-encoded `CancelTaskRequest(target_task_id="<uuid>", reason="...")` as
its payload. The worker cancels a running target or removes a queued-but-
undispatched target and reports the outcome (`cancelled`, `not_running`,
`ignored`, `not_found`) in its own result.

Reliable cancellation needs `max_concurrent_tasks >= 2`: with a single slot
the cancel task queues behind its target and cannot run. Because MQTT has no
status store, there is no key space to read a cancellation back from — the
in-process cancellation machinery is identical to every other transport. The
status publisher does advertise the outcome: a deliberate cancel publishes a
retained `cancelled` `TaskStatus` carrying the original `TaskData` (a
timeout/shutdown cancel publishes `failed` instead); see
[Task Status and Progress Publishing](#task-status-and-progress-publishing).

### Instance registry

`MqttWorker` overrides `_register_instance`/`_unregister_instance` (the
`BasicWorker` hooks): it publishes this instance's retained liveness marker to
`scietex/{service}/workers/{instance_id}` with `status="active"` on startup and
`status="inactive"` on shutdown. The record is **never deleted** — a clean
departure is visible as an `inactive` record that expires after `inactive_ttl`.
Both publishes are best-effort; a failed publish logs a WARNING, reports into
`TransportHealth`, and continues.

The retained heartbeat carries a `MessageExpiryInterval` of `active_ttl`, so a
worker that dies without shutting down leaves a record that expires on its own.
An ungraceful disconnect is covered by a Last Will (`Will` with
`WillDelayInterval` and `MessageExpiryInterval`) that publishes
`status="inactive"`; a reconnect before the delay cancels the Will. See
{doc}`worker_registry`.

## Wire Format

Each task message payload is a **versioned transport envelope**, not a bare
`TaskData`. The envelope is `TaskEnvelope` (from
`scietex.service.task_handler.schemas`):

| Field | Type | Default | Description |
|---|---|---|---|
| `version` | `int` | `1` | Wire-format version |
| `data` | `bytes` | `b""` | Serialized task payload (version 1: msgpack-encoded `TaskData`) |

The whole `TaskEnvelope` is msgpack-encoded as the message payload. The
durable wire value is therefore:

```
msgpack(TaskEnvelope(version=1, data=msgpack(TaskData)))
```

The task id **is** part of the envelope: `TaskData.task_id` is a required
`str` field (v5.0.0), so the id travels inside the decoded `TaskData`. A
message carrying an undecodable envelope is logged and skipped without
crashing the message loop. The handler contract (`TaskData`) is unchanged, and
handlers **never see the envelope** — they receive the decoded `TaskData`.

Encoding and decoding are centralized in the shared, transport-agnostic
helpers `encode_task_envelope(task_data)` and `decode_task_envelope(payload)`
from `scietex.service.task_handler.wire`.

## Delivery Semantics

### The aiomqtt v2.5.1 constraint

The chosen client is **aiomqtt v2.5.1**, which wraps paho-mqtt and does
**not** expose manual acknowledgement. paho auto-acks when its `on_message`
callback returns — which happens as soon as aiomqtt enqueues the message,
*before* the handler runs. Therefore:

- The broker considers a QoS 2 message delivered as soon as it is enqueued
  locally.
- A crash mid-handler does **not** cause broker redelivery.
- Wire-level QoS 2 gives **at-most-once** processing at the application
  layer.

This is a property of the library, not a design choice. Adopting aiomqtt v3
would remove the limitation, but v3 is currently uninstallable alongside
`scietex.logging`, whose `AsyncMqttHandler` is pinned to the v2.5.1 API.

### The durable inbox

To restore at-least-once delivery, `MqttTransport` persists every received
message to a durable inbox **before** it is handed to the processor, and
dedupes on replay. The flow:

1. The aiomqtt message loop receives a message.
2. `_handle_message` decodes the envelope and calls `inbox.put(task_id,
   task_data)` — the task id comes from the decoded `TaskData.task_id` — which
   persists the envelope with a `pending` marker.
3. `MqttTransport.fetch` drains the inbox into the processor queue, and
   `on_started` marks the entry `in-flight`.
4. On terminal completion, `ack` writes a tombstone and removes the entry.
5. On startup, `recover_pending_tasks` replays every non-terminal entry back
   into the queue.

A task id that is already terminal (a live tombstone) is skipped on replay,
so a completed task is never re-processed. The inbox is the source of truth
for "has this task been processed".

### Inbox Backend

Two backends are selectable via `inbox_backend`:

| Backend | Store | Delivery | Multi-process |
|---|---|---|---|
| `"sqlite"` (default) | a WAL-mode SQLite database at `<conf_dir>/inbox.sqlite3` | at-least-once | yes — cross-process claim/lease |
| `"memory"` / `"none"` | in-process dict | at-most-once | n/a (explicit opt-out) |

**SQLite backend.** The shared durable store (`inbox_backend="sqlite"`). It
opens one WAL-mode database at `<conf_dir>/inbox.sqlite3` (overridable with
`inbox_path`) with `check_same_thread=False` and `isolation_level=None`,
serializes every access behind an `asyncio.Lock` + `asyncio.to_thread`, and
issues explicit `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK`. A cross-process
**claim/lease** makes the drain safe for multiple workers: `claim` wins only
when the row is unclaimed or its lease has expired, so two workers draining one
store never process the same task id. A crashed peer's entry is reclaimed once
its lease lapses; `refresh` renews a live claim over the watchdog tick, and
`release` returns a rejected/requeued entry to the pool. `inbox_lease_ttl`
(seconds, `[1, 86400]`, `None` derives it) governs the lease lifetime. Only the
data lane uses this backend: the **control inbox is always in-memory and
per-process**, independent of `inbox_backend`. It is never durable and never
shared, because a broadcast control command must fan out to every worker and
control is event-only — it is not replayed across a restart.

`inbox_ttl` (seconds, default one day) governs tombstone dedupe and entry
expiry; `None` disables expiry (the explicit unbounded-growth opt-out). Pruning
is a periodic watchdog maintenance pass, not a side effect of load: expired
tombstones (and, only when `inbox_ttl` is set, expired entries) are removed by
`MqttInbox.prune_expired()`, which the worker's watchdog invokes on its own
schedule. Every worker prunes independently — there is no leader election,
because the prune DELETE is idempotent and indexed, so redundant maintenance
across workers sharing one store is safe and self-healing (if one worker is
down, the others still prune). The schedule is **jittered** so N workers
sharing one `inbox.sqlite3` do not fire the same DELETE on the same tick
(thundering herd / `BEGIN IMMEDIATE` lock contention): `inbox_prune_interval`
(base seconds, default `60.0`, range `[1.0, 3600.0]`) and `inbox_prune_jitter`
(fractional, default `0.25`, range `[0.0, 1.0]`; `0.0` disables jitter) set the
next deadline to `now + interval * (1 + uniform(-jitter, +jitter))`. The first
pass is not jittered (it runs on the first watchdog tick). The bounded horizon
caps dedup memory to one day by default.

`inbox_backend="memory"` (or its alias `"none"`) is the explicit at-most-once
opt-out: the worker uses a `MemoryInbox` that buffers entries in process only,
persists nothing to disk, and does not replay on startup. The worker
**refuses to start** with at-least-once semantics if `inbox_backend` is
`"sqlite"` but no inbox could be built (for example, `inbox_path` points at an
existing file). Fail loud, not silent.

### Retry Semantics

On a retryable error (`TaskResult(status="error", retryable=True)`),
`TaskProcessor` calls `requeue` before `ack`:

- `requeue` re-publishes the envelope to the task topic at `task_qos` under
  the **same task id**. The inbox entry is left non-terminal, so it is also
  redelivered by recovery after a crash.
- `ack` deliberately skips writing the tombstone for a retryable error (it
  only drops the in-process marker). If it wrote the tombstone, `inbox.put`
  would skip the retry copy when it arrives and the retry would be silently
  lost (AR-077b mirror).

Permanent failures (`retryable=False`) are tombstoned and dropped.

## Task Status and Progress Publishing

`MqttTransport` publishes each task's lifecycle to per-task topics as
fire-and-forget observability. This is a status **publisher**, not a status
**store**: the messages are write-only and consumed by subscription, there is
no read-back or query API, and the broker's retained message is delivery state
rather than an application data store.

### Topic scheme

The topics are derived once at construction from `status_topic_prefix`, with
`{service}` substituted exactly as for `task_topic`:

```
status_topic   = f"{status_topic_prefix}/{task_id}/status"
progress_topic = f"{status_topic_prefix}/{task_id}/progress"
```

| Purpose | Topic (default prefix) | QoS | Retain |
|---|---|---|---|
| Task status | `scietex/{service}/tasks/{task_id}/status` | `status_qos` (default `1`) | yes |
| Task progress | `scietex/{service}/tasks/{task_id}/progress` | `progress_qos` (default `0`) | no |

`{task_id}` is the string form of the task `UUID` — the same value carried in
`TaskData.task_id`. Every status publish is retained, so the
broker keeps the latest `TaskStatus` per task and a late subscriber still sees
the final state. Retained status also carries an MQTT 5 message-expiry interval
(`status_ttl`, default 24h; `None` disables expiry), so the broker ages out the
per-task marker instead of keeping one forever. Each status publish
(`queued`/`running`/terminal) overwrites the retained message and resets the
expiry clock, so the terminal status lives `status_ttl` from completion. Once a
client has received a copy, expiry does not affect it — this only bounds
broker-side retained storage. Progress is never retained and carries no expiry:
a late subscriber must not receive a stale progress tick.

The prefix is not consumed by the worker, so a subscriber and the worker must
agree on it out of band.

Subscription examples:

| Want | Subscription |
|---|---|
| Status of every task for a service | `scietex/{service}/tasks/+/status` |
| Progress of every task for a service | `scietex/{service}/tasks/+/progress` |
| Everything about every task | `scietex/{service}/tasks/+/#` |
| Everything about one task | `scietex/{service}/tasks/{task_id}/#` |
| Status only, one task | `scietex/{service}/tasks/{task_id}/status` |

### Payload schemas

No new schema is introduced. Both payloads are `msgspec.msgpack`-encoded and
reuse the existing task-handler structs:

- Status topic payload: `TaskStatus` (`scietex.service.task_handler.schemas`).
- Progress topic payload: `TaskProgress`.

`TaskStatus` carries the task id as a field as well as in the topic, so a
status message remains self-describing when copied off the wire. Every status
message sets the default `TaskProgress()` (no granular value); granular values
travel only on the progress topic. The progress payload always sets
`progress=True`.

### Lifecycle events

| Event | `status` | Payload notes |
|---|---|---|
| Task accepted into the worker queue (fetch or recovery) | `queued` | — |
| Handler started | `running` | — |
| Success | `completed` | `result` is the handler's result payload |
| Non-retryable error | `failed` | `error`/`error_code` from the `TaskResult` |
| Deliberate cancellation | `cancelled` | `data` is the original `TaskData`; `error` is `"canceled"` |
| Timeout or shutdown cancellation | `failed` | `error` is `"canceled"` |

A **retryable error publishes no terminal status.** `requeue` runs before `ack`
and publishes `queued`, then `ack` returns early without a terminal publish, so
a subscriber sees the task transition `running -> queued -> running ...` until
it succeeds or fails permanently. `on_drain` publishes nothing
(the task is neither terminal nor restarted).

### Progress throttling

Progress reports can be far more frequent than the broker should be asked to
publish, so `MqttTransport` coalesces them in-process (per task). A progress
tick publishes when any of these holds:

- It is the task's first tick (always published, so a subscriber sees progress
  begin).
- `progress_min_interval > 0` and at least that many seconds have elapsed since
  the last publish.
- `progress_min_delta > 0` and the absolute change since the last published
  value is at least that large.
- Both thresholds are `0` (throttling disabled; every call publishes).

Otherwise the newest value is kept as `pending` (the newest wins, never a
backlog) and published on the next eligible tick. Any `pending` value is
flushed on a non-retryable `ack`, immediately before the terminal status, so a
completion is preceded by the final reported value. On `requeue` and `on_drain`
the throttle state is dropped without flushing, because a stale value would
misrepresent a fresh run.

### Failure semantics

Status and progress are observability: a publish failure must never fail,
block, requeue, or slow a task, and must never corrupt inbox state. Every
status/progress publish is wrapped so no exception escapes the transport hook;
`asyncio.CancelledError` still propagates. A failed status publish is logged at
WARNING and a failed progress publish at DEBUG (progress is high-frequency, so
WARNING would be noise); both report to `TransportHealth`, which drives the
existing single reconnect path. Inbox mutations are independent of the
publish, so a status failure can never turn a completed task into a redelivered
one or vice versa. Setting `status_publish_enabled=False` suppresses all
status/progress publishes; envelope requeue is unaffected because it is
delivery, not observability.

### Subscribing to task status

Any MQTT 5 client can subscribe to the wildcard topics. The examples below
assume a service named `worker`; replace it with the service name. With
`mosquitto_sub`:

```bash
# Status of every task, with the topic printed before each payload.
mosquitto_sub -h localhost -p 1883 -V mqttv5 \
  -t 'scietex/worker/tasks/+/status' -v

# Progress of a single task.
mosquitto_sub -h localhost -p 1883 -V mqttv5 \
  -t 'scietex/worker/tasks/<task_id>/progress' -v
```

The payloads are msgpack, so decode them with
`msgspec.msgpack.decode(payload, type=TaskStatus)` (or `TaskProgress`) to read
the fields.

## Example

```python
import asyncio
import json

from scietex.service.mqtt import MqttWorker, MqttWorkerConfig
from scietex.service.task_handler import (
    TaskCapabilities,
    TaskData,
    TaskHandler,
    TaskResult,
)


class EmailHandler(TaskHandler):
    """Handles email-sending tasks."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["send_email"]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        payload = json.loads(task_data.payload)
        # await self.smtp_client.send(payload["to"], payload["subject"], payload["body"])
        return TaskResult(
            status="success",
            payload=json.dumps({"sent_to": payload["to"]}).encode(),
        )


class MyMqttWorker(MqttWorker):
    """An MQTT-backed task processor."""

    def __init__(self, config: MqttWorkerConfig | None = None):
        super().__init__(config)

    async def initialize(self) -> bool:
        """Register handlers and prepare MQTT resources."""
        self.add_task_handler(EmailHandler)
        return await super().initialize()


async def main():
    worker = MyMqttWorker(
        MqttWorkerConfig(
            service_name="email_service",
            version="1.0.0",
            queue_size=10,
            max_concurrent_tasks=4,
            heartbeat_interval=10,
            watchdog_interval=2,
            task_qos=2,
        )
    )

    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration

### YAML Configuration File

`MqttWorker` reads `mqtt.yml` from the config directory. The read (and, when
missing, the write of a default file) is deferred to the first
`connect()`/`initialize()` call — construction has no filesystem side effects
(AR-066). The file is created automatically with default values if it does
not exist. If the file exists but is invalid (unparseable), a `RuntimeError`
is raised and the file is left untouched. When `create_default=False` is
passed to `read_mqtt_config`, a missing file or directory raises
`RuntimeError` instead of writing defaults.

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

`tls_context` is runtime-only: an `ssl.SSLContext` cannot be expressed in
`mqtt.yml` and serializes as `null`.

### Programmatic Configuration

You can also pass an `MqttConfig` directly:

```python
import ssl

from scietex.service.mqtt import MqttConfig, MqttWorkerConfig

config = MqttConfig(
    host="broker.example.com",
    port=8883,
    username="myuser",
    password="secret",
    tls_context=ssl.create_default_context(),
)

worker = MyMqttWorker(MqttWorkerConfig(service_name="svc", mqtt_config=config))
```

### Async Logging

The worker runs one operational `aiomqtt.Client` for task intake,
heartbeats, registry, and requeue; the `AsyncMqttHandler` used for async log
entries owns its **own** independent connection (AR-059/061). The handler is
constructed with `mqtt_config=` (a scalar dict translated from the typed
`MqttConfig` by `logging_handler_config`) on the first successful
`connect()`, so it builds, closes, and reconnects its own client
autonomously; the worker never injects or re-points `handler.client`. The
handler is registered once and reused across restarts, and is stopped in
`cleanup()` before `disconnect()`.

`logging_handler_config` deliberately omits `clean_start` and
`session_expiry_interval`: the external handler's `mqtt_config` dict uses the
MQTT 3.1.1-style `clean_session` field, while `MqttConfig` models the MQTT 5
session pair, and the logging connection uses the handler's own session
defaults.

## Configuration Reference

### MqttConfig

Immutable connection configuration, mapping to aiomqtt v2.5.1 `Client`
scalar options. MQTT 5 is the target protocol, so the session fields are the
MQTT 5 ones (`clean_start`, `session_expiry_interval`).

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | `str` | `"localhost"` | Hostname or IP address of the broker |
| `port` | `int` | `1883` | Port the broker listens on; valid range `[1, 65535]` |
| `username` | `str \| None` | `None` | Optional username for broker authentication |
| `password` | `str \| None` | `None` | Optional password for broker authentication |
| `identifier` | `str \| None` | `None` | Optional MQTT client identifier; `None` lets the broker assign one |
| `keepalive` | `int` | `60` | Keep-alive interval in seconds; valid range `[0, 65535]` |
| `clean_start` | `bool` | `False` | If `True`, the broker discards previous session state (MQTT 5) |
| `session_expiry_interval` | `int` | `0` | Session expiry in seconds; valid range `[0, 4294967295]`. `0` ends the session when the connection closes |
| `transport` | `Literal["tcp", "websockets", "unix"]` | `"tcp"` | Underlying transport |
| `timeout` | `float \| None` | `None` | Optional socket timeout in seconds |
| `tls_insecure` | `bool \| None` | `None` | If `True`, skip certificate verification (not recommended for production) |
| `tls_context` | `ssl.SSLContext \| None` | `None` | Optional preconfigured TLS context; runtime-only, cannot be expressed in `mqtt.yml` |

Note: `session_expiry_interval` is validated by `MqttWorkerConfig` but is not
forwarded to the aiomqtt v2.5.1 scalar `Client` constructor, which has no
such keyword (it would require paho CONNECT properties). The worker uses
`clean_start` for session behavior; `session_expiry_interval` remains part of
the config schema for forward compatibility.

### MqttWorkerConfig

Extends `TaskProcessorConfig` (inheriting service identity and task-queue
fields).

| Field | Type | Default | Description |
|---|---|---|---|
| `mqtt_config` | `MqttConfig \| None` | `None` | Optional connection config; `None` reads `mqtt.yml` lazily at first connect |
| `task_topic` | `str` | `"scietex/{service}/tasks"` | Topic tasks are consumed from |
| `task_qos` | `int` | `2` | QoS for task messages; valid range `[0, 2]` |
| `inbox_backend` | `Literal["memory", "none", "sqlite"]` | `"sqlite"` | Durable inbox backend; `"sqlite"` is the shared multi-process store, `"memory"`/`"none"` the explicit at-most-once opt-out |
| `inbox_path` | `str \| None` | `None` | Path to the inbox store; `None` derives `<conf_dir>/inbox.sqlite3` |
| `inbox_ttl` | `int \| None` | `86400` | TTL in seconds for inbox entries and tombstones (one day); valid range `[1, 2592000]`; `None` disables expiry |
| `inbox_lease_ttl` | `int \| None` | `None` | Claim-lease lifetime in seconds for the SQLite backend; valid range `[1, 86400]`; `None` derives `max(1, int(max(2*heartbeat_interval, 3*watchdog_interval)))` |
| `inbox_prune_interval` | `float` | `60.0` | Base seconds between inbox maintenance passes; valid range `[1.0, 3600.0]` |
| `inbox_prune_jitter` | `float` | `0.25` | Fractional jitter applied to `inbox_prune_interval` (plus or minus this fraction); valid range `[0.0, 1.0]`; `0.0` disables jitter |
| `log_topic` | `str` | `"scietex/{service}/{instance_id}/log"` | Topic worker logs are published to |
| `log_qos` | `int` | `0` | QoS for log messages; valid range `[0, 2]` |
| `log_retain` | `bool` | `False` | Publish log messages with the retained flag |
| `log_message_expiry` | `int \| None` | `86400` | MQTT 5 message-expiry interval in seconds for log publishes; valid range `[1, 2592000]`; `None` disables expiry |
| `status_publish_enabled` | `bool` | `True` | Master switch for all status/progress publishing; `False` restores the no-op behavior |
| `status_topic_prefix` | `str` | `"scietex/{service}/tasks"` | Prefix for the per-task status/progress topics; `{service}` is substituted at construction |
| `status_qos` | `int` | `1` | QoS for `TaskStatus` publishes; valid range `[0, 2]` |
| `status_ttl` | `int \| None` | `86400` | MQTT 5 message-expiry interval in seconds for retained `TaskStatus` publishes; valid range `[1, 2592000]`; `None` disables expiry |
| `progress_qos` | `int` | `0` | QoS for `TaskProgress` publishes; valid range `[0, 2]` |
| `progress_min_interval` | `float` | `1.0` | Minimum seconds between progress publishes; valid range `[0.0, 3600.0]`; `0` disables the interval threshold |
| `progress_min_delta` | `float` | `0.0` | Minimum absolute progress change that forces a publish; valid range `[0.0, 100.0]`; `0` disables the delta threshold |

### read_mqtt_config()

`read_mqtt_config(conf_dir: Path | None, *, create_default: bool = True) -> MqttConfig`
reads `mqtt.yml` from the config directory, mirroring `read_valkey_config`.
With `create_default=True` (the default) a missing file is created with
default values; with `create_default=False` the read is write-free and a
missing file or directory raises `RuntimeError`. A present-but-invalid file
always raises `RuntimeError` and is left untouched.

## Data Schemas

### Registry / heartbeat payload

The retained heartbeat message is the msgpack encoding of the shared
`Heartbeat` struct (from `scietex.service.heartbeat`), published to
`scietex/{service_name}/workers/{instance_id}` at QoS 1 with `retain=True` and
a `MessageExpiryInterval` of `active_ttl`.

| Field | Type | Description |
|---|---|---|
| `service` | `str` | Name of the publishing service |
| `instance_id` | `str` | Unique identifier of the worker instance |
| `status` | `"active" \| "inactive"` | Liveness state |
| `heartbeat_interval` | `float` | Seconds between heartbeats |
| `start_time` | `datetime` | Worker start time (msgpack timestamp) |
| `ttl` | `float` | Lifetime the consumer should apply to this record |
| `timestamp` | `datetime` | Time this heartbeat was produced (msgpack timestamp) |

The same struct is published by `ValkeyWorker`, so a client decodes one shape
regardless of transport. See {doc}`worker_registry`.
