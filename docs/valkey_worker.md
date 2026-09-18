# ValkeyWorker

The `ValkeyWorker` is a Valkey-backed async task processor that extends
`TaskProcessor` with Valkey stream-based task distribution, heartbeat
publishing, and async logging. It uses the `glide` client for all Valkey
operations.

## Overview

```python
from scietex.service.valkey import ValkeyWorker
```

`ValkeyWorker` adds Valkey-specific operations on top of
`TaskProcessor`:

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
  │  │  AsyncValkeyHandler  │───►│  scietex:{svc}:log stream  │     │
  │  │  (log entries)       │    │  msgpack-encoded entries   │     │
  │  └─────────────────────┘    └────────────────────────────┘     │
  └─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
                 ┌─────────────────┐
                 │   Valkey Server  │
                 │                  │
                 │  Stream:         │
                 │  scietex:{svc}:  │
                 │  tasks           │
                 │                  │
                 │  Group:          │
                 │  scietex:{svc}:  │
                 │  task_group      │
                 └─────────────────┘
```

## Key Names

The Valkey key space is split into two namespaces. Service-scoped keys are
shared across all replicas of a service; worker-scoped keys are unique per
`instance_id` (auto-generated):

| Resource | Key Pattern | Scope |
|---|---|---|
| Task stream | `scietex:{service_name}:tasks` | service-scoped |
| Consumer group | `scietex:{service_name}:task_group` | service-scoped |
| Worker registry | `scietex:{service_name}:workers` | service-scoped |
| Consumer name | `scietex:{service_name}:{instance_id}` | worker-scoped |
| Task tracking key | `scietex:{service_name}:task:{task_id}` | per task |
| Task lease key | `scietex:{service_name}:lease:{task_id}` | per task |
| Heartbeat key | `scietex:{service_name}:{instance_id}:status` | worker-scoped |
| Log stream | `scietex:{service_name}:log` (configurable via `log_stream_name`) | service-scoped |

## Constants

| Constant | Default | Description |
|---|---|---|
| `DEFAULT_MAX_TASKS_QUEUE_SIZE` | `100` | Default max queue size (inherited) |
| `DEFAULT_MAX_CONCURRENT_TASKS` | `10` | Default max concurrent tasks (inherited) |
| `task_timeout` (`TaskProcessorConfig`) | `3` | Default task timeout in seconds (inherited) |
| `DEFAULT_HEARTBEAT_INTERVAL` | `10` | Default heartbeat interval in seconds |
| `DEFAULT_WATCHDOG_INTERVAL` | `1` | Default watchdog check interval in seconds |
| `claim_min_idle_ms` (`ValkeyWorkerConfig`) | `1000` | Outer idle floor (ms) for `XAUTOCLAIM`; the per-entry lease is the authoritative liveness check |
| `LEASE_TTL_HEARTBEAT_MULTIPLIER` | `2` | Heartbeat multiplier in the derived per-entry lease TTL |
| `LEASE_TTL_WATCHDOG_MULTIPLIER` | `3` | Watchdog multiplier in the derived per-entry lease TTL |
| `MIN_TASK_LEASE_TTL_SECONDS` | `1` | Floor of the derived per-entry lease TTL |
| `MIN_TASK_LEASE_TTL` (`ValkeyWorkerConfig`) | `1` | Lower bound (seconds) of the configurable `task_lease_ttl` |
| `MAX_TASK_LEASE_TTL` (`ValkeyWorkerConfig`) | `86400` | Upper bound (seconds) of the configurable `task_lease_ttl` (24 hours) |
| Task lease TTL | derived `20` | `task_lease_ttl` when set; otherwise `max(1, int(max(2 * heartbeat_interval, 3 * watchdog_interval)))` seconds |
| `MIN_TASK_TRACKING_TTL` | `1` | Floor (seconds) of the task tracking TTL |
| `MAX_TASK_TRACKING_TTL` | `2592000` | Ceiling (seconds) of the task tracking TTL (30 days) |
| `DEFAULT_TASK_TRACKING_TTL` | `86400` | Default task tracking TTL in seconds (24 hours); applied when `task_tracking_ttl` is `None` |

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
| `valkey_config` | `ValkeyConfig \| None` | `None` | The Valkey configuration used by this worker. When no explicit config was given at construction, it is loaded lazily from disk at first connect, so it is `None` until then |
| `client` | `GlideClient \| None` | `None` | The active Valkey client (``None`` until connected) |
| `transport_health` | `TransportHealth` | — | Connection-health supervisor: aggregates transport failures, owns the single reconnect path, and surfaces `connected`/`degraded`/`last_error`/`failure_count`/`down_duration` |

### Inherited from TaskProcessor

| Property | Type | Default | Description |
|---|---|---|---|
| `queue_size` | `int` | `100` | Maximum size of the internal task queue |
| `max_concurrent_tasks` | `int` | `10` | Maximum tasks processed in parallel |
| `task_handlers` | `Mapping[str, TaskHandler]` | — | Currently active (started) handlers, as a read-only `MappingProxyType` view |
| `running_tasks` | `Mapping[UUID, TaskTracker]` | — | Snapshot of currently running tasks and their trackers, delegated to the composed `TaskLifecycle`; a copy, not a live view |

## Constructor

`ValkeyWorker` takes a single immutable configuration object
(`ValkeyWorkerConfig`, from `scietex.service.valkey.config`, which extends
`TaskProcessorConfig`), or `None` to use the struct defaults. It also accepts
an optional keyword-only `client_factory` — an async callable used by
`connect()` to build the `GlideClient`:

```python
import logging

from scietex.service import ValkeyWorker, ValkeyWorkerConfig

worker = ValkeyWorker(
    ValkeyWorkerConfig(
        service_name="service",
        version="0.0.1",
        conf_dir=None,
        logging_level=logging.DEBUG,
        heartbeat_interval=None,
        watchdog_interval=None,
        queue_size=None,
        max_concurrent_tasks=None,
        valkey_config=None,
        log_stream_name="scietex:{service}:log",
        task_fetch_batch_size=10,
        claim_min_idle_ms=None,
        task_tracking_ttl=None,
        task_lease_ttl=None,
    )
)
```

### Client factory

`ValkeyWorker.__init__(config=None, *, client_factory: ClientFactory | None = None)`
where `ClientFactory = Callable[[GlideClientConfiguration], Awaitable[GlideClient]]`.
When omitted, the factory defaults to `GlideClient.create`. Supplying one lets
embedders and tests inject a client without monkeypatching `GlideClient`:

```python
async def my_factory(client_config):
    return await GlideClient.create(client_config)

worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc"), client_factory=my_factory)
```

The factory runs inside `connect()`, so the full connect contract (PING,
logging-handler construction, connectivity signal) is preserved.

`ValkeyWorkerConfig` adds these fields on top of `TaskProcessorConfig`:

| Field | Default | Description |
|---|---|---|
| `valkey_config` | `None` | Custom Valkey configuration (`ValkeyConfig`). If `None`, `valkey.yml` is read lazily from the config directory at first connect (not at construction). PubSub listening is expressed via `ValkeyConfig.pubsub_config` (a `ValkeyPubSubConfig`) |
| `log_stream_name` | `"scietex:{service}:log"` | Name of the Valkey stream used for log entries; `{service}` is substituted with `service_name` at construction |
| `task_fetch_batch_size` | `10` | Maximum number of stream entries read per `XREADGROUP` call |
| `claim_min_idle_ms` | `None` (default `1000`) | Outer idle floor (ms) before `XAUTOCLAIM` considers reclaiming a pending entry during startup recovery; the per-entry lease is the authoritative liveness check (see [Duplicate processing in scale-out](#duplicate-processing-in-scale-out)) |
| `task_tracking_ttl` | `None` (default `86400`) | Server-side TTL in seconds for task tracking records; `None` resolves to `DEFAULT_TASK_TRACKING_TTL` (`86400` s / 24 h). Valid range `[1, 2592000]` |
| `task_lease_ttl` | `None` (derived) | Server-side TTL in seconds for per-entry leases; `None` derives `max(1, int(max(2 * heartbeat_interval, 3 * watchdog_interval)))`. Valid range `[1, 86400]` |

All `TaskProcessorConfig` and `WorkerConfig` fields are inherited.
Configuration is immutable: values are fixed at construction, and
out-of-range values raise `msgspec.ValidationError`.

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
`scietex:{service_name}:{instance_id}:status` with a TTL set to twice the
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
`TaskProcessor.cleanup()`, then closes the Valkey connection.

### return_task_to_queue()

Re-queue a task by appending it to the Valkey task stream.

```python
async def return_task_to_queue(self, task_id: UUID, task_data: TaskData) -> None:
    """Encode TaskData into a versioned envelope, append to task stream."""
```

Encodes `task_data` into a versioned `TaskEnvelope` (see [Wire Format](#wire-format))
and appends a new entry to the stream. The entry key is the string
representation of `task_id`.

### fetch_tasks()

Fetch a batch of tasks from the Valkey task stream and enqueue them.

```python
async def fetch_tasks(self) -> bool:
    """XREADGROUP with block_ms=1000, decode envelope, enqueue (non-blocking)."""
```

On the first call, recovers entries left pending by a previous run (see
[At-Least-Once Delivery](#at-least-once-delivery)). Then reads up to
`task_fetch_batch_size` entries (default `10`) from the task stream using
`XREADGROUP` with `block_ms=1000` and the configured consumer group,
decodes each versioned envelope payload into a `TaskData` struct (see
[Wire Format](#wire-format)), and enqueues it via
the non-blocking `enqueue_task()` as a `(UUID, TaskData)` tuple. Each
accepted entry's id is recorded in the transport's entry-id map and its
per-entry lease is written at enqueue-accept (ownership begins when the entry
is recorded), so a task is protected from a peer's recovery for its whole
queue wait, not just while it runs. The stream entries are NOT acknowledged
here — they stay in the consumer group's pending list until
`on_task_completed()` acks them after the handler finishes. If the queue is
full, an entry is left pending (deferred, and its lease not written) and is
never blocking. On read errors, disconnects and attempts to reconnect to
Valkey. Returns `True` if at least one task was enqueued, `False` otherwise.

### on_task_completed()

Acknowledge the stream entry for a completed task.

```python
async def on_task_completed(
    self, task_id, task_data, task_result, *, cancel_reason=None
):
    """Publish a terminal tracking record, then XACK + XDEL the entry."""
```

Called by the base `TaskProcessor` when a task's processing
terminates (success, error, or cancellation). Publishes a terminal
`TaskStatus` tracking record to the task tracking key, then looks up the
stream entry id recorded at fetch time and `XACK`s + `XDEL`s it, so the entry
leaves the consumer group's pending list only after the handler's work on it is
done. `task_result` is `None` when the task was cancelled before
producing a result, and `cancel_reason` identifies why (`"deliberate"`,
`"timeout"`, or `"shutdown"`, or `None` for a normal completion).

When `task_result` is `None`:

- A deliberate `cancel_task` (`cancel_reason == "deliberate"`) writes
  `status="cancelled"` with the original `TaskData` embedded in the record's
  `data` field and `error="canceled"`.
- Timeout and shutdown cancellations keep the existing `status="failed"` /
  `"canceled"` shape.

When a `task_result` is present the record is `status="completed"` on success
or `status="failed"` otherwise, with the result payload and error-code fields
carried over. Tracking is observability only — a failed tracking write is
logged as a WARNING and never fails or requeues the task itself.

### _write_task_progress()

Update the progress of the `running` tracking record for a task.

```python
async def _write_task_progress(self, task_id: UUID, value: float) -> None:
    """Update the tracking record's progress for a running task."""
```

`ValkeyWorker` overrides the base `TaskProcessor._write_task_progress()`
no-op hook (invoked via `TaskCapabilities.report_progress()`, which clamps
`value` to `[0.0, 100.0]`) and delegates to
`TaskStatusStore.update_progress()`. It `GET`s
the task tracking key, msgpack-decodes the stored
`TaskStatus`, replaces `progress` with
`TaskProgress(progress=True, value=value)` and `updated_at` with the current
UTC time, then rewrites the record. When the key is missing the progress
update is dropped and logged at DEBUG; when the stored payload fails to
decode it returns without writing. A failed read is logged as a WARNING and
never fails or requeues the task.

### watchdog()

Refresh per-entry leases, then run the inherited watchdog.

```python
async def watchdog(self) -> None:
    """Refresh per-entry leases, then run the base watchdog."""
```

Overrides `TaskProcessor.watchdog()` to refresh per-entry leases before
`super().watchdog()`. The refresh is delegated to the injected
`ValkeyTransport` (`refresh_leases()`), which rewrites the lease for every
task in the transport's entry-id map — the authoritative ownership map covering
both queued and running tasks — so a live lease always outlives its refresh
window even when the base watchdog blocks on a cancellation wait, and a queued
task's lease stays alive indefinitely regardless of queue wait (as long as the
event loop is healthy). In normal operation a task leaves the map only in
`on_task_completed()`, which also deletes the lease, so a cancelled or
completed task stops being refreshed and its entry becomes reclaimable;
`cleanup()` clears the map on shutdown (see
[Duplicate processing in scale-out](#duplicate-processing-in-scale-out)).

### Task cancellation and external requeue

`ValkeyWorker` inherits the built-in `cancel_task` handler from
`TaskProcessor` (auto-registered in `TaskProcessor.__init__`). A `cancel_task`
entry is submitted on the same stream as any other task, carrying a
msgpack-encoded `CancelTaskRequest(target_task_id="<uuid>", reason="...")` as
its payload. The worker cancels a running target (same pattern as the
watchdog) or removes a queued-but-undispatched target, and reports the
outcome (`cancelled`, `not_running`, `ignored`, `not_found`) in its own result.

A deliberate cancel never requeues automatically. Instead, the worker writes a
`TaskStatus` tracking record with `status="cancelled"`, `error="canceled"`, and
the original `TaskData` embedded in `data`, under the task tracking key
`scietex:{service_name}:task:{task_id}`. An external process can then:

1. `GET scietex:{service_name}:task:{task_id}`
2. msgpack-decode it into a `TaskStatus` and read its `data` field (the
   original `TaskData`)
3. modify the payload/fields as needed
4. resubmit the task under a **new** task id via
   `encode_task_envelope` on the task stream

Timeout and shutdown cancellations keep the existing `status="failed"` /
`"canceled"` record and do not embed `data`.

Reliable cancellation needs `max_concurrent_tasks >= 2`: with a single slot the
cancel task queues behind its target and cannot run. Tasks still unread in the
stream are not cancellable and yield `not_running`.

### purge_task_stream()

The stream-purge capability is a standalone operational utility, not a
`ValkeyWorker` method. It reads, acknowledges, and deletes every entry in
a task stream so an operator can clear it without running a worker.

```python
from scietex.service.valkey import purge_task_stream

result = await purge_task_stream(client, stream_name, group_name, consumer_name)
if result.errors:
    print(f"partial purge: purged {result.entries_purged}, errors: {result.errors}")
```

Reads and acknowledges every entry in the stream via `XREADGROUP` (both
pending and unclaimed), then deletes them with `XDEL`. Also purges any
remaining entries via `XREAD`. Returns a `PurgeResult` (`entries_purged`,
`errors`); failures are also logged but never raised, so a caller that ignores
the return value still sees the log output. See
`src/scietex/service/valkey/purge.py`.

## Wire Format

Each task stream entry's value is a **versioned transport envelope**, not a
bare `TaskData`. The envelope is `TaskEnvelope` (from
`scietex.service.task_handler.schemas`):

| Field | Type | Default | Description |
|---|---|---|---|
| `version` | `int` | `1` | Wire-format version |
| `data` | `bytes` | `b""` | Serialized task payload (version 1: msgpack-encoded `TaskData`) |

The whole `TaskEnvelope` is msgpack-encoded as the entry value. The durable
stream value is therefore:

```
msgpack(TaskEnvelope(version=1, data=msgpack(TaskData)))
```

Version 1 wraps a msgpack-encoded `TaskData`; a future version may carry a
different payload, which is what the `version` field exists for. The handler
contract (`TaskData`) is unchanged, and handlers **never see the envelope** —
they receive the decoded `TaskData`. This decoupling lets the wire format
evolve independently of the in-process handler contract (AR-064).

Encoding and decoding are centralized in the shared, transport-agnostic
helpers `encode_task_envelope(task_data)` and
`decode_task_envelope(payload)` from `scietex.service.task_handler.wire`.
`ValkeyTransport.requeue()` encodes through `encode_task_envelope`, and
`fetch()` / `recover_pending_tasks()` decode through
`decode_task_envelope`. A payload that is not a valid envelope, or that
carries an unknown version, decodes to `None` and the entry is skipped with
an ERROR log — intake never crashes on an unrecognized wire payload.

## At-Least-Once Delivery

`ValkeyWorker` delivers each task at least once: a stream entry is
acknowledged and deleted only after its handler finishes, so a crash
mid-processing redelivers the task on restart.

- `ValkeyTransport.recover_pending_tasks()` — On the first `fetch()`, uses
  `XAUTOCLAIM` to claim pending entries idle for at least `claim_min_idle_ms`
  and re-enqueue them, redelivering tasks that were read but never
  acknowledged before a crash. Each claimed entry's per-entry lease is
  acquired atomically (`SET ... NX`) before enqueueing, so concurrent
  recoveries on two replicas cannot both reclaim it; a claimed entry whose
  lease is held by another worker is skipped (see
  [Duplicate processing in scale-out](#duplicate-processing-in-scale-out)).
- **Entry-id map** — A `dict[UUID, str | bytes]` mapping each accepted
  task's UUID to the stream entry id it was read from, recorded at
  enqueue-accept time. It lives on the `ValkeyTransport` and doubles as the
  lease-refresh ownership map, so it stays authoritative until
  `on_task_completed()` pops it.
- `ValkeyTransport.ack()` — Called when a task's processing terminates.
  Looks up the recorded entry id and `XACK`s + `XDEL`s it, removing the
  entry from the pending list only after the handler's work is done, then
  deletes the lease.

If the queue is full when fetching (or during recovery), the entry is
left pending and redelivered on a later poll rather than dropped; during
recovery the lease acquired for it is rolled back first, so this worker does
not hold a lease on an entry it never accepted.

### Duplicate processing in scale-out

Delivery is *at least once*, not *exactly once*: under specific conditions an
entry can be processed by more than one worker. Two gates guard the
`XAUTOCLAIM` pending-recovery path:

- `claim_min_idle_ms` (default `1000` ms) is the outer, server-side gate:
  recovery only considers entries whose pending-list idle time exceeds it.
- a **per-entry lease** is the inner, authoritative liveness check: recovery
  acquires it atomically before re-enqueuing a reclaimed candidate and skips
  candidates already held by another worker.

#### Per-entry lease

The lease key is `scietex:{service_name}:lease:{task_id}` — a distinct prefix
from the task tracking key `scietex:{service_name}:task:{task_id}`. Its value
is the holder's consumer name (`scietex:{service_name}:{instance_id}`) and it
carries a server-side TTL. The TTL is configurable via
`ValkeyWorkerConfig.task_lease_ttl` (valid range `[1, 86400]` seconds); when
`None`, it is derived from the worker's timing settings:

```
max(1, int(max(2 * heartbeat_interval, 3 * watchdog_interval))) seconds
```

With the defaults (heartbeat `10` s, watchdog `1` s) the derived TTL is
**20 s**. The module constants `LEASE_TTL_HEARTBEAT_MULTIPLIER = 2`,
`LEASE_TTL_WATCHDOG_MULTIPLIER = 3`, and `MIN_TASK_LEASE_TTL_SECONDS = 1`
express the derivation; `MIN_TASK_LEASE_TTL`/`MAX_TASK_LEASE_TTL` bound the
configurable field.

Lifecycle:

- **Acquired at enqueue-accept.** `ValkeyTransport.fetch()` writes the lease
  right after recording the entry id in its entry-id map;
  `recover_pending_tasks()` acquires it before enqueueing. Ownership therefore
  begins when the entry is accepted, so a task sitting in the internal queue is
  leased for its whole queue wait — the window where a starting replica could
  reclaim an unstarted task is closed.
- **Refreshed** by the `ValkeyWorker.watchdog()` override, which calls
  `ValkeyTransport.refresh_leases()` for every task in the entry-id map — the
  authoritative map covering queued *and* running tasks — before running
  `super().watchdog()`. `on_started()` also rewrites the lease alongside the
  `running` tracking record.
- **Deleted** in `ValkeyTransport.ack()` on every terminal path, after the
  `XACK`/`XDEL` of the stream entry. It is also released on two early-exit
  paths: the queue-full rollback in `recover_pending_tasks()` (the entry was
  never accepted, so this worker must not hold its lease) and
  `on_drain()` on shutdown drain (this worker will not run the task, so a
  restart or peer can reclaim it immediately instead of waiting up to the lease
  TTL). `requeue()` also deletes the lease, since the requeued copy reuses the
  same `task_id` and must not inherit a stale lease (AR-077b).
- **Acquired atomically in recovery.** `recover_pending_tasks()` calls
  `TaskLeaseManager.acquire(task_id)`, which uses `SET ... NX`
  (`ConditionalChange.ONLY_IF_DOES_NOT_EXIST`) so two replicas booting
  concurrently cannot both reclaim the same pending entry. It returns `True`
  when this worker holds (or already held) the lease and `False` when another
  holder owns it. On a glide error it returns `True` — fail-open, because an
  uncertain state must not block reclaim. `fetch()` still uses the plain
  `TaskLeaseManager.write()`, since `XREADGROUP ">"` delivers each new entry to
  exactly one consumer, so there is no concurrent claimant to race.
- **Consulted as an ownership guard** in `recover_pending_tasks()`. A
  reclaimed candidate whose atomic acquire fails (a live holder owns it) is
  skipped — left pending, not enqueued, and not recorded in the entry-id map
  — and marks recovery incomplete (`recovery_complete=False`) so `recovered`
  stays `False` and recovery retries on the next poll. A candidate already
  owned by *this* worker (present in the entry-id map) is also skipped, but
  does not mark recovery incomplete.

Reclaim is non-destructive: `recover_pending_tasks()` only `XADD`s a copy of
a reclaimed entry, never `XACK`/`XDEL`s the original, so a false skip costs
only recovery latency (bounded by the lease TTL) while a false reclaim costs a
duplicate. Exactly-once is not claimed.

#### Residual windows

The lease narrows, but does not eliminate, duplicate processing. Handlers must
remain idempotent.

The previously documented **queued (pre-start) window is now closed**: the
lease is acquired at enqueue-accept and refreshed over the transport's entry-id
map, so a task waiting in the internal queue is protected for its whole queue
wait. The genuinely residual windows that remain are:

- **Lease expiry races.** A lease expiring concurrently with a reclaim or a
  Valkey outage can still duplicate work: the entry's idle time exceeds
  `claim_min_idle_ms`, the lease lapses, and a starting replica reclaims it
  while the original worker is still running it.
- **Lease-write failure.** `TaskLeaseManager.write()` swallows glide errors (a
  lease failure must never break task processing), so a failed accept-time
  write leaves the entry unprotected and a failed watchdog refresh lets a live
  lease expire. Either collapses the guard back to idle-time only.
- **Watchdog blocking.** `super().watchdog()` can block up to
  `task_cancellation_timeout` (default `5` s) waiting on a cancellation; the
  `20` s TTL tolerates roughly four such blocks. Setting
  `task_cancellation_timeout` near or above the lease TTL could let a live
  lease expire mid-block.
- **Unbounded-timeout tasks.** Tasks with `timeout <= 0` are never cancelled by
  the watchdog, so their lease renews indefinitely and they are never
  recovered. This is accepted: the operator opted into unbounded execution.
- **Event-loop stalls.** CPU-bound handlers block the event loop and therefore
  block lease refresh; the generous TTL is the only mitigation.
- **Recovery churn while a queued lease is peer-held (accepted).** A peer's
  lease on a still-queued entry keeps `lease_skipped = True`, so
  `recovery_complete` stays `False` and `recovered` remains `False`: recovery
  re-scans the pending list on every intake poll for the whole queue-wait
  duration. This is deliberate — recovery is the only reclaim path, so marking
  it complete would strand the entry if the holder died (AR-051). No backoff
  was added; it is possible future work.

Guidance:

- **Single-consumer deployments are safe.** A lone `ValkeyWorker` never
  reclaims its own in-flight entry — recovery runs once on the first
  `fetch_tasks()`, before any task is in flight in that process.
- **For multi-replica deployments**, `claim_min_idle_ms` is now only an outer
  gate and no longer needs to exceed the maximum handler duration; the lease
  is the authoritative liveness check. Keep handlers idempotent regardless —
  duplicates are reduced, not eliminated.

## Example

```python
import asyncio
import json
import uuid
from uuid import uuid4

from scietex.service.valkey import ValkeyWorker, ValkeyWorkerConfig
from scietex.service.task_handler import (
    TaskCapabilities,
    TaskData,
    TaskHandler,
    TaskResult,
)
from scietex.service.task_handler import TaskTimeout


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


class MyValkeyWorker(ValkeyWorker):
    """A Valkey-backed task processor."""

    def __init__(self, config: ValkeyWorkerConfig | None = None):
        super().__init__(config)

    async def initialize(self) -> bool:
        """Register handlers and prepare Valkey resources."""
        self.add_task_handler(EmailHandler)
        return await super().initialize()


async def main():
    worker = MyValkeyWorker(
        ValkeyWorkerConfig(
            service_name="email_service",
            version="1.0.0",
            queue_size=10,
            max_concurrent_tasks=4,
            heartbeat_interval=10,
            watchdog_interval=2,
        )
    )

    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration

### YAML Configuration File

`ValkeyWorker` reads configuration from `valkey.yml` in the config
directory. The read (and, when missing, the write of a default file) is
deferred to the first `connect()`/`initialize()` call — construction has no
filesystem side effects (AR-066). The file is created automatically with
default values if it does not exist. If the file exists but is invalid
(unparseable), a `RuntimeError` is raised and the file is left untouched.

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

You can also pass a `ValkeyConfig` directly:

```python
from scietex.service.valkey import (
    ValkeyConfig,
    ValkeyNode,
    ValkeyBackoffStrategy,
    ValkeyBaseConfig,
    ValkeyUserCredentials,
    ValkeyWorkerConfig,
)

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

worker = MyValkeyWorker(ValkeyWorkerConfig(valkey_config=config))
```

### Async Logging

The worker runs one operational `GlideClient` for task/heartbeat/registry
traffic, and the `AsyncValkeyHandler` used for async log entries owns its own
independent connection (AR-059/061). The handler is constructed with
`valkey_config=` (a scalar dict translated from the typed `ValkeyConfig`) on the
first successful `connect()`, so it builds, closes, and reconnects its own
client autonomously; the worker never injects or re-points `handler.client`.
The handler is registered once and reused across restarts.

## Configuration Reference

### ValkeyConfig

Top-level configuration combining base, advanced, and PubSub settings.

| Field | Type | Default | Description |
|---|---|---|---|
| `base_config` | `ValkeyBaseConfig` | `ValkeyBaseConfig()` | Basic connection parameters |
| `advanced_config` | `ValkeyAdvancedConfig` | `ValkeyAdvancedConfig()` | Advanced connection settings |
| `pubsub_config` | `ValkeyPubSubConfig` | `ValkeyPubSubConfig()` | PubSub control-channel settings (`listening`, `parse_control_message`) |

### ValkeyPubSubConfig

PubSub control-channel settings.

| Field | Type | Default | Description |
|---|---|---|---|
| `listening` | `bool` | `False` | Subscribe the worker's client to the control channels |
| `parse_control_message` | `Callable[[PubSubMsg, Any], None] \| None` | `None` | Callback invoked for each incoming PubSub message; runtime-only (not YAML-encodable) |

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
`scietex:{service_name}:{instance_id}:status` with a TTL set to twice the
heartbeat interval.

| Field | Type | Default | Description |
|---|---|---|---|
| `service` | `str` | *(required)* | Name of the publishing service |
| `instance_id` | `str` | *(required)* | Unique identifier of the worker instance |
| `status` | `Literal["active", "inactive"]` | *(required)* | Current worker status |
| `heartbeat_interval` | `float` | *(required)* | Interval in seconds between heartbeats |
| `start_time` | `datetime` | *(required)* | UTC timestamp when the worker started |
| `timestamp` | `datetime` | `datetime.now(timezone.utc)` | UTC timestamp of this heartbeat entry |

### TaskProgress

Granular progress reported by a task handler. Embedded in the `progress`
field of a `TaskStatus` tracking record and updated by
`ValkeyWorker._write_task_progress()` via `TaskCapabilities.report_progress()`.

| Field | Type | Default | Description |
|---|---|---|---|
| `progress` | `bool` | `False` | Whether the handler reports granular progress |
| `value` | `float` | `0.0` | Progress value, only meaningful when `progress` is `True` |

## PubSub Broadcasting

PubSub listening is opt-in through the typed schema. Set
`ValkeyConfig.pubsub_config` to a `ValkeyPubSubConfig` with `listening=True`
and a `parse_control_message` callback:

```python
from scietex.service import ValkeyConfig, ValkeyPubSubConfig, ValkeyWorker, ValkeyWorkerConfig

config = ValkeyConfig(
    pubsub_config=ValkeyPubSubConfig(
        listening=True,
        parse_control_message=my_callback,  # (PubSubMsg, Any) -> None
    ),
)
worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=config))
```

When `listening` is `True`, the worker's client subscribes to:

| Channel | Pattern | Description |
|---|---|---|
| `scietex:{service_name}:{instance_id}` | Exact | Service-specific channel for this worker |
| `scietex:broadcast` | Exact | Broadcast channel for all workers in the service |

Each incoming message is delivered to `parse_control_message`. The callback is
runtime-only: it cannot be expressed in `valkey.yml` (a callable is not
YAML-encodable and serializes as `null`), so a `listening: true` loaded from
YAML subscribes with no callback and drops messages. Configure PubSub
programmatically when a callback is required.
