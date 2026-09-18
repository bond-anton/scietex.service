# Components

For each major component: purpose, main classes/functions, public interface,
dependencies, dependents. Line numbers refer to the module given.

## 1. Worker core — `BasicWorker`

**File:** `src/scietex/service/basic_worker.py`

**Purpose:** Foundation for daemon workers: identity (`service_name`,
`instance_id`, `version`), lifecycle orchestration, signal-driven graceful
shutdown, async logging handler management, and subclass hooks for
heartbeat/watchdog/initialize/cleanup. The worker composes four components —
`ManagerRuntime` (manager discovery/runtime), `LoggingLifecycle`
(logging-handler lifecycle), `WorkerLifecycle` (state machine, events, stop-task
guard, `start_time`), and `SignalHandler` (SIGINT/SIGTERM registration) — the
latter two extracted in AR-087; `BasicWorker` keeps only identity, config, and
the thin `start`/`stop`/`exit`/`_startup`/`_shutdown` orchestrators.

**Main symbols:**
- `ServiceStatus` (STOPPED/STARTING/RUNNING/STOPPING) — line 38
- `class BasicWorker` — line 54
- Constructor — `__init__(config: WorkerConfig | None = None)`; stores the
  immutable `WorkerConfig` (from `config.py`), resolves identity/conf_dir/
  logging_level, and constructs all four components: `ManagerRuntime` +
  `LoggingLifecycle` + `WorkerLifecycle` + `SignalHandler`.
  Timing/retry fields are validated at construction — an out-of-range value
  raises `msgspec.ValidationError`, and `None` resolves to the matching
  `DEFAULT_*` constant in `config.py` at read time (no runtime clamping)
- Config type mechanism (AR-069): class attribute `_config_type: ClassVar
  [type[WorkerConfig]]` (82) tells the base which concrete config struct to
  instantiate when `config=None`. Subclasses override it to their own config
  type (e.g. `TaskProcessor`→`TaskProcessorConfig`, `ValkeyWorker`→
  `ValkeyWorkerConfig`) so the base stores the concrete type and subclass
  constructors no longer re-store / double-instantiate
- Delegators (thin, to the composed components): `_setup_signal_handlers` 358
  → `SignalHandler.setup()` (Windows-safe no-op), `_remove_signal_handlers` 379
  → `SignalHandler.remove()`, `_request_exit` 370 → `WorkerLifecycle.request_exit()`,
  `_force_stopped` 481 → `WorkerLifecycle.force_stopped()`; properties `state`,
  `events`, `start_time` read from `WorkerLifecycle`
- Lifecycle orchestrators: `_startup` 398, `start` 453, `_shutdown` 491,
  `stop` 538, `exit` 582
- Cancellation terminal-state helper: `_force_stopped` 481 (AR-017 — forces
  STOPPED + `exit` event on startup/shutdown cancellation; delegated to
  `WorkerLifecycle.force_stopped()`)
- Hooks: `initialize` 388, `heartbeat` 592, `watchdog` 604, `cleanup` 624,
  `_register_instance` 633, `_unregister_instance` 643
- Built-in managers: module-level `_heartbeat_manager` 654 and
  `_watchdog_manager` 664, registered via `register_manager(BasicWorker, ...)`
  (674, 680) with `name="Heartbeat"`/`"Watchdog"` and
  `attribute_name="_heartbeat_manager"`/`"_watchdog_manager"` — no longer
  `@Manager`-decorated methods (AR-087)
- `_setup_signal_handlers` called from `start()` (478), not `__init__`;
  `_remove_signal_handlers` called from `stop()` (560)

**Public interface:** constructor takes a single immutable `WorkerConfig`
(`config.py`) or `None`; all properties are read-only (no runtime setters):
`state`, `events` (read-only `MappingProxyType` of two `asyncio.Event`s:
`"exit_requested"`, `"exit"`), `service_name`, `instance_id`, `version`,
`conf_dir`, `logger`, `logging_level`, `heartbeat_interval`,
`watchdog_interval`, `start_time`, `logger_handler_timeout`,
`manager_shutdown_timeout`, `manager_max_retries`, `manager_restart_backoff`,
`manager_runtime` (the `ManagerRuntime`, AR-071), `failed_managers` (list of
`FAILED` manager names, AR-063).
Extension contract: override
`initialize/heartbeat/watchdog/cleanup`, add `@Manager` methods. Two newer
subclass hooks govern registry-set membership: `_register_instance` (633) —
called by `_startup()` after `initialize()` succeeds and before managers
start — and `_unregister_instance` (643) — called by `_shutdown()` after
managers stop and before `cleanup()` teardown. Both are no-ops in the base;
`ValkeyWorker` overrides them (worker.py:502, 527) to `SADD`/
`SREM` its `instance_id` into the worker registry set.

**Dependencies:** `.manager.runtime` (`ManagerRuntime`), `.log_handlers.lifecycle`
(`LoggingLifecycle`), `.lifecycle` (`WorkerLifecycle`), `.signal_handler`
(`SignalHandler`), `.manager` (`register_manager`), `.log_handlers`
(`parse_logging_level`), `.utils` (`prepare_conf_dir`, `print_scietex_logo`);
external `scietex.logging.ConsoleHandler`.

**Depended on by:** `TaskProcessor` (extends); `ManagerRuntime`,
`LoggingLifecycle`, `WorkerLifecycle`, and `SignalHandler` (back-reference to
the owning worker); `task_handler` (indirectly, via `TaskHandlerContext`).

## 2. Manager runtime — `ManagerRuntime`

**File:** `src/scietex/service/manager/runtime.py`

**Purpose:** Extracted from `BasicWorker` (AR-003). Owns manager
discovery, lifecycle bookkeeping, and the restart-on-error loop. Reads config
off the worker's public properties.

**Main symbols:** `class ManagerRuntime` (18). Constructor (27) takes the
owning worker and owns three dicts: `statuses` (35), `tasks` (36), `errors`
(37).
- `iter_manager_definitions()` (49) — walks `type(self.worker).__mro__`
  **most-derived-first** (70), reading each class's own
  `__manager_registry__` list (populated by `Manager.__set_name__` and
  `register_manager`) and de-duplicating names via a `seen` set so a
  subclass override shadows the base definition. When two managers
  independently pick the same `name=`, a WARNING is logged naming the
  colliding manager and the class it was found on (AR-068); the first
  (most-derived) definition still wins, so the collision is surfaced rather
  than silently dropped. A class that redefines a base manager's attribute
  name without re-decorating it also logs an advisory WARNING (AR-086
  failure mode 2), because the plain attribute produces no registry entry and
  discovery falls through to the base manager.
- `run_manager(name, manager)` (138) — runs `manager.method(self.worker)` in a
  `while True` loop (111); on a non-`CancelledError` exception records the error
  (120) and retries after `manager_restart_backoff` (139), giving up when
  `consecutive_failures > manager_max_retries` — i.e. on the
  (max_retries+1)-th consecutive failure (122–130). A successful iteration
  resets the `consecutive_failures` counter to 0 (141–142), so the budget
  counts consecutive failures only. `CancelledError` stops cleanly (117–118).
  The retry happens **inside the same task** — the manager never cancels
  itself. `finally` (145–161) runs `manager.cleanup`, marks STOPPED, and
  removes the task from tracking. A manager that gave up (exhausted the retry
  budget) is instead ended in the terminal `FAILED` state (AR-063) so the
  death is observable rather than silent.
- `failed_managers` (property, 40) — names whose `statuses[name]` is
  `ManagerStatus.FAILED` (the recorded exception for each is in `errors`).
- `start_manager` (220), `stop_manager` (241), `start_managers` (268),
  `stop_managers` (278).

**Public interface:** methods above; constructor takes `worker`.

**Dependencies:** `.manager` (`Manager`, `ManagerStatus`); stdlib.
**Depended on by:** `BasicWorker` (constructs and forwards to it).

## 3. Logging lifecycle — `LoggingLifecycle`

**File:** `src/scietex/service/log_handlers/lifecycle.py`

**Purpose:** Extracted from `BasicWorker` (AR-003). Owns async
logging-handler registration and start/stop with status bookkeeping.

**Main symbols:** `class LoggingLifecycle` (18). Constructor (27) takes the
owning worker and owns the `statuses` dict (35).
- `register_logger_handler(handler)` (37) — sets the handler level and
  attaches it to the worker logger; the handler is registered once and reused
  across start/stop cycles. The unused `name` parameter was removed (AR-070);
  statuses are keyed by `handler.name` or `handler.__class__.__name__`
  (lifecycle.py:64).
- `start_handlers()` (51) — starts each `AsyncLoggingHandler` whose recorded
  status is not RUNNING, with `logger_handler_timeout`; sets status RUNNING on
  success, FAILED on timeout/exception so it is retried on the next start
  (AR-020).
- `shut_down_handlers()` (93) — stops each handler (idempotent
  `stop_logging()`), sets status STOPPED.

**Dependencies:** `.log_handlers` (`LoggerStatus`), external
`scietex.logging.AsyncLoggingHandler`.
**Depended on by:** `BasicWorker` (constructs and forwards to it).

## 4. Manager decorator — `Manager` / `ManagerStatus`

**File:** `src/scietex/service/manager/__init__.py`

**Purpose:** A class-based decorator turning an async method into a "managed
loop". The worker (via `ManagerRuntime`) reads the managers recorded in each
class's `__manager_registry__` across the MRO, runs their `method` in an
infinite loop under an `asyncio.Task`, restarts
on error, and invokes an optional `cleanup` callable on stop.

**Main symbols:** `ManagerStatus` (16), `Manager` (26). Attributes: `name`,
`cleanup`, `method`. `Manager.__call__` (91) returns `self` (decorator
identity); `Manager.__get__` (108) binds the wrapped method to the instance
(descriptor protocol). `ManagerStatus` values: `STARTING`, `RUNNING`,
`STOPPING`, `STOPPED`, and terminal `FAILED` (AR-063) — set when a manager
exhausts its retry budget instead of stopping cleanly.

**Public interface:** `@Manager(name=..., cleanup=...)`.

**Dependencies:** stdlib only. **Depended on by:** `BasicWorker`,
`TaskProcessor` (decorated managers), examples (`@Manager("cruncher")`).

## 5. Logging helpers — module `log_handlers/__init__.py` (in-package)

**File:** `src/scietex/service/log_handlers/__init__.py`

**Purpose:** `LoggerStatus` (track async logging handler state: STOPPED /
RUNNING / FAILED) and `parse_logging_level()` (accepts short/long strings or
ints, e.g. `"D"`, `"DBG"`, `"DEBUG"` → `logging.DEBUG`).

**Public interface:** `LoggerStatus` (14), `parse_logging_level` (28),
`DEFAULT_LOGGING_LEVEL` (11). **Dependencies:** stdlib. **Depended on by:**
`BasicWorker`, `LoggingLifecycle`.

## 6. Task schemas

**File:** `src/scietex/service/task_handler/schemas.py`

**Purpose:** Immutable typed contracts shared by handlers and processor, using
`msgspec` (also gives msgpack/YAML serialization).

| Schema | Fields |
|---|---|
| `TaskTimeout` (23) | `timeout: float\|None`, `timeout_action: "requeue"\|"discard"` |
| `TaskData` (37) | `task: str`, `timeout: TaskTimeout`, `canceled_action`, `payload: bytes` |
| `TaskEnvelope` (57) | `version: int`, `data: bytes` — the versioned transport envelope wrapping a serialized `TaskData` (AR-064) |
| `TaskResult` (75) | `status: "success"\|"error"`, `error: str`, `processed_at: datetime`, `payload: bytes`, `error_code: str`, `retryable: bool`, `partial: bool` |
| `TaskStatus` (117) | `task_id: str`, `service: str`, `task: str`, `status: "queued"\|"running"\|"completed"\|"failed"\|"cancelled"`, `progress: TaskProgress`, `result: bytes\|None`, `data: TaskData\|None`, `error: str`, `error_code: str`, `created_at: datetime`, `updated_at: datetime` |

`TaskResult.processed_at` uses `msgspec.field(default_factory=lambda:
datetime.now(timezone.utc))` (99) so each instance gets its own timestamp
(AR-012). The error-taxonomy fields (`error_code`/`retryable`/`partial`,
added AR-022) are optional and default to "no extra information", so
handlers that only set `status`/`error` keep working unchanged.

`schemas.py` also defines `CANCEL_TASK_TYPE = "cancel_task"` (15) and
`CancelReason` (20). The built-in handler for that task type lives in
`task_handler/cancel.py`: `CancelTaskHandler` (59), `CancelTaskRequest` (35),
`CancelTaskResponse` (47), `CancelOutcome` (29), and `CancelCallback` (32).
`TaskProcessor` auto-registers the handler in `__init__` and injects its own
`_cancel_task` callback.

`TaskEnvelope` is the durable wire format (AR-064): the transport persists a
versioned envelope, not a bare `TaskData`, so the handler contract and the
on-the-wire format evolve independently. Encoding/decoding lives in
`task_handler/wire.py` (`encode_task_envelope`/`decode_task_envelope`).

`TaskTracker` is not a wire schema: it is the in-memory runtime handle
(`worker_task`/`data`/`started`) that tracks a running task and now lives in
`task_handler/runtime.py`.

**Public interface:** constructors only (frozen). **Dependencies:** `msgspec`.
**Depended on by:** `task_handler.basic`, `task_handler.runtime`,
`task_handler.wire`, `task_processor`,
`valkey` (msgpack round-trip of `TaskData` via the envelope), examples, tests.

## 7. Task handler contract — `TaskHandler` / `TaskHandlerContext`

**File:** `src/scietex/service/task_handler/basic.py`,
`src/scietex/service/task_handler/context.py`,
`src/scietex/service/task_handler/capabilities.py`

**Purpose:** ABC for pluggable task handlers with lifecycle and dispatch
contract; a narrow context decouples handlers from the worker.

**Main symbols / interface:**
- `TaskHandlerContext` (context.py:7) — frozen dataclass with `service_name`,
  `instance_id`, `logger`; replaces the full worker reference.
- `TaskCapabilities` (capabilities.py:8) — frozen dataclass holding the task id
  and a progress writer; `report_progress(value)` clamps to `[0.0, 100.0]` and
  forwards to the transport hook. Passed per call to `handle`.
- `__init__(name, context)` (22) — stores `name`, `context`, `logger =
  context.logger`, `_is_initialized=False` (no `self.worker`)
- abstract `supported_tasks -> list[str]` (37), abstract
  `handle(task_data, *, capabilities) -> TaskResult` (46)
- `supports(task_type) -> bool` (64) — membership in `supported_tasks`
- `initialize() -> bool` (76, default True), `cleanup()` (87)
- `start()` (95) sets `_is_initialized = await initialize()`; `stop()` (109)
  runs `cleanup()`, resets flag; `is_ready` (120)

**Dependencies:** `.context`, `.capabilities`, `.schemas`. **Depended on by:**
`TaskProcessor` (registry + dispatch), examples, tests.

## 8. Task processor — `TaskProcessor`

**File:** `src/scietex/service/task_processor.py`

**Purpose:** Adds concurrent in-process task execution on top of the worker:
external tasks are enqueued (override `fetch_tasks`), a `TaskManager` dequeues
and dispatches to handlers, a `Watchdog` cancels timed-out tasks, and shutdown
drains/cancels in-flight work. Per-task lifecycle state (the running tracker
and its cancel reason) is owned by a composed `TaskLifecycle` (AR-088).

**Main symbols:** `class TaskProcessor(BasicWorker)` (46).
Overrides `_config_type` (73) to `TaskProcessorConfig`, so the base
instantiates the concrete config when `config=None` and `__init__` reads its
fields from `self._config` rather than re-storing (AR-069).
Properties: `task_handlers` 147, `running_tasks` 161 (a snapshot `Mapping`
delegated to `TaskLifecycle`), `queue_size` 171, `max_concurrent_tasks` 176.
Registry/dispatch: `add_task_handler` 267 (takes the handler class plus an
optional keyword-only `name` and arbitrary `**handler_kwargs`; the lifecycle
key is the resolved name — `name` if given, otherwise `handler_class.__name__`
— so multiple instances of one class can coexist under distinct keys, a
duplicate resolved key raises; the map stores a `(class, handler_kwargs)`
tuple and the kwargs are forwarded to the handler constructor on every
instantiation), `_start_task_handler` 317
(unpacks the tuple, builds a `TaskHandlerContext` at 336–340, and calls
`handler_class(handler_name, context, **handler_kwargs)` at 341),
`_stop_task_handler` 361, `remove_task_handler` 387, `_find_task_handler` 403,
`process_task` 651.
Queue access: `enqueue_task` 180, `dequeue_task` 201, `task_queue_empty` 193,
`task_queue_full` 197 (the raw `task_queue` attribute is no longer exposed;
non-blocking `put_nowait`/`get_nowait` underneath). State:
`__task_handlers_map`/`__task_handlers` (103–104; the map holds
`(class, handler_kwargs)` tuples keyed by resolved name), `_task_lifecycle`
(93, the composed per-task lifecycle state), `__task_queue` (139, bounded
`asyncio.Queue[(UUID, TaskData)]`).
Managers: `@Manager("TaskManager") task_manager` 705 (inner `handle_task`
wrapper at 717), `@Manager("TaskQueueManager") task_queue_manager` 819.
Hooks: `fetch_tasks` 800, `return_task_to_queue` 422, `on_task_completed` 523
(transport ack seam), `initialize` 567 (starts handlers), `cleanup` 603
(drains queue, cancels running tasks, stops handlers), `watchdog` 838.

**Config constants:** timing/retry MIN/MAX/DEFAULT bounds live in `config.py`
(single source of truth); the task-queue defaults are
`DEFAULT_MAX_TASKS_QUEUE_SIZE=100` and `DEFAULT_MAX_CONCURRENT_TASKS=10`
(AR-055). The task-level timing knobs moved there too (AR-062):
`TaskProcessorConfig.task_timeout` (default 3, bounds `[0.1, 3600]`; `<= 0`
means "no timeout"), `task_queue_fetch_timeout` (default 1, `[0.01, 60]`), and
`task_cancellation_timeout` (default 5, `[0.1, 60]`). Each resolves once in
`TaskProcessor.__init__` into a private attribute read by the watchdog/
task_manager hot loops; no processor-local timing constants remain.

`TaskProcessorConfig.auto_tune` (bool, default `False`) makes the worker derive
`max_concurrent_tasks` from `os.cpu_count()` at startup when
`max_concurrent_tasks` is left unset (`None`); an explicit
`max_concurrent_tasks` always wins (the resolution lives in
`TaskProcessor.__init__`). The CPU count is a poor proxy for an I/O-bound
asyncio workload and does not reflect container CPU limits, so I/O-bound
services should set `max_concurrent_tasks` explicitly.

**Public interface:** constructor takes a single immutable
`TaskProcessorConfig` (`config.py`, extends `WorkerConfig`) or `None`; no
runtime setters. Properties (`task_handlers` — a read-only `MappingProxyType`
view; `running_tasks` — a snapshot `Mapping` delegated to `TaskLifecycle`, so
callers may iterate it while tasks are added or removed; `queue_size`,
`max_concurrent_tasks`, `task_manager_sleep_time`,
`task_queue_manager_sleep_time`, `task_handler_start_timeout`,
`task_handler_stop_timeout` — all read-only), and queue methods
`enqueue_task`/`dequeue_task`/`task_queue_empty`/`task_queue_full`.

**Dependencies:** `.basic_worker`, `.manager`, `.task_handler`,
`.task_lifecycle` (`TaskLifecycle`), `.transport`
(`TaskTransport`/`TaskSink`/`InMemoryTransport`).
**Depended on by:** `ValkeyWorker`, examples, tests.

## 9. Transport seam — `transport.py`

**File:** `src/scietex/service/transport.py`

**Purpose:** The explicit task-delivery contract (AR-072). Replaces the former
implicit set of ordering-sensitive template-method hooks with two Protocols and
a working in-process default, so `TaskProcessor` depends on an interface rather
than on subclass overrides.

**Main symbols:** `TaskSink` Protocol (19) — the enqueue surface a transport
delivers into: `task_queue_full() -> bool` and
`enqueue_task(task_id, task_data) -> bool` (a `TaskProcessor` satisfies it
structurally, no adapter). `TaskTransport` Protocol (32) — all async:
`fetch(sink) -> bool`, `requeue(task_id, task_data)`, `release(task_id)`,
`on_started(task_id, task_data)`,
`ack(task_id, task_data, task_result, *, cancel_reason=None)`,
`on_progress(task_id, value)`, `on_drain(task_id, task_data)`.
`InMemoryTransport` (62) — the default, deque-backed implementation; public
`submit(task_id, task_data)` feeds it (not part of the Protocol), `fetch` drains
while the sink is not full, and `on_drain` requeues iff
`canceled_action == "requeue"`.

**Public interface:** the two Protocols (structural typing — no inheritance
required) and `InMemoryTransport(*, logger)`.

**Dependencies:** `.task_handler.schemas` only — no `glide`, no `valkey`.
**Depended on by:** `TaskProcessor` (composes one via keyword-only `transport=`,
default `InMemoryTransport`), `valkey/transport.py` (implements the Protocol),
package `__init__.py`.

## 10. Valkey worker — `ValkeyWorker`

**File:** `src/scietex/service/valkey/worker.py`

**Purpose:** Makes `TaskProcessor` consume from / write to a Valkey stream
via the `glide` `GlideClient`; publishes heartbeats; pushes logs to a Valkey
stream through an `AsyncValkeyHandler`.

**Main symbols:** `class ValkeyWorker(TaskProcessor)` (52).
Overrides `_config_type` (85) to `ValkeyWorkerConfig`, so the base instantiates
the concrete config when `config=None` and `__init__` reads its fields from
`self._config` rather than re-storing (AR-069).
Constructor — `__init__(config: ValkeyWorkerConfig | None = None, *,
client_factory: ClientFactory | None = None)` (accepts `config.valkey_config`;
when `None`, defers the disk read to `_ensure_client_config()`, called at first
connect — AR-066, so construction is side-effect-free; `client_factory` is the
AR-074 injection seam, defaulting to `GlideClient.create`),
`connect` (`_client_factory` + PING under `_client_lock`; `_client`
assigned only after PING succeeds; then ensures the logging handler and
starts it), `disconnect`, `heartbeat` (writes msgpack `Heartbeat` to
`...:status` with TTL 2×interval), `initialize` (start handlers, connect,
`xgroup_create`), `cleanup` (super + stop logging handler + disconnect),
`_register_instance` (`SADD` `instance_id` into the registry set),
`_unregister_instance` (`SREM` it back out).

Delivery is delegated to the injected `ValkeyTransport` (AR-072): the worker
composes `self._valkey_transport` and assigns it to `self._transport`, so the
six former hook overrides (`fetch_tasks`, `return_task_to_queue`,
`on_task_started`, `on_task_completed`, `_write_task_progress`,
`_on_queue_drain_task_processing`) are gone — the base `TaskProcessor` hooks
remain as thin delegators to the composed transport. The worker also composes
the `TransportHealth` (AR-075), `TaskLeaseManager` (AR-073), and
`TaskStatusStore` (AR-073) collaborators and injects them into the transport.

Connection ownership (AR-059/061): the worker runs one operational
`GlideClient` for heartbeat, registry, intake, and task completion;
`connect()`/`disconnect()` serialize the create→ping→assign and close→null
sequences behind `_client_lock`, and intake reconnects only on glide
errors. The logging handler is an independent owner: `_ensure_logging_handler`
constructs `AsyncValkeyHandler` with `valkey_config=` (a scalar dict
translated from the typed config by `logging_handler_config`) from the typed
`ValkeyConfig`, so the handler builds/closes/reconnects its own
connection and the worker never touches `handler.client` (AR-076/AR-085
removed the raw-`GlideClientConfiguration` fallback).

**Key names** (constructed in `__init__`): status key
`scietex:{service}:{instance_id}:status`, task stream
`scietex:{service}:tasks`, group
`scietex:{service}:task_group`, consumer
`scietex:{service}:{instance_id}`, registry set
`scietex:{service}:workers`. The stream and group are service-scoped so
replicas share one queue; the consumer/status keys are worker-scoped per
auto-generated `instance_id`. The entry-id map and `recovered` flag now live
on `ValkeyTransport` (see §15).

The registry set is the enumeration index: `_register_instance` `SADD`s
the `instance_id` on startup and `_unregister_instance` `SREM`s it on
shutdown — both best-effort (a failure logs a WARNING, reports into
`TransportHealth`, and continues). Liveness
is the status-key TTL refreshed by `heartbeat()`, so a stale member left by a
crashed replica is tolerated (the operator probes each member's status key).

**Public interface:** constructor takes a single immutable `ValkeyWorkerConfig`
(`valkey/config.py`, extends `TaskProcessorConfig`) or `None`, plus the
keyword-only `client_factory`; properties `valkey_config` (`ValkeyConfig |
None`), `client`, `transport_health` (`TransportHealth`).

**Dependencies:** `..task_processor`, `..task_handler.TaskData`,
`..task_handler.wire` (`encode_task_envelope`/`decode_task_envelope`, AR-064),
`.schemas.Heartbeat`, `.config` (`ValkeyWorkerConfig`), `.transport`
(`ValkeyTransport`), `.health` (`TransportHealth`), `.lease`
(`TaskLeaseManager`), `.tracking` (`TaskStatusStore`), external
`scietex.logging.AsyncValkeyHandler`, `._glide` (guarded glide names, AR-048),
`msgspec`.
**Depended on by:** `valkey/__init__.py`, package `__init__.py` (guarded),
example `examples/valkey_async_service.py`.

## 11. Valkey configuration — `valkey/config.py`

**File:** `src/scietex/service/valkey/config.py`

**Purpose:** Typed config that mirrors glide options, plus YAML persistence and
schema→glide translation. Also hosts `ValkeyWorkerConfig` (the worker-level
config struct) so its optional `glide`-typed field stays out of the
always-imported core `config.py`.

**Main symbols:** frozen structs `ValkeyNode` (32), `ValkeyUserCredentials`
(44), `ValkeyBackoffStrategy` (56), `ValkeyTlsAdvancedConfiguration` (87),
`ValkeyAdvancedConfig` (115), `ValkeyBaseConfig` (163), `ValkeyPubSubConfig`
(145, `listening` + runtime-only `parse_control_message`), `ValkeyConfig`
(241, `base_config` + `advanced_config` + `pubsub_config`);
`ValkeyWorkerConfig` (265, extends `TaskProcessorConfig` with `valkey_config`
(`ValkeyConfig | None`), `log_stream_name`, `task_fetch_batch_size`,
`claim_min_idle_ms`, `task_tracking_ttl`, `task_lease_ttl`); `read_valkey_config(conf_dir)`
— creates `valkey.yml` with defaults only if the file is missing; raises
`RuntimeError` on a present-but-invalid file, never overwriting it;
`generate_glide_config(valkey_config, service_name, worker_id)` (converts to
`GlideClientConfiguration`, validates `read_from`/`protocol`, and builds PubSub
subscriptions from `valkey_config.pubsub_config` when `listening` is set).

**Public interface:** struct constructors; config conversion properties
(`addresses`, `credentials`, `reconnect_strategy`, `to_advanced_config`, ...).

**Dependencies:** `msgspec`; `._glide` (glide names via the single guarded
import, AR-048); `..config` (`TaskProcessorConfig`); `.._validation`
(`validate_range`, AR-079). **Depended on by:** `ValkeyWorker`,
`valkey/transport.py`, `valkey/__init__.py`, tests.

## 12. Valkey transport — `valkey/transport.py`

**File:** `src/scietex/service/valkey/transport.py`

**Purpose:** The Valkey implementation of the core `TaskTransport` Protocol
(AR-072). Owns the stream operations that were formerly `ValkeyWorker` hook
overrides, so the worker keeps only lifecycle concerns.

**Main symbols:** `class ValkeyTransport` — receives all collaborators by
injection (`config`, `service_name`, `consumer_name`, `stream_name`,
`group_name`, `client_provider`, `health`, `lease`, `status`, `entry_ids`,
`logger`). Methods: `fetch(sink)` (`xreadgroup` → `decode_task_envelope` →
`enqueue_task`; does **not** ack on enqueue; a glide error reports into
`TransportHealth` and triggers `recover()`), `recover_pending_tasks(sink)`
(`XAUTOCLAIM` pending entries on first fetch; decodes via
`decode_task_envelope`, skipping unknown-version/invalid entries with an ERROR
log), `requeue(task_id, task_data)` (`xadd` re-queue via `encode_task_envelope`,
then deletes the lease — AR-077b), `release(task_id)` (lease delete only),
`on_started`, `ack(task_id, task_data, task_result, *, cancel_reason=None)`
(`xack`+`xdel` the entry after the handler finishes; skips the lease delete for
a retryable error result — AR-077b), `on_progress`, `on_drain` (durable drain:
deletes the lease without re-enqueueing), and `refresh_leases()` (rewrites
leases for every task in the entry-id map; called by the worker watchdog).

**State owned:** the entry-id map (`task UUID → stream entry id`, for deferred
acknowledgement) and the `recovered` flag (one-time pending recovery).

**Dependencies:** `.config`, `.health`, `.lease`, `.tracking`, `._glide`,
`..task_handler.wire`. **Depended on by:** `ValkeyWorker` (injected as
`self._transport`).

## 13. Transport health — `health.py` (core)

**File:** `src/scietex/service/health.py` (re-exported from
`src/scietex/service/valkey/health.py` for back-compat)

**Purpose:** Connection-health supervisor (AR-075). Aggregates every transport
failure across the worker and its collaborators, owns the single reconnect
path, and surfaces one CRITICAL per sustained outage.

**Main symbols:** `DEFAULT_TRANSPORT_DOWN_THRESHOLD_SECONDS = 30.0`;
`class TransportHealth(*, reconnect, is_connected, logger,
transport_name="Transport", down_threshold=30.0, reconnect_cooldown=1.0,
clock=time.monotonic)`.
Properties `connected`, `degraded`, `last_error`, `failure_count`,
`down_duration`. Methods: `mark_connected()`, `mark_disconnected()`,
`report_failure(exc)` (sync, non-blocking — records state and requests a
reconnect), `async recover()` (the single reconnect owner: `asyncio.Lock` dedup
+ cooldown + supervised retry), `critical_report() -> str | None` (one message
per down episode past the threshold; the message names `transport_name`, so
`ValkeyWorker` passes `"Valkey"` and `MqttWorker` passes `"MQTT"`).

**Dependencies:** `asyncio`, `logging`, `time`, `collections.abc` only — no
transport imports. **Depended on by:** `ValkeyWorker` and `MqttWorker` (each
exposed via `transport_health`), `ValkeyTransport`, `TaskLeaseManager`,
`TaskStatusStore`, `MqttTransport`.

**Layering (AR-089):** `TransportHealth` is deliberately transport-agnostic —
it takes only injected callables and knows nothing about Valkey or MQTT. It
was hoisted from `valkey/health.py` to core when the MQTT transport was added,
so a new transport can reuse it without a feature→feature dependency;
`valkey/health.py` remains as a back-compat re-export.

## 14. Valkey task lease — `valkey/lease.py`

**File:** `src/scietex/service/valkey/lease.py`

**Purpose:** Per-entry lease store (AR-073 extraction). Guards a task against
concurrent processing by a peer replica.

**Main symbols:** constants `LEASE_TTL_HEARTBEAT_MULTIPLIER = 2`,
`LEASE_TTL_WATCHDOG_MULTIPLIER = 3`, `MIN_TASK_LEASE_TTL_SECONDS = 1`;
`derive_task_lease_ttl(heartbeat_interval, watchdog_interval) -> int`
(`max(1, int(max(2*heartbeat_interval, 3*watchdog_interval)))`);
`class TaskLeaseManager(*, service_name, consumer_name, lease_ttl,
client_provider, logger, report_failure=None)` with `key(task_id)`
(`scietex:{service}:lease:{id}`), `write(task_id)` (SET with consumer name +
TTL), `acquire(task_id)` (SET NX; `True` on error, fail-safe), `delete(task_id)`
(DEL), `refresh(task_ids)` (write per id over a snapshot).

**Dependencies:** `._glide` (`ClientProvider`, glide error classes).
**Depended on by:** `ValkeyWorker`, `ValkeyTransport`.

## 15. Valkey task status — `valkey/tracking.py`

**File:** `src/scietex/service/valkey/tracking.py`

**Purpose:** Per-task status store (AR-073 extraction). Records the running and
terminal status of each task for external observers.

**Main symbols:** `class TaskStatusStore(*, service_name, tracking_ttl,
client_provider, logger, report_failure=None)` with `key(task_id)`
(`scietex:{service}:task:{id}`), `record_running(task_id, task_data)`,
`record_terminal(task_id, task_data, task_result, cancel_reason=None)`, and
`update_progress(task_id, value)` (read-modify-write; preserves other fields,
drops the update when the record is absent (DEBUG log), silent on
`DecodeError`).

**Dependencies:** `._glide`, `..task_handler` (schemas). **Depended on by:**
`ValkeyWorker`, `ValkeyTransport`.

## 16. Valkey heartbeat schema

**File:** `src/scietex/service/valkey/schemas.py`
**Purpose/content:** `Heartbeat` (16) (frozen Struct) with `service`,
`instance_id`, `status`, `heartbeat_interval`, `start_time`, `timestamp` —
`timestamp` uses `msgspec.field(default_factory=...)` (39) for a per-instance
value. msgpack-serialized by `ValkeyWorker.heartbeat`.

## 17. Valkey stream purge utility — `purge.py`

**File:** `src/scietex/service/valkey/purge.py`

**Purpose:** Standalone operational utility to purge a Valkey task stream —
reads, acknowledges, and deletes every entry so an operator can clear a stream
without running a worker. Independent of `ValkeyWorker` (AR-043: moved off the
worker class, which previously carried it as dead code).

**Main symbols:** `PurgeResult` (24, frozen dataclass: `entries_purged` count and
`errors` tuple), `purge_task_stream(client, stream_name, group_name,
consumer_name, logger=None)` (37) — orchestrates the purge and returns a
`PurgeResult`; private helpers `_purge_group_entries` (80, `XREADGROUP` + `XACK`
+ `XDEL` loop, returns `int` count), `_purge_stream_entries`
(107, `XREAD` + `XDEL` loop, returns `int` count), `_stream_entry_ids` (126).

**Dependencies:** none at runtime (`GlideClient` imported only under
`TYPE_CHECKING`); the caller supplies an open client. **Depended on by:**
`valkey/__init__.py`.

## 18. Utilities

- **`utils/config.py`** — `prepare_conf_dir()` (32): returns first existing dir
  in order `conf_dir` arg → `SCIETEX_CONFIG_DIR` env → `$XDG_CONFIG_HOME/scietex`
  → `~/.config/scietex` → `/etc/scietex` → `/usr/local/etc/scietex` →
  `./config` (CWD); creates `~/.config/scietex` if none exist.
- **`utils/logo.py`** — `print_scietex_logo(service_name, version)` (34) prints
  ASCII banner using `..version.__version__`.

## 19. External async logging backend — `scietex.logging`

Installed dependency (>=2.0.0). The package embeds this framework's log sink.
Consumed classes:
- `AsyncLoggingHandler(logging.Handler)` — pure machinery base class with
  per-backend `asyncio.Queue`s + worker coroutines;
  `start_logging()`/`stop_logging()`/`emit()`. Both concrete handlers subclass
  it.
- `ConsoleHandler(AsyncLoggingHandler)` — console sink. Constructed with no
  arguments; identity comes from the stdlib logger name it is registered on
  (e.g. `f"{service_name}:{instance_id}"`).
- `AsyncBrokerHandler` — adds a broker queue + `_worker` that connects,
  formats records into dicts, `send_message()`; accepts an injected `client`
  and, when one is provided, never closes it (`_owns_client=False`).
- `AsyncValkeyHandler(AsyncBrokerHandler)` — `xadd` to a stream. `ValkeyWorker`
  constructs it with `valkey_config=` (a dict of scalar
  `GlideClientConfiguration` options translated from the typed `ValkeyConfig`)
  on the first successful `connect()`, so the handler owns
  an independent connection and reconnects autonomously.
- `AsyncMqttHandler(AsyncBrokerHandler)` — publishes log records to an MQTT
  topic. `MqttWorker` constructs it with `mqtt_config=` (a dict of scalar
  `aiomqtt` options translated from the typed `MqttConfig` by
  `mqtt/logging.py:logging_handler_config`) on the first successful
  `connect()`, so the handler owns an independent connection and reconnects
  autonomously. Its `mqtt_config` dict uses the MQTT 3.1.1-style
  `clean_session` field, so the MQTT 5 session fields are omitted from the
  translation.
- `ScietexFormatter`.

**Important:** a handler built from `valkey_config=` owns and closes its own
client (autonomous reconnect/backoff); a handler built from the `client=` kwarg
never closes it — the caller owns its lifetime and recovery. `ValkeyWorker` uses
the former (the raw-`GlideClientConfiguration` fallback that used the latter was
removed in AR-076/AR-085).

## 20. MQTT transport — `mqtt/transport.py`

**File:** `src/scietex/service/mqtt/transport.py`

**Purpose:** The MQTT implementation of the core `TaskTransport` Protocol,
added in v4.4.0 (AR-089's second transport). Owns inbox draining, requeue,
terminal acknowledgement, and drain handling, so `MqttWorker` keeps only
lifecycle concerns — the same split as `ValkeyTransport`.

**Main symbols:** `MqttPublish` (a `Callable[[str, bytes, int],
Awaitable[None]]` publish seam injected by the worker, since the worker owns
the client) and `class MqttTransport`, which receives every collaborator by
injection (`config`, `service_name`, `topic`, `inbox`, `health`, `publish`,
`logger`). Methods: `fetch(sink)` (first call replays non-terminal inbox
entries via `recover_pending_tasks`, then drains the inbox's non-terminal
snapshot into `sink.enqueue_task`, stopping on backpressure and skipping
already-enqueued ids), `recover_pending_tasks(sink)` (returns a
`(recovery_complete, enqueued)` tuple), `requeue(task_id, task_data)`
(re-publishes the envelope at `task_qos` under the same id), `release(task_id)`
(drops the in-process marker only), `on_started(task_id, task_data)` (marks the
inbox entry in-flight), `ack(task_id, task_data, task_result, *,
cancel_reason=None)` (tombstones the entry; a retryable error result skips the
tombstone so the requeued copy is accepted — AR-077b mirror), `on_progress`
(no-op), `on_drain` (drops the marker, leaving the entry pending for recovery),
and `refresh_leases()` (no-op parity with `ValkeyTransport`).

**State owned:** the `recovered` flag (one-time pending recovery) and the
`_enqueued` set (task ids handed to the sink but not yet terminal, so the
inbox snapshot is not re-enqueued on every poll).

**Composition:** `MqttWorker` builds `TransportHealth` → `FileMqttInbox` →
`MqttTransport`, then assigns the transport to `TaskProcessor._transport`.
`MqttWorkerConfig` (in `mqtt/config.py`) extends `TaskProcessorConfig` with
`mqtt_config`, `task_topic`, `task_qos`, `inbox_backend`, `inbox_path`,
`inbox_ttl`, `log_topic`, `log_qos`, `log_retain`, `status_publish_enabled`,
`status_topic_prefix`, `status_qos`, `status_ttl`, `progress_qos`,
`progress_min_interval`, and `progress_min_delta`.

**Dependencies:** `.config`, `.inbox`, `..health`, `..task_handler.schemas`,
`..task_handler.wire`, `..transport`. **Depended on by:** `MqttWorker`
(injected as `self._transport`), `mqtt/__init__.py`.

## 21. MQTT durable inbox — `mqtt/inbox.py`

**File:** `src/scietex/service/mqtt/inbox.py`

**Purpose:** At-least-once delivery for MQTT. aiomqtt v2.5.1 auto-acks at the
broker before the handler runs, so wire QoS cannot provide at-least-once.
Persisting every received message before it is handed to the processor, and
deduping on replay, restores it. The backend is transitional — aiomqtt v3's
manual ack removes the need — so the `MqttInbox` Protocol keeps that migration
to an implementation swap.

**Main symbols:** `MqttInbox` (Protocol: `put`/`mark_in_flight`/
`mark_terminal`/`pending`/`recover`), `FileMqttInbox(path, *, logger,
ttl=None)`, and `MemoryInbox()`. `FileMqttInbox` stores one JSON entry per task
(`{task_id}.json`, carrying task id, lifecycle state, creation epoch, and
base64 envelope) and a `{task_id}.done` tombstone on terminal completion;
tombstones and expired entries are pruned on load. All file I/O runs via
`asyncio.to_thread` under an `asyncio.Lock`. `MemoryInbox` buffers entries in a
dict for the at-most-once opt-out: `recover` returns `[]` (nothing survives a
restart) and there is no tombstone.

**Dependencies:** `..task_handler.schemas`, `..task_handler.wire`; stdlib.
**Depended on by:** `MqttWorker` (builds it), `MqttTransport` (drains it).

## 22. MQTT logging-handler config — `mqtt/logging.py`

**File:** `src/scietex/service/mqtt/logging.py`

**Purpose:** Translates a typed `MqttConfig` into the plain scalar dict the
external `AsyncMqttHandler` expects via `mqtt_config=` — the direct analogue
of the Valkey translator.

**Main symbols:** `logging_handler_config(mqtt_config) -> dict`, carrying
`host`/`port`/`username`/`password`/`identifier`/`keepalive`/`transport`/
`timeout`/`tls_insecure`. The MQTT 5 session fields (`clean_start`,
`session_expiry_interval`) are deliberately omitted because the handler's dict
schema uses the 3.1.1-style `clean_session` field and the logging connection
uses its own session defaults.

**Dependencies:** `.config` (`MqttConfig`). **Depended on by:** `MqttWorker`
(`_ensure_logging_handler`), `mqtt/__init__.py`.
