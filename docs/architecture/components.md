# Components

For each major component: purpose, main classes/functions, public interface,
dependencies, dependents. Line numbers refer to the module given.

## 1. Worker core — `BasicWorker`

**File:** `src/scietex/service/basic_worker.py`

**Purpose:** Foundation for daemon workers: identity (`service_name`,
`instance_id`, `version`), lifecycle state machine, signal-driven graceful
shutdown, async logging handler management, and subclass hooks for
heartbeat/watchdog/initialize/cleanup. Manager discovery/runtime and
logging-handler lifecycle are delegated to `ManagerRuntime` and
`LoggingLifecycle` (constructed in `__init__`); the worker keeps only identity,
config, and the state machine.

**Main symbols:**
- `ServiceStatus` (STOPPED/STARTING/RUNNING/STOPPING) — line 40
- `class BasicWorker` — line 56
- Constructor — `__init__(config: WorkerConfig | None = None)`; stores the
  immutable `WorkerConfig` (from `config.py`), resolves identity/conf_dir/
  logging_level, and constructs `ManagerRuntime` + `LoggingLifecycle`.
  Timing/retry fields are validated at construction — an out-of-range value
  raises `msgspec.ValidationError`, and `None` resolves to the matching
  `DEFAULT_*` constant in `config.py` at read time (no runtime clamping)
- Config type mechanism (AR-069): class attribute `_config_type: ClassVar
  [type[WorkerConfig]]` (84) tells the base which concrete config struct to
  instantiate when `config=None`. Subclasses override it to their own config
  type (e.g. `TaskProcessor`→`TaskProcessorConfig`, `ValkeyWorker`→
  `ValkeyWorkerConfig`) so the base stores the concrete type and subclass
  constructors no longer re-store / double-instantiate
- Signals: `_setup_signal_handlers` 350 (Windows-safe no-op),
  `_remove_signal_handlers` 380
- Lifecycle: `_startup` 404, `start` 459, `_shutdown` 501, `stop` 548, `exit` 591
- Cancellation terminal-state helper: `_force_stopped` 487 (AR-017 — forces
  STOPPED + `exit` event on startup/shutdown cancellation)
- Hooks: `initialize` 394, `heartbeat` 623, `watchdog` 635, `cleanup` 655,
  `_register_instance` 664, `_unregister_instance` 674
- Built-in managers: `@Manager(name="Heartbeat") _heartbeat_manager` 601,
  `@Manager(name="Watchdog") _watchdog_manager` 612
- `_setup_signal_handlers` called from `start()` (484), not `__init__`;
  `_remove_signal_handlers` called from `stop()` (569)

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
subclass hooks govern registry-set membership: `_register_instance` (664) —
called by `_startup()` after `initialize()` succeeds and before managers
start — and `_unregister_instance` (674) — called by `_shutdown()` after
managers stop and before `cleanup()` teardown. Both are no-ops in the base;
`ValkeyWorker` overrides them (worker.py:481, 504) to `SADD`/
`SREM` its `instance_id` into the worker registry set.

**Dependencies:** `.manager.runtime` (`ManagerRuntime`), `.log_handlers.lifecycle`
(`LoggingLifecycle`), `.manager` (`Manager`), `.log_handlers`
(`parse_logging_level`), `.utils` (`prepare_conf_dir`, `print_scietex_logo`);
external `scietex.logging.ConsoleHandler`.

**Depended on by:** `TaskProcessor` (extends); `ManagerRuntime` and
`LoggingLifecycle` (back-reference to the owning worker); `task_handler`
(indirectly, via `TaskHandlerContext`).

## 2. Manager runtime — `ManagerRuntime`

**File:** `src/scietex/service/manager/runtime.py`

**Purpose:** Extracted from `BasicWorker` (AR-003). Owns manager
discovery, lifecycle bookkeeping, and the restart-on-error loop. Reads config
off the worker's public properties.

**Main symbols:** `class ManagerRuntime` (18). Constructor (27) takes the
owning worker and owns three dicts: `statuses` (35), `tasks` (36), `errors`
(37).
- `iter_manager_definitions()` (49) — iterates `type(self.worker).__mro__`
  **most-derived-first** (64), de-duplicating names via a `seen` set so a
  subclass override shadows the base definition. When two managers
  independently pick the same `name=`, a WARNING is logged naming the
  colliding manager and the class it was found on (AR-068); the first
  (most-derived) definition still wins, so the collision is surfaced rather
  than silently dropped.
- `run_manager(name, manager)` (82) — runs `manager.method(self.worker)` in a
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
- `failed_managers` (property, 39) — names whose `statuses[name]` is
  `ManagerStatus.FAILED` (the recorded exception for each is in `errors`).
- `start_manager` (163), `stop_manager` (184), `start_managers` (211),
  `stop_managers` (221).

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
loop". The worker (via `ManagerRuntime`) detects `Manager` instances in the
MRO, runs their `method` in an infinite loop under an `asyncio.Task`, restarts
on error, and invokes an optional `cleanup` callable on stop.

**Main symbols:** `ManagerStatus` (14), `Manager` (24). Attributes: `name`,
`cleanup`, `method`. `Manager.__call__` (57) returns `self` (decorator
identity); `Manager.__get__` (74) binds the wrapped method to the instance
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
| `TaskTimeout` (16) | `timeout: float\|None`, `timeout_action: "requeue"\|"discard"` |
| `TaskData` (30) | `task: str`, `timeout: TaskTimeout`, `canceled_action`, `payload: bytes` |
| `TaskEnvelope` (48) | `version: int`, `data: bytes` — the versioned transport envelope wrapping a serialized `TaskData` (AR-064) |
| `TaskResult` (66) | `status: "success"\|"error"`, `error: str`, `processed_at: datetime`, `payload: bytes`, `error_code: str`, `retryable: bool`, `partial: bool` |
| `TaskTracker` (97) | `worker_task: asyncio.Task`, `data: TaskData`, `started: int\|float` |

`TaskResult.processed_at` uses `msgspec.field(default_factory=lambda:
datetime.now(timezone.utc))` (90) so each instance gets its own timestamp
(AR-012). The error-taxonomy fields (`error_code`/`retryable`/`partial`,
added AR-022) are optional and default to "no extra information", so
handlers that only set `status`/`error` keep working unchanged.

`TaskEnvelope` is the durable wire format (AR-064): the transport persists a
versioned envelope, not a bare `TaskData`, so the handler contract and the
on-the-wire format evolve independently. Encoding/decoding lives in
`task_handler/wire.py` (`encode_task_envelope`/`decode_task_envelope`).

**Public interface:** constructors only (frozen). **Dependencies:** `msgspec`.
**Depended on by:** `task_handler.basic`, `task_handler.wire`, `task_processor`,
`valkey` (msgpack round-trip of `TaskData` via the envelope), examples, tests.

## 7. Task handler contract — `TaskHandler` / `TaskHandlerContext`

**File:** `src/scietex/service/task_handler/basic.py`,
`src/scietex/service/task_handler/context.py`

**Purpose:** ABC for pluggable task handlers with lifecycle and dispatch
contract; a narrow context decouples handlers from the worker.

**Main symbols / interface:**
- `TaskHandlerContext` (context.py:7) — frozen dataclass with `service_name`,
  `instance_id`, `logger`; replaces the full worker reference.
- `__init__(name, context)` (21) — stores `name`, `context`, `logger =
  context.logger`, `_is_initialized=False` (no `self.worker`)
- abstract `supported_tasks -> list[str]` (34), abstract `handle(task_data) ->
  TaskResult` (44)
- `supports(task_type) -> bool` (60) — membership in `supported_tasks`
- `initialize() -> bool` (72, default True), `cleanup()` (83)
- `start()` (91) sets `_is_initialized = await initialize()`; `stop()` (105)
  runs `cleanup()`, resets flag; `is_ready` (115)

**Dependencies:** `.context`, `.schemas`. **Depended on by:**
`TaskProcessor` (registry + dispatch), examples, tests.

## 8. Task processor — `TaskProcessor`

**File:** `src/scietex/service/task_processor.py`

**Purpose:** Adds concurrent in-process task execution on top of the worker:
external tasks are enqueued (override `fetch_tasks`), a `TaskManager` dequeues
and dispatches to handlers, a `Watchdog` cancels timed-out tasks, and shutdown
drains/cancels in-flight work.

**Main symbols:** `class TaskProcessor(BasicWorker)` (34).
Overrides `_config_type` (61) to `TaskProcessorConfig`, so the base
instantiates the concrete config when `config=None` and `__init__` reads its
fields from `self._config` rather than re-storing (AR-069).
Properties: `task_handlers` 121, `running_tasks` 133 (read-only
`MappingProxyType` views), `queue_size` 138, `max_concurrent_tasks` 143.
Registry/dispatch: `add_task_handler` 234 (takes the handler class plus an
optional keyword-only `name` and arbitrary `**handler_kwargs`; the lifecycle
key is the resolved name — `name` if given, otherwise `handler_class.__name__`
— so multiple instances of one class can coexist under distinct keys, a
duplicate resolved key raises; the map stores a `(class, handler_kwargs)`
tuple and the kwargs are forwarded to the handler constructor on every
instantiation), `_start_task_handler` 276
(unpacks the tuple, builds a `TaskHandlerContext` at 297–301, and calls
`handler_class(handler_name, context, **handler_kwargs)` at 302),
`_stop_task_handler` 320, `remove_task_handler` 346, `_find_task_handler` 362,
`process_task` 495.
Queue access: `enqueue_task` 147, `dequeue_task` 168, `task_queue_empty` 160,
`task_queue_full` 164 (the raw `task_queue` attribute is no longer exposed;
non-blocking `put_nowait`/`get_nowait` underneath). State:
`__task_handlers_map`/`__task_handlers` (81–82; the map holds
`(class, handler_kwargs)` tuples keyed by resolved name), `__running_tasks`
(85), `__task_queue` (118, bounded `asyncio.Queue[(UUID, TaskData)]`).
Managers: `@Manager("TaskManager") task_manager` 547 (inner `handle_task`
wrapper at 560), `@Manager("TaskQueueManager") task_queue_manager` 651.
Hooks: `fetch_tasks` 633, `return_task_to_queue` 380, `on_task_completed` 393
(transport ack seam), `initialize` 410 (starts handlers), `cleanup` 448
(drains queue, cancels running tasks, stops handlers), `watchdog` 671.

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
`TaskProcessor.__init__`).

**Public interface:** constructor takes a single immutable
`TaskProcessorConfig` (`config.py`, extends `WorkerConfig`) or `None`; no
runtime setters. Properties (`task_handlers`, `running_tasks` — read-only
`MappingProxyType` views; `queue_size`, `max_concurrent_tasks`,
`task_manager_sleep_time`, `task_queue_manager_sleep_time`,
`task_handler_start_timeout`, `task_handler_stop_timeout` — all read-only),
and queue methods `enqueue_task`/`dequeue_task`/`task_queue_empty`/
`task_queue_full`.

**Dependencies:** `.basic_worker`, `.manager`, `.task_handler`.
**Depended on by:** `ValkeyWorker`, examples, tests.

## 9. Valkey worker — `ValkeyWorker`

**File:** `src/scietex/service/valkey/worker.py`

**Purpose:** Makes `TaskProcessor` consume from / write to a Valkey stream
via the `glide` `GlideClient`; publishes heartbeats; pushes logs to a Valkey
stream through an `AsyncValkeyHandler`.

**Main symbols:** `class ValkeyWorker(TaskProcessor)` (71).
Overrides `_config_type` (100) to `ValkeyWorkerConfig`, so the base instantiates
the concrete config when `config=None` and `__init__` reads its fields from
`self._config` rather than re-storing (AR-069).
Constructor — `__init__(config: ValkeyWorkerConfig | None = None)` (accepts
`config.valkey_config`; when `None`, defers the disk read to
`_ensure_client_config()`, called at first connect — AR-066, so construction
is side-effect-free),
`connect` 283 (`GlideClient.create` + PING under `_client_lock`; `_client`
assigned only after PING succeeds, 326; then ensures the logging handler and
starts it), `disconnect` 342, `heartbeat` 368 (writes msgpack `Heartbeat` to
`...:status` with TTL 2×interval), `initialize` 409 (start handlers, connect,
`xgroup_create`), `cleanup` 459 (super + stop logging handler + disconnect),
`return_task_to_queue` 525 (`xadd` re-queue via `encode_task_envelope`),
`_recover_pending_tasks` 545
(`XAUTOCLAIM` pending entries on first fetch; decodes via
`decode_task_envelope`, skipping unknown-version/invalid entries with an ERROR
log), `fetch_tasks` 607
(`xreadgroup` → `decode_task_envelope` → `enqueue_task`; does **not** ack on
enqueue; a glide
error triggers disconnect+reconnect, other errors propagate),
`on_task_completed` 682 (`xack`+`xdel` the entry after the handler finishes),
`_register_instance` 481 (`SADD` `instance_id` into the registry set),
`_unregister_instance` 504 (`SREM` it back out).

Connection ownership (AR-059/061): the worker runs one operational
`GlideClient` for heartbeat, registry, intake, and task completion;
`connect()`/`disconnect()` serialize the create→ping→assign and close→null
sequences behind `_client_lock` (166), and intake reconnects only on glide
errors. The logging handler is an independent owner: `_ensure_logging_handler`
(248) constructs `AsyncValkeyHandler` with `valkey_config=` (a scalar dict
translated from the typed config by `_logging_handler_config`, 45) when a typed
`ValkeyConfig` is available, so the handler builds/closes/reconnects its own
connection and the worker never touches `handler.client`; only a raw
`GlideClientConfiguration` falls back to `client=` injection (see §14).

**Key names** (constructed in `__init__`): status key
`scietex:{service}:{instance_id}:status`, task stream
`scietex:{service}:tasks`, group
`scietex:{service}:task_group`, consumer
`scietex:{service}:{instance_id}`, registry set
`scietex:{service}:workers`. The stream and group are service-scoped so
replicas share one queue; the consumer/status keys are worker-scoped per
auto-generated `instance_id`. `_task_entry_ids` (176) maps task UUID → stream
entry id for deferred acknowledgement; `_recovered` (180) guards one-time
pending recovery.

The registry set is the enumeration index: `_register_instance` (481) `SADD`s
the `instance_id` on startup and `_unregister_instance` (504) `SREM`s it on
shutdown — both best-effort (a failure logs a WARNING and continues). Liveness
is the status-key TTL refreshed by `heartbeat()`, so a stale member left by a
crashed replica is tolerated (the operator probes each member's status key).

**Public interface:** constructor takes a single immutable `ValkeyWorkerConfig`
(`valkey/config.py`, extends `TaskProcessorConfig`) or `None`; properties
`valkey_config`, `client`.

**Dependencies:** `..task_processor`, `..task_handler.TaskData`,
`..task_handler.wire` (`encode_task_envelope`/`decode_task_envelope`, AR-064),
`.schemas.Heartbeat`, `.config` (`ValkeyWorkerConfig`), external
`scietex.logging.AsyncValkeyHandler`, `._glide` (guarded glide names, AR-048),
`msgspec`.
**Depended on by:** `valkey/__init__.py`, package `__init__.py` (guarded),
example `examples/valkey_async_service.py`.

## 10. Valkey configuration — `valkey/config.py`

**File:** `src/scietex/service/valkey/config.py`

**Purpose:** Typed config that mirrors glide options, plus YAML persistence and
schema→glide translation. Also hosts `ValkeyWorkerConfig` (the worker-level
config struct) so its optional `glide`-typed field stays out of the
always-imported core `config.py`.

**Main symbols:** frozen structs `ValkeyNode` (31), `ValkeyUserCredentials`
(43), `ValkeyBackoffStrategy` (55), `ValkeyTlsAdvancedConfiguration` (86),
`ValkeyAdvancedConfig` (114), `ValkeyBaseConfig` (144), `ValkeyConfig` (222);
`ValkeyWorkerConfig` (239, extends `TaskProcessorConfig` with `valkey_config`,
`log_stream_name`, `task_fetch_batch_size`, `claim_min_idle_ms`); `read_valkey_config(conf_dir)`
(275) — creates `valkey.yml` with defaults only if the file is missing; raises
`RuntimeError` on a present-but-invalid file (329), never overwriting it;
`generate_glide_config(...)` (334, converts to `GlideClientConfiguration`,
validates `read_from`/`protocol`, optional PubSub subscriptions when
`listening=True`).

**Public interface:** struct constructors; config conversion properties
(`addresses`, `credentials`, `reconnect_strategy`, `to_advanced_config`, ...).

**Dependencies:** `msgspec`; `._glide` (glide names via the single guarded
import, AR-048); `..config` (`TaskProcessorConfig`,
`_validate_range`). **Depended on by:** `ValkeyWorker`, `valkey/__init__.py`,
tests.

## 11. Valkey heartbeat schema

**File:** `src/scietex/service/valkey/schemas.py`
**Purpose/content:** `Heartbeat` (16) (frozen Struct) with `service`,
`instance_id`, `status`, `heartbeat_interval`, `start_time`, `timestamp` —
`timestamp` uses `msgspec.field(default_factory=...)` (38) for a per-instance
value. msgpack-serialized by `ValkeyWorker.heartbeat`.

## 12. Valkey stream purge utility — `purge.py`

**File:** `src/scietex/service/valkey/purge.py`

**Purpose:** Standalone operational utility to purge a Valkey task stream —
reads, acknowledges, and deletes every entry so an operator can clear a stream
without running a worker. Independent of `ValkeyWorker` (AR-043: moved off the
worker class, which previously carried it as dead code).

**Main symbols:** `purge_task_stream(client, stream_name, group_name,
consumer_name, logger=None)` (22) — orchestrates the purge; private helpers
`_purge_group_entries` (60, `XREADGROUP` + `XACK` + `XDEL` loop), `_purge_stream_entries`
(82, `XREAD` + `XDEL` loop), `_stream_entry_ids` (96).

**Dependencies:** none at runtime (`GlideClient` imported only under
`TYPE_CHECKING`); the caller supplies an open client. **Depended on by:**
`valkey/__init__.py`.

## 13. Utilities

- **`utils/config.py`** — `prepare_conf_dir()` (33): returns first existing dir
  in order `conf_dir` arg → `SCIETEX_CONFIG_DIR` env → `$XDG_CONFIG_HOME/scietex`
  → `~/.config/scietex` → `/etc/scietex` → `/usr/local/etc/scietex` →
  `./config` (CWD); creates `~/.config/scietex` if none exist.
- **`utils/logo.py`** — `print_scietex_logo(service_name, version)` (34) prints
  ASCII banner using `..version.__version__`.

## 14. External async logging backend — `scietex.logging`

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
  on the first successful `connect()` (worker.py:273–279), so the handler owns
  an independent connection and reconnects autonomously. Only when the worker
  was given a raw `GlideClientConfiguration` does it fall back to `client=`
  injection (worker.py:266–272).
- `ScietexFormatter`.

**Important:** a handler built from `valkey_config=` owns and closes its own
client (autonomous reconnect/backoff); a handler built from the `client=` kwarg
never closes it — the caller owns its lifetime and recovery. `ValkeyWorker` uses
the former by default and the latter only for the raw-`GlideClientConfiguration`
fallback.
