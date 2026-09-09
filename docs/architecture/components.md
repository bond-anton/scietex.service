# Components

For each major component: purpose, main classes/functions, public interface,
dependencies, dependents. Line numbers refer to the module given.

## 1. Worker core — `BasicWorker`

**File:** `src/scietex/service/basic_async_worker.py`

**Purpose:** Foundation for daemon workers: identity (`service_name`,
`instance_id`, `version`), lifecycle state machine, signal-driven graceful
shutdown, async logging handler management, and subclass hooks for
heartbeat/watchdog/initialize/cleanup. Manager discovery/runtime and
logging-handler lifecycle are delegated to `ManagerRuntime` and
`LoggingLifecycle` (constructed in `__init__`); the worker keeps only identity,
config, and the state machine.

**Main symbols:**
- `ServiceStatus` (STOPPED/STARTING/RUNNING/STOPPING) — line 53
- `class BasicWorker` — line 69
- Constructor — `__init__(config: WorkerConfig | None = None)`; stores the
  immutable `WorkerConfig` (from `config.py`), resolves identity/conf_dir/
  logging_level, and constructs `ManagerRuntime` + `LoggingLifecycle`.
  Timing/retry fields are validated at construction — an out-of-range value
  raises `msgspec.ValidationError`, and `None` resolves to the matching
  `DEFAULT_*` constant in `config.py` at read time (no runtime clamping)
- Signals: `_setup_signal_handlers` 501 (Windows-safe no-op),
  `_remove_signal_handlers` 531
- Lifecycle: `_startup` 625, `start` 671, `_shutdown` 713, `stop` 757, `exit` 800
- Cancellation terminal-state helper: `_force_stopped` 699 (AR-017 — forces
  STOPPED + `exit` event on startup/shutdown cancellation)
- Hooks: `initialize` 575, `heartbeat` 832, `watchdog` 844, `cleanup` 856,
  `_register_instance` 873, `_unregister_instance` 883
- Built-in managers: `@Manager(name="Heartbeat") _heartbeat_manager` 810,
  `@Manager(name="Watchdog") _watchdog_manager` 821
- `_setup_signal_handlers` called from `start()` (696), not `__init__`;
  `_remove_signal_handlers` called from `stop()` (778)

**Public interface:** constructor takes a single immutable `WorkerConfig`
(`config.py`) or `None`; all properties are read-only (no runtime setters):
`state`, `events` (read-only `MappingProxyType` of two `asyncio.Event`s:
`"exit_requested"`, `"exit"`), `service_name`, `instance_id`, `version`,
`conf_dir`, `logger`, `logging_level`, `heartbeat_interval`,
`watchdog_interval`, `start_time`, `logger_handler_timeout`,
`manager_shutdown_timeout`, `manager_max_retries`, `manager_restart_backoff`.
Extension contract: override
`initialize/heartbeat/watchdog/cleanup`, add `@Manager` methods. Two newer
subclass hooks govern registry-set membership: `_register_instance` (873) —
called by `_startup()` after `initialize()` succeeds and before managers
start — and `_unregister_instance` (883) — called by `_shutdown()` after
managers stop and before `cleanup()` teardown. Both are no-ops in the base;
`ValkeyWorker` overrides them (worker.py:386, 408) to `SADD`/
`SREM` its `instance_id` into the worker registry set.

**Dependencies:** `.manager.runtime` (`ManagerRuntime`), `.logging.lifecycle`
(`LoggingLifecycle`), `.manager` (`Manager`), `.logging`
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
- `iter_manager_definitions()` (39) — iterates `type(self.worker).__mro__`
  **most-derived-first** (52), de-duplicating names via a `seen` set so a
  subclass override shadows the base definition.
- `run_manager(name, manager)` (62) — runs `manager.method(self.worker)` in a
  `while True` loop (83); on a non-`CancelledError` exception records the error
  (92) and retries after `manager_restart_backoff` (110), giving up when
  `consecutive_failures > manager_max_retries` — i.e. on the
  (max_retries+1)-th consecutive failure (94–101). A successful iteration
  resets the `consecutive_failures` counter to 0 (112–113), so the budget
  counts consecutive failures only. `CancelledError` stops cleanly (89–90).
  The retry happens **inside the same task** — the manager never cancels
  itself. `finally` (116–125) runs `manager.cleanup`, marks STOPPED, and
  removes the task from tracking.
- `start_manager` (130), `stop_manager` (151), `start_managers` (178),
  `stop_managers` (188).

**Public interface:** methods above; constructor takes `worker`.

**Dependencies:** `.manager` (`Manager`, `ManagerStatus`); stdlib.
**Depended on by:** `BasicWorker` (constructs and forwards to it).

## 3. Logging lifecycle — `LoggingLifecycle`

**File:** `src/scietex/service/logging/lifecycle.py`

**Purpose:** Extracted from `BasicWorker` (AR-003). Owns async
logging-handler registration and start/stop with status bookkeeping.

**Main symbols:** `class LoggingLifecycle` (18). Constructor (27) takes the
owning worker and owns the `statuses` dict (35).
- `register_logger_handler(handler, name)` (37) — sets the handler level and
  attaches it to the worker logger; the handler is registered once and reused
  across start/stop cycles. The `name` parameter is **unused** (AR-031): it is
  accepted only because `ValkeyWorker._ensure_logging_handler` passes it
  through; statuses are keyed by `handler.name` or
  `handler.__class__.__name__` instead (lifecycle.py:40–57).
- `start_handlers()` (62) — starts each `AsyncLoggingHandler` whose recorded
  status is not RUNNING, with `logger_handler_timeout`; sets status RUNNING on
  success, FAILED on timeout/exception so it is retried on the next start
  (AR-020).
- `shut_down_handlers()` (104) — stops each handler (idempotent
  `stop_logging()`), sets status STOPPED.

**Dependencies:** `.logging` (`LoggerStatus`), external
`scietex.logging.AsyncLoggingHandler`.
**Depended on by:** `BasicWorker` (constructs and forwards to it).

## 4. Manager decorator — `Manager` / `ManagerStatus`

**File:** `src/scietex/service/manager/__init__.py`

**Purpose:** A class-based decorator turning an async method into a "managed
loop". The worker (via `ManagerRuntime`) detects `Manager` instances in the
MRO, runs their `method` in an infinite loop under an `asyncio.Task`, restarts
on error, and invokes an optional `cleanup` callable on stop.

**Main symbols:** `ManagerStatus` (14), `Manager` (23). Attributes: `name`,
`cleanup`, `method`. `Manager.__call__` (56) returns `self` (decorator
identity); `Manager.__get__` (73) binds the wrapped method to the instance
(descriptor protocol).

**Public interface:** `@Manager(name=..., cleanup=...)`.

**Dependencies:** stdlib only. **Depended on by:** `BasicWorker`,
`TaskProcessor` (decorated managers), examples (`@Manager("cruncher")`).

## 5. Logging helpers — module `logging/__init__.py` (in-package)

**File:** `src/scietex/service/logging/__init__.py`

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
| `TaskResult` (48) | `status: "success"\|"error"`, `error: str`, `processed_at: datetime`, `payload: bytes`, `error_code: str`, `retryable: bool`, `partial: bool` |
| `TaskTracker` (85) | `worker_task: asyncio.Task`, `data: TaskData`, `started: int\|float` |

`TaskResult.processed_at` uses `msgspec.field(default_factory=lambda:
datetime.now(timezone.utc))` (76) so each instance gets its own timestamp
(AR-012). The error-taxonomy fields (`error_code`/`retryable`/`partial`,
added AR-022) are optional and default to "no extra information", so
handlers that only set `status`/`error` keep working unchanged.

**Public interface:** constructors only (frozen). **Dependencies:** `msgspec`.
**Depended on by:** `task_handler.basic`, `async_tasks_processor`,
`valkey` (msgpack round-trip of `TaskData`), examples, tests.

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

**File:** `src/scietex/service/async_tasks_processor.py`

**Purpose:** Adds concurrent in-process task execution on top of the worker:
external tasks are enqueued (override `fetch_tasks`), a `TaskManager` dequeues
and dispatches to handlers, a `Watchdog` cancels timed-out tasks, and shutdown
drains/cancels in-flight work.

**Main symbols:** `class TaskProcessor(BasicWorker)` (46).
Properties: `task_handlers` 154, `running_tasks` 166 (read-only
`MappingProxyType` views), `queue_size` 171, `max_concurrent_tasks` 176.
Registry/dispatch: `add_task_handler` 314 (takes the handler class plus an
optional keyword-only `name`; the lifecycle key is the resolved name — `name`
if given, otherwise `handler_class.__name__` — so multiple instances of one
class can coexist under distinct keys, a duplicate resolved key raises),
`_start_task_handler` 360
(builds a `TaskHandlerContext` at 381–385), `_stop_task_handler` 404,
`remove_task_handler` 423, `_find_task_handler` 433, `process_task` 544.
Queue access: `enqueue_task` 181, `dequeue_task` 202, `task_queue_empty` 194,
`task_queue_full` 198 (the raw `task_queue` attribute is no longer exposed;
non-blocking `put_nowait`/`get_nowait` underneath). State:
`__task_handlers_map`/`__task_handlers` (112–113), `__running_tasks` (116),
`__task_queue` (120, bounded `asyncio.Queue[(UUID, TaskData)]`).
Managers: `@Manager("TaskManager") task_manager` 595 (inner `handle_task`
wrapper at 608), `@Manager("TaskQueueManager") task_queue_manager` 670.
Hooks: `fetch_tasks` 661, `return_task_to_queue` 451, `on_task_completed` 463
(transport ack seam), `initialize` 480 (starts handlers), `cleanup` 498
(drains queue, cancels running tasks, stops handlers), `watchdog` 685.

**Config constants:** timing/retry MIN/MAX/DEFAULT bounds live in `config.py`
(single source of truth); the task-queue defaults are
`DEFAULT_MAX_TASKS_QUEUE_SIZE=100` and `DEFAULT_MAX_CONCURRENT_TASKS=10`
(AR-055). Remaining processor-local constants stay in this module:
`DEFAULT_TASK_TIMEOUT=3`, `TASK_QUEUE_FETCH_TIMEOUT=1`,
`WORKER_TASK_CANCELLATION_TIMEOUT=5`.

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

**Dependencies:** `.basic_async_worker`, `.manager`, `.task_handler`.
**Depended on by:** `ValkeyWorker`, examples, tests.

## 9. Valkey worker — `ValkeyWorker`

**File:** `src/scietex/service/valkey/worker.py`

**Purpose:** Makes `TaskProcessor` consume from / write to a Valkey stream
via the `glide` `GlideClient`; publishes heartbeats; pushes logs to a Valkey
stream through an `AsyncValkeyHandler`.

**Main symbols:** `class ValkeyWorker(TaskProcessor)` (53).
Constructor — `__init__(config: ValkeyWorkerConfig | None = None)` (accepts
`config.valkey_config` or falls back to `read_valkey_config`),
`connect` 232 (`GlideClient.create` + PING; `_client` assigned only
after PING succeeds, 261; then wires the shared client into the logging handler),
`disconnect` 277, `heartbeat` 291 (writes msgpack `Heartbeat` to `...:status`
with TTL 2×interval), `initialize` 327 (start handlers, connect,
`xgroup_create`), `cleanup` 362 (super + disconnect),
`return_task_to_queue` 438 (`xadd` re-queue), `_recover_pending_tasks` 457
(`XAUTOCLAIM` pending entries on first fetch), `fetch_tasks` 513
(`xreadgroup` → decode → `enqueue_task`; does **not** ack on enqueue),
`on_task_completed` 572 (`xack`+`xdel` the entry after the handler finishes),
`_register_instance` 386 (`SADD` `instance_id` into the registry set),
`_unregister_instance` 408 (`SREM` it back out).

Single client (AR-018): the worker runs one `GlideClient` shared with the
logging handler. `_ensure_logging_handler` (207) constructs the
`AsyncValkeyHandler` with the worker's client injected on the first successful
`connect()`, and `disconnect()` (277) clears the handler's reference before
closing the shared client — the worker is the sole teardown owner (see §H9).

**Key names** (constructed in `__init__`): status key
`scietex:{service}:{instance_id}:status`, task stream
`scietex:{service}:tasks`, group
`scietex:{service}:task_group`, consumer
`scietex:{service}:{instance_id}`, registry set
`scietex:{service}:workers`. The stream and group are service-scoped so
replicas share one queue; the consumer/status keys are worker-scoped per
auto-generated `instance_id`. `_task_entry_ids` (170) maps task UUID → stream
entry id for deferred acknowledgement; `_recovered` (174) guards one-time
pending recovery.

The registry set is the enumeration index: `_register_instance` (386) `SADD`s
the `instance_id` on startup and `_unregister_instance` (408) `SREM`s it on
shutdown — both best-effort (a failure logs a WARNING and continues). Liveness
is the status-key TTL refreshed by `heartbeat()`, so a stale member left by a
crashed replica is tolerated (the operator probes each member's status key).

**Public interface:** constructor takes a single immutable `ValkeyWorkerConfig`
(`valkey/config.py`, extends `TaskProcessorConfig`) or `None`; properties
`valkey_config`, `client`.

**Dependencies:** `..async_tasks_processor`, `..task_handler.TaskData`,
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

**Main symbols:** frozen structs `ValkeyNode` (38), `ValkeyUserCredentials`
(50), `ValkeyBackoffStrategy` (62), `ValkeyTlsAdvancedConfiguration` (93),
`ValkeyAdvancedConfig` (121), `ValkeyBaseConfig` (151), `ValkeyConfig` (229);
`ValkeyWorkerConfig` (241, extends `TaskProcessorConfig` with `valkey_config`,
`log_stream_name`, `task_fetch_batch_size`); `read_valkey_config(conf_dir)`
(268) — creates `valkey.yml` with defaults only if the file is missing; raises
`RuntimeError` on a present-but-invalid file (306), never overwriting it;
`generate_glide_config(...)` (311, converts to `GlideClientConfiguration`,
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

- **`utils/conf.py`** — `prepare_conf_dir()` (33): returns first existing dir
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
  (e.g. `f"{service_name}.{instance_id}"`).
- `AsyncBrokerHandler` — adds a broker queue + `_worker` that connects,
  formats records into dicts, `send_message()`; accepts an injected `client`
  and, when one is provided, never closes it (`_owns_client=False`).
- `AsyncValkeyHandler(AsyncBrokerHandler)` — `xadd` to a stream. `ValkeyWorker`
  injects its own `GlideClient` via the `client` kwarg on the first successful
  `connect()` (worker.py:207–223), so logging shares the worker's
  single connection rather than opening a second one.
- `ScietexFormatter`.

**Important:** when a client is injected via the `client` kwarg, the handler
never closes it — the caller owns its lifetime and recovery. `ValkeyWorker`
injects its single client, so the worker is the sole teardown owner.
