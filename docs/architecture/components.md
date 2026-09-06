# Components

For each major component: purpose, main classes/functions, public interface,
dependencies, dependents. Line numbers refer to the module given.

## 1. Worker core — `BasicAsyncWorker`

**File:** `src/scietex/service/basic_async_worker.py`

**Purpose:** Foundation for daemon workers: identity (`service_name`,
`worker_id`, `version`), lifecycle state machine, signal-driven graceful
shutdown, async logging handler management, and subclass hooks for
heartbeat/watchdog/initialize/cleanup. Manager discovery/runtime and
logging-handler lifecycle are delegated to `ManagerRuntime` and
`LoggingLifecycle` (constructed in `__init__`); the worker keeps only identity,
config, and the state machine.

**Main symbols:**
- `ServiceStatus` (STOPPED/STARTING/RUNNING/STOPPING) — line 53
- `class BasicAsyncWorker` — line 69
- Constructor — line 93; clamps all intervals/timeouts to module constants
  (lines 26–48); constructs `ManagerRuntime` + `LoggingLifecycle` (145–146)
- Forwarding wrappers (delegate to the extracted components, kept for
  subclass/test compatibility):
  - `_iter_manager_definitions()` — line 489 (→ `ManagerRuntime.iter_manager_definitions`)
  - manager wrappers: `_run_manager` 568, `_start_manager` 576, `_stop_manager`
    584, `_start_managers` 592, `_stop_managers` 600
  - logging wrappers: `_register_logger_handler` 528, `_logger_start_handlers`
    541, `_logger_shut_down_handlers` 549
- Signals: `_setup_signal_handlers` 497 (Windows-safe no-op),
  `_remove_signal_handlers` 514
- Lifecycle: `_startup` 608, `start` 653, `_shutdown` 681, `stop` 724, `exit` 767
- Hooks: `initialize` 558, `heartbeat` 799, `watchdog` 811, `cleanup` 823
- Built-in managers: `@Manager(name="Heartbeat") _heartbeat_manager` 777,
  `@Manager(name="Watchdog") _watchdog_manager` 788
- `_setup_signal_handlers` called from `start()` (678), not `__init__`;
  `_remove_signal_handlers` called from `stop()` (745)

**Public interface:** constructor + read-only properties (with setters where
config is mutable at runtime): `state`, `events` (read-only `MappingProxyType`
of two `asyncio.Event`s: `"exit_requested"`, `"exit"`), `service_name`,
`worker_id`, `version`, `conf_dir`, `logger`, `logging_level`,
`heartbeat_interval`, `watchdog_interval`, `start_time`,
`logger_handler_timeout`, `manager_shutdown_timeout`, `manager_max_retries`,
`manager_restart_backoff`. Extension contract: override
`initialize/heartbeat/watchdog/cleanup`, add `@Manager` methods.

**Dependencies:** `.manager_runtime` (`ManagerRuntime`), `.logging_lifecycle`
(`LoggingLifecycle`), `.manager` (`Manager`), `.logging`
(`parse_logging_level`), `.utils` (`prepare_conf_dir`, `print_scietex_logo`);
external `scietex.logging.AsyncBaseHandler`.

**Depended on by:** `AsyncTaskProcessor` (extends); `ManagerRuntime` and
`LoggingLifecycle` (back-reference to the owning worker); `task_handler`
(indirectly, via `TaskHandlerContext`).

## 2. Manager runtime — `ManagerRuntime`

**File:** `src/scietex/service/manager_runtime.py`

**Purpose:** Extracted from `BasicAsyncWorker` (AR-003). Owns manager
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
  (92) and retries after `manager_restart_backoff` (110), giving up after
  `manager_max_retries` consecutive failures (94–101). `CancelledError` stops
  cleanly (89–90). The retry happens **inside the same task** — the manager
  never cancels itself. `finally` (116–125) runs `manager.cleanup`, marks
  STOPPED, and removes the task from tracking.
- `start_manager` (127), `stop_manager` (148), `start_managers` (170),
  `stop_managers` (180).

**Public interface:** methods above; constructor takes `worker`.

**Dependencies:** `.manager` (`Manager`, `ManagerStatus`); stdlib.
**Depended on by:** `BasicAsyncWorker` (constructs and forwards to it).

## 3. Logging lifecycle — `LoggingLifecycle`

**File:** `src/scietex/service/logging_lifecycle.py`

**Purpose:** Extracted from `BasicAsyncWorker` (AR-003). Owns async
logging-handler registration and start/stop with status bookkeeping.

**Main symbols:** `class LoggingLifecycle` (18). Constructor (27) takes the
owning worker and owns the `statuses` dict (35).
- `register_logger_handler(handler, name)` (37) — sets the handler level and
  attaches it to the worker logger; the handler is registered once and reused
  across start/stop cycles.
- `start_handlers()` (57) — starts each `AsyncBaseHandler` whose recorded
  status is not RUNNING, with `logger_handler_timeout`; sets status RUNNING.
- `shut_down_handlers()` (93) — stops each handler (idempotent
  `stop_logging()`), sets status STOPPED.

**Dependencies:** `.logging` (`LoggerStatus`), external
`scietex.logging.AsyncBaseHandler`.
**Depended on by:** `BasicAsyncWorker` (constructs and forwards to it).

## 4. Manager decorator — `Manager` / `ManagerStatus`

**File:** `src/scietex/service/manager.py`

**Purpose:** A class-based decorator turning an async method into a "managed
loop". The worker (via `ManagerRuntime`) detects `Manager` instances in the
MRO, runs their `method` in an infinite loop under an `asyncio.Task`, restarts
on error, and invokes an optional `cleanup` callable on stop.

**Main symbols:** `ManagerStatus` (14), `Manager` (23). Attributes: `name`,
`cleanup`, `method`. `Manager.__call__` (56) returns `self` (decorator
identity); `Manager.__get__` (73) binds the wrapped method to the instance
(descriptor protocol).

**Public interface:** `@Manager(name=..., cleanup=...)`.

**Dependencies:** stdlib only. **Depended on by:** `BasicAsyncWorker`,
`AsyncTaskProcessor` (decorated managers), examples (`@Manager("cruncher")`).

## 5. Logging helpers — module `logging.py` (in-package)

**File:** `src/scietex/service/logging.py`

**Purpose:** `LoggerStatus` (track async logging handler state) and
`parse_logging_level()` (accepts short/long strings or ints, e.g. `"D"`,
`"DBG"`, `"DEBUG"` → `logging.DEBUG`).

**Public interface:** `LoggerStatus` (14), `parse_logging_level` (26),
`DEFAULT_LOGGING_LEVEL` (11). **Dependencies:** stdlib. **Depended on by:**
`BasicAsyncWorker`, `LoggingLifecycle`.

## 6. Task schemas

**File:** `src/scietex/service/task_handler/schemas.py`

**Purpose:** Immutable typed contracts shared by handlers and processor, using
`msgspec` (also gives msgpack/YAML serialization).

| Schema | Fields |
|---|---|
| `TaskTimeout` (16) | `timeout: float\|None`, `timeout_action: "requeue"\|"discard"` |
| `TaskData` (30) | `task: str`, `timeout: TaskTimeout`, `canceled_action`, `payload: bytes` |
| `TaskResult` (48) | `status: "success"\|"error"`, `error: str`, `processed_at: datetime`, `payload: bytes` |
| `TaskTracker` (64) | `worker_task: asyncio.Task`, `data: TaskData`, `started: int\|float` |

`TaskResult.processed_at` uses `msgspec.field(default_factory=lambda:
datetime.now(timezone.utc))` (60) so each instance gets its own timestamp
(AR-012).

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
  `worker_id`, `logger`; replaces the full worker reference.
- `__init__(name, context)` (21) — stores `name`, `context`, `logger =
  context.logger`, `_is_initialized=False` (no `self.worker`)
- abstract `supported_tasks -> list[str]` (34), abstract `handle(task_data) ->
  TaskResult` (44)
- `supports(task_type) -> bool` (60) — membership in `supported_tasks`
- `initialize() -> bool` (72, default True), `cleanup()` (83)
- `start()` (91) sets `_is_initialized = await initialize()`; `stop()` (105)
  runs `cleanup()`, resets flag; `is_ready` (115)

**Dependencies:** `.context`, `.schemas`. **Depended on by:**
`AsyncTaskProcessor` (registry + dispatch), examples, tests.

## 8. Task processor — `AsyncTaskProcessor`

**File:** `src/scietex/service/async_tasks_processor.py`

**Purpose:** Adds concurrent in-process task execution on top of the worker:
external tasks are enqueued (override `fetch_tasks`), a `TaskManager` dequeues
and dispatches to handlers, a `Watchdog` cancels timed-out tasks, and shutdown
drains/cancels in-flight work.

**Main symbols:** `class AsyncTaskProcessor(BasicAsyncWorker)` (46).
Properties: `task_handlers` 154, `running_tasks` 166 (read-only
`MappingProxyType` views), `queue_size` 171, `max_concurrent_tasks` 176.
Registry/dispatch: `add_task_handler` 314, `_start_task_handler` 330 (builds a
`TaskHandlerContext` at 351–355), `_stop_task_handler` 371,
`remove_task_handler` 390, `_find_task_handler` 400, `process_task` 509.
Queue access: `enqueue_task` 181, `dequeue_task` 202, `task_queue_empty` 194,
`task_queue_full` 198 (the raw `task_queue` attribute is no longer exposed;
non-blocking `put_nowait`/`get_nowait` underneath). State:
`__task_handlers_map`/`__task_handlers` (112–113), `__running_tasks` (116),
`__task_queue` (120, bounded `asyncio.Queue[(UUID, TaskData)]`).
Managers: `@Manager("TaskManager") task_manager` 549 (inner `handle_task`
wrapper at 562), `@Manager("TaskQueueManager") task_queue_manager` 624.
Hooks: `fetch_tasks` 615, `return_task_to_queue` 418, `on_task_completed` 430
(transport ack seam), `initialize` 447 (starts handlers), `cleanup` 465
(drains queue, cancels running tasks, stops handlers), `watchdog` 639.

**Config constants:** `DEFAULT_MAX_TASKS_QUEUE_SIZE=2` (21),
`DEFAULT_MAX_CONCURRENT_TASKS=2` (23), `DEFAULT_TASK_TIMEOUT=3` (26),
`TASK_QUEUE_FETCH_TIMEOUT=1` (29), `DEFAULT_MANAGER_SLEEP_TIME=0.01` (31),
`WORKER_TASK_CANCELLATION_TIMEOUT=5` (35), handler start/stop timeouts (37–43,
default 5 s).

**Public interface:** constructor kwargs (`queue_size`,
`max_concurrent_tasks`, `task_manager_sleep_time`,
`task_queue_manager_sleep_time`, `task_handler_start_timeout`,
`task_handler_stop_timeout`), properties (`task_handlers`, `running_tasks` —
read-only `MappingProxyType` views; `queue_size`, `max_concurrent_tasks`,
`task_manager_sleep_time`, `task_queue_manager_sleep_time`,
`task_handler_start_timeout`, `task_handler_stop_timeout`), and queue methods
`enqueue_task`/`dequeue_task`/`task_queue_empty`/`task_queue_full`.

**Dependencies:** `.basic_async_worker`, `.manager`, `.task_handler`.
**Depended on by:** `ValkeyWorker`, examples, tests.

## 9. Valkey worker — `ValkeyWorker`

**File:** `src/scietex/service/valkey/valkey_async_worker.py`

**Purpose:** Makes `AsyncTaskProcessor` consume from / write to a Valkey stream
via the `glide` `GlideClient`; publishes heartbeats; pushes logs to a Valkey
stream through an `AsyncValkeyHandler`.

**Main symbols:** `class ValkeyWorker(AsyncTaskProcessor)` (52).
Constructor 67 (accepts `valkey_config` or falls back to `read_valkey_config`,
128–130; registers an `AsyncValkeyHandler` with credentials 154–163),
`connect` 204 (`GlideClient.create` + PING; `_client` assigned only after PING
succeeds, 229), `disconnect` 242, `heartbeat` 253 (writes msgpack `Heartbeat`
to `...:status` with TTL 2×interval), `initialize` 289 (start handlers,
connect, `xgroup_create`), `cleanup` 320 (super + disconnect), `purge_tasks`
330, `return_task_to_queue` 405 (`xadd` re-queue), `_recover_pending_tasks`
424 (`XAUTOCLAIM` pending entries on first fetch), `fetch_tasks` 480
(`xreadgroup` → decode → `enqueue_task`; does **not** ack on enqueue),
`on_task_completed` 539 (`xack`+`xdel` the entry after the handler finishes).

**Key names** (constructed in `__init__`, lines 166–169): status key
`scietex:{service}:{worker_id}:status`, task stream
`scietex:{service}:{worker_id}:tasks`, group
`scietex:{service}:{worker_id}:task_group`, consumer
`scietex:{service}:{worker_id}`. `_task_entry_ids` (174) maps task UUID → stream
entry id for deferred acknowledgement; `_recovered` (178) guards one-time
pending recovery.

**Public interface:** properties `valkey_config`, `client`; constructor kwargs
(`valkey_config`, `log_stream_name`, ...).

**Dependencies:** `..async_tasks_processor`, `..task_handler.TaskData`,
`.schemas.Heartbeat`, `.valkey_config`, external
`scietex.logging.AsyncValkeyHandler`, `glide`, `msgspec`.
**Depended on by:** `valkey/__init__.py`, package `__init__.py` (guarded),
example `examples/valkey_async_service.py`.

## 10. Valkey configuration — `valkey_config.py`

**File:** `src/scietex/service/valkey/valkey_config.py`

**Purpose:** Typed config that mirrors glide options, plus YAML persistence and
schema→glide translation.

**Main symbols:** frozen structs `ValkeyNode` (36), `ValkeyUserCredentials`
(48), `ValkeyBackoffStrategy` (60), `ValkeyTlsAdvancedConfiguration` (91),
`ValkeyAdvancedConfig` (119), `ValkeyBaseConfig` (149), `ValkeyConfig` (227);
`read_valkey_config(conf_dir)` (239) — creates `valkey.yml` with defaults only
if the file is missing; raises `RuntimeError` on a present-but-invalid file
(277), never overwriting it; `generate_glide_config(...)` (282, converts to
`GlideClientConfiguration`, validates `read_from`/`protocol`, optional PubSub
subscriptions when `listening=True`).

**Public interface:** struct constructors; config conversion properties
(`addresses`, `credentials`, `reconnect_strategy`, `to_advanced_config`, ...).

**Dependencies:** `msgspec`; `glide` types (unguarded import with an explicit
`ImportError` + install hint). **Depended on by:** `ValkeyWorker`,
`valkey/__init__.py`, tests.

## 11. Valkey heartbeat schema

**File:** `src/scietex/service/valkey/schemas.py`
**Purpose/content:** `Heartbeat` (16) (frozen Struct) with `service`,
`worker_id`, `status`, `heartbeat_interval`, `start_time`, `timestamp` —
`timestamp` uses `msgspec.field(default_factory=...)` (38) for a per-instance
value. msgpack-serialized by `ValkeyWorker.heartbeat`.

## 12. Utilities

- **`utils/conf.py`** — `prepare_conf_dir()` (33): returns first existing dir
  in order `conf_dir` arg → `SCIETEX_CONFIG_DIR` env → `$XDG_CONFIG_HOME/scietex`
  → `~/.config/scietex` → `/etc/scietex` → `/usr/local/etc/scietex` →
  `./config` (CWD); creates `~/.config/scietex` if none exist.
- **`utils/logo.py`** — `print_scietex_logo(service_name, version)` (34) prints
  ASCII banner using `..version.__version__`.

## 13. External async logging backend — `scietex.logging`

Installed dependency (>=1.1.0). The package embeds this framework's log sink.
Consumed classes:
- `AsyncBaseHandler(logging.Handler)` — per-backend `asyncio.Queue`s +
  worker coroutines; `start_logging()`/`stop_logging()`/`emit()`. Console
  worker enabled unless `stdout_enable=False`.
- `AsyncBrokerHandler` — adds a broker queue + `_worker` that connects,
  formats records into dicts, `send_message()`.
- `AsyncValkeyHandler(AsyncBrokerHandler)` — own `GlideClient`; `xadd` to a
  stream. `ValkeyWorker` passes Valkey connection credentials to it
  (valkey_async_worker.py:141–153).
- `ScietexFormatter`.

**Important:** each async handler holds its **own** transport client and
internal asyncio tasks, i.e. Valkey logging opens a second GlideClient beside
`ValkeyWorker.client`.
