# Components

For each major component: purpose, main classes/functions, public interface,
dependencies, dependents. Line numbers refer to the module given.

## 1. Worker core — `BasicAsyncWorker`

**File:** `src/scietex/service/basic_async_worker.py`

**Purpose:** Foundation for daemon workers: identity (`service_name`,
`worker_id`, `version`), lifecycle state machine, signal-driven graceful
shutdown, async logging handler management, discovery/execution of
`@Manager` loops, and subclass hooks for heartbeat/watchdog/initialize/cleanup.

**Main symbols:**
- `ServiceStatus` (STOPPED/STARTING/RUNNING/STOPPING) — line 42
- `class BasicAsyncWorker` — line 58
- Constructor — line 82; clamps all intervals/timeouts to module constants
  (lines 23–39)
- `_iter_manager_definitions()` — line 402 (iterates `reversed(type(self).__mro__)`)
- Manager wrappers: `_run_manager` 504, `_start_manager` 539, `_stop_manager`
  560, `_restart_manager` 580, `_start_managers` 591, `_stop_managers` 601
- Lifecycle: `_startup` 611, `start` 652, `_shutdown` 679, `stop` 722, `exit` 764
- Logging: `_logger_start_handlers` 429, `_logger_shut_down_handlers` 465
- Hooks: `initialize` 494, `heartbeat` 796, `watchdog` 808, `cleanup` 820
- Built-in managers: `@Manager(name="Heartbeat") _heartbeat_manager` 774,
  `@Manager(name="Watchdog") _watchdog_manager` 785
- `_setup_signal_handlers` 417 — registers SIGINT/SIGTERM in `__init__`

**Public interface:** constructor + mutable properties with setters:
`state`, `events` (dict of two `asyncio.Event`s: `"exit_requested"`,
`"exit"`), `service_name`, `worker_id`, `version`, `conf_dir`, `logger`,
`logging_level`, `heartbeat_interval`, `watchdog_interval`, `start_time`,
`logger_handler_timeout`, `manager_shutdown_timeout`. Extension contract:
override `initialize/heartbeat/watchdog/cleanup`, add `@Manager` methods.

**Dependencies:** `.manager` (`Manager`, `ManagerStatus`); `.logging`
(`LoggerStatus`, `parse_logging_level`); `.utils` (`prepare_conf_dir`,
`print_scietex_logo`); external `scietex.logging.AsyncBaseHandler`.

**Depended on by:** `AsyncTaskProcessor` (extends), `.utils.logo` (no),
`task_handler.basic` (type-only).

## 2. Manager runtime — `Manager` / `ManagerStatus`

**File:** `src/scietex/service/manager.py`

**Purpose:** A class-based decorator turning an async method into a "managed
loop". The worker detects `Manager` instances in the MRO, runs their `method`
in an infinite `while True` under an `asyncio.Task`, restarts on error, and
invokes an optional `cleanup` callable on stop.

**Main symbols:** `ManagerStatus` (15), `Manager` (24). Attributes: `name`,
`cleanup`, `method`. `Manager.__call__` returns `self` (decorator identity).

**Public interface:** `@Manager(name=..., cleanup=...)`.

**Dependencies:** stdlib only. **Depended on by:** `BasicAsyncWorker`,
`AsyncTaskProcessor` (decorated managers), examples (`@Manager("cruncher")`).

## 3. Logging helpers — module `logging.py` (in-package)

**File:** `src/scietex/service/logging.py`

**Purpose:** `LoggerStatus` (track async logging handler state) and
`parse_logging_level()` (accepts short/long strings or ints, e.g. `"D"`,
`"DBG"`, `"DEBUG"` → `logging.DEBUG`).

**Public interface:** `LoggerStatus`, `parse_logging_level`,
`DEFAULT_LOGGING_LEVEL`. **Dependencies:** stdlib. **Depended on by:**
`BasicAsyncWorker`.

## 4. Task schemas

**File:** `src/scietex/service/task_handler/schemas.py`

**Purpose:** Immutable typed contracts shared by handlers and processor, using
`msgspec` (also gives msgpack/YAML serialization).

| Schema | Fields |
|---|---|
| `TaskTimeout` (16) | `timeout: float\|None`, `timeout_action: "requeue"\|"discard"` |
| `TaskData` (30) | `task: str`, `timeout: TaskTimeout`, `canceled_action`, `payload: bytes` |
| `TaskResult` (48) | `status: "success"\|"error"`, `error: str`, `processed_at: datetime`, `payload: bytes` |
| `TaskTracker` (64) | `worker_task: asyncio.Task`, `data: TaskData`, `started: int\|float` |

**Public interface:** constructors only (frozen). **Dependencies:** `msgspec`.
**Depended on by:** `task_handler.basic`, `async_tasks_processor`,
`valkey` (msgpack round-trip of `TaskData`), examples, tests.

## 5. Task handler contract — `TaskHandler`

**File:** `src/scietex/service/task_handler/basic.py`

**Purpose:** ABC for pluggable task handlers with lifecycle and dispatch
contract.

**Main symbols / interface:**
- `__init__(name, worker)` — stores `name`, `worker` (`BasicAsyncWorker`),
  `logger = worker.logger`, `_is_initialized=False`
- abstract `supported_tasks -> list[str]` (40), abstract `handle(task_data) ->
  TaskResult` (49)
- `supports(task_type) -> bool` (64) — membership in `supported_tasks`
- `initialize() -> bool` (76, default True), `cleanup()` (87)
- `start()` (95) sets `_is_initialized = await initialize()`; `stop()` (109)
  runs `cleanup()`, resets flag; `is_ready` (119)

**Dependencies:** `.schemas`; `basic_async_worker` type-only.
**Depended on by:** `AsyncTaskProcessor` (registry + dispatch), examples,
tests.

## 6. Task processor — `AsyncTaskProcessor`

**File:** `src/scietex/service/async_tasks_processor.py`

**Purpose:** Adds concurrent in-process task execution on top of the worker:
external tasks are enqueued (override `fetch_tasks`), a `TaskManager` dequeues
and dispatches to handlers, a `Watchdog` cancels timed-out tasks, and shutdown
drains/requeues.

**Main symbols:** `class AsyncTaskProcessor(BasicAsyncWorker)` (44).
Registry/dispatch: `add_task_handler` 284, `remove_task_handler` 343,
`_start_task_handler` 300, `_stop_task_handler` 324, `_find_task_handler`
353, `process_task` 433. State: `__task_queue` (line 118, bounded
`asyncio.Queue[(UUID, TaskData)]`), `__running_tasks` (line 114).
Managers: `@Manager("TaskManager") task_manager` 476 (inner `handle_task`
wrapper at 489), `@Manager("TaskQueueManager") task_queue_manager` 515.
Hooks: `fetch_tasks` 506, `return_task_to_queue` 371, `initialize` 383
(starts handlers), `cleanup` 397 (drains queue, cancels running tasks,
requeues per `canceled_action`, stops handlers), `watchdog` 530.

**Config constants:** `DEFAULT_MAX_TASKS_QUEUE_SIZE=2`, `DEFAULT_MAX_CONCURRENT_TASKS=2`,
`DEFAULT_TASK_TIMEOUT=3`, `TASK_QUEUE_FETCH_TIMEOUT=1`,
`WORKER_TASK_CANCELLATION_TIMEOUT=5`, handler start/stop timeouts (5s default).

**Public interface:** constructor kwargs (`queue_size`,
`max_concurrent_tasks`, `task_manager_sleep_time`,
`task_queue_manager_sleep_time`, `task_handler_start_timeout`,
`task_handler_stop_timeout`), properties (`task_handlers`, `running_tasks`,
`queue_size`, `max_concurrent_tasks`, `task_queue`).

**Dependencies:** `.basic_async_worker`, `.manager`, `.task_handler`.
**Depended on by:** `ValkeyWorker`, examples, tests.

## 7. Valkey worker — `ValkeyWorker`

**File:** `src/scietex/service/valkey/valkey_async_worker.py`

**Purpose:** Makes `AsyncTaskProcessor` consume from / write to a Valkey stream
via the `glide` `GlideClient`; publishes heartbeats; pushes logs to a Valkey
stream through an `AsyncValkeyHandler`.

**Main symbols:** `class ValkeyWorker(AsyncTaskProcessor)` (51).
Constructor 66 (accepts `valkey_config` or falls back to `read_valkey_config`),
`connect` 180 (`GlideClient.create` + PING), `disconnect` 204, `heartbeat`
215 (writes msgpack `Heartbeat` to `...:status` with TTL 2×interval),
`initialize` 251 (start handlers, connect, `xgroup_create`),
`cleanup` 282 (super + disconnect), `purge_tasks` 292,
`return_task_to_queue` 367 (`xadd` re-queue), `fetch_tasks` 386
(`xreadgroup` → decode → `task_queue.put` → `xack`/`xdel`).

**Key names** (constructed in `__init__`, lines 150–153): status key
`scietex:{service}:{worker_id}:status`, task stream
`scietex:{service}:{worker_id}:tasks`, group
`scietex:{service}:{worker_id}:task_group`, consumer
`scietex:{service}:{worker_id}`.

**Public interface:** properties `valkey_config`, `client`; constructor kwargs
(`valkey_config`, `log_stream_name`, ...).

**Dependencies:** `..async_tasks_processor`, `..task_handler.TaskData`,
`.schemas.Heartbeat`, `.valkey_config`, external `scietex.logging.AsyncValkeyHandler`,
`glide`, `msgspec`.
**Depended on by:** `valkey/__init__.py`, package `__init__.py` (guarded),
example `examples/valkey_async_service.py`.

## 8. Valkey configuration — `valkey_config.py`

**File:** `src/scietex/service/valkey/valkey_config.py`

**Purpose:** Typed config that mirrors glide options, plus YAML persistence and
schema→glide translation.

**Main symbols:** frozen structs `ValkeyNode` (36), `ValkeyUserCredentials`
(48), `ValkeyBackoffStrategy` (60), `ValkeyTlsAdvancedConfiguration` (91),
`ValkeyAdvancedConfig` (119), `ValkeyBaseConfig` (149), `ValkeyConfig` (227);
`read_valkey_config(conf_dir)` (239, reads/creates `valkey.yml` via msgspec
yaml, silent default fallback); `generate_glide_config(...)` (277, converts to
`GlideClientConfiguration`, validates `read_from`/`protocol`, optional PubSub
subscriptions when `listening=True`).

**Public interface:** struct constructors; config conversion properties
(`addresses`, `credentials`, `reconnect_strategy`,
`to_advanced_config`, ...).

**Dependencies:** `msgspec`; `glide` types (unguarded import with an explicit
`ImportError` + install hint). **Depended on by:** `ValkeyWorker`,
`valkey/__init__.py`, tests.

## 9. Valkey heartbeat schema

**File:** `src/scietex/service/valkey/schemas.py`
**Purpose/content:** `Heartbeat` (frozen Struct) with `service`, `worker_id`,
`status`, `heartbeat_interval`, `start_time`, `timestamp`. msgpack-serialized
by `ValkeyWorker.heartbeat`.

## 10. Utilities

- **`utils/conf.py`** — `prepare_conf_dir()` (33): returns first existing dir
  in order `conf_dir` arg → `SCIETEX_CONFIG_DIR` env → `$XDG_CONFIG_HOME/scietex`
  → `~/.config/scietex` → `/etc/scietex` → `/usr/local/etc/scietex` →
  `./config` (CWD); creates `~/.config/scietex` if none exist.
- **`utils/logo.py`** — `print_scietex_logo(service_name, version)` prints
  ASCII banner using `..version.__version__`.

## 11. External async logging backend — `scietex.logging`

Installed dependency (>=1.0.0). The package embeds this framework's log sink.
Consumed classes:
- `AsyncBaseHandler(logging.Handler)` — per-backend `asyncio.Queue`s +
  worker coroutines; `start_logging()`/`stop_logging()`/`emit()`. Console
  worker enabled unless `stdout_enable=False`.
- `AsyncBrokerHandler` — adds a broker queue + `_worker` that connects,
  formats records into dicts, `send_message()`.
- `AsyncValkeyHandler(AsyncBrokerHandler)` — own `GlideClient`; `xadd` to a
  stream.
- `ScietexFormatter`.

**Important:** each async handler holds its **own** transport client and
internal asyncio tasks, i.e. Valkey logging opens a second GlideClient beside
`ValkeyWorker.client`.
