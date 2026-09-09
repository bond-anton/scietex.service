# Repository / package structure

Layout of the repository and the Python package.

## Top-level repository layout

| Path | Contents |
|---|---|
| `src/scietex/service/` | The package (see below). Marked PEP 561 via `py.typed` |
| `examples/` | Runnable blueprints: `basic_worker.py`, `task_processor.py`, `valkey_async_service.py` |
| `tests/` | Pytest suite, one file per component plus `test_version.py`; Valkey tests mock `GlideClient` (no server needed) |
| `docs/` | Usage docs (`index.md`, per-component guides); `docs/architecture/` is this map |
| `pyproject.toml` | Package metadata, deps, extras (`valkey`, `dev`, `test`, `lint`), setuptools build config, and pytest config (`[tool.pytest.ini_options]`) |
| `tox.ini` | Tox environments: `format`, `lint`, `type`, `py{314}` (coverage) |
| `.ruff.toml`, `cspell.json` | Ruff and spell-check config |
| `.github/workflows/` | CI: `python-lint.yml`, `python-package.yml` (tests with a Redis service container), `python-publish.yml` (PyPI on release) |
| `AGENTS.md`, `README.md`, `LICENSE` | Developer instructions, public docs, MIT license |

> Pytest configuration lives only in `pyproject.toml`
> (`[tool.pytest.ini_options]`, lines 46–48); `pytest.ini` was deleted (AR-013).

## Package layout (`src/scietex/service/`)

| Module | Responsibility |
|---|---|
| `__init__.py` | Public API. Always exports `__version__`, `BasicWorker`, `TaskProcessor`, `Manager`, `WorkerConfig`, `TaskProcessorConfig`. In a guarded `try/except ImportError` block, additionally imports and re-exports the Valkey surface (`ValkeyWorker`, config types including `ValkeyWorkerConfig`) and sets the `VALKEY_AVAILABLE` flag. The guard makes the package importable without `valkey-glide`, while non-`ImportError` exceptions propagate so real Valkey bugs surface at import (AR-019) |
| `version.py` | Single source `__version__ = "4.0.0"` (also read by setuptools dynamic version) |
| `config.py` | `WorkerConfig` + `TaskProcessorConfig` — immutable `msgspec.Struct`s (`frozen=True`) replacing the old per-worker constructor kwargs. Also holds the MIN/MAX/DEFAULT constants (single source of truth for timing/retry bounds and the task-queue defaults, `DEFAULT_MAX_TASKS_QUEUE_SIZE=100` / `DEFAULT_MAX_CONCURRENT_TASKS=10`). `__post_init__` validates ranges and raises `msgspec.ValidationError` on an out-of-range value; a `None` field resolves to its `DEFAULT_*` constant at read time |
| `manager/__init__.py` | `Manager` class-decorator (name + optional cleanup callable, stores `method`) and `ManagerStatus` enum |
| `manager/runtime.py` | `ManagerRuntime(worker)`: manager discovery across the class MRO (`iter_manager_definitions`), start/stop bookkeeping (`statuses`/`tasks`/`errors`), and the bounded restart-on-error loop (`run_manager`). Extracted from `BasicWorker` (AR-003) |
| `logging/lifecycle.py` | `LoggingLifecycle(worker)`: async logging-handler registration and start/stop with `statuses` bookkeeping. Extracted from `BasicWorker` (AR-003) |
| `basic_async_worker.py` | `BasicWorker` + `ServiceStatus`. Owns identity/config, the lifecycle state machine, signal handlers (registered in `start()`), startup/shutdown orchestration, and the built-in `Heartbeat`/`Watchdog` managers. Delegates manager runtime and logging lifecycle to `ManagerRuntime`/`LoggingLifecycle` via forwarding wrappers |
| `logging/__init__.py` | `LoggerStatus` (STOPPED/RUNNING/FAILED), `parse_logging_level()`, `DEFAULT_LOGGING_LEVEL` |
| `async_tasks_processor.py` | `TaskProcessor(BasicWorker)`. Task registry maps (`__task_handlers_map` class→instance, `__task_handlers` active instances), bounded task queue (accessed via `enqueue_task`/`dequeue_task`/`task_queue_empty`/`task_queue_full` — the raw `task_queue` is no longer public), `running_tasks` (`UUID → TaskTracker`), `@Manager("TaskManager") task_manager`, `@Manager("TaskQueueManager") task_queue_manager`, `process_task()`, watchdog timeout logic, handler start/stop, drain-and-cancel cleanup, `on_task_completed()` ack seam |
| `task_handler/__init__.py` | Re-exports `TaskHandler`, `TaskHandlerContext`, `TaskData`, `TaskResult`, `TaskTimeout`, `TaskTracker` |
| `task_handler/context.py` | `TaskHandlerContext` — frozen dataclass (`service_name`, `instance_id`, `logger`) passed to handlers instead of the full worker |
| `task_handler/schemas.py` | Frozen `msgspec.Struct` schemas (see [`components.md`](./components.md)) |
| `task_handler/basic.py` | `TaskHandler(ABC)`; imports `.context` (no reference to `BasicWorker`) |
| `utils/__init__.py` | Re-exports `prepare_conf_dir`, `print_scietex_logo` |
| `utils/conf.py` | `prepare_conf_dir()` + `_resolve_xdg_path()` config-dir search |
| `utils/logo.py` | ASCII `LOGO` template and `print_scietex_logo()` |
| `valkey/__init__.py` | Re-exports `ValkeyWorker`, config types, and `purge_task_stream` from the sibling modules |
| `valkey/_glide.py` | Private module — the single guarded `from glide import (...)` re-exporting the full union of glide names used by the valkey package (incl. aliases `GlideConnectionError`, `GlideTimeoutError`). Importing it raises `ImportError` with an install hint when `valkey-glide` is absent (AR-048) |
| `valkey/config.py` | Typed config structs (`ValkeyConfig`, `ValkeyBaseConfig`, ...) + `ValkeyWorkerConfig` (worker-level config struct extending `TaskProcessorConfig`) + `read_valkey_config()` (YAML; raises `RuntimeError` on invalid file, creates defaults only if missing) + `generate_glide_config()` (schema→`GlideClientConfiguration`). Imports its `glide` names from `valkey/_glide.py` (the single guarded import, AR-048) |
| `valkey/worker.py` | `ValkeyWorker(TaskProcessor)` + stream/connection logic. Imports its `glide` names from `valkey/_glide.py` (AR-048); the `scietex.logging.AsyncValkeyHandler` import is unguarded at module top |
| `valkey/purge.py` | Standalone `purge_task_stream()` operational utility (read+ack+delete every stream entry); no runtime `glide` import (`TYPE_CHECKING` only), importing `GlideClient` from `valkey/_glide.py` (AR-048) |
| `valkey/schemas.py` | `Heartbeat` msgpack schema |

## Notable module boundaries

- **Core ⇄ Valkey**: the only core→Valkey edge is the guarded re-export in
  `__init__.py`. Core modules never import `valkey`. Direction is
  Valkey → core (`ValkeyWorker` extends `TaskProcessor`).
- **Worker ⇄ task_handler**: `async_tasks_processor.py` imports
  `task_handler`; `task_handler` no longer references `basic_async_worker`
  (handlers receive a `TaskHandlerContext`, so there is no runtime or type
  cycle).
- **Worker ⇄ runtime components**: `basic_async_worker.py` imports
  `manager/runtime` and `logging/lifecycle`, which hold only a back-reference
  to the worker under `TYPE_CHECKING` (no runtime cycle).
- **Valkey internal split**: config schema/loader (`valkey/config.py`) is
  independent of the worker (`worker.py`); both import their `glide` names
  through the shared guarded module `valkey/_glide.py` (AR-048). `valkey/config`
  can be used/tested without a worker, but not without `glide`.
- **Package ⇄ external `scietex.logging`**: `basic_async_worker.py` and
  `valkey/worker.py` attach external logging handlers. The worker
  treats them uniformly through `start_logging()`/`stop_logging()` +
  `handler.name` (via `LoggingLifecycle`).
- **Stale artifacts present in the tree** (not source): `build/`
  (`build/lib/scietex/service/` still contains `logo.py` — the flat logo that
  predates the `utils/` split — `valkey/valkey_async_worker_messaging.py`, and
  a `task_handlers/` directory), `src/scietex.service.egg-info/`, `*.pyc` under
  `src`, `.tox/`, `.coverage`. Ignore when reading the map.
