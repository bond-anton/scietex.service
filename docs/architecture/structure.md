# Repository / package structure

Layout of the repository and the Python package.

## Top-level repository layout

| Path | Contents |
|---|---|
| `src/scietex/service/` | The package (see below). Marked PEP 561 via `py.typed` |
| `examples/` | Runnable blueprints: `async_service.py`, `async_task_processor.py`, `valkey_async_service.py` |
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
| `__init__.py` | Public API. Always exports `__version__`, `BasicAsyncWorker`, `AsyncTaskProcessor`, `Manager`. In a guarded `try/except ImportError` block, additionally imports and re-exports the Valkey surface (`ValkeyWorker`, config types) and sets the `VALKEY_AVAILABLE` flag. The guard makes the package importable without `valkey-glide`, while non-`ImportError` exceptions propagate so real Valkey bugs surface at import (AR-019) |
| `version.py` | Single source `__version__ = "3.1.0"` (also read by setuptools dynamic version) |
| `manager.py` | `Manager` class-decorator (name + optional cleanup callable, stores `method`) and `ManagerStatus` enum |
| `manager_runtime.py` | `ManagerRuntime(worker)`: manager discovery across the class MRO (`iter_manager_definitions`), start/stop bookkeeping (`statuses`/`tasks`/`errors`), and the bounded restart-on-error loop (`run_manager`). Extracted from `BasicAsyncWorker` (AR-003) |
| `logging_lifecycle.py` | `LoggingLifecycle(worker)`: async logging-handler registration and start/stop with `statuses` bookkeeping. Extracted from `BasicAsyncWorker` (AR-003) |
| `basic_async_worker.py` | `BasicAsyncWorker` + `ServiceStatus`. Owns identity/config, the lifecycle state machine, signal handlers (registered in `start()`), startup/shutdown orchestration, and the built-in `Heartbeat`/`Watchdog` managers. Delegates manager runtime and logging lifecycle to `ManagerRuntime`/`LoggingLifecycle` via forwarding wrappers |
| `logging.py` | `LoggerStatus` (STOPPED/RUNNING/FAILED), `parse_logging_level()`, `DEFAULT_LOGGING_LEVEL` |
| `async_tasks_processor.py` | `AsyncTaskProcessor(BasicAsyncWorker)`. Task registry maps (`__task_handlers_map` class→instance, `__task_handlers` active instances), bounded task queue (accessed via `enqueue_task`/`dequeue_task`/`task_queue_empty`/`task_queue_full` — the raw `task_queue` is no longer public), `running_tasks` (`UUID → TaskTracker`), `@Manager("TaskManager") task_manager`, `@Manager("TaskQueueManager") task_queue_manager`, `process_task()`, watchdog timeout logic, handler start/stop, drain-and-cancel cleanup, `on_task_completed()` ack seam |
| `task_handler/__init__.py` | Re-exports `TaskHandler`, `TaskHandlerContext`, `TaskData`, `TaskResult`, `TaskTimeout`, `TaskTracker` |
| `task_handler/context.py` | `TaskHandlerContext` — frozen dataclass (`service_name`, `worker_id`, `logger`) passed to handlers instead of the full worker |
| `task_handler/schemas.py` | Frozen `msgspec.Struct` schemas (see [`components.md`](./components.md)) |
| `task_handler/basic.py` | `TaskHandler(ABC)`; imports `.context` (no reference to `BasicAsyncWorker`) |
| `utils/__init__.py` | Re-exports `prepare_conf_dir`, `print_scietex_logo` |
| `utils/conf.py` | `prepare_conf_dir()` + `_resolve_xdg_path()` config-dir search |
| `utils/logo.py` | ASCII `LOGO` template and `print_scietex_logo()` |
| `valkey/__init__.py` | Re-exports `ValkeyWorker` and config types from the two sibling modules |
| `valkey/valkey_config.py` | Typed config structs + `read_valkey_config()` (YAML; raises `RuntimeError` on invalid file, creates defaults only if missing) + `generate_glide_config()` (schema→`GlideClientConfiguration`). Imports `glide` unguarded → hard `ImportError` with install hint if `glide` is absent |
| `valkey/valkey_async_worker.py` | `ValkeyWorker(AsyncTaskProcessor)` + stream/connection logic. Imports `glide` and `scietex.logging.AsyncValkeyHandler` unguarded at module top |
| `valkey/schemas.py` | `Heartbeat` msgpack schema |

## Notable module boundaries

- **Core ⇄ Valkey**: the only core→Valkey edge is the guarded re-export in
  `__init__.py`. Core modules never import `valkey`. Direction is
  Valkey → core (`ValkeyWorker` extends `AsyncTaskProcessor`).
- **Worker ⇄ task_handler**: `async_tasks_processor.py` imports
  `task_handler`; `task_handler` no longer references `basic_async_worker`
  (handlers receive a `TaskHandlerContext`, so there is no runtime or type
  cycle).
- **Worker ⇄ runtime components**: `basic_async_worker.py` imports
  `manager_runtime` and `logging_lifecycle`, which hold only a back-reference
  to the worker under `TYPE_CHECKING` (no runtime cycle).
- **Valkey internal split**: config schema/loader (`valkey_config.py`) is
  independent of the worker (`valkey_async_worker.py`); `valkey_config` can be
  used/tested without a worker, but not without `glide`.
- **Package ⇄ external `scietex.logging`**: `basic_async_worker.py` and
  `valkey/valkey_async_worker.py` attach external logging handlers. The worker
  treats them uniformly through `start_logging()`/`stop_logging()` +
  `handler.name` (via `LoggingLifecycle`).
- **Stale artifacts present in the tree** (not source): `build/`,
  `src/scietex.service.egg-info/`, `*.pyc` under `src` (including
  `redis_async_worker`, `utils/managers`, `utils/helpers` — modules removed by
  the `@Manager` refactor), `.tox/`, `.coverage`. Ignore when reading the map.
