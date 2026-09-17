# Repository / package structure

Layout of the repository and the Python package.

## Top-level repository layout

| Path | Contents |
|---|---|
| `src/scietex/service/` | The package (see below). Marked PEP 561 via `py.typed` |
| `examples/` | Runnable blueprints: `basic_worker.py`, `manager_cleanup.py`, `manager_collision.py`, `task_processor.py`, `named_task_handlers.py`, `stateful_handler.py`, `valkey_async_service.py`, `valkey_pubsub_worker.py`, `valkey_perf.py`, `progress_and_cancel.py` |
| `tests/` | Pytest suite: two test packages (`valkey/`, `task_processor/`), each with a shared `_helpers.py`, plus top-level `test_*.py` modules; Valkey tests mock `GlideClient` (no server needed) |
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
| `__init__.py` | Public API. Always exports `__version__`, `BasicWorker`, `TaskProcessor`, `Manager`, `WorkerConfig`, `TaskProcessorConfig`, and the transport seam (`TaskTransport`, `TaskSink`, `InMemoryTransport`). In a guarded `try/except ImportError` block, additionally imports and re-exports the Valkey surface (`ValkeyWorker`, config types including `ValkeyWorkerConfig` and `ValkeyPubSubConfig`) and sets the `VALKEY_AVAILABLE` flag. The guard makes the package importable without `valkey-glide`, while non-`ImportError` exceptions propagate so real Valkey bugs surface at import (AR-019) |
| `version.py` | Single source `__version__ = "4.3.0"` (also read by setuptools dynamic version) |
| `config.py` | `WorkerConfig` + `TaskProcessorConfig` — immutable `msgspec.Struct`s (`frozen=True`) replacing the old per-worker constructor kwargs. Also holds the MIN/MAX/DEFAULT constants (single source of truth for timing/retry bounds, the task-queue defaults `DEFAULT_MAX_TASKS_QUEUE_SIZE=100` / `DEFAULT_MAX_CONCURRENT_TASKS=10`, and the task-level timing fields `task_timeout`/`task_queue_fetch_timeout`/`task_cancellation_timeout`, AR-062). `__post_init__` validates ranges and raises `msgspec.ValidationError` on an out-of-range value; a `None` field resolves to its `DEFAULT_*` constant at read time |
| `_validation.py` | Shared `validate_range()` helper used by both `config.py` and `valkey/config.py` (AR-079) — promoted from the private `config._validate_range` so the Valkey package no longer imports a private core symbol |
| `manager/__init__.py` | `Manager` class-decorator (`name` required, non-empty string — the manager's identity — plus optional `cleanup` callable; stores `method`), `ManagerStatus` enum, and `MANAGER_REGISTRY_ATTR` (`"__manager_registry__"`: a per-class ordered `list[Manager]` stored on the owner's own `__dict__`, populated by `Manager.__set_name__`). Also exports `register_manager(owner, method, *, name, cleanup=None, attribute_name=None, replace=True)` — the explicit post-creation registration path (`name` required keyword-only; `attribute_name` only binds `owner.<attribute_name>`, never the identity; `replace=True` upserts in place by `name`, `replace=False` appends) |
| `manager/runtime.py` | `ManagerRuntime(worker)`: manager discovery by walking `type(worker).__mro__` and reading each class's own `__manager_registry__` list (`iter_manager_definitions`, yielding `(manager.name, manager)` most-derived-first; logging a WARNING on a `name=` collision so the first/most-derived definition wins — AR-068, and on a plain attribute that shadows a base manager without re-decorating — AR-086), start/stop bookkeeping (`statuses`/`tasks`/`errors`), and the bounded restart-on-error loop (`run_manager`), which ends a give-up manager in terminal `FAILED` (AR-063) and exposes `failed_managers`. Extracted from `BasicWorker` (AR-003) |
| `log_handlers/lifecycle.py` | `LoggingLifecycle(worker)`: async logging-handler registration and start/stop with `statuses` bookkeeping. Extracted from `BasicWorker` (AR-003) |
| `lifecycle.py` | `WorkerLifecycle(worker)`: owns the `ServiceStatus` state machine, `start_time`, both lifecycle events (`exit_requested`/`exit`), and the pending stop-task guard, exposing `state`/`start_time`/`events` properties plus `request_exit()`/`force_stopped()`/`_wait_until_stopped()`. Also hosts `WAIT_FOR_SERVICE_STOPPED_DELAY`. Extracted from `BasicWorker` (AR-087) |
| `signal_handler.py` | `SignalHandler(worker)`: owns SIGINT/SIGTERM registration/removal (`setup()`/`remove()`) with a module-level `weakref.WeakKeyDictionary` last-worker-wins ownership registry keyed by the running loop, so one worker's `remove()` cannot unregister another worker's handlers. Extracted from `BasicWorker` (AR-087) |
| `basic_worker.py` | `BasicWorker` + `ServiceStatus`. Owns identity/config plus startup/shutdown orchestration, and composes four components: `ManagerRuntime`, `LoggingLifecycle`, `WorkerLifecycle` (AR-087: state machine, events, stop-task guard), and `SignalHandler` (AR-087: signal registration). Built-in `Heartbeat`/`Watchdog` are module-level manager functions registered via `register_manager(BasicWorker, ..., attribute_name="_heartbeat_manager"/"_watchdog_manager")`, not `@Manager`-decorated methods. Config storage (AR-069): the base declares a class-level `_config_type` (`WorkerConfig`) and constructs it when `config=None`; `TaskProcessor`/`ValkeyWorker` override it to `TaskProcessorConfig`/`ValkeyWorkerConfig` so the base stores the correct concrete type and subclasses no longer re-store. Delegates to `ManagerRuntime`/`LoggingLifecycle`/`WorkerLifecycle`/`SignalHandler` directly (the AR-045 forwarding wrappers were removed; the worker calls the components' methods itself) |
| `log_handlers/__init__.py` | `LoggerStatus` (STOPPED/RUNNING/FAILED), `parse_logging_level()`, `DEFAULT_LOGGING_LEVEL` |
| `task_lifecycle.py` | `TaskLifecycle` — worker-free owner of the per-task running tracker map and the cancel-reason map, extracted from `TaskProcessor` (AR-088). API: `register(task_id, tracker)`, `trackers()` (returns a snapshot `Mapping[UUID, TaskTracker]`), `get(task_id)`, `mark_cancelled(task_id, reason)`, `remove_tracker(task_id)`, `take_cancel_reason(task_id)`. `remove_tracker` and `take_cancel_reason` are deliberately separate so the watchdog can drop a tracker whose handler ignored cancellation while leaving the `"timeout"` reason for the eventual ack. `TaskTracker` remains in `task_handler/runtime.py`; `CancelReason` remains in `task_handler/schemas.py` |
| `task_processor.py` | `TaskProcessor(BasicWorker)`. Task registry maps (`__task_handlers_map` name→`(class, handler_kwargs)`, `__task_handlers` active instances), bounded task queue (accessed via `enqueue_task`/`dequeue_task`/`task_queue_empty`/`task_queue_full` — the raw `task_queue` is no longer public), `running_tasks` (a `UUID → TaskTracker` snapshot delegated to the composed `TaskLifecycle`), `@Manager("TaskManager") task_manager`, `@Manager("TaskQueueManager") task_queue_manager`, `process_task()`, watchdog timeout logic, handler start/stop, drain-and-cancel cleanup. Composes a `TaskLifecycle` (per-task running tracker + cancel reason, AR-088) and a `TaskTransport` (keyword-only `transport=`, default `InMemoryTransport`); the six delivery hooks (`fetch_tasks`, `return_task_to_queue`, `on_task_started`, `on_task_completed`, `_write_task_progress`, `_on_queue_drain_task_processing`) remain as thin delegators to it (AR-072) |
| `transport.py` | Core delivery seam (AR-072): `TaskSink` Protocol (`task_queue_full`/`enqueue_task`), `TaskTransport` Protocol (`fetch`/`requeue`/`release`/`on_started`/`ack`/`on_progress`/`on_drain`), and `InMemoryTransport` — the default deque-backed in-process transport (feed it with `submit(task_id, task_data)`). Imports only `task_handler.schemas`; no `glide` dependency |
| `task_handler/__init__.py` | Re-exports `TaskHandler`, `TaskHandlerContext`, `TaskCapabilities`, `CancelTaskHandler`, `CancelTaskRequest`, `CancelTaskResponse`, `CancelCallback`, `CancelOutcome`, `CANCEL_TASK_TYPE`, `CancelReason`, `TaskData`, `TaskResult`, `TaskTimeout`, `TaskStatus`, `TaskEnvelope`, and the wire helpers `encode_task_envelope`/`decode_task_envelope`. `TaskTracker` is also re-exported, from `runtime.py` |
| `task_handler/context.py` | `TaskHandlerContext` — frozen dataclass (`service_name`, `instance_id`, `logger`) passed to handlers instead of the full worker |
| `task_handler/capabilities.py` | `TaskCapabilities` — frozen dataclass carrying the per-call task id and progress writer; `report_progress(value)` clamps to `[0.0, 100.0]` and forwards to the transport hook. Passed to `handle` as the keyword-only `capabilities` argument |
| `task_handler/schemas.py` | Frozen `msgspec.Struct` wire schemas (see [`components.md`](./components.md)), including `TaskEnvelope` — the versioned transport envelope (AR-064) |
| `task_handler/runtime.py` | In-memory runtime handles: `TaskTracker` (holds the running `asyncio.Task`, its `TaskData`, and a monotonic start time). Unlike `schemas.py`, these are live process-local objects and are never serialized |
| `task_handler/wire.py` | Transport-agnostic wire helpers `encode_task_envelope(task_data)` / `decode_task_envelope(payload)` — msgpack-encode/decode the versioned `TaskEnvelope` that wraps a serialized `TaskData` (AR-064). Shared by `ValkeyWorker` today and any future MQTT/Kafka worker |
| `task_handler/basic.py` | `TaskHandler(ABC)`; imports `.context`, `.capabilities`, `.schemas` (no reference to `BasicWorker`) |
| `task_handler/cancel.py` | Built-in `cancel_task` handler: `CancelTaskHandler` + `CancelTaskRequest`/`CancelTaskResponse` payload schemas and the `CancelOutcome`/`CancelCallback` types. Transport-agnostic — delegates cancellation to a callback injected by the owning processor |
| `utils/__init__.py` | Re-exports `prepare_conf_dir`, `print_scietex_logo` |
| `utils/config.py` | `prepare_conf_dir()` + `_resolve_xdg_path()` config-dir search |
| `utils/logo.py` | ASCII `LOGO` template and `print_scietex_logo()` |
| `valkey/__init__.py` | Re-exports `ValkeyWorker`, config types, and `purge_task_stream`/`PurgeResult` from the sibling modules |
| `valkey/_glide.py` | Private module — the single guarded `from glide import (...)` re-exporting the full union of glide names used by the valkey package (incl. aliases `GlideConnectionError`, `GlideTimeoutError`). Importing it raises `ImportError` with an install hint when `valkey-glide` is absent (AR-048) |
| `valkey/config.py` | Typed config structs (`ValkeyConfig`, `ValkeyBaseConfig`, `ValkeyPubSubConfig`, ...) + `ValkeyWorkerConfig` (worker-level config struct extending `TaskProcessorConfig`, incl. `claim_min_idle_ms` and `task_lease_ttl`, AR-062/AR-077) + `read_valkey_config()` (YAML; raises `RuntimeError` on invalid file, creates defaults only if missing) + `generate_glide_config()` (schema→`GlideClientConfiguration`). Imports its `glide` names from `valkey/_glide.py` (the single guarded import, AR-048) |
| `valkey/worker.py` | `ValkeyWorker(TaskProcessor)` + stream/connection logic. Composes a `ValkeyTransport` (assigned to `self._transport`) and the `TransportHealth`/`TaskLeaseManager`/`TaskStatusStore` collaborators; exposes `client_factory=` (AR-074), `transport_health`, and `valkey_config`. Imports its `glide` names from `valkey/_glide.py` (AR-048); the `scietex.logging.AsyncValkeyHandler` import is unguarded at module top |
| `valkey/transport.py` | `ValkeyTransport` — the Valkey implementation of the core `TaskTransport` Protocol (AR-072): stream intake (`fetch`), recovery (`recover_pending_tasks`), requeue/ack/progress/drain, and lease refresh (`refresh_leases`). Owns the entry-id map and `recovered` flag; receives the lease/status/health collaborators by injection |
| `valkey/health.py` | `TransportHealth` (AR-075) — connection-health supervisor: aggregates glide failures, owns the single reconnect path (`recover()` with lock dedup + cooldown), and emits one CRITICAL per sustained outage (`critical_report()`). Imports no `glide` types |
| `valkey/lease.py` | `TaskLeaseManager` (AR-073) — per-entry lease store (`key`/`write`/`acquire`/`delete`/`refresh`) plus `derive_task_lease_ttl()` and the `LEASE_TTL_*`/`MIN_TASK_LEASE_TTL_SECONDS` constants |
| `valkey/tracking.py` | `TaskStatusStore` (AR-073) — per-task status records (`key`/`record_running`/`record_terminal`/`update_progress`) |
| `valkey/purge.py` | Standalone `purge_task_stream()` operational utility (read+ack+delete every stream entry, returning a `PurgeResult`); no runtime `glide` import (`TYPE_CHECKING` only), importing `GlideClient` from `valkey/_glide.py` (AR-048) |
| `valkey/schemas.py` | `Heartbeat` msgpack schema |

## Notable module boundaries

- **Core ⇄ Valkey**: the only core→Valkey edge is the guarded re-export in
  `__init__.py`. Core modules never import `valkey`. Direction is
  Valkey → core (`ValkeyWorker` extends `TaskProcessor`).
- **Worker ⇄ task_handler**: `task_processor.py` imports
  `task_handler`; `task_handler` no longer references `basic_worker`
  (handlers receive a `TaskHandlerContext`, so there is no runtime or type
  cycle).
- **Worker ⇄ runtime components**: `basic_worker.py` imports
  `manager/runtime`, `log_handlers/lifecycle`, `lifecycle`, and `signal_handler`,
  which hold only a back-reference to the worker under `TYPE_CHECKING` (no
  runtime cycle).
- **Valkey internal split**: config schema/loader (`valkey/config.py`) is
  independent of the worker (`worker.py`); both import their `glide` names
  through the shared guarded module `valkey/_glide.py` (AR-048). `valkey/config`
  can be used/tested without a worker, but not without `glide`.
- **Core ⇄ transport**: `transport.py` is the core delivery contract and imports
  only `task_handler.schemas`; `valkey/transport.py` implements it. The
  `ValkeyTransport` composes the Valkey-specific collaborators
  (`valkey/config`, `valkey/health`, `valkey/lease`, `valkey/tracking`), while
  core never imports any of them (AR-072).
- **Package ⇄ external `scietex.logging`**: `basic_worker.py` and
  `valkey/worker.py` attach external logging handlers. The worker
  treats them uniformly through `start_logging()`/`stop_logging()` +
  `handler.name` (via `LoggingLifecycle`).
- **Stale artifacts present in the tree** (not source): `build/`
  (`build/lib/scietex/service/` still contains `logo.py` — the flat logo that
  predates the `utils/` split — `valkey/valkey_async_worker_messaging.py`, and
  a `task_handlers/` directory), `src/scietex.service.egg-info/`, `*.pyc` under
  `src`, `.tox/`, `.coverage`. Ignore when reading the map.
