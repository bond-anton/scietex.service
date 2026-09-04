# Hotspots — areas deserving deeper architectural investigation

Locations where the codebase shows structural tension. Each entry records
**what the code does** and **why it is significant**; it does **not** propose a
fix. Items are *analysis* unless stated otherwise. Verified-by-execution facts
are flagged.

## H1. `BasicAsyncWorker` is a large, multi-responsibility class

- **Location:** `src/scietex/service/basic_async_worker.py:58` (~770 lines of
  the 827-line module).
- **What:** a single class owns: identity/configuration, the lifecycle state
  machine, signal registration, async-logging handler lifecycle, the manager
  discovery + task runtime (`_run/_start/_stop/_restart_manager`), startup and
  shutdown orchestration, and the default heartbeat/watchdog hooks.
- **Why significant:** every subclass inherits all of these; the class is the
  de-facto "service container". Extension points are interleaved with
  machinery, and manager-task bookkeeping lives in the same object as
  domain hooks. Hard to reason about or extend in isolation.

## H2. Manager error-handling relies on private per-worker bookkeeping

- **Location:** `basic_async_worker.py:504-589` (`_run_manager`,
  `_start_manager`, `_stop_manager`, `_restart_manager`).
- **What:** managers are restarted "automatically on error": any non-cancelled
  exception is recorded in `__manager_errors` and `_restart_manager` is awaited
  inside the except branch.
- **Why significant:** restart is unbounded (no backoff/limit) → a persistently
  failing manager yields a restart loop that also re-runs its `finally`
  cleanup each cycle. The error record `__manager_errors` is only cleared on the
  next `_start_manager`. Behavior under repeated failure is unverified
  (`UNKNOWN`).

## H3. Manager "restart" path appears to cancel the running task itself

- **Location:** `basic_async_worker.py:580-589` (`_restart_manager`) calling
  `_stop_manager` (560), which does `self.__manager_tasks[name].cancel()` and
  then `await asyncio.wait_for(self.__manager_tasks[name], ...)`.
- **What:** when a manager method raises, the except branch cancels and awaits
  the **same currently-executing task** (`asyncio` treats `CancelledError` as
  `BaseException`, so the surrounding `except Exception` cannot contain it).
- **Why significant:** the documented "restart on error" (README, docstrings,
  AGENTS) contradicts the mechanics. A controlled simulation of this exact
  control flow (self-cancel inside the except branch) ended the manager task
  with `CancelledError` and never spawned a replacement — i.e. the automatic
  restart appears ineffective. Flagged for verification in the real worker
  (`UNKNOWN` until exercised).

## H4. Worker logging lifecycle is not resumable after shutdown

- **Location:** `basic_async_worker.py:465-492`
  (`_logger_shut_down_handlers`), plus external
  `scietex.logging/basic_handler.py` (`stop_logging()` calls `self.close()`).
- **What:** after shutdown, each `AsyncBaseHandler` is closed, yet
  `__loggers_statuses[handler]` is set to `LoggerStatus.RUNNING` (line 492,
  not STOPPED). A later `_logger_start_handlers` skips handlers whose recorded
  status is RUNNING.
- **Why significant:** the state model for logging handlers and the actual
  handler lifecycle are inconsistent; restarting the same worker instance after
  a stop leaves the logger non-functional. Tension between "service restartable"
  (state machine supports STOPPED → RUNNING again) and handler one-shotness.

## H5. Managers are started before `initialize()` completes

- **Location:** `basic_async_worker.py:636-640` (`_startup`: `_start_managers`
  precedes `initialize()`).
- **What:** manager tasks are spawned and begin running before the subclass
  `initialize()` hook returns; `ValkeyWorker` connects to Valkey only inside
  `initialize()`.
- **Why significant:** built-in managers defensively no-op (heartbeat checks
  `start_time`/`client`; task intake checks `client`), but the contract is
  implicit — a custom `@Manager` or task handler that depends on
  `initialize()`-created resources can race. Relies on ordering luck rather
  than an explicit barrier.

## H6. Single-worker-per-process assumption (signal + event ownership)

- **Location:** `basic_async_worker.py:417-427` (`_setup_signal_handlers`,
  called from `__init__`); `events` property (181).
- **What:** every constructed worker registers SIGINT/SIGTERM on the running
  loop; later registrations replace earlier ones, so only the **last
  constructed** worker reacts to signals. The `events` dict is handed out by
  reference (external code can set/clear it).
- **Why significant:** the framework implicitly assumes one worker per process.
  Multiple workers in one process (allowed by the constructor) break signal
  shutdown and share one event loop. Also, `__init__` calls
  `asyncio.get_running_loop()`, so workers cannot be constructed outside a
  running loop.

## H7. Shutdown can stall or be skipped on cancellation

- **Location:** `basic_async_worker.py:679-720` (`_shutdown`).
- **What:** `_shutdown` has no rollback if it is cancelled mid-way (e.g. during
  `_stop_managers`); its `except asyncio.CancelledError` swallows the
  cancellation without re-raising or forcing STOPPED/`exit`.
- **Why significant:** a second stop/exit during shutdown may leave the state at
  STOPPING and `exit` unset, and `start()` in that state waits on a poll loop.
  Cleanup ordering (managers → cleanup → loggers) is sequential and
  timeout-guarded, but an unexpected cancellation path is not.

## H8. Task completion results are dropped; retry/duplicate semantics are loose

- **Location:** `async_tasks_processor.py:489-502` (`handle_task` inner
  wrapper) and `process_task` (433); `watchdog` (530); `ValkeyWorker.fetch_tasks`
  ack-on-enqueue (`valkey_async_worker.py:386`).
- **What:** `TaskResult` is logged and discarded — never acked to the
  transport, published, or stored. Valkey stream entries are `XACK`+`XDEL`ed as
  soon as they enter the in-process queue, so a crash/exception after that point
  loses the task. On timeout, watchdog cancels the task and re-`XADD`s it
  regardless of whether the handler actually stopped (handler may swallow
  cancellation); requeue appends to the stream tail (ordering lost).
- **Why significant:** the distributed contract is effectively
  at-most-once-once-enqueued with re-enqueue on timeout, but nothing enforces
  dedup, ordering, or result delivery. Whether callers rely on results is
  `UNKNOWN`.

## H9. `ValkeyWorker` opens two independent GlideClients

- **Location:** `valkey_async_worker.py:139-147` (constructs
  `AsyncValkeyHandler`, which owns a client) and 192 (`self._client =
  await GlideClient.create(...)`).
- **What:** task/heartbeat traffic uses one `GlideClient`; log traffic uses a
  second client inside the external logging handler, each configured from the
  same `_client_config`.
- **Why significant:** two connection lifecycles must be managed and torn down
  (worker `disconnect()` in `cleanup`; handler `disconnect()` in
  `stop_logging`). Connection failure modes and resource accounting are split
  across two owners.

## H10. Connection handling treats ping-failure and exception asymmetrically

- **Location:** `valkey_async_worker.py:180-202` (`connect`), 251-280
  (`initialize`).
- **What:** on `GlideClient.create` exception, `connect` returns False and
  leaves `_client=None` (initialize then fails); on a **failed PING**,
  `connect` prints and returns False but leaves `_client` set, so
  `initialize()` (which only checks `client is not None`) proceeds as if
  connected.
- **Why significant:** connectivity success is not consistently propagated;
  heartbeat/fetch later guard on `client` truthiness. Reconnect logic exists
  only in `fetch_tasks`'s exception handler (436-437), nowhere else.

## H11. Task stream and group are namespaced per `worker_id`

- **Location:** `valkey_async_worker.py:151-153`.
- **What:** stream, group, and consumer names embed `service_name` **and**
  `worker_id`. Two `ValkeyWorker`s with different `worker_id`s read **different
  streams**; horizontal scale-out requires replicas that share the same
  `(service_name, worker_id)` to form a consumer group on one stream.
- **Why significant:** the intended distribution model ("distributed task
  queues", docstring) is more precisely *replicated consumers of a per-identity
  stream*. This naming couples scaling topology to the worker_id identity and
  to the runtime key conventions.

## H12. Usage documentation diverges from the code

- **Location:** `docs/basic_async_worker.md`, `docs/async_task_processor.md`,
  `docs/task_handler.md`, `docs/valkey_async_worker.md`, plus `README.md`,
  `AGENTS.md`.
- **What (verified vs source):** MRO discovery order described as
  most-derived→base but code iterates `reversed(mro)` (base first); config-dir
  precedence tables omit `SCIETEX_CONFIG_DIR` / XDG / create-default steps;
  `TaskResult.processed_at` and `Heartbeat.timestamp` defaults are evaluated at
  import time (single shared value), though docs imply per-record timestamps;
  `valkey_async_worker.md` shows `purge_tasks()` in shutdown though no shutdown
  path calls it; one doc example claims `_find_task_handler("email")` returns a
  handler whose `supported_tasks` is `["send_email"]`.
- **Why significant:** the docs are the intended-architecture record for the
  next review; discrepancies mark where design intent and implementation have
  drifted.

## H13. Duplicated/inconsistent developer configuration

- **Location:** `pyproject.toml` (`[tool.pytest.ini_options] pythonpath =
  ["src"]`) vs `pytest.ini` (`pythonpath = .`, `addopts = --capture=no`); the
  `build/` and `src/scietex.service.egg-info/` artifacts and stale `*.pyc`
  (e.g. removed `redis_async_worker`, `utils/managers`, `utils/helpers`) sit
  inside the repo tree.
- **Why significant:** two pytest config sources with different `pythonpath`
  can make test-time imports diverge from installed-package imports; stale
  compiled modules hint at a removed Redis worker and older manager subsystem
  that the current `@Manager` design replaced (relevant to change history).

## H14. `pyaml` dependency is unused; `DEFAULT_MAX_OUTPUT_QUEUE_SIZE` is dead

- **Location:** `pyproject.toml:18` (declares `pyaml>=26.2.1`); `manager.py:12`.
- **What:** no import of `pyaml` exists in `src/`, `tests/`, or `examples/`;
  YAML handling uses `msgspec.yaml`. `DEFAULT_MAX_OUTPUT_QUEUE_SIZE` is never
  referenced.
- **Why significant:** possible legacy cruft in the declared dependency surface
  that the next review may want to trace (including whether `scietex.logging`
  requires `pyaml` transitively — `UNKNOWN`).

## H15. Typed schemas contain time/identity defaults evaluated once at import

- **Location:** `task_handler/schemas.py:60` (`TaskResult.processed_at =
  datetime.now(timezone.utc)`), `valkey/schemas.py:38` (`Heartbeat.timestamp`).
- **What:** `msgspec.Struct` defaults are class-level; `datetime.now(...)` runs
  once at import, so any `TaskResult`/`Heartbeat` constructed without an
  explicit timestamp shares the module-import timestamp.
- **Why significant:** the fields *look* like per-instance times. Handlers that
  omit `processed_at` all stamp the same value. (Valkey heartbeat always passes
  an explicit timestamp, so the practical impact is limited to task results.)

## H16. Task processing result/error policy is centralized but coarse

- **Location:** `async_tasks_processor.py:433-474` (`process_task`),
  284-369 (handler registry).
- **What:** one `process_task` maps any handler failure to a single `TaskResult
  (status="error")` string; no structured error taxonomy, no retry count, no
  per-task backoff; dispatch is first-match over active handlers by
  `supports()` while registration keys are unrelated to task types.
- **Why significant:** policy for every failure/timeout/retry decision is
  concentrated in the processor's watchdog + a stringly error field; the
  `TaskHandler` contract (declares `supported_tasks`, implements `handle`) has
  no way to express partial progress or custom requeue intent, so all recovery
  is delegated to `return_task_to_queue` at the processor level.
