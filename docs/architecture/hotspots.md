# Hotspots — areas deserving deeper architectural investigation

Locations where the codebase shows structural tension. Each entry records
**what the code does** and **why it is significant**; it does **not** propose a
fix. Items are *analysis* unless stated otherwise. Verified-by-execution facts
are flagged. Entries resolved by the AR-003..AR-040 refactors are marked
**Resolved** and retained for change history.

## Status summary

| # | Status | Resolution |
|---|---|---|
| H1 | Resolved | AR-003 — split into `ManagerRuntime` + `LoggingLifecycle` |
| H2 | Resolved | AR-003 — bounded restart in `ManagerRuntime.run_manager` |
| H3 | Resolved | AR-003 — retry inlined; no self-cancel |
| H4 | Resolved | 2026-09-06 — handlers restartable in place |
| H5 | Resolved | AR-007 — `initialize()` before `ManagerRuntime.start_managers()` |
| H6 | Resolved | AR-015 + AR-008 — signals in `start()`; read-only views |
| H7 | Resolved | AR-017 — `_force_stopped()` forces STOPPED on cancellation |
| H8 | Resolved | AR-005 — at-least-once delivery |
| H9 | Resolved | AR-018 — single shared GlideClient shipped in v3 |
| H10 | Resolved | AR-006 + AR-010 — truthful `connect()` |
| H11 | Resolved | AR-023 — shared-queue topology (service-scoped stream/group) |
| H12 | Open | usage docs still drift (out of scope here) |
| H13 | Resolved | AR-013 — single pytest config |
| H14 | Resolved | AR-013 — `pyaml` dropped; dead constant removed |
| H15 | Resolved | AR-012 — per-instance `msgspec` timestamps |
| H16 | Resolved | AR-022 — structured error taxonomy fields on `TaskResult` |
| H17 | Resolved | AR-033 — single-exit-task guard (`_request_exit`, basic_worker.py:336) |
| H18 | Open | AR-031 — unused `name` param in `LoggingLifecycle.register_logger_handler` (log_handlers/lifecycle.py:40-57) |

## H1. `BasicWorker` is a large, multi-responsibility class

- **Location:** `src/scietex/service/basic_worker.py:55`.
- **What:** a single class owned: identity/configuration, the lifecycle state
  machine, signal registration, async-logging handler lifecycle, the manager
  discovery + task runtime, startup and shutdown orchestration, and the default
  heartbeat/watchdog hooks.
- **Why significant:** every subclass inherited all of these; the class was the
  de-facto "service container".

**Resolved (AR-003):** manager discovery/runtime and logging-handler lifecycle
were extracted to `ManagerRuntime` (manager/runtime.py) and `LoggingLifecycle`
(log_handlers/lifecycle.py). `BasicWorker` now keeps identity/config and the
state machine, delegating manager and logging-handler bookkeeping to the
extracted components directly.

## H2. Manager error-handling relies on private per-worker bookkeeping

- **Location:** was `basic_worker.py` (`_run_manager`/`_restart_manager`);
  now `manager/runtime.py:62-125`.
- **What:** managers were restarted "automatically on error" with unbounded
  restart and no backoff.
- **Why significant:** a persistently failing manager yielded an unbounded
  restart loop.

**Resolved (AR-003):** restart moved into `ManagerRuntime.run_manager`, which is
bounded by `manager_max_retries` (default 5) with a `manager_restart_backoff`
delay (default 1 s) between attempts; the error record lives in
`ManagerRuntime.errors`.

## H3. Manager "restart" path appears to cancel the running task itself

- **Location:** was `_restart_manager`; now inlined in
  `manager/runtime.py:62-125`.
- **What:** the old restart path cancelled and awaited the **same
  currently-executing task**.
- **Why significant:** a raising manager ended as `CancelledError` rather than
  restarting.

**Resolved (AR-003):** `run_manager` retries inside its own `while True` loop
(83–111) — the manager task never cancels itself; `CancelledError` stops it
cleanly and the `finally` block (116–125) runs cleanup and removes the task from
tracking.

## H4. Worker logging lifecycle is not resumable after shutdown

- **Location:** `log_handlers/lifecycle.py:104-133` (`shut_down_handlers`), plus
  external `scietex.logging` (`stop_logging()` calls `self.close()`).
- **What:** after shutdown, each `AsyncLoggingHandler` was closed yet recorded as
  RUNNING, so a later start skipped it.
- **Why significant:** the state model for logging handlers and the actual
  handler lifecycle were inconsistent.

**Resolved (2026-09-06):** `shut_down_handlers` records STOPPED, and
scietex.logging >= 2.0.0 handlers are restartable in place, so a second `start()`
restarts the same handler instances. See
`docs/reviews/architecture/2026-09-05.md`.

## H5. Managers are started before `initialize()` completes

- **Location:** was `_startup` (`_start_managers` preceded `initialize()`).
- **What:** manager tasks spawned and ran before the subclass `initialize()`
  hook returned; `ValkeyWorker` connects to Valkey only inside `initialize()`.
- **Why significant:** a custom `@Manager` or handler depending on
  `initialize()`-created resources could race.

**Resolved (AR-007):** `_startup` now calls `initialize()` before
`ManagerRuntime.start_managers()`.

## H6. Single-worker-per-process assumption (signal + event ownership)

- **Location:** was `basic_worker.py` (`_setup_signal_handlers` called
  from `__init__`); `events` property.
- **What:** every constructed worker registered SIGINT/SIGTERM on the running
  loop; only the last constructed worker reacted to signals; `events` was handed
  out by reference. `__init__` called `asyncio.get_running_loop()`.
- **Why significant:** the framework implicitly assumed one worker per process
  and could not construct workers outside a running loop.

**Resolved (AR-015 + AR-008):** signal handlers are registered in `start()`
(`_setup_signal_handlers`, basic_worker.py:319, Windows-safe no-op) and
removed in `stop()` (`_remove_signal_handlers`, 349); `__init__` no longer
touches the loop, so workers may be constructed outside a running loop.
`events` (basic_worker.py:143), `task_handlers` (task_processor.py:103)
and `running_tasks` (115) now return read-only `MappingProxyType` views.

## H7. Shutdown can stall or be skipped on cancellation

- **Location:** `basic_worker.py:470-515` (`_shutdown`).
- **What:** `_shutdown` has no rollback if it is cancelled mid-way (e.g. during
  `ManagerRuntime.stop_managers()`); its `except asyncio.CancelledError` swallows the
  cancellation without re-raising or forcing STOPPED/`exit`.
- **Why significant:** a second stop/exit during shutdown may leave the state at
  STOPPING and `exit` unset, and `start()` in that state waits on a poll loop.
  Cleanup ordering (managers → cleanup → loggers) is sequential and
  timeout-guarded, but an unexpected cancellation path is not.

**Resolved (AR-017):** `_shutdown` (and `_startup`) now catch `CancelledError`,
call `_force_stopped()` (basic_worker.py:456) — which sets
`state = STOPPED`, clears `start_time`, and sets the `exit` event if
`exit_requested` — then re-raise, so a cancelled startup/shutdown always lands
in a terminal state and the worker can be restarted.

## H8. Task completion results are dropped; retry/duplicate semantics are loose

- **Location:** was `handle_task`/`process_task` + ack-on-enqueue in
  `fetch_tasks`.
- **What:** `TaskResult` was logged and discarded; Valkey stream entries were
  `XACK`+`XDEL`ed as soon as they entered the in-process queue, so a crash after
  that point lost the task.
- **Why significant:** the distributed contract was effectively at-most-once.

**Resolved (AR-005):** delivery is now at-least-once. `fetch_tasks`
(worker.py:489) records the entry id in `_task_entry_ids` without
acking; `on_task_completed` (565) `XACK`+`XDEL`s the entry only after the
handler's work terminates. `_recover_pending_tasks` (426) uses `XAUTOCLAIM` on
the first fetch to redeliver entries left pending by a crash. Enqueue is
non-blocking (`enqueue_task`); a full queue defers the entry to the next poll
(AR-016).

## H9. `ValkeyWorker` opens two independent GlideClients

- **Location:** `worker.py` (constructs `AsyncValkeyHandler`, which
  owns a client) and `connect` (`GlideClient.create`).
- **What:** task/heartbeat traffic uses one `GlideClient`; log traffic uses a
  second client inside the external logging handler, each configured from the
  same `_client_config`.
- **Why significant:** two connection lifecycles must be managed and torn down
  (worker `disconnect()` in `cleanup`; handler `disconnect()` in
  `stop_logging`). Connection failure modes and resource accounting are split
  across two owners.

**Resolved (AR-018):** `ValkeyWorker` now runs a single `GlideClient` shared
with the logging handler. `_ensure_logging_handler`
(worker.py:168) constructs the `AsyncValkeyHandler` with the
worker's client injected (via the `scietex.logging>=2.0.0` client-injection
seam) on the first successful `connect()`, so the handler never owns or closes
the shared client; `disconnect()` (235) clears the handler's reference before
closing the client, making the worker the sole teardown owner. The handler is
reused across restarts, and `connect()` keeps it on the worker's current client
across reconnects.

## H10. Connection handling treats ping-failure and exception asymmetrically

- **Location:** `worker.py:190-233` (`connect`), 290-323
  (`initialize`).
- **What:** on `GlideClient.create` exception, `connect` returned False and left
  `_client=None`; on a **failed PING**, it previously left `_client` set, so
  `initialize()` (which only checks `client is not None`) proceeded as if
  connected.
- **Why significant:** connectivity success was not consistently propagated.

**Resolved (AR-006 + AR-010):** `connect()` assigns `_client` only after PING
succeeds (219) and closes a client that failed its ping (229-232), so
`self.client` truthiness is a reliable connectivity signal and a half-connected
worker is never observable.

## H11. Task stream and group are namespaced per `worker_id`

- **Location:** `worker.py` key construction (now ~129-133).
- **What:** in v3, stream, group, and consumer names embed `service_name` **and**
  `worker_id`. Two `ValkeyWorker`s with different `worker_id`s read **different
  streams**; horizontal scale-out required replicas that share the same
  `(service_name, worker_id)` to form a consumer group on one stream.
- **Why significant:** the intended distribution model ("distributed task
  queues", docstring) is more precisely *replicated consumers of a per-identity
  stream*. This naming coupled scaling topology to the worker_id identity and
  to the runtime key conventions.

*Resolved (AR-023, v4.0.0):* the key space is split. The task stream
(`scietex:{service}:tasks`) and consumer group (`scietex:{service}:task_group`)
are now **service-scoped**, so multiple replicas consume one shared queue.
The consumer (`scietex:{service}:{instance_id}`) and status key
(`scietex:{service}:{instance_id}:status`) remain **worker-scoped** per
auto-generated `instance_id`. A service-scoped worker registry set
(`scietex:{service}:workers`) was added (SADD on startup, SREM on shutdown),
and the XAUTOCLAIM recovery floor was raised to `DEFAULT_CLAIM_MIN_IDLE_MS = 1000`
so a replica's startup recovery does not reclaim entries a slow-but-alive
handler on another replica is still processing.

## H12. Usage documentation diverges from the code

- **Location:** `docs/basic_worker.md`, `docs/task_processor.md`,
  `docs/task_handler.md`, `docs/valkey_worker.md`, plus `README.md`,
  `AGENTS.md`.
- **What:** the usage docs are not kept in lockstep with the code. Discrepancies
  previously noted — MRO discovery order and import-time `processed_at` /
  `timestamp` defaults — are fixed in the source (AR-003, AR-012), but the
  usage docs themselves have not been re-verified against v3.1.0 in this
  rewrite.
- **Why significant:** the docs are the intended-architecture record; drift
  between usage guides and the code marks where design intent and implementation
  have diverged. (Out of scope for this architecture-map rewrite.)

## H13. Duplicated/inconsistent developer configuration

- **Location:** was `pyproject.toml` `[tool.pytest.ini_options]` vs `pytest.ini`.
- **What:** two pytest config sources with different `pythonpath` could make
  test-time imports diverge from installed-package imports.
- **Why significant:** divergent import paths between test and package.

**Resolved (AR-013):** `pytest.ini` was deleted; pytest configuration lives only
in `pyproject.toml` (`[tool.pytest.ini_options]`, lines 46-48).

## H14. `pyaml` dependency is unused; `DEFAULT_MAX_OUTPUT_QUEUE_SIZE` is dead

- **Location:** was `pyproject.toml:18` (`pyaml>=26.2.1`) and `manager/__init__.py:12`.
- **What:** no import of `pyaml` existed anywhere; `DEFAULT_MAX_OUTPUT_QUEUE_SIZE`
  was never referenced.
- **Why significant:** legacy cruft in the declared dependency surface.

**Resolved (AR-013):** `pyaml` was dropped and `pyyaml>=6.0` added (required by
`msgspec.yaml`); `DEFAULT_MAX_OUTPUT_QUEUE_SIZE` was removed from `manager/__init__.py`.

## H15. Typed schemas contain time/identity defaults evaluated once at import

- **Location:** was `task_handler/schemas.py` (`TaskResult.processed_at =
  datetime.now(...)`) and `valkey/schemas.py` (`Heartbeat.timestamp`).
- **What:** `msgspec.Struct` defaults were class-level; `datetime.now(...)` ran
  once at import, so instances without an explicit timestamp shared the import
  timestamp.
- **Why significant:** the fields *looked* like per-instance times.

**Resolved (AR-012):** both fields now use
`msgspec.field(default_factory=lambda: datetime.now(timezone.utc))`
(`task_handler/schemas.py:76`, `valkey/schemas.py:38`), producing a per-instance
value.

## H16. Task processing result/error policy is centralized but coarse

- **Location:** `task_processor.py:470-520` (`process_task`), 216-353
  (handler registry).
- **What:** one `process_task` maps any handler failure to a single `TaskResult
  (status="error")` string; no structured error taxonomy, no retry count, no
  per-task backoff; dispatch is first-match over active handlers by
  `supports()` while registration keys are unrelated to task types.
- **Why significant:** policy for every failure/timeout/retry decision is
  concentrated in the processor's watchdog + a stringly error field; the
  `TaskHandler` contract (declares `supported_tasks`, implements `handle`) has
  no way to express partial progress or custom requeue intent, so all recovery
  is delegated to `return_task_to_queue` at the processor level.

**Resolved (AR-022, v4):** `TaskResult` carries the error-taxonomy fields
`error_code`, `retryable`, `partial` (task_handler/schemas.py:78-81) — all
defaulting to "no extra information" so existing handlers keep working; the
redundant `retry_count`/`requeue` fields are dropped. `process_task` treats a
handler that *raises* as permanent (`retryable=False`) and passes a
handler-returned `TaskResult` through unchanged; framework-level failures
(empty `task` field, no matching handler) remain permanent. `handle_task`
executes a single retry: a `retryable=True` error is requeued via
`return_task_to_queue` before the transport entry is acked (XADD then XACK);
permanent errors are acked and dropped. Registration keys handlers by the
resolved handler name: `add_task_handler` takes the handler class plus an
optional keyword-only `name`, defaulting to `handler_class.__name__` as the
lifecycle key (single instance per resolved key, a duplicate resolved name
raises). The optional `name` lets the same class be registered under several
distinct keys, e.g. to split one class's task types across instances via
name-derived `supported_tasks`.
