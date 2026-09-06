# Lifecycles

Runtime lifecycles: startup, normal operation, shutdown, cleanup, background
workers, and resource ownership. Facts unless marked *analysis* or `UNKNOWN`.

## Worker lifecycle state machine

States: `ServiceStatus` (STOPPED → STARTING → RUNNING → STOPPING → STOPPED).
Transitions are driven by `BasicAsyncWorker` (`basic_async_worker.py`).

Two coordination events exist per worker in `self.events` (a read-only
`MappingProxyType` view of two `asyncio.Event`s): `"exit_requested"` (set by
`exit()`) and `"exit"` (set when fully stopped).

### Startup

Public: `worker.start()` (671). It:
1. Guards: if RUNNING or STARTING → warn and return.
2. If STOPPING/STOPPED → registers signal handlers (`_setup_signal_handlers`,
   696) and spawns task `"Start"` running `_startup()` (625).

`_startup()`:
1. If not STOPPED, waits (0.1 s poll) for a prior shutdown to finish.
2. Sets STARTING; prints logo.
3. `_logger_start_handlers()` (558) → `LoggingLifecycle.start_handlers()` —
   starts each async handler not yet running, with `logger_handler_timeout`.
4. `initialize()` (575) — subclass hook; must return truthy.
   - `AsyncTaskProcessor.initialize` (480) starts every registered task handler
     (`_start_task_handler`, awaited per handler).
   - `ValkeyWorker.initialize` (388) calls super then `connect()` and creates
     the consumer group (`xgroup_create`, `make_stream=True`; swallows
     "already exists" errors).
5. `_start_managers()` (609) → `ManagerRuntime.start_managers()` — discover
   `@Manager`s via `ManagerRuntime.iter_manager_definitions()`
   (manager_runtime.py:39) and start each as a named task.
6. Sets `start_time` (UTC) and state = RUNNING.

Failure: if `initialize()` returns `False` → `RuntimeError("Initialization
failed")` → `_startup` calls `stop()` → shutdown begins. If `_startup` is
cancelled, it logs, forces `_force_stopped()` (STOPPED + `exit` event), and
re-raises — no stranded STARTING state (AR-017).

> Ordering note: `initialize()` runs **before** `_start_managers()` (steps 4
> and 5). Managers and handlers may depend on resources created by
> `initialize()` (e.g. a Valkey client), so this ordering removes the previous
> startup race (see §H5, resolved).

### Normal operation

- Manager tasks run their decorated method in `while True`
  (`ManagerRuntime.run_manager`, manager_runtime.py:62). Each iteration is the
  method body; built-ins sleep then act:
  - Heartbeat → `heartbeat()` every `heartbeat_interval`.
  - Watchdog → `watchdog()` every `watchdog_interval`.
  - TaskManager → pull one task per pass (bounded by `max_concurrent_tasks`).
  - TaskQueueManager → `fetch_tasks()` when queue not full, sleep.
- Task handler instances are idle between dispatches; `is_ready` gates dispatch.

### Shutdown

Signal (`SIGINT`/`SIGTERM`) → `exit()` (800) sets `exit_requested`, calls
`stop()`.

`stop()` (757):
- STOPPED → clear/set exit events, remove signal handlers
  (`_remove_signal_handlers`, 778), return.
- STOPPING → set exit event if `exit_requested`, return.
- RUNNING/STARTING → spawn task `"Stop"` running `_shutdown()` (713).

`_shutdown()`:
1. State = STOPPING.
2. `_stop_managers()` (617) → `ManagerRuntime.stop_managers()` — cancel each
   manager task; wait per-manager up to `manager_shutdown_timeout` (default 2 s).
3. `cleanup()` — subclass hook. Chain:
   - `AsyncTaskProcessor.cleanup` (498): drain `task_queue` (items fetched from
     a durable transport stay pending there and are redelivered on restart);
     cancel running per-task workers (wait up to
     `WORKER_TASK_CANCELLATION_TIMEOUT=5 s`); requeue only if the handler
     actually stopped and `canceled_action=="requeue"`; stop all task handlers
     (`_stop_task_handler`, per-handler 5 s timeout).
   - `ValkeyWorker.cleanup` (423): super then `disconnect()` (close glide
     client).
4. `_logger_shut_down_handlers()` (566) →
   `LoggingLifecycle.shut_down_handlers()` — stop each async logging handler
   with per-handler timeout; overall `loggers_timeout =
   handlers × logger_handler_timeout + 1`.
5. `start_time = None`; state = STOPPED.
6. If `exit_requested` was set → clear it, set `exit` event.

If `_shutdown` is cancelled, it logs "Shutdown task cancelled", forces
`_force_stopped()` (STOPPED + `exit` event if `exit_requested` was set), then
re-raises — so a cancelled shutdown never strands the worker in STOPPING
(AR-017, §H7).

### `exit()` vs waiting

`exit()` returns immediately after spawning the shutdown task; the canonical
pattern `await worker.events["exit"].wait()` blocks until `_shutdown` sets the
`exit` event.

## Manager lifecycle (per manager)

States: `ManagerStatus` STARTING → RUNNING → STOPPING → STOPPED, tracked by
`ManagerRuntime` (manager_runtime.py).

1. `ManagerRuntime.start_manager` (130): if task exists → debug-return; set
   STARTING, clear error, `create_task(run_manager(name, manager))`.
2. `ManagerRuntime.run_manager` (62): logs start; `while True: await
   manager.method(self.worker)`.
3. On method exception (non-`CancelledError`): record error (92), increment
   `consecutive_failures`, and retry after `manager_restart_backoff` (110) —
   bounded by `manager_max_retries` (default 5), after which the manager gives
   up (94–101). The retry runs **inside the same task**; the manager never
   cancels itself.
4. `CancelledError` → clean stop. `finally` (116–125): set STOPPING, run
   optional `manager.cleanup(self.worker)`, set STOPPED, remove the task from
   tracking.

## Task handler lifecycle

`TaskHandler` (`task_handler/basic.py`): `start()` (91) → `initialize()` →
`_is_initialized` set from its return; `is_ready` True only if initialize
returned True. `stop()` (105) → `cleanup()` → `_is_initialized=False`.
Processor starts handlers in `initialize` (before RUNNING and before managers
start) and stops them in `cleanup` (during shutdown). Handlers may also be
added/removed at runtime via `add_task_handler` (spawns async start when
RUNNING) / `remove_task_handler`.

## Async logging handler lifecycle

- `BasicAsyncWorker.__init__` attaches `AsyncBaseHandler` (console);
  `ValkeyWorker.__init__` additionally attaches `AsyncValkeyHandler`.
- Lifecycle is owned by `LoggingLifecycle` (logging_lifecycle.py): started in
  `start_handlers` (startup), stopped in `shut_down_handlers` (shutdown), each
  bounded by `logger_handler_timeout`.
- The external `scietex.logging` handlers (>= 1.1.0) are restartable in place:
  `start_logging()`/`stop_logging()` may be called repeatedly on the same event
  loop. `start_handlers` starts each handler whose recorded status is not
  RUNNING; `shut_down_handlers` calls the idempotent `stop_logging()` and
  records STOPPED. `statuses` tracks STOPPED/RUNNING/FAILED per handler name
  (a handler that fails to start is recorded FAILED so it is retried on the
  next `start_handlers`, AR-020).

## Resource ownership map

| Resource | Owner | Acquired | Released |
|---|---|---|---|
| Logger + async handlers | worker (via `LoggingLifecycle`) | `__init__` / startup | shutdown step 4 |
| Manager asyncio tasks | worker (via `ManagerRuntime`) | `_start_managers` | `_stop_managers` |
| Internal task queue, `running_tasks` | `AsyncTaskProcessor` | `__init__` | drained in `cleanup` |
| Task handler instances | processor (created per handler name) | `initialize` | `cleanup` |
| Handler `is_ready` state | each `TaskHandler` | `start()` | `stop()` |
| GlideClient (`ValkeyWorker.client`) | worker | `initialize`→`connect` | `cleanup`→`disconnect` |
| GlideClient inside `AsyncValkeyHandler` | the log handler | `start_logging` (its `_worker`) | `stop_logging` (its `disconnect`) |
| Signal handlers (SIGINT/SIGTERM) | loop (per started worker) | `start()` (`_setup_signal_handlers`) | `stop()` (`_remove_signal_handlers`) |

`UNKNOWN` — explicit process-exit path when a worker stops without a signal
(e.g. plain `stop()` from user code): the loop is not closed by the library;
consumer must manage loop/process exit.
