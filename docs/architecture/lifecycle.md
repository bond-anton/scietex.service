# Lifecycles

Runtime lifecycles: startup, normal operation, shutdown, cleanup, background
workers, and resource ownership. Facts unless marked *analysis* or `UNKNOWN`.

## Worker lifecycle state machine

States: `ServiceStatus` (STOPPED → STARTING → RUNNING → STOPPING → STOPPED).
Transitions are driven by `BasicAsyncWorker` (`basic_async_worker.py`).

Two coordination events exist per worker in `self.events`:
`"exit_requested"` (set by `exit()`) and `"exit"` (set when fully stopped).

### Startup

Public: `worker.start()` (652). It:
1. Guards: if RUNNING or STARTING → warn and return.
2. If STOPPING/STOPPED → spawns task `"Start"` running `_startup()` (611).

`_startup()`:
1. If not STOPPED, waits (0.1 s poll) for a prior shutdown to finish.
2. Sets STARTING; prints logo.
3. `_logger_start_handlers()` (429) — for each logger handler not yet running,
   `await handler.start_logging()` with `logger_handler_timeout`.
4. `_start_managers()` (591) — discover `@Manager`s via
   `_iter_manager_definitions()` (402) and start each as a named task.
5. `initialize()` (494) — subclass hook; must return truthy.
   - `AsyncTaskProcessor.initialize` (383) starts every registered task handler
     (`_start_task_handler`, awaited per handler).
   - `ValkeyWorker.initialize` (251) calls super then `connect()` and creates
     the consumer group (`xgroup_create`, `make_stream=True`; swallows
     "already exists" errors).
6. Sets `start_time` (UTC) and state = RUNNING.

Failure: if `initialize()` returns `False` → `RuntimeError("Initialization
failed")` → `_startup` calls `stop()` → shutdown begins. If `_startup` is
cancelled, it logs and re-raises (state may remain STARTING — *analysis*: no
reset branch).

> Ordering note (*analysis*): managers start **before** `initialize()` runs
> (steps 4 and 5). Built-in managers are safe because they no-op until
> `start_time`/`client` are set, but a user `@Manager` that touches resources
> created in `initialize()` can race startup (see §H5).

### Normal operation

- Manager tasks run their decorated method in `while True` (`_run_manager`,
  504). Each iteration is the method body; built-ins sleep then act:
  - Heartbeat → `heartbeat()` every `heartbeat_interval`.
  - Watchdog → `watchdog()` every `watchdog_interval`.
  - TaskManager → pull one task per pass (bounded by `max_concurrent_tasks`).
  - TaskQueueManager → `fetch_tasks()` when queue not full, sleep 0.01 s.
- Task handler instances are idle between dispatches; `is_ready` gates dispatch.

### Shutdown

Signal (`SIGINT`/`SIGTERM`) → `exit()` (764) sets `exit_requested`, calls
`stop()`.

`stop()` (722):
- STOPPED → clear/set exit events, return (no shutdown work).
- STOPPING → set exit event if `exit_requested`, return.
- RUNNING/STARTING → spawn task `"Stop"` running `_shutdown()` (679).

`_shutdown()`:
1. State = STOPPING.
2. `_stop_managers()` (601) — cancel each manager task; wait per-manager up to
   `manager_shutdown_timeout` (default 2 s).
3. `cleanup()` — subclass hook. Chain:
   - `AsyncTaskProcessor.cleanup` (397): drain `task_queue` →
     `return_task_to_queue` each item; cancel running per-task workers (wait up
     to `WORKER_TASK_CANCELLATION_TIMEOUT=5 s`); requeue if
     `canceled_action=="requeue"`; stop all task handlers
     (`_stop_task_handler`, per-handler 5 s timeout).
   - `ValkeyWorker.cleanup` (282): super then `disconnect()` (close glide
     client).
4. `_logger_shut_down_handlers()` (465): stop each async logging handler with
   per-handler timeout; overall `loggers_timeout =
   handlers × logger_handler_timeout + 1`.
5. `start_time = None`; state = STOPPED.
6. If `exit_requested` was set → clear it, set `exit` event.

If `_shutdown` is cancelled, it logs "Shutdown task cancelled" and swallows the
cancellation without re-raising (*analysis*: state can be left at STOPPING and
`exit` unset — see §H7).

### `exit()` vs waiting

`exit()` returns immediately after spawning the shutdown task; the canonical
pattern `await worker.events["exit"].wait()` blocks until `_shutdown` sets the
`exit` event.

## Manager lifecycle (per manager)

States: `ManagerStatus` STARTING → RUNNING → STOPPING → STOPPED.

1. `_start_manager` (539): if task exists → debug-return; set STARTING, clear
   error, `create_task(_run_manager(name, manager))`.
2. `_run_manager` (504): logs start; `while True: await manager.method(self)`.
3. On method exception (non-`CancelledError`): record error in
   `__manager_errors`, log, and `await self._restart_manager(name, manager)`.
4. `finally`: set STOPPING, run optional `manager.cleanup(self)`, set STOPPED.

> *Analysis* — the restart path (§H3) cancels and awaits the manager's **own**
> task (`_stop_manager` → `self.__manager_tasks[name].cancel()` then
> `wait_for` the same task), so in practice a raising manager appears to end as
> CancelledError rather than restarting. Deserves verification.

## Task handler lifecycle

`TaskHandler` (`task_handler/basic.py`): `start()` (95) → `initialize()` →
`_is_initialized` set from its return; `is_ready` True only if initialize
returned True. `stop()` (109) → `cleanup()` → `_is_initialized=False`.
Processor starts handlers in `initialize` (before RUNNING) and stops them in
`cleanup` (during shutdown). Handlers may also be added/removed at runtime via
`add_task_handler` (spawns async start when RUNNING) / `remove_task_handler`.

## Async logging handler lifecycle

- `BasicAsyncWorker.__init__` attaches `AsyncBaseHandler` (console);
  `ValkeyWorker.__init__` additionally attaches `AsyncValkeyHandler`.
- Started in `_logger_start_handlers` (startup), stopped in
  `_logger_shut_down_handlers` (shutdown), each bounded by
  `logger_handler_timeout`.
- The external `scietex.logging` handlers spawn their own internal worker tasks
  on `start_logging()` and call `self.close()` at the end of `stop_logging()`
  (`basic_handler.py:235`) — the handler is **closed** after shutdown.

> *Analysis* — after one shutdown the logger handlers are closed but
> `__loggers_statuses` is left as RUNNING (`_logger_shut_down_handlers`,
> basic_async_worker.py:492), so a second `start()` on the same instance skips
> restarting them. Instance reuse across start/stop cycles appears unreliable
> (§H4).

## Resource ownership map

| Resource | Owner | Acquired | Released |
|---|---|---|---|
| Logger + async handlers | worker | `__init__` / startup | shutdown step 4 |
| Manager asyncio tasks | worker | `_start_managers` | `_stop_managers` |
| Internal `task_queue`, `running_tasks` | `AsyncTaskProcessor` | `__init__` | drained in `cleanup` |
| Task handler instances | processor (created per handler name) | `initialize` | `cleanup` |
| Handler `is_ready` state | each `TaskHandler` | `start()` | `stop()` |
| GlideClient (`ValkeyWorker.client`) | worker | `initialize`→`connect` | `cleanup`→`disconnect` |
| GlideClient inside `AsyncValkeyHandler` | the log handler | `start_logging` (its `_worker`) | `stop_logging` (its `disconnect`) |
| Signal handlers (SIGINT/SIGTERM) | loop (per last-constructed worker) | `__init__` | never removed |

`UNKNOWN` — explicit process-exit path when a worker stops without a signal
(e.g. plain `stop()` from user code): the loop is not closed by the library;
consumer must manage loop/process exit.
