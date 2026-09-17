# Lifecycles

Runtime lifecycles: startup, normal operation, shutdown, cleanup, background
workers, and resource ownership. Facts unless marked *analysis* or `UNKNOWN`.

## Worker lifecycle state machine

States: `ServiceStatus` (STOPPED → STARTING → RUNNING → STOPPING → STOPPED).
The state, `start_time`, both lifecycle events, and the pending stop-task guard
are owned by `WorkerLifecycle` (`lifecycle.py`, extracted in AR-087); the
transitions are driven by `BasicWorker`'s `_startup`/`_shutdown` orchestrators
(`basic_worker.py`), which write `self._lifecycle.state` and delegate
`request_exit()`/`force_stopped()`.

Two coordination events exist per worker in `self.events` (a read-only
`MappingProxyType` view of two `asyncio.Event`s owned by `WorkerLifecycle`):
`"exit_requested"` (set by `exit()`) and `"exit"` (set when fully stopped).

Signal handling (SIGINT/SIGTERM) is owned by `SignalHandler`
(`signal_handler.py`, extracted in AR-087). Registration/removal is
coordinated through a module-level `weakref.WeakKeyDictionary` keyed by the
running event loop with a **last-worker-wins** rule: when two workers share one
loop, the second worker's `setup()` becomes the new owner of the loop's
SIGINT/SIGTERM handlers, and the first worker's later `remove()` is a no-op —
so one worker's shutdown cannot silently unregister another worker's
graceful-shutdown handlers. Weak keys mean a garbage-collected loop drops its
entry with no explicit cleanup.

### Startup

Public: `worker.start()` (453). It:
1. Guards: if RUNNING or STARTING → warn and return.
2. If STOPPING/STOPPED → registers signal handlers (`_setup_signal_handlers`,
   478) and spawns task `"Start"` running `_startup()` (398).

`_startup()`:
1. If not STOPPED, waits (0.1 s poll) for a prior shutdown to finish.
2. Sets STARTING; prints logo.
3. `LoggingLifecycle.start_handlers()` — starts each async handler not yet
   running, with `logger_handler_timeout`.
4. `initialize()` (388) — subclass hook; must return truthy.
   - `TaskProcessor.initialize` (567) starts every registered task handler
     (`_start_task_handler`, awaited per handler).
   - `ValkeyWorker.initialize` (444) calls super then `connect()` and creates
     the consumer group (`xgroup_create`, `make_stream=True`; swallows
     "already exists" errors).
5. `_register_instance()` (633) — subclass hook, runs only after
   `initialize()` succeeded (transport/client exists) and before managers
   start. Base is a no-op; `ValkeyWorker` overrides it to `SADD` its
   `instance_id` into the worker registry set (best-effort: a failure logs a
   WARNING and does not fail startup).
6. Sets `start_time` (UTC) — before the managers start, so the heartbeat
   manager's immediate first beat is not skipped by the `start_time` guard
   (AR-049).
7. `ManagerRuntime.start_managers()` — discover `@Manager`s by walking the
   class MRO and reading each class's own `__manager_registry__` via
   `ManagerRuntime.iter_manager_definitions()` (manager/runtime.py:49), start
   each as a named task, then set state = RUNNING.

Failure: if `initialize()` returns `False` → `RuntimeError("Initialization
failed")` → `_startup` calls `stop()` → shutdown begins. If `_startup` is
cancelled, it logs, forces `_force_stopped()` (STOPPED + `exit` event), and
re-raises — no stranded STARTING state (AR-017).

> Ordering note: `initialize()` runs **before** `_register_instance()`, which
> runs **before** `ManagerRuntime.start_managers()` (steps 4–6). Managers and
> handlers may depend on resources created by `initialize()` (e.g. a Valkey
> client), so this ordering removes the previous startup race (see §H5,
> resolved); instance registration is deferred until after `initialize()` so
> the transport/client exists.

### Normal operation

- Manager tasks run their decorated method in `while True`
  (`ManagerRuntime.run_manager`, manager/runtime.py:138). Each iteration is the
  method body; built-ins sleep then act:
  - Heartbeat → `heartbeat()` every `heartbeat_interval`.
  - Watchdog → `watchdog()` every `watchdog_interval`.
  - TaskManager → pull one task per pass (bounded by `max_concurrent_tasks`).
  - TaskQueueManager → `fetch_tasks()` when queue not full, sleep.
- Task handler instances are idle between dispatches; `is_ready` gates dispatch.

### Shutdown

Signal (`SIGINT`/`SIGTERM`) → `SignalHandler`, which invokes the worker's
`_request_exit` (370) delegator → `WorkerLifecycle.request_exit()`, which
spawns a single `"StopTask"` running `exit()` (582); `exit()` sets
`exit_requested` and calls `stop()`. Repeat signals are deduplicated: a pending
stop task or an already-set `exit_requested` short-circuits so only one
shutdown runs (AR-033). The dedup guard lives in
`WorkerLifecycle.request_exit()`.

`stop()` (538):
- STOPPED → clear/set exit events, remove signal handlers
  (`_remove_signal_handlers`, 560 → `SignalHandler.remove()`), return.
- STOPPING → set exit event if `exit_requested`, return.
- RUNNING/STARTING → spawn task `"Stop"` running `_shutdown()` (491).

`_shutdown()`:
1. State = STOPPING.
2. `ManagerRuntime.stop_managers()` — cancel each
   manager task; wait per-manager up to `manager_shutdown_timeout` (default 2 s).
3. `_unregister_instance()` (643) — subclass hook, runs after managers stop
   and before `cleanup()` teardown, deliberately while the transport is still
   open (`cleanup()` may disconnect it). Base is a no-op; `ValkeyWorker`
   overrides it to `SREM` its `instance_id` from the worker registry set
   (best-effort: a failure logs a WARNING and does not fail shutdown).
4. `cleanup()` — subclass hook. Chain:
   - `TaskProcessor.cleanup` (603): drain `task_queue` (items fetched from
     a durable transport stay pending there and are redelivered on restart);
     cancel running per-task workers (wait up to the configured
     `task_cancellation_timeout`, default 5 s); requeue only if the handler
     actually stopped and `canceled_action=="requeue"`; stop all task handlers
     (`_stop_task_handler`, per-handler 5 s timeout).
   - `ValkeyWorker.cleanup` (480): super then `disconnect()` (close glide
     client).
5. `LoggingLifecycle.shut_down_handlers()` — stop each async logging handler
   with per-handler timeout; overall `loggers_timeout =
   handlers × logger_handler_timeout + 1`.
6. `start_time = None`; state = STOPPED.
7. If `exit_requested` was set → clear it, set `exit` event.

If `_shutdown` is cancelled, it logs "Shutdown task cancelled", forces
`_force_stopped()` (STOPPED + `exit` event if `exit_requested` was set), then
re-raises — so a cancelled shutdown never strands the worker in STOPPING
(AR-017, §H7).

### `exit()` vs waiting

`exit()` returns immediately after spawning the shutdown task; the canonical
pattern `await worker.events["exit"].wait()` blocks until `_shutdown` sets the
`exit` event.

## Manager lifecycle (per manager)

States: `ManagerStatus` STARTING → RUNNING → STOPPING → STOPPED (terminal),
with a give-up path to terminal FAILED (AR-063) when the retry budget is
exhausted, tracked by `ManagerRuntime` (manager/runtime.py).

1. `ManagerRuntime.start_manager` (220): if task exists → debug-return; set
   STARTING, clear error, `create_task(run_manager(name, manager))`.
2. `ManagerRuntime.run_manager` (138): logs start; `while True: await
   manager.method(self.worker)`.
3. On method exception (non-`CancelledError`): record error (177), increment
   `consecutive_failures`, and retry after `manager_restart_backoff` (196) —
   the manager gives up when `consecutive_failures > manager_max_retries`
   (default 5), i.e. on the (max_retries+1)-th consecutive failure (178–186).
   A successful iteration resets `consecutive_failures` to 0 (198–199), so the
   retry budget counts **consecutive** failures only. The retry runs **inside
   the same task**; the manager never cancels itself.
4. `CancelledError` → clean stop. `finally` (202–218): set STOPPING, run
   optional `manager.cleanup(self.worker)`, set STOPPED (or FAILED, AR-063, if
   the manager gave up in step 3), remove the task from tracking.

## Task handler lifecycle

`TaskHandler` (`task_handler/basic.py`): `start()` (91) → `initialize()` →
`_is_initialized` set from its return; `is_ready` True only if initialize
returned True. `stop()` (105) → `cleanup()` → `_is_initialized=False`.
Processor starts handlers in `initialize` (before RUNNING and before managers
start) and stops them in `cleanup` (during shutdown). Handlers may also be
added/removed at runtime via `add_task_handler` (spawns async start when
RUNNING) / `remove_task_handler`.

## Task lifecycle state

Per-task running state — the `TaskTracker` (holding the worker `asyncio.Task`,
its `TaskData`, and a monotonic start time) and the cancel reason — is owned by
`TaskLifecycle` (`task_lifecycle.py`), composed by `TaskProcessor.__init__`
(AR-088). `TaskProcessor.running_tasks` delegates to
`TaskLifecycle.trackers()` and returns a **snapshot**, not a live view, so
callers may iterate it while tasks are added, removed, or cancelled.

The tracker map and the cancel-reason map have joined lifetimes but are popped
independently. `TaskLifecycle.remove_tracker` drops a tracker without
consuming its cancel reason, and `TaskLifecycle.take_cancel_reason` consumes
the reason. This split lets the watchdog drop the tracker of a task whose
handler ignored cancellation while leaving the `"timeout"` reason in place for
the eventual ack, so the transport can still distinguish a deliberate cancel
from a timeout. The task manager consumes the reason with
`take_cancel_reason` when it acks the transport entry from `handle_task`'s
`finally`.

## Async logging handler lifecycle

- `BasicWorker.__init__` attaches `ConsoleHandler` (console);
  `ValkeyWorker` builds and attaches the `AsyncValkeyHandler` lazily on the
  first successful `connect()` (via `_ensure_logging_handler`, not in
  `__init__`).
- Lifecycle is owned by `LoggingLifecycle` (log_handlers/lifecycle.py): started in
  `start_handlers` (startup), stopped in `shut_down_handlers` (shutdown), each
  bounded by `logger_handler_timeout`.
- The external `scietex.logging` handlers (>= 2.0.0) are restartable in place:
  `start_logging()`/`stop_logging()` may be called repeatedly on the same event
  loop. `start_handlers` starts each handler whose recorded status is not
  RUNNING; `shut_down_handlers` calls the idempotent `stop_logging()` and
  records STOPPED. `statuses` tracks STOPPED/RUNNING/FAILED per handler name
  (a handler that fails to start is recorded FAILED so it is retried on the
  next `start_handlers`, AR-020).

## Resource ownership map

| Resource | Owner | Acquired | Released |
|---|---|---|---|
| Logger + async handlers | worker (via `LoggingLifecycle`) | `__init__` / startup | shutdown step 5 |
| Manager asyncio tasks | worker (via `ManagerRuntime`) | `ManagerRuntime.start_managers` | `ManagerRuntime.stop_managers` |
| Internal task queue | `TaskProcessor` | `__init__` | drained in `cleanup` |
| Per-task running tracker + cancel reason (`TaskLifecycle`) | `TaskLifecycle` (composed by `TaskProcessor`) | `register`/`mark_cancelled` | `remove_tracker`/`take_cancel_reason` on task completion; drained in `cleanup` |
| Task transport (`TaskProcessor._transport`) | `TaskProcessor` (default `InMemoryTransport`) / `ValkeyWorker` (injects `ValkeyTransport`) | `__init__` | drained in `cleanup` |
| Task handler instances | processor (created per handler name) | `initialize` | `cleanup` |
| Handler `is_ready` state | each `TaskHandler` | `start()` | `stop()` |
| GlideClient (`ValkeyWorker.client`) | worker | `initialize`→`connect` | `cleanup`→`disconnect` |
| Transport health state (`TransportHealth`) | worker (via `ValkeyWorker.transport_health`) | `__init__` | `mark_disconnected` on `disconnect` |
| Per-task leases (`TaskLeaseManager`) | worker (injected into `ValkeyTransport`) | `fetch`/`recover_pending_tasks` (enqueue-accept) | `ack`/`on_drain`/`requeue`, or TTL expiry |
| Per-task status records (`TaskStatusStore`) | worker (injected into `ValkeyTransport`) | `on_started`/`on_progress` | TTL expiry (`task_tracking_ttl`) |
| Worker registry-set membership (`SADD`/`SREM`) | worker (via `_register_instance`/`_unregister_instance`) | startup step 5 (`_register_instance`) | shutdown step 3 (`_unregister_instance`) |
| Logging `AsyncValkeyHandler` worker loop | worker (via `LoggingLifecycle`) | `connect()` → `handler.start_logging()` | shutdown (`stop_logging`) |
| Signal handlers (SIGINT/SIGTERM) | loop, owned by the last worker to call `setup()` (via `SignalHandler`'s weak-key registry) | `start()` (`_setup_signal_handlers` → `SignalHandler.setup()`) | `stop()` (`_remove_signal_handlers` → `SignalHandler.remove()`, no-op unless owner) |

## Exit contract

The `exit` event is set **iff** an exit was requested — by `exit()` or by a
SIGINT/SIGTERM signal — and `exit_requested` is then cleared. A bare `stop()`
performs the full graceful shutdown (managers stopped, `_unregister_instance`,
`cleanup()`, loggers stopped, state `STOPPED`) but deliberately leaves `exit`
unset: it is the lower-level primitive and does not represent a requested exit.
Consequently `await worker.stop()` followed by
`await worker.events["exit"].wait()` **hangs** — this is by design, not a bug.

The canonical programmatic exit is `await worker.exit()`. `exit()` does **not**
block: it sets `exit_requested` and triggers shutdown via `stop()`, then returns
immediately. Callers that must wait for the worker to be fully stopped should
await `worker.events["exit"]` (see `docs/basic_worker.md`, "Stopping").

The library does **not** own loop or process exit. It never registers an
`atexit`/loop-close hook and never closes the loop. If the loop ends without
`exit()` or `stop()` — the main coroutine returns or raises, or `sys.exit()` is
called while the worker is `RUNNING` — `_shutdown()` never runs and worker-level
cleanup does not happen. The embedder owns process exit and must guarantee
`exit()`/`stop()` (for example via `try`/`finally`, or by relying on the signal
path).

| Exit path | `_shutdown` runs? | `cleanup()` / `_unregister_instance()` / log flush | `exit` event |
|---|---|---|---|
| SIGINT / SIGTERM | yes | yes | set |
| `await worker.exit()` | yes | yes | set |
| `await worker.stop()` | yes | yes | **unset** |
| loop returns / unhandled exception / `sys.exit()` | **no** | **no — resources leak** | unset |
| SIGKILL | no (uncatchable) | no | unset |
