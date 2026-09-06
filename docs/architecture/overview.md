# Overview

`scietex.service` is an asyncio worker/daemon framework. A consuming project
subclasses one of the worker classes, optionally implements handlers, and runs
an asyncio loop. There is **no CLI and no long-lived process of its own** — it
is a library whose entry point is the consumer's own `main()`.

## Major subsystems

| Subsystem | Location | Responsibility |
|---|---|---|
| Worker core | `src/scietex/service/basic_async_worker.py` | `BasicAsyncWorker`: identity, state machine, signal handling, async logging setup, manager discovery/runtime, default heartbeat/watchdog/cleanup hooks, restart/shutdown orchestration |
| Manager runtime | `src/scietex/service/manager.py` | `@Manager` class-decorator and `ManagerStatus`; wraps an async method into a managed loop that the worker runs as a task with restart-on-error semantics |
| Logging helpers | `src/scietex/service/logging.py` | `LoggerStatus` enum and `parse_logging_level()` string/int normalization |
| Task processing | `src/scietex/service/async_tasks_processor.py` | `AsyncTaskProcessor`: in-process bounded task queue, concurrency limit, handler registry/dispatch, timeout watchdog, drain/requeue on shutdown |
| Task handler contract | `src/scietex/service/task_handler/` | `TaskHandler` ABC + typed schemas `TaskData`, `TaskResult`, `TaskTimeout`, `TaskTracker` (frozen `msgspec.Struct`) |
| Valkey integration | `src/scietex/service/valkey/` | `ValkeyWorker` (stream transport over glide), typed Valkey config schema + YAML loader + schema→glide converter (`valkey_config.py`), `Heartbeat` schema |
| Utilities | `src/scietex/service/utils/` | `prepare_conf_dir()` config-dir resolution (`conf.py`); ASCII logo printer (`logo.py`) |
| Public surface | `src/scietex/service/__init__.py` | Re-exports core symbols; guarded optional import of Valkey exports |
| Async logging backend (external) | `scietex.logging` package (dependency) | `AsyncBaseHandler` (console), `AsyncValkeyHandler` (Valkey stream logs), `AsyncBrokerHandler`, `ScietexFormatter` |

## How subsystems interact

```
                 consumer application (examples/*, user code)
                                  │  subclasses / instantiates
                                  ▼
        BasicAsyncWorker ─────────┼───────── Manager runtime (@Manager)
        state machine, signals,   │         discovers decorated methods
        logger + async handlers   │         → runs each as asyncio.Task
                                  ▼
        AsyncTaskProcessor  (extends worker; adds TaskManager +
        TaskQueueManager managers, task_queue, running_tasks)
              │                                │
              │ registers/starts handlers      │ process_task dispatch
              ▼                                ▼
        TaskHandler  ◄──────────────────  task_handler.schemas
        (supports/handle/start/stop)     (TaskData/TaskResult/...)
              ▲
              │ (fetch_tasks / return_task_to_queue overridden)
        ValkeyWorker — glide GlideClient — Valkey streams/groups
              │
              └── AsyncValkeyHandler (scietex.logging) — log stream
```

Interaction notes:

- **Workers own the loop.** `BasicAsyncWorker` provides the only place where
  asyncio tasks are created for periodic/background behavior (manager tasks,
  logger tasks inside handlers).
- **Handlers are invoked by the processor, not by the worker.** Dispatch is
  type-based: first active handler whose `supports(task_type)` returns `True`
  wins.
- **Valkey transport is isolated** in the `valkey` subpackage; `ValkeyWorker`
  only *overrides* hooks (`fetch_tasks`, `return_task_to_queue`,
  `heartbeat`, `initialize`, `cleanup`) that the core defines as no-ops.
- **Async logging crosses the package boundary**: the worker attaches handlers
  from the external `scietex.logging` package and drives their
  `start_logging()`/`stop_logging()` lifecycle (`BasicAsyncWorker._logger_start_handlers` /
  `_logger_shut_down_handlers`).

## Application entry points

The package is a library. Each runnable artifact is a consumer:

| Entry | Class used | Behavior |
|---|---|---|
| `examples/async_service.py` | `BasicAsyncWorker` + custom `@Manager("cruncher")` | Minimal daemon; prints logo; runs managers until SIGINT/SIGTERM |
| `examples/async_task_processor.py` | `AsyncTaskProcessor` + three `TaskHandler`s + in-memory source | Feeds tasks from an in-memory list, processes concurrently |
| `examples/valkey_async_service.py` | `ValkeyWorker` | Connects to Valkey, consumes a task stream |

Pattern (all examples and README follow it):

```python
async def main():
    worker = MyWorker(...)  # constructed INSIDE a running loop
    await worker.start()  # spawns "Start" task → RUNNING
    await worker.events["exit"].wait()


asyncio.run(main())  # SIGINT/SIGTERM → exit() → STOPPED
```

Two important constraints derive from `BasicAsyncWorker.__init__` /
`_setup_signal_handlers` (`basic_async_worker.py:417-427`):

1. The worker **must be constructed inside a running event loop**
   (`asyncio.get_running_loop()` is called in `__init__`).
2. Signal handlers are registered per instance via
   `loop.add_signal_handler(SIGINT/SIGTERM)`; the **last constructed worker in
   a process owns the signals** (see [`hotspots.md`](./hotspots.md) §H6).

## Important runtime processes

Runtime consists of several concurrent asyncio task groups, all within one
process/loop:

| Process (asyncio task / queue) | Spawned by | Runs until |
|---|---|---|
| `Start` task → `_startup()` | `BasicAsyncWorker.start()` | state → `RUNNING` (or init failure → `stop()`) |
| `Stop` task → `_shutdown()` | `BasicAsyncWorker.stop()` / signal | state → `STOPPED`, `exit` event set |
| Manager task `Heartbeat` → `_heartbeat_manager` | `_start_managers()` | cancelled on shutdown |
| Manager task `Watchdog` → `_watchdog_manager` | `_start_managers()` | cancelled on shutdown |
| Manager task `TaskManager` → `task_manager` (processor only) | `_start_managers()` | cancelled on shutdown |
| Manager task `TaskQueueManager` → `task_queue_manager` (processor only) | `_start_managers()` | cancelled on shutdown |
| Per-logger console worker (`scietex.logging` `AsyncBaseHandler._console_logging_worker`) | `_logger_start_handlers()` → handler `start_logging()` | handler `stop_logging()` during shutdown |
| Per-logger Valkey log worker (`AsyncBrokerHandler._worker` → connects, `xadd`) | same | handler `stop_logging()` during shutdown |
| Per-task worker task (`handle_task` wrapper) | `AsyncTaskProcessor.task_manager` | task `handle()` returns/raises, or watchdog cancellation |

Ownership summary: the **worker owns** manager tasks and the internal
`task_queue`/`running_tasks`; the **logging handlers own** their internal
queues/worker tasks and each has its own GlideClient (Valkey); **task handlers
own** their initialization state (`is_ready`) but not their own tasks — they run
inline inside processor-created tasks.

`UNKNOWN` — no evidence of a separate thread pool or multiprocessing anywhere
in the package; concurrency is purely cooperative asyncio.
