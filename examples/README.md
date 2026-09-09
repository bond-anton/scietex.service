# Examples

Runnable blueprints for the `scietex.service` package. Each example is a
self-contained `main()` you can run with `python -m examples.<name>`.
All of them follow the same lifecycle pattern:

```python
await worker.start()
await worker.events["exit"].wait()
```

Stop any example with `SIGINT` (Ctrl+C) or `SIGTERM`.

| Example | Requires Valkey | Demonstrates |
|---|---|---|
| [`basic_worker.py`](#basic_workerpy) | no | `BasicWorker` with a custom `@Manager` loop and `heartbeat`/`watchdog`/`cleanup` overrides |
| [`manager_cleanup.py`](#manager_cleanuppy) | no | `@Manager(name=..., cleanup=...)` with a visible teardown action on shutdown |
| [`manager_collision.py`](#manager_collisionpy) | no | `@Manager` name-collision WARNING: two managers sharing one `name=`, first definition wins (AR-068) |
| [`task_processor.py`](#task_processorpy) | no | `TaskProcessor` with multiple task handlers, `TaskData`/`TaskResult`/`TaskTimeout`, concurrent processing |
| [`named_task_handlers.py`](#named_task_handlerspy) | no | Registering one handler class as multiple named instances (AR-053) |
| [`stateful_handler.py`](#stateful_handlerpy) | no | A stateful handler that mutates shared state injected via `**handler_kwargs` |
| [`valkey_async_service.py`](#valkey_async_servicepy) | yes | `ValkeyWorker` consuming a task stream via a programmatic `ValkeyConfig` |
| [`valkey_pubsub_worker.py`](#valkey_pubsub_workerpy) | yes | A `ValkeyWorker` subclass that also subscribes to PubSub control channels |

## basic_worker.py

```bash
python -m examples.basic_worker
```

The minimal daemon. Subclasses `BasicWorker`, overrides
`initialize`/`heartbeat`/`watchdog`/`cleanup`, and adds a custom
`@Manager(name="cruncher")` loop that pulls simulated numbers and pushes a
result once per second. Constructs the worker with a `WorkerConfig`.

## manager_cleanup.py

```bash
python -m examples.manager_cleanup
```

Demonstrates a `@Manager` with a `cleanup=` callable (AR-067). The
`DataService` subclass opens a simulated session in `initialize()`, and its
`@Manager(name="data_pump", cleanup=close_session)` loop pushes simulated
batches while that session is open. On graceful shutdown the manager is
cancelled and its `cleanup` callable runs, logging `Manager cleanup: session
closed` — visible proof the seam executes outside the worker's own `cleanup`.

## manager_collision.py

```bash
python -m examples.manager_collision
```

Demonstrates the AR-068 manager name-collision warning. The `CollidingService`
subclass declares two `@Manager(name="worker")` methods — `_primary_loop` and
`_duplicate_loop` — that independently pick the same `name=`. On startup,
`ManagerRuntime` discovery logs a WARNING naming the colliding manager and the
class/method it was found on, and only the first (most-derived, first-declared)
manager runs: you see `[primary] tick` every second and never `[duplicate] tick`.

## task_processor.py

```bash
python -m examples.task_processor
```

A concurrent task processor. Defines three handlers —
`DataProcessingHandler`, `ReportGenerationHandler`, and
`ImageProcessingHandler` — each declaring its `supported_tasks`, and an
`InMemoryTaskSource` that simulates an external queue. `TaskProcessorService`
overrides `fetch_tasks()` (returns `bool`) to drain the source and
`return_task_to_queue()` to re-queue timed-out tasks. The example also shows
per-task timeouts via `TaskTimeout` (the `resize_image` task is configured to
time out).

## named_task_handlers.py

```bash
python -m examples.named_task_handlers
```

Demonstrates the AR-053 `add_task_handler(HandlerClass, name="...")` form.
A single `NamedTaskHandler` class is registered twice — as `"alpha"` and
`"beta"` — with its `supported_tasks` derived from `self.name`, so one class
serves two disjoint task-type slices.

## stateful_handler.py

```bash
python -m examples.stateful_handler
```

Demonstrates stateful handlers. `add_task_handler(CountingHandler,
counter=shared_counter)` injects a shared mutable `SharedCounter` via
`**handler_kwargs`. The handler increments the counter on every handled task,
and the same object is re-injected on every (re)instantiation, so its state
survives a stop/start cycle (runtime-mutated instance state would not).

## valkey_async_service.py

```bash
python -m examples.valkey_async_service
```

A `ValkeyWorker` configured with a programmatic `ValkeyConfig`
(`ValkeyBaseConfig` + `ValkeyAdvancedConfig`). Connects to a Valkey server at
`localhost:6379`, creates the task consumer group, and consumes the
`scietex:{service_name}:tasks` stream. Requires a running Valkey/Redis server
and the `valkey` extra:

```bash
pip install "scietex.service[valkey]"
```

## valkey_pubsub_worker.py

```bash
python -m examples.valkey_pubsub_worker
```

A `ValkeyWorker` subclass that also subscribes to the PubSub control channels
(`scietex:{service_name}:{instance_id}` and `scietex:broadcast`). It builds a
`GlideClientConfiguration` with
`generate_glide_config(..., listening=True)` and passes a
`parse_control_message` callback, then injects it as the `valkey_config`.
Requires a running Valkey server and the `valkey` extra.
