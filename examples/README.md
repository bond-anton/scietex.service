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
| [`task_processor.py`](#task_processorpy) | no | `TaskProcessor` with multiple task handlers, `TaskData`/`TaskResult`/`TaskTimeout`, concurrent processing |
| [`named_task_handlers.py`](#named_task_handlerspy) | no | Registering one handler class as multiple named instances (AR-053) |
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
