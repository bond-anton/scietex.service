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
| [`valkey_pubsub_worker.py`](#valkey_pubsub_workerpy) | yes | A `ValkeyWorker` that also subscribes to PubSub control channels via `ValkeyPubSubConfig` |
| [`valkey_perf.py`](#valkey_perfpy) | yes | Single-process `ValkeyWorker` consumption-throughput benchmark |
| [`progress_and_cancel.py`](#progress_and_cancelpy) | yes | Progress reporting via `report_progress` and cancelling a running task with `cancel_task` |
| [`mqtt_worker.py`](#mqtt_workerpy) | MQTT | `MqttWorker` consuming tasks from a broker, with retained status and throttled progress publishing |
| [`mqtt_perf.py`](#mqtt_perfpy) | MQTT | Single-process `MqttWorker` consumption-throughput benchmark |

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
per-task timeouts via `TaskTimeout`: the `resize_image` task is configured to
time out after 1s with `timeout_action="discard"`, so it is dropped rather than
requeued and the example drains cleanly.

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

A `ValkeyWorker` that also subscribes to the PubSub control channels
(`scietex:{service_name}:{instance_id}` and `scietex:broadcast`). Listening is
expressed through the typed schema: `ValkeyConfig(pubsub_config=ValkeyPubSubConfig(listening=True, parse_control_message=...))`.
The worker subscribes its own client and delivers each message to the
`parse_control_message` callback; the directed channel uses the worker's own
`instance_id`. Requires a running Valkey server and the `valkey` extra.

## valkey_perf.py

```bash
python -m examples.valkey_perf --tasks 10000
```

A single-process `ValkeyWorker` consumption-throughput benchmark. A producer
preloads `N` tasks into the `scietex:{service_name}:tasks` stream (one `XADD`
per task), then the worker starts and drains them; only the drain is timed.
Reports end-to-end throughput and a median steady-state rate. Requires a
running Valkey/Redis server and the `valkey` extra:

```bash
pip install "scietex.service[valkey]"
```

Key knobs: `--tasks`/`-n` (total tasks preloaded then drained), `--host`/
`--port`, `--max-concurrent-tasks`, `--queue-size` (defaults to `--tasks` so
the whole backlog buffers without back-pressure), `--task-fetch-batch-size`,
`--task-timeout`, `--heartbeat-interval`, and `--keep-stream` (skip the
pre-load flush of the benchmark stream).

## progress_and_cancel.py

```bash
python -m examples.progress_and_cancel
```

A `ValkeyWorker` runs a long `long_job` handler that reports granular progress,
while a producer client submits the job and then cancels it. The producer
`XADD`s the job, polls its tracking record
(`scietex:{service_name}:task:{task_id}`) to watch `progress.value` climb, then
submits a `cancel_task` task whose payload is a msgpack `CancelTaskRequest`
naming the target id. The built-in `CancelTaskHandler` cancels the running
target, whose terminal status becomes `cancelled` and embeds the original
`TaskData` — the example decodes it and resubmits under a new id.

The handler reports progress through the per-call `TaskCapabilities` object the
processor passes to `handle` — `capabilities.report_progress(value)` — so no
registration-time injection is needed:
`worker.add_task_handler(LongJobHandler)`.
Cancellation needs a free worker slot for the cancel task itself, so the
example uses `max_concurrent_tasks=4`; with `1` the cancel request would queue
behind its target and degrade to `not_running`. Requires a running Valkey/Redis
server and the `valkey` extra:

```bash
pip install "scietex.service[valkey]"
```

## mqtt_worker.py

```bash
python -m examples.mqtt_worker --host 127.0.0.1
```

An `MqttWorker` consuming tasks from a local MQTT 5 broker. The worker
subscribes to `scietex/{service}/tasks` (QoS 2), persists each received message
to its durable file inbox, and drains it into the processor queue. A producer
client publishes a `TaskEnvelope` with the task id in the MQTT 5 user property
`scietex-task-id`, then subscribes to the per-task status topic
(`scietex/{service}/tasks/{task_id}/status`) and prints the lifecycle:
`queued` -> `running` -> `completed`. The `long_job` handler reports progress
through `capabilities.report_progress`, which the worker publishes as throttled
`TaskProgress` messages on `.../progress`.

Requires a running MQTT 5 broker and the `mqtt` extra:

```bash
pip install "scietex.service[mqtt]"
```

Note: on hosts where `localhost` resolves to IPv6 first, pass `--host 127.0.0.1`
if the broker only listens on IPv4.

## mqtt_perf.py

```bash
python -m examples.mqtt_perf --host 127.0.0.1 --tasks 10000
```

A single-process `MqttWorker` consumption-throughput benchmark. The worker
starts and subscribes first, then a producer publishes `N` `perf` tasks; the
timed window runs from the first publish until every task is acknowledged, so
it covers the full push -> inbox -> pull -> handler pipeline. Reports total
throughput and a median steady-state rate.

The durable inbox dominates the cost. On a local broker the file-backed inbox
sustains roughly 120-160 tasks/sec, while the default in-memory backend
(at-most-once, no disk) reaches roughly 4800-5000 tasks/sec -- a ~35x
difference. The benchmark defaults to the in-memory backend so it measures the
transport and handler pipeline in isolation; pass `--inbox-backend file` to
measure the durability cost. `--task-queue-manager-sleep-time` tunes the poll
interval (default 0.01s).

Requires a running MQTT 5 broker and the `mqtt` extra:

```bash
pip install "scietex.service[mqtt]"
```
