# Data flows

Primary data flows. Each flow lists source → processing → destination, key
transformations, and any async boundaries (queues/events/tasks).

## F1. In-process task processing (core flow)

**Source:** external caller/enqueue sites — subclass `fetch_tasks()` puts
`TaskData` into the worker's internal queue, or a producer calls
`enqueue_task()` directly.

**Processing chain:**
1. `TaskProcessor.task_queue_manager` (`@Manager("TaskQueueManager")`) — while the queue is not full, invokes the
   subclass/`ValkeyWorker` `fetch_tasks()`; then sleeps
   `task_queue_manager_sleep_time` (default 0.01 s).
2. `TaskProcessor.task_manager` (`@Manager("TaskManager")`) — if `len(running_tasks) < max_concurrent_tasks`,
   pops `task_data` off `task_queue` with a fetch timeout of
   `task_queue_fetch_timeout` (default 1 s),
   wraps `handle_task` in an `asyncio.Task`, and records the tracker via
   `TaskLifecycle.register(task_data_id(task_data), TaskTracker(...))` (the
   composed lifecycle state, AR-088) — the id is derived from
   `TaskData.task_id`.
3. `TaskExecutor._handle_task` calls
   `process_task(task_data)`.
4. `process_task`: guards the empty-`task` case first — an empty
   `task_data.task` returns `TaskResult(status="error", error="Task data must
   contain 'task' field")` — then selects a handler with
   `_find_task_handler` (`handler.supports(task_type)`, first match among
   **active/started** handlers).
5. Dispatch is gated by `handler.is_ready`: only a found **and
   initialized** handler runs `await handler.handle(task_data, capabilities=...)`.
   A `handle()`
   exception is converted into `TaskResult(status="error", error=str(e))` with
   the default `retryable=False` — a **raised exception is permanent** (a
   handler that wants a retry must return a `retryable=True` result
   explicitly), so an unhandled exception cannot create an infinite requeue
   loop under retry-once. No handler / not ready → error result
   ("No handler found for task type ...").

**Destination:** the `TaskResult` is returned to `TaskExecutor._handle_task`,
whose `finally` runs `TaskExecutor._settle`, which
removes the tracker (`TaskLifecycle.remove_tracker`), calls
`task_queue.task_done()`, and passes the consumed cancel reason
(`TaskLifecycle.take_cancel_reason`) to the ack. Then: a
`retryable=True` error result is requeued via
`return_task_to_queue(task_data)` **before** acking
(`TaskExecutor._apply_retry_policy`) — the
retry copy is made durable (XADD) before the original is dropped (XACK) — and
then `on_task_completed(task_data, task_result)` is invoked — the
transport-agnostic ack/result-sink seam. The base delegates to `transport.ack`;
`ValkeyTransport.ack` does `XACK`+`XDEL` on the stream entry. A requeue failure
is logged and the entry is still acked (the retry copy is lost, but the entry
must not stay pending forever).

**Async boundaries:** `asyncio.Queue` (bounded, `queue_size` default 100) between
intake and dispatch; per-task `asyncio.Task`; concurrency cap
`max_concurrent_tasks` (default 10). When `TaskProcessorConfig.auto_tune` is
`True` and `max_concurrent_tasks` is unset, the cap is derived from
`os.cpu_count()` at startup instead of the static default.

## F2. Valkey task intake / transport (ValkeyWorker)

**Source:** external producer writes task entries into Valkey stream
`scietex:{service}:tasks`. Entry shape: one field-value pair per
message — **field = the fixed `TASK_FIELD` (`b"task"`), value =
msgpack-encoded versioned `TaskEnvelope` wrapping a `TaskData`** (written by
`ValkeyTransport.requeue` via `encode_task_envelope`, `valkey/transport.py`);
the task id travels inside the wrapped `TaskData.task_id`.

**Processing chain (`ValkeyTransport.fetch`):**
1. On the first call only, `recover_pending_tasks` runs `XAUTOCLAIM` to
   re-enqueue entries left pending by a previous crash (at-least-once).
2. `XREADGROUP` on group `...:task_group`, consumer `...`, key `>`, count 1,
   `block_ms=1000`.
3. Per entry: decode value →
   `decode_task_envelope(payload)` (→ `TaskData`; an invalid payload or unknown
   version returns `None` and the entry is skipped with an ERROR log);
   `enqueue_task(task_data)` (non-blocking; a full queue leaves the entry
   pending — its id is not recorded — to be redelivered on a later poll) — now
   flows through F1. On
   success the entry id is recorded in the transport's entry-id map.
4. Decode errors: logged, entry skipped. Read errors: `disconnect()` +
   `connect()` (reconnect).

**Transformation:** msgpack envelope `bytes` → `TaskData` struct → typed
in-memory queue
items. The stream entry is **NOT acknowledged on enqueue**; it stays in the
consumer group's pending list until `ValkeyTransport.ack` acks it after the
handler's work terminates (see F1 destination note).

**Destination:** internal `task_queue` of the worker → F1.

## F3. Requeue / retry flow

**Source/trigger:** (a) watchdog timeout, (b) worker shutdown drain, (c) task
cancellation during cleanup, (d) capped error-path retry via
`TaskResult.retryable` (an error result with `retryable=True` is requeued in
`TaskExecutor._apply_retry_policy` before acking, at most once per task id; a second
consecutive retryable failure is acked terminal with `retryable=False` — see
F1).

**Path:** `TaskProcessor.watchdog` delegates to `TaskExecutor.watchdog`,
which cancels `worker_task` when
`elapsed > task_data.timeout.timeout` (or the configured `task_timeout`, default
3), waits up to the configured `task_cancellation_timeout` (default 5), and only
if the handler actually
stopped (`worker_task.done()`) calls `return_task_to_queue(task_data)` when
`timeout_action == "requeue"`. Base `return_task_to_queue`
delegates to `transport.requeue`; `ValkeyTransport.requeue` does `XADD` back to
the same task stream (tail), re-entering F2/F1. A handler that ignores
cancellation is not requeued (its entry stays pending and is redelivered on
restart).

**Shutdown drain** (`TaskProcessor.cleanup`): queued-but-undispatched items are
handed to `transport.on_drain` — `InMemoryTransport` requeues each per its
`canceled_action`, while `ValkeyTransport` deletes the lease without
re-enqueueing (the entry stays pending and is redelivered on restart); in-flight
running tasks are requeued through the same hook only after their handler is
confirmed stopped, when `canceled_action == "requeue"`.

**Note:** requeue via `XADD` appends to the **tail** of the stream — original
ordering is not preserved. The original entry is acknowledged by
`handle_task`'s `finally` when the handler stops, so a requeued task yields
exactly one retry copy (see §H8 for the swallowed-cancellation caveat). The
error-path budget is exactly one requeue then terminal: the per-task-id counter
is in-memory and per-execution, so a durable transport redelivering a
previously-requeued task after a restart starts a fresh budget. The watchdog's
timeout-driven requeue (`timeout_action == "requeue"`) is a separate axis and is
not gated by this budget.

## F4. Handler dispatch (selection)

**Source:** `TaskData.task` string. **Processing:** `_find_task_handler` iterates `task_handlers` dict (active instances) and returns the first
`handler.supports(task_type)`. **Destination:**
`handler.handle(task_data, capabilities=...)`.
Selection is by `supported_tasks` membership, **not** by a registration key
(the `add_task_handler` key is the resolved handler name — the handler class
name by default, or an explicit `name` keyword — so the same class can now be
registered under several distinct names). Because the first active match wins,
a class's per-instance task sets must not overlap.

## F5. Heartbeat flow

**Source:** `_heartbeat_manager` (a
`@Manager(name="Heartbeat")`-decorated method) — sleeps
`heartbeat_interval`, calls `self.heartbeat()`, repeats.
`ValkeyWorker.heartbeat` is the only concrete override.

**Processing/destination:** encodes `Heartbeat` struct (msgpack) and writes it
to key `scietex:{service}:{instance_id}:status` with TTL = 2 ×
`heartbeat_interval` (glide `ExpirySet`). Skipped when `client is None` or
`start_time is None`. **Errors are swallowed** (logged at WARNING) — a failed
heartbeat never surfaces.

## F6. Log flow

**Source:** any `self.logger.*` call inside workers/handlers.

**Processing:** standard `logging` → attached handlers:
- `ConsoleHandler` (console; registered in `BasicWorker.__init__`
  via `LoggingLifecycle.register_logger_handler`, constructed with
  `formatter=theme.console_formatter()`) — `emit()` puts each record into an
  internal `asyncio.Queue` per backend; the worker task formats with the
  `ScietexFormatter` instance the theme supplied and writes to stdout. Identity
  comes from the stdlib logger name it is registered on.
- `AsyncValkeyHandler` (constructed lazily on the first successful
  `connect()` via `_ensure_logging_handler`) — owns its own `GlideClient`, built from a `valkey_config=`
  dict translated from the typed `ValkeyConfig` (AR-059/061), so logging no
  longer shares the worker's client; formats records to a dict and `xadd`s to
  the log stream `scietex:{service}:{instance_id}:log` (default; both
  placeholders substituted at construction, so each worker logs to its own
  stream). The write carries an approximate `MAXLEN ~ log_stream_maxlen` trim
  (default 10000; `None` leaves the stream unbounded).

**Destination:** stdout / Valkey log stream. **Async boundary:** per-handler
asyncio queues + worker tasks; lifecycle driven by
`LoggingLifecycle.start_handlers` /
`shut_down_handlers`, with a per-handler
timeout (`logger_handler_timeout`, default 2 s).

## F7. Configuration flow

**Source:** config dir (resolved by `prepare_conf_dir`), i.e. `valkey.yml` in the chosen dir, or programmatic
`ValkeyConfig`.

**Path:** when `config.valkey_config` is provided, `ValkeyWorker.__init__`
builds the `GlideClientConfiguration` from it directly. When it is `None`, the
read is deferred to `_ensure_client_config()` (called at first connect, AR-066):
`read_valkey_config(self.conf_dir)` loads or creates `valkey.yml`
(msgspec YAML, strict decode; a present-but-invalid file raises `RuntimeError`,
only a missing file is created with defaults) →
`ValkeyConfig` → `generate_glide_config(...)` → `GlideClientConfiguration`
→ `GlideClient.create` in `connect()`.

## F8. Control / PubSub

PubSub listening is opt-in through the typed schema:
`ValkeyConfig.pubsub_config = ValkeyPubSubConfig(listening=True, parse_control_message=...)`.
When `listening` is set, `generate_glide_config` subscribes the worker's own
client to channels `scietex:{service}` and `scietex:broadcast`
(valkey/config.py). Each received message is delivered to the
`parse_control_message` callback. The callback is runtime-only (a callable
cannot be expressed in `valkey.yml`), so a YAML `listening: true` subscribes
with no callback and drops messages. `examples/valkey_pubsub_worker.py` is the
reference consumer.

## F9. Control plane (command lane)

**Source:** a submitter addresses a control command (`task:cancel`, `worker:*`,
`config:*`) through the `ControlPublisher` Protocol — `direct(instance_id, ...)`
targets one worker, `broadcast(...)` targets every worker, and
`resolve_owner(task_id)` maps a task to its owning worker's `instance_id`.

**Channel layout (per transport):** a *directed* channel per worker and a
*broadcast* channel per service, separate from the data plane.
`ValkeyControlPublisher` `XADD`s the versioned envelope to
`scietex:{service}:control:{instance_id}` (directed) or
`scietex:{service}:control` (broadcast), trimming each with
`MAXLEN ~ control_stream_maxlen`; `MqttControlPublisher` publishes the envelope
to `scietex/{service}/control/{instance_id}` or `scietex/{service}/control`
(event-only, `retain=False`, at `control_qos`). `resolve_owner` reads the Valkey
tracking record's `TaskStatus.instance_id` (`scietex:{service}:task:{id}`) or the
MQTT retained owner marker `scietex/{service}/tasks/{task_id}/owner`.

**Processing:** the worker reads its control channel with plain `XREAD` + a
`$`-seeded in-memory cursor (Valkey; no consumer group) or a subscription
(MQTT), and routes the decoded `TaskData` into the dedicated control lane
(`TaskProcessor.__control_queue`, `enqueue_control_task`), which is not counted
against `max_concurrent_tasks` — the lane has its own concurrency ceiling
(`DEFAULT_CONTROL_CONCURRENCY = 4`). Lane routing is channel-driven, not
type-driven: a handler declares `control: ClassVar[bool]` and is filed in the
control or data registry accordingly.

**Destination:** the matching control handler runs and returns a `TaskResult`
whose `payload` is the msgpack-encoded response struct. Control is never
retried: a publish failure is raised so the submitter learns the command was
not sent, and a command published while a worker is down is skipped, not
replayed.

## F10. Worker watcher (client view over heartbeats)

**Source:** the `Heartbeat` structs `ValkeyWorker.heartbeat` (stored at
`scietex:{service}:{instance_id}:status`) and `MqttWorker.heartbeat` (retained
on `scietex/{service}/workers/{instance_id}`) publish.

**Processing:** a `WorkerWatcher` owns a `WorkerRegistry` and a swappable
`WatchBackend`. `PollingBackend` `SCAN`s the status keys
(`scietex:{service}:*:status`) and reads each; `SubscribeBackend` subscribes to
the wildcard `scietex/{service}/workers/+` and buffers delivered messages. Both
decode via the shared `decode_heartbeat` (msgpack → `Heartbeat`, skipping
undecodable/expired payloads). The registry upserts by `instance_id` (ignoring
out-of-order older heartbeats) and evicts a record once its payload `ttl` has
elapsed since the client received it.

**Destination:** `WorkerWatcher.snapshot()` returns the current live workers;
the async `watch()` iterator reconciles each poll tick and yields a
`WorkerEvent` per change — `ADDED` (first sighting), `UPDATED` (heartbeat
changed), or `EXPIRED` (local TTL eviction, carrying the worker's last-known
record).
