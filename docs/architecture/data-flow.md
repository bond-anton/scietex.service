# Data flows

Primary data flows. Each flow lists source → processing → destination, key
transformations, and any async boundaries (queues/events/tasks).

## F1. In-process task processing (core flow)

**Source:** external caller/enqueue sites — subclass `fetch_tasks()` puts
`(UUID, TaskData)` tuples into the worker's internal queue, or a producer calls
`enqueue_task()` directly.

**Processing chain:**
1. `TaskProcessor.task_queue_manager` (`task_processor.py:625`,
   `@Manager("TaskQueueManager")`) — while the queue is not full, invokes the
   subclass/`ValkeyWorker` `fetch_tasks()`; then sleeps
   `task_queue_manager_sleep_time` (default 0.01 s).
2. `TaskProcessor.task_manager` (`task_processor.py:523`,
   `@Manager("TaskManager")`) — if `len(running_tasks) < max_concurrent_tasks`,
   pops `(task_id, task_data)` off `task_queue` with a 1 s fetch timeout,
   wraps `handle_task` in an `asyncio.Task`, records
   `running_tasks[task_id] = TaskTracker(...)`.
3. `handle_task` (inner, 535) calls `process_task(task_id, task_data)`.
4. `process_task` (470): guards the empty-`task` case first — an empty
   `task_data.task` returns `TaskResult(status="error", error="Task data must
   contain 'task' field")` (495–504) — then selects a handler with
   `_find_task_handler` (`handler.supports(task_type)`, first match among
   **active/started** handlers).
5. Dispatch is gated by `handler.is_ready` (507): only a found **and
   initialized** handler runs `await handler.handle(task_data)`. A `handle()`
   exception is converted into `TaskResult(status="error", error=str(e))` with
   the default `retryable=False` — a **raised exception is permanent** (a
   handler that wants a retry must return a `retryable=True` result
   explicitly), so an unhandled exception cannot create an infinite requeue
   loop under retry-once. No handler / not ready → error result
   ("No handler found for task type ...").

**Destination:** the `TaskResult` is returned to `handle_task`, whose `finally`
pops the `running_tasks` entry and calls `task_queue.task_done()`, then: a
`retryable=True` error result is requeued via
`return_task_to_queue(task_id, task_data)` **before** acking (568–578) — the
retry copy is made durable (XADD) before the original is dropped (XACK) — and
then `on_task_completed(task_id, task_data, task_result)` is invoked — the
transport-agnostic ack/result-sink seam. `ValkeyWorker` overrides it to
`XACK`+`XDEL` the stream entry. A requeue failure is logged and the entry is
still acked (the retry copy is lost, but the entry must not stay pending
forever).

**Async boundaries:** `asyncio.Queue` (bounded, `queue_size` default 100) between
intake and dispatch; per-task `asyncio.Task`; concurrency cap
`max_concurrent_tasks` (default 10). When `TaskProcessorConfig.auto_tune` is
`True` and `max_concurrent_tasks` is unset, the cap is derived from
`os.cpu_count()` at startup instead of the static default.

## F2. Valkey task intake / transport (ValkeyWorker)

**Source:** external producer writes task entries into Valkey stream
`scietex:{service}:tasks`. Entry shape: one field-value pair per
message — **field = task UUID string, value = msgpack-encoded `TaskData`**
(written by `return_task_to_queue`, `worker.py:407`).

**Processing chain (`fetch_tasks`, 489):**
1. On the first call only, `_recover_pending_tasks` runs `XAUTOCLAIM` to
   re-enqueue entries left pending by a previous crash (at-least-once).
2. `XREADGROUP` on group `...:task_group`, consumer `...`, key `>`, count 1,
   `block_ms=1000`.
3. Per entry: decode field → UUID, decode value →
   `msgspec.msgpack.decode(payload, type=TaskData)`; `enqueue_task(UUID(task_id),
   task_data)` (non-blocking; a full queue leaves the entry pending — its id is
   not recorded — to be redelivered on a later poll) — now flows through F1. On
   success the entry id is recorded in `_task_entry_ids[task_id]`.
4. Decode errors: logged, entry skipped. Read errors: `disconnect()` +
   `connect()` (reconnect).

**Transformation:** msgpack `bytes` → `TaskData` struct → typed in-memory queue
items. The stream entry is **NOT acknowledged on enqueue**; it stays in the
consumer group's pending list until `on_task_completed` acks it after the
handler's work terminates (see F1 destination note).

**Destination:** internal `task_queue` of the worker → F1.

## F3. Requeue / retry flow

**Source/trigger:** (a) watchdog timeout, (b) worker shutdown drain, (c) task
cancellation during cleanup, (d) retry-once via `TaskResult.retryable` (an
error result with `retryable=True` is requeued in `handle_task`'s `finally`
before acking — see F1).

**Path:** `TaskProcessor.watchdog` (644) cancels `worker_task` when
`elapsed > task_data.timeout.timeout` (or `DEFAULT_TASK_TIMEOUT=3`), waits up
to `WORKER_TASK_CANCELLATION_TIMEOUT`, and only if the handler actually
stopped (`worker_task.done()`) calls `return_task_to_queue(task_id,
task_data)` when `timeout_action == "requeue"`. Base `return_task_to_queue`
(355) is a no-op; `ValkeyWorker` (407) does `XADD` back to the same task
stream (tail), re-entering F2/F1. A handler that ignores cancellation is not
requeued (its entry stays pending and is redelivered on restart).

**Shutdown drain** (`TaskProcessor.cleanup`, 423): queued-but-undispatched
items are dropped (their transport entries stay pending and are redelivered on
restart); in-flight running tasks are requeued through the same hook only after
their handler is confirmed stopped, when `canceled_action == "requeue"`.

**Note:** requeue via `XADD` appends to the **tail** of the stream — original
ordering is not preserved. The original entry is acknowledged by
`handle_task`'s `finally` when the handler stops, so a requeued task yields
exactly one retry copy (see §H8 for the swallowed-cancellation caveat).

## F4. Handler dispatch (selection)

**Source:** `TaskData.task` string. **Processing:** `_find_task_handler`
(337) iterates `task_handlers` dict (active instances) and returns the first
`handler.supports(task_type)`. **Destination:** `handler.handle(task_data)`.
Selection is by `supported_tasks` membership, **not** by a registration key
(the `add_task_handler` key is the resolved handler name — the handler class
name by default, or an explicit `name` keyword — so the same class can now be
registered under several distinct names). Because the first active match wins,
a class's per-instance task sets must not overlap.

## F5. Heartbeat flow

**Source:** `@Manager("Heartbeat") _heartbeat_manager`
(`basic_worker.py:571`) — sleeps `heartbeat_interval`, calls
`self.heartbeat()`, repeats. `ValkeyWorker.heartbeat` (249) is the only
concrete override.

**Processing/destination:** encodes `Heartbeat` struct (msgpack) and writes it
to key `scietex:{service}:{instance_id}:status` with TTL = 2 ×
`heartbeat_interval` (glide `ExpirySet`). Skipped when `client is None` or
`start_time is None`. **Errors are swallowed** (logged at WARNING) — a failed
heartbeat never surfaces.

## F6. Log flow

**Source:** any `self.logger.*` call inside workers/handlers.

**Processing:** standard `logging` → attached handlers:
- `ConsoleHandler` (console; registered in `BasicWorker.__init__`
  (`basic_worker.py:116`) via `LoggingLifecycle.register_logger_handler`
  (`log_handlers/lifecycle.py:37`)) — `emit()` puts each record into an internal
  `asyncio.Queue` per backend; worker task formats with `ScietexFormatter`
  and writes to stdout. Identity comes from the stdlib logger name it is
  registered on.
- `AsyncValkeyHandler` (constructed lazily on the first successful
  `connect()` via `_ensure_logging_handler`,
  `worker.py:203`) — owns its own `GlideClient`, built from a `valkey_config=`
  dict translated from the typed `ValkeyConfig` (AR-059/061), so logging no
  longer shares the worker's client; formats records to a dict and `xadd`s to
  the log stream `scietex:log` (default). A raw `GlideClientConfiguration`
  falls back to the `client=` injection seam.

**Destination:** stdout / Valkey log stream. **Async boundary:** per-handler
asyncio queues + worker tasks; lifecycle driven by
`LoggingLifecycle.start_handlers` (`log_handlers/lifecycle.py:62`) /
`shut_down_handlers` (`log_handlers/lifecycle.py:104`), with a per-handler
timeout (`logger_handler_timeout`, default 2 s).

## F7. Configuration flow

**Source:** config dir (resolved by `prepare_conf_dir`,
`utils/config.py:33`), i.e. `valkey.yml` in the chosen dir, or programmatic
`ValkeyConfig`.

**Path:** `ValkeyWorker.__init__`: if `config.valkey_config` is `None`,
`read_valkey_config(self.conf_dir)` loads or creates `valkey.yml`
(msgspec YAML, strict decode; a present-but-invalid file raises `RuntimeError`,
only a missing file is created with defaults) →
`ValkeyConfig` → `generate_glide_config(...)` → `GlideClientConfiguration`
→ `GlideClient.create` in `connect()`.

## F8. Control / PubSub (defined but unused in package)

`generate_glide_config` supports `listening=True` + `parse_control_message`
callback → subscribes to channels `scietex:{service}:{instance_id}` and
`scietex:broadcast` (valkey/config.py:338-348). **`ValkeyWorker` always
passes `listening=False`**; nothing in the package consumes control messages.
The PubSub path exists only in config/translation code (`UNKNOWN` consumers —
likely future or external).
