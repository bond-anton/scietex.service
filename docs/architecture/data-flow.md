# Data flows

Primary data flows. Each flow lists source → processing → destination, key
transformations, and any async boundaries (queues/events/tasks).

## F1. In-process task processing (core flow)

**Source:** external caller/enqueue sites — subclass `fetch_tasks()` puts
`(UUID, TaskData)` tuples into the worker's internal queue, or a producer calls
`enqueue_task()` directly.

**Processing chain:**
1. `AsyncTaskProcessor.task_queue_manager` (`async_tasks_processor.py:515`,
   `@Manager("TaskQueueManager")`) — while the queue is not full, invokes the
   subclass/`ValkeyWorker` `fetch_tasks()`; then sleeps
   `task_queue_manager_sleep_time` (default 0.01 s).
2. `AsyncTaskProcessor.task_manager` (`async_tasks_processor.py:476`,
   `@Manager("TaskManager")`) — if `len(running_tasks) < max_concurrent_tasks`,
   pops `(task_id, task_data)` off `task_queue` with a 1 s fetch timeout,
   wraps `handle_task` in an `asyncio.Task`, records
   `running_tasks[task_id] = TaskTracker(...)`.
3. `handle_task` (inner, 489) calls `process_task(task_id, task_data)`.
4. `process_task` (433): validates `task_data.task`, selects a handler with
   `_find_task_handler` (`handler.supports(task_type)`, first match among
   **active/started** handlers), calls `await handler.handle(task_data)`.
5. Exceptions from `handle()` are converted into
   `TaskResult(status="error", error=str(e))`; no handler → error result.

**Destination:** the `TaskResult` is returned to `handle_task`, which logs it
at DEBUG and invokes `on_task_completed(task_id, task_data, task_result)` in
its `finally` — the transport-agnostic ack/result-sink seam. `ValkeyWorker`
overrides it to `XACK`+`XDEL` the stream entry. `running_tasks` entry is
popped and `task_queue.task_done()` called.

**Async boundaries:** `asyncio.Queue` (bounded, `queue_size` default 2) between
intake and dispatch; per-task `asyncio.Task`; concurrency cap
`max_concurrent_tasks` (default 2).

## F2. Valkey task intake / transport (ValkeyWorker)

**Source:** external producer writes task entries into Valkey stream
`scietex:{service}:{worker_id}:tasks`. Entry shape: one field-value pair per
message — **field = task UUID string, value = msgpack-encoded `TaskData`**
(written by `return_task_to_queue`, `valkey_async_worker.py:405`).

**Processing chain (`fetch_tasks`, 470):**
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
cancellation during cleanup.

**Path:** `AsyncTaskProcessor.watchdog` (604) cancels `worker_task` when
`elapsed > task_data.timeout.timeout` (or `DEFAULT_TASK_TIMEOUT=3`), waits up
to `WORKER_TASK_CANCELLATION_TIMEOUT`, and only if the handler actually
stopped (`worker_task.done()`) calls `return_task_to_queue(task_id,
task_data)` when `timeout_action == "requeue"`. Base `return_task_to_queue`
(383) is a no-op; `ValkeyWorker` (405) does `XADD` back to the same task
stream (tail), re-entering F2/F1. A handler that ignores cancellation is not
requeued (its entry stays pending and is redelivered on restart).

**Shutdown drain** (`AsyncTaskProcessor.cleanup`, 430): queued-but-undispatched
items are dropped (their transport entries stay pending and are redelivered on
restart); in-flight running tasks are requeued through the same hook only after
their handler is confirmed stopped, when `canceled_action == "requeue"`.

**Note:** requeue via `XADD` appends to the **tail** of the stream — original
ordering is not preserved. The original entry is acknowledged by
`handle_task`'s `finally` when the handler stops, so a requeued task yields
exactly one retry copy (see §H8 for the swallowed-cancellation caveat).

## F4. Handler dispatch (selection)

**Source:** `TaskData.task` string. **Processing:** `_find_task_handler`
(353) iterates `task_handlers` dict (active instances) and returns the first
`handler.supports(task_type)`. **Destination:** `handler.handle(task_data)`.
Selection is by `supported_tasks` membership, **not** by the registration key
used in `add_task_handler` (keys are registry names; one key maps to one class
but the same class may be registered under several keys, and the same task type
may match several handlers — first active wins).

## F5. Heartbeat flow

**Source:** `@Manager("Heartbeat") _heartbeat_manager`
(`basic_async_worker.py:774`) — sleeps `heartbeat_interval`, calls
`self.heartbeat()`, repeats. `ValkeyWorker.heartbeat` (215) is the only
concrete override.

**Processing/destination:** encodes `Heartbeat` struct (msgpack) and writes it
to key `scietex:{service}:{worker_id}:status` with TTL = 2 ×
`heartbeat_interval` (glide `ExpirySet`). Skipped when `client is None` or
`start_time is None`. **Errors are swallowed** (logged at DEBUG) — a failed
heartbeat never surfaces.

## F6. Log flow

**Source:** any `self.logger.*` call inside workers/handlers.

**Processing:** standard `logging` → attached handlers:
- `AsyncBaseHandler` (console; registered in `BasicAsyncWorker.__init__`
  (`basic_async_worker.py:153`) via `LoggingLifecycle.register_logger_handler`
  (`logging_lifecycle.py:55`)) — `emit()` puts each record into an internal
  `asyncio.Queue` per backend; worker task formats with `ScietexFormatter`
  and writes to stdout.
- `AsyncValkeyHandler` (added in `ValkeyWorker.__init__`,
  `valkey_async_worker.py:139-147`, `stdout_enable=False`) — its own
  `GlideClient`; formats records to a dict and `xadd`s to log stream
  `scietex:log` (default).

**Destination:** stdout / Valkey log stream. **Async boundary:** per-handler
asyncio queues + worker tasks; lifecycle driven by
`LoggingLifecycle.start_handlers` (`logging_lifecycle.py:57`) /
`shut_down_handlers` (`logging_lifecycle.py:93`), exposed as worker wrappers
`_logger_start_handlers` (`basic_async_worker.py:541`) /
`_logger_shut_down_handlers` (`basic_async_worker.py:549`), with a per-handler
timeout (`logger_handler_timeout`, default 2 s).

## F7. Configuration flow

**Source:** config dir (resolved by `prepare_conf_dir`,
`utils/conf.py:33`), i.e. `valkey.yml` in the chosen dir, or programmatic
`ValkeyConfig`.

**Path:** `ValkeyWorker.__init__` (127-138): if no `valkey_config` argument,
`read_valkey_config(self.conf_dir)` loads or creates `valkey.yml`
(msgspec YAML, strict decode; a present-but-invalid file raises `RuntimeError`,
only a missing file is created with defaults) →
`ValkeyConfig` → `generate_glide_config(...)` → `GlideClientConfiguration`
→ `GlideClient.create` in `connect()` (180).

## F8. Control / PubSub (defined but unused in package)

`generate_glide_config` supports `listening=True` + `parse_control_message`
callback → subscribes to channels `scietex:{service}:{worker_id}` and
`scietex:broadcast` (valkey_config.py:304-315). **`ValkeyWorker` always
passes `listening=False`**; nothing in the package consumes control messages.
The PubSub path exists only in config/translation code (`UNKNOWN` consumers —
likely future or external).
