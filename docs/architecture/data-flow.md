# Data flows

Primary data flows. Each flow lists source → processing → destination, key
transformations, and any async boundaries (queues/events/tasks).

## F1. In-process task processing (core flow)

**Source:** external caller/enqueue sites — subclass `fetch_tasks()` puts
`(UUID, TaskData)` tuples into the worker's internal queue, or a producer calls
`task_queue.put()` directly.

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

**Destination:** the `TaskResult` is returned to `handle_task`, which **only
logs at DEBUG and discards it** (there is no result sink, ack, or delivery).
`running_tasks` entry is popped and `task_queue.task_done()` called.

**Async boundaries:** `asyncio.Queue` (bounded, `queue_size` default 2) between
intake and dispatch; per-task `asyncio.Task`; concurrency cap
`max_concurrent_tasks` (default 2).

## F2. Valkey task intake / transport (ValkeyWorker)

**Source:** external producer writes task entries into Valkey stream
`scietex:{service}:{worker_id}:tasks`. Entry shape: one field-value pair per
message — **field = task UUID string, value = msgpack-encoded `TaskData`**
(written by `return_task_to_queue`, `valkey_async_worker.py:367-384`).

**Processing chain (`fetch_tasks`, 386):**
1. `XREADGROUP` on group `...:task_group`, consumer `...`, key `>`, count 1,
   `block_ms=1000`.
2. Per entry: decode field → UUID, decode value →
   `msgspec.msgpack.decode(payload, type=TaskData)`; `await
   task_queue.put((UUID(task_id), task_data))` — now flows through F1.
3. After successful put: `XACK` + `XDEL` the entry.
4. Decode errors: logged, entry skipped. Read errors: `disconnect()` +
   `connect()` (reconnect), task lost to retry by group semantics.

**Transformation:** msgpack `bytes` → `TaskData` struct → typed in-memory queue
items. The stream entry is **acknowledged/deleted on enqueue**, i.e. once in
the in-process queue the Valkey copy is gone.

**Destination:** internal `task_queue` of the worker → F1.

## F3. Requeue / retry flow

**Source/trigger:** (a) watchdog timeout, (b) worker shutdown drain, (c) task
cancellation during cleanup.

**Path:** `AsyncTaskProcessor.watchdog` (530) cancels `worker_task` when
`elapsed > task_data.timeout.timeout` (or `DEFAULT_TASK_TIMEOUT=3`); if
`timeout_action == "requeue"`, calls `return_task_to_queue(task_id,
task_data)`. Base `return_task_to_queue` (371) is a no-op; `ValkeyWorker`
(367) does `XADD` back to the same task stream (tail), re-entering F2/F1.

**Shutdown drain** (`AsyncTaskProcessor.cleanup`, 397): remaining
`task_queue` items and in-flight running tasks are requeued through the same
hook when `canceled_action == "requeue"`.

**Note:** requeue via `XADD` appends to the **tail** of the stream — original
ordering is not preserved, and a task can be re-enqueued even if the underlying
handler ignores cancellation (see §H8).

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
- `AsyncBaseHandler` (console; added in `BasicAsyncWorker.__init__`,
  `basic_async_worker.py:133`) — `emit()` puts each record into an internal
  `asyncio.Queue` per backend; worker task formats with `ScietexFormatter`
  and writes to stdout.
- `AsyncValkeyHandler` (added in `ValkeyWorker.__init__`,
  `valkey_async_worker.py:139-147`, `stdout_enable=False`) — its own
  `GlideClient`; formats records to a dict and `xadd`s to log stream
  `scietex:log` (default).

**Destination:** stdout / Valkey log stream. **Async boundary:** per-handler
asyncio queues + worker tasks; lifecycle driven by
`BasicAsyncWorker._logger_start_handlers` (429) / `_logger_shut_down_handlers`
(465) with a per-handler timeout (`logger_handler_timeout`, default 2 s).

## F7. Configuration flow

**Source:** config dir (resolved by `prepare_conf_dir`,
`utils/conf.py:33`), i.e. `valkey.yml` in the chosen dir, or programmatic
`ValkeyConfig`.

**Path:** `ValkeyWorker.__init__` (127-138): if no `valkey_config` argument,
`read_valkey_config(self.conf_dir)` loads or creates `valkey.yml`
(msgspec YAML, strict decode, silent fallback to defaults on any error) →
`ValkeyConfig` → `generate_glide_config(...)` → `GlideClientConfiguration`
→ `GlideClient.create` in `connect()` (180).

## F8. Control / PubSub (defined but unused in package)

`generate_glide_config` supports `listening=True` + `parse_control_message`
callback → subscribes to channels `scietex:{service}:{worker_id}` and
`scietex:broadcast` (valkey_config.py:304-315). **`ValkeyWorker` always
passes `listening=False`**; nothing in the package consumes control messages.
The PubSub path exists only in config/translation code (`UNKNOWN` consumers —
likely future or external).
