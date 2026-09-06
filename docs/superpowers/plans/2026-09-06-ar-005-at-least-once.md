# AR-005 At-Least-Once Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix AR-005 (HIGH) by defining and implementing an **at-least-once** delivery contract for `scietex.service`: a Valkey stream entry is acknowledged only after the handler's work on it terminates (success, terminal error, or graceful cancellation); on crash/timeout the entry stays pending and is redelivered. No dedup store; exactly-once is out of scope.

**Architecture:** The stream is the durable source of truth. `AsyncTaskProcessor.handle_task` becomes the single acknowledgement point: it invokes a new transport-agnostic completion hook `on_task_completed(task_id, task_data, task_result)` in its `finally`, so the entry is acked exactly when the handler's work on that entry ends — wherever that end comes from. `ValkeyWorker` overrides the hook to `XACK`+`XDEL` the stream entry id it recorded at fetch time (a `task_id -> entry_id` map replaces the old ack-on-enqueue). `fetch_tasks` stops `XACK`/`XDEL`-ing on enqueue and instead records the entry id. A one-shot `XAUTOCLAIM` recovery pass at the start of the first `fetch_tasks` re-enqueues entries left pending by a previous crash. The watchdog and shutdown cleanup requeue a task **only after** its handler is confirmed stopped, so a handler that swallows `CancelledError` is not duplicated (its entry stays pending and is redelivered on restart).

**Tech Stack:** Python 3.10+, asyncio, `msgspec`, optional `valkey-glide` (`GlideClient.xautoclaim`/`xack`/`xdel`/`xreadgroup`).

**Spec:** Finding AR-005 in `docs/reviews/architecture/2026-09-04.md` (lines 265-304). The delivery-guarantee decision (at-least-once, no dedup) was confirmed by the user and is **not** revisited here. Executors should read the review finding and `docs/architecture/data-flow.md` (flows F1-F3, which this plan changes).

## Global Constraints

- **Do not revisit the at-least-once decision.** No dedup store, no exactly-once. Exactly-once may be layered later (out of scope).
- Verification commands (from AGENTS.md): `ruff check src/`, `ty check src/`, `pytest tests/`. Order: lint → type → test. Run within the project venv via `uv run`.
- No new dependencies. No emoji in code/comments. Match existing style (docstrings, `self.logger.log(...)`, name-mangled private attrs, `const`-preferred, early return).
- Keep the public API stable. The only new public surface is the `on_task_completed` override hook (a no-op by default) and the internal `_recover_pending_tasks`/`_task_entry_ids` on `ValkeyWorker`. No existing public method signature changes.
- `uv.lock` is gitignored — never `git add uv.lock`.
- `ValkeyWorker` tests mock `GlideClient` via `DummyClient`/`monkeypatch` (`tests/test_valkey_worker.py`); no live Valkey server is required.
- The working tree is clean on `main` (AR-006/AR-010 already committed as `92e9ff4`). Commit only the files each task touches.
- **No existing test asserts the old ack-on-enqueue behavior.** `DummyClient.xreadgroup` returns `None`, so `fetch_tasks` never reaches the `XACK`/`XDEL` in any current test. Removing it breaks nothing; new tests cover the new behavior.

---

## Design Decisions (recommended)

1. **Ack-on-completion seam: `on_task_completed(task_id, task_data, task_result)`.** The base `AsyncTaskProcessor` gains a public override hook, default no-op, invoked from `handle_task`'s `finally` with the final `TaskResult` (or `None` when the task was cancelled before producing a result). This is the single point where the transport learns "this entry's work is done." It doubles as the minimal result sink the review asks for (every completed result flows through it). `ValkeyWorker` overrides it to `XACK`+`XDEL`. A dedicated result-sink separate from the ack is deferred (optional future work).

2. **Ack in `finally`, including on cancellation.** `handle_task`'s `finally` runs on success, error, and `CancelledError` (which `except Exception` does not catch). Acking there means a task cancelled by the watchdog/cleanup is acked exactly when its handler actually stops. This is what prevents the watchdog-requeue from duplicating: the original entry is acked in `finally`, then the watchdog appends one fresh retry entry. A crash mid-processing never reaches `finally`, so the entry stays pending and is redelivered — the at-least-once guarantee.

3. **`ValkeyWorker` tracks `task_id -> stream_entry_id` in `_task_entry_ids`.** `XACK` needs the stream entry id (e.g. `b"123-0"`), but `handle_task` only carries the `task_id` (the entry's field). `fetch_tasks` and `_recover_pending_tasks` record `self._task_entry_ids[UUID(task_id)] = entry_id` after a successful `task_queue.put`; `on_task_completed` pops and acks it. This assumes one pending entry per `task_id` (a producer reusing a UUID for two deliveries is a producer bug; documented in Risks). The in-process queue item type `(UUID, TaskData)` is unchanged.

4. **`fetch_tasks` stops `XACK`/`XDEL`-ing on enqueue.** It reads via `XREADGROUP '>'`, decodes, `put`s, and records the entry id — nothing more. The entry stays in the consumer group's pending list until `on_task_completed` acks it. `XDEL` is still performed (after `XACK`) in `on_task_completed` so the stream does not grow unboundedly; `XDEL` after `XACK` is safe (the entry is already out of the PEL).

5. **Crash recovery via one-shot `XAUTOCLAIM` at the start of the first `fetch_tasks`.** `_startup` starts managers **before** `initialize()` (basic_async_worker.py:770 vs 773), so recovery cannot live in `initialize()` (it would race the already-running `task_queue_manager`). Instead `fetch_tasks` runs `_recover_pending_tasks()` once (guarded by `self._recovered`) before its first `'>'` read. `fetch_tasks` calls are serialized by the single `TaskQueueManager` loop, and recovery runs before any `'>'` read this run, so it never double-claims an in-flight entry. `XAUTOCLAIM(stream, group, consumer, min_idle_time_ms=0, start="0-0", count=10)` claims every pending entry idle ≥0 ms and returns `[next_start, {entry_id: [[field, value]]}, [deleted_ids]]`; the loop advances `start` until `next_start == "0-0"`. Recovery is startup-only: mid-run disconnects leave in-flight entries that ack normally, and a crash is recovered on the next start.

6. **Watchdog requeues only after the handler is confirmed stopped (defect 4).** Reorder the timeout branch to `cancel()` → `wait_for(worker_task, WORKER_TASK_CANCELLATION_TIMEOUT)` → **then**, if `worker_task.done()`, `return_task_to_queue` (append a fresh retry) when `timeout_action == "requeue"`. If the handler is still running after the wait (it swallowed `CancelledError`), do **not** requeue — log an error and drop tracking. Rationale: the running handler will ack its own entry when it eventually finishes (no duplicate, no loss); if it never finishes, the entry stays pending and the next restart's recovery redelivers it (no loss). Requeueing a still-running handler would run the task twice concurrently. This matches the review's "make requeue conditional on the task actually having been cancelled." The accepted tradeoff is a sub-millisecond loss window between the `finally`-ack and the watchdog's `XADD` (documented in Risks).

7. **Shutdown cleanup is at-least-once-correct.** Two changes to `AsyncTaskProcessor.cleanup`: (a) the in-process queue drain **no longer calls `return_task_to_queue`** — items fetched from a durable transport are already pending there and would be duplicated by an `XADD`; they are dropped and redelivered by recovery on restart (for a non-durable transport `return_task_to_queue` is a no-op anyway, so no regression). (b) The running-task cancel loop requeues only after the handler is confirmed stopped, mirroring the watchdog. Subclasses whose transport does not keep items pending after enqueue must override `cleanup` to requeue drained items (documented).

8. **Structured error type and retry-count/dead-letter are deferred.** The review lists them as "consider," not requirements. `TaskResult` stays stringly (`error: str`); the completion hook receives it as-is. A per-task attempt/lease is the proper fix for hung handlers and is out of scope (noted as optional future work).

---

## File Structure

- `src/scietex/service/async_tasks_processor.py` — add `on_task_completed` hook; rewrite `handle_task` (502-526) to call it in `finally`; rewrite `watchdog` (562-601) to requeue-only-if-stopped; rewrite `cleanup` (413-447) drain + cancel semantics.
- `src/scietex/service/valkey/valkey_async_worker.py` — add `_task_entry_ids` + `_recovered` in `__init__`; rewrite `fetch_tasks` (415-466) to record entry id and not ack; add `on_task_completed` override; add `_recover_pending_tasks`; import `TaskResult`.
- `docs/architecture/data-flow.md` — update F2/F3 to the ack-on-completion + recovery contract.
- `tests/test_async_task_processor.py` — add completion-hook, watchdog-no-requeue-on-stuck, and cleanup-drain tests.
- `tests/test_valkey_worker.py` — extend `DummyClient`; add fetch-no-ack, on_task_completed-ack, and recovery tests.

---

### Task 1: Add the `on_task_completed` hook and make `handle_task` ack on completion

**Files:**
- Modify: `src/scietex/service/async_tasks_processor.py` (add hook after `return_task_to_queue` ~line 393; rewrite `handle_task` 502-526)
- Test: `tests/test_async_task_processor.py` (append)

**Interfaces:**
- Consumes: `TaskResult` (imported line 17), `UUID`, `TaskData`.
- Produces: `async def on_task_completed(self, task_id: UUID, task_data: TaskData, task_result: TaskResult | None) -> None` — default no-op override hook. `handle_task` now captures the result and calls it in `finally` (with `None` on cancellation), so the transport is notified exactly when the handler's work on the entry terminates. Later tasks rely on this hook being invoked on success, error, and cancellation.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_async_task_processor.py`:

```python
class RecordingProcessor(DemoProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.completed: list = []

    async def on_task_completed(self, task_id, task_data, task_result):
        self.completed.append((task_id, task_data, task_result))


@pytest.mark.asyncio
async def test_handle_task_invokes_completion_hook():
    """handle_task must invoke on_task_completed with the final result (AR-005)."""
    proc = RecordingProcessor()
    proc.add_task_handler("dummy", DummyHandler)
    await proc._start_task_handler("dummy")
    await proc.start()
    try:
        t_id = uuid4()
        await proc.task_queue.put((t_id, TaskData(task="dummy", payload=b'{"value": 5}')))
        for _ in range(100):
            if proc.completed:
                break
            await asyncio.sleep(0.01)
        assert len(proc.completed) == 1
        cid, cdata, cresult = proc.completed[0]
        assert cid == t_id
        assert cdata.task == "dummy"
        assert cresult.status == "success"
    finally:
        await proc.stop()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_async_task_processor.py::test_handle_task_invokes_completion_hook -v`
Expected: FAILS — `proc.completed` stays empty because `handle_task` never calls `on_task_completed`.

- [ ] **Step 3: Add the hook and rewrite `handle_task`**

Add this method after `return_task_to_queue` (after line 393):

```python
    async def on_task_completed(
        self,
        task_id: UUID,
        task_data: TaskData,
        task_result: TaskResult | None,
    ) -> None:
        """Notify the transport that a task's processing has terminated.

        Called by ``handle_task`` when a task's work ends — on success, on
        a terminal error, or on cancellation — with the final
        ``TaskResult``, or ``None`` when the task was cancelled before
        producing a result. Subclasses that source tasks from a durable
        transport (e.g. ``ValkeyWorker``) override this to acknowledge the
        transport entry so it is removed only after the handler's work on
        it is done (at-least-once). The default is a no-op.
        """
```

Replace the inner `handle_task` (lines 502-526) with:

```python
        async def handle_task(t_id: UUID, t_data: TaskData):
            result: TaskResult | None = None
            try:
                result = await self.process_task(t_id, t_data)
                self.logger.log(
                    logging.DEBUG,
                    "Task %s (%s) finished with status %s",
                    t_data.task,
                    t_id,
                    result.status,
                )
            except Exception as exc:
                # process_task is expected to return an error TaskResult for
                # every failure, but a defensive catch guarantees no exception
                # escapes into the unawaited task (which would surface as an
                # unretrieved task exception).
                self.logger.log(
                    logging.ERROR,
                    "Task %s (%s) raised unexpectedly: %s",
                    t_data.task,
                    t_id,
                    exc,
                )
            finally:
                self.running_tasks.pop(t_id, None)
                self.task_queue.task_done()
                try:
                    # Ack the transport entry exactly when the handler's work
                    # on it ends (success, error, or cancellation). On
                    # CancelledError, result is None and the hook still runs.
                    await self.on_task_completed(t_id, t_data, result)
                except Exception as exc:
                    # A transport ack failure must never crash handle_task or
                    # leak into the unawaited task; the entry stays pending
                    # and is redelivered on restart (at-least-once).
                    self.logger.log(
                        logging.ERROR,
                        "Failed to acknowledge task %s (%s): %s",
                        t_data.task,
                        t_id,
                        exc,
                    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_async_task_processor.py -v`
Expected: all pass, including the new test and the pre-existing AR-006/AR-010 tests.

- [ ] **Step 5: Verify lint and type**

Run: `uv run ruff check src/scietex/service/async_tasks_processor.py tests/test_async_task_processor.py`
Expected: no errors.

Run: `uv run ty check src/`
Expected: passes.

- [ ] **Step 6: Commit**

```bash
git add src/scietex/service/async_tasks_processor.py tests/test_async_task_processor.py
git commit -m "feat: ack transport on task completion via on_task_completed hook (AR-005)"
```

---

### Task 2: Watchdog requeues only after the handler is confirmed stopped

**Files:**
- Modify: `src/scietex/service/async_tasks_processor.py:562-601`
- Test: `tests/test_async_task_processor.py` (append)

**Interfaces:**
- Consumes: `WORKER_TASK_CANCELLATION_TIMEOUT` (module constant, line 33), `return_task_to_queue`, `running_tasks`.
- Produces: `watchdog()` that, on timeout, cancels, waits up to `WORKER_TASK_CANCELLATION_TIMEOUT`, and only then requeues (when `timeout_action == "requeue"`) **if** `worker_task.done()`. A handler still running after the wait is logged and dropped from tracking without requeue (its entry stays pending; `handle_task`'s `finally` acks it when it eventually finishes). `running_tasks` is always popped after handling.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_async_task_processor.py`:

```python
class StubbornHandler(TaskHandler):
    async def handle(self, task_data: TaskData) -> TaskResult:
        try:
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            # Swallow cancellation and keep running briefly.
            await asyncio.sleep(0.3)
        return TaskResult(status="success", error="No error")

    @property
    def supported_tasks(self) -> list[str]:
        return ["stubborn"]


@pytest.mark.asyncio
async def test_watchdog_does_not_requeue_when_handler_ignores_cancellation(monkeypatch):
    """A handler that swallows CancelledError must not be requeued by the
    watchdog: it is still running, so requeueing would run it twice (AR-005)."""
    import scietex.service.async_tasks_processor as mod

    # Shorten the cancellation wait so the test does not block for 5s.
    monkeypatch.setattr(mod, "WORKER_TASK_CANCELLATION_TIMEOUT", 0.05)
    proc = DemoProcessor()
    proc.add_task_handler("stubborn", StubbornHandler)
    await proc._start_task_handler("stubborn")
    await proc.start()
    try:
        t_id = uuid4()
        await proc.task_queue.put(
            (
                t_id,
                TaskData(
                    task="stubborn",
                    payload=b"{}",
                    timeout=TaskTimeout(timeout=0.1, timeout_action="requeue"),
                ),
            )
        )
        # Wait past the watchdog interval (default 1s) plus the cancel wait so
        # the watchdog has acted and decided not to requeue.
        await asyncio.sleep(1.6)
        assert not any(tid == t_id for tid, _ in proc.requeued)
        # Let the stubborn handler finish so no dangling task remains.
        await asyncio.sleep(0.5)
    finally:
        await proc.stop()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_async_task_processor.py::test_watchdog_does_not_requeue_when_handler_ignores_cancellation -v`
Expected: FAILS — the current watchdog requeues unconditionally on timeout, so `proc.requeued` contains `t_id`.

- [ ] **Step 3: Rewrite the `watchdog` timeout branch**

Replace lines 579-601 (the body of the `if 0 < timeout < ...` block) with:

```python
                self.logger.log(
                    logging.WARNING,
                    "Task %s (%s) exceeded timeout and will be canceled.",
                    task_tracker.data.task,
                    task_id,
                )
                task_tracker.worker_task.cancel()
                try:
                    await asyncio.wait_for(
                        task_tracker.worker_task,
                        timeout=WORKER_TASK_CANCELLATION_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    self.logger.log(logging.ERROR, "Timeout canceling Task %s.", task_id)
                except asyncio.CancelledError:
                    pass
                if task_tracker.worker_task.done():
                    # The handler actually stopped; handle_task's finally has
                    # already acknowledged the transport entry. Requeue a fresh
                    # delivery only now, so a handler that ignores cancellation
                    # cannot cause the task to run twice.
                    if task_tracker.data.timeout.timeout_action == "requeue":
                        self.logger.log(
                            logging.WARNING,
                            "Task %s (%s) will be returned to queue.",
                            task_tracker.data.task,
                            task_id,
                        )
                        await self.return_task_to_queue(task_id, task_tracker.data)
                else:
                    # The handler ignored cancellation and is still running. It
                    # will acknowledge its entry when it eventually finishes;
                    # requeueing now would run the task twice. Leave the entry
                    # pending so a restart redelivers it if the handler never
                    # returns.
                    self.logger.log(
                        logging.ERROR,
                        "Task %s (%s) ignored cancellation; not requeueing to avoid duplicate work.",
                        task_tracker.data.task,
                        task_id,
                    )
                self.running_tasks.pop(task_id, None)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_async_task_processor.py -v`
Expected: all pass, including the new test and the pre-existing `test_watchdog_requeues_timed_out_task` (its `SlowHandler` respects cancellation, so it is `done()` and still requeues).

- [ ] **Step 5: Verify lint and type**

Run: `uv run ruff check src/scietex/service/async_tasks_processor.py tests/test_async_task_processor.py`
Expected: no errors.

Run: `uv run ty check src/`
Expected: passes.

- [ ] **Step 6: Commit**

```bash
git add src/scietex/service/async_tasks_processor.py tests/test_async_task_processor.py
git commit -m "fix: watchdog requeues only after handler is confirmed stopped (AR-005)"
```

---

### Task 3: Make shutdown `cleanup` at-least-once-correct

**Files:**
- Modify: `src/scietex/service/async_tasks_processor.py:413-447`
- Test: `tests/test_async_task_processor.py` (append)

**Interfaces:**
- Consumes: `return_task_to_queue`, `running_tasks`, `task_queue`, `WORKER_TASK_CANCELLATION_TIMEOUT`.
- Produces: `cleanup()` that (a) drains the in-process queue **without** calling `return_task_to_queue` (items from a durable transport are already pending there and would be duplicated by an `XADD`; they are redelivered by recovery on restart), and (b) cancels running tasks and requeues only after the handler is confirmed stopped (mirroring Task 2).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_async_task_processor.py`:

```python
@pytest.mark.asyncio
async def test_cleanup_drain_does_not_requeue_queued_tasks():
    """cleanup must drop queued-but-undispatched tasks without requeueing
    them: their transport entries stay pending and are redelivered on
    restart, so an XADD here would duplicate them (AR-005)."""
    proc = DemoProcessor()
    t_id = uuid4()
    await proc.task_queue.put((t_id, TaskData(task="dummy", payload=b"{}")))
    await proc.cleanup()
    assert not any(tid == t_id for tid, _ in proc.requeued)
    assert proc.task_queue.empty()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_async_task_processor.py::test_cleanup_drain_does_not_requeue_queued_tasks -v`
Expected: FAILS — the current drain calls `return_task_to_queue`, so `proc.requeued` contains `t_id`.

- [ ] **Step 3: Rewrite `cleanup`**

Replace lines 413-447 (the whole `cleanup` method) with:

```python
    async def cleanup(self):
        """
        Cleanup everything before exit.

        This method is intended to be overridden by subclasses to perform
        service-specific cleanup such as closing database connections,
        releasing resources, or sending final status updates.
        """
        await super().cleanup()
        # Drain the in-process queue. Items fetched from a durable transport
        # (e.g. a Valkey stream) are still pending there and will be
        # redelivered on restart, so they must NOT be re-enqueued here (that
        # would duplicate them). Subclasses whose transport does not keep
        # items pending after enqueue must override cleanup to requeue drained
        # items.
        while not self.task_queue.empty():
            self.task_queue.get_nowait()
            self.task_queue.task_done()
        self.logger.debug("Task queue is empty")

        # Cancel and requeue running tasks. A task is requeued only after its
        # handler has actually stopped (handle_task's finally acknowledges the
        # transport entry); a handler that ignores cancellation is left pending
        # so a restart redelivers it rather than running it twice.
        for task_id, task_tracker in list(self.running_tasks.items()):
            if not task_tracker.worker_task.done():
                task_tracker.worker_task.cancel()
                try:
                    await asyncio.wait_for(
                        task_tracker.worker_task,
                        timeout=WORKER_TASK_CANCELLATION_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    self.logger.log(logging.ERROR, "Timeout canceling Task %s.", task_id)
                except asyncio.CancelledError:
                    pass
                if task_tracker.worker_task.done() and task_tracker.data.canceled_action == "requeue":
                    self.logger.log(logging.WARNING, "Task %s will be returned to queue.", task_id)
                    await self.return_task_to_queue(task_id, task_tracker.data)
        self.logger.debug("All tasks cancelled")

        # Cleanup task handlers
        for handler_name in self.__task_handlers_map:
            await self._stop_task_handler(handler_name)
        self.logger.debug("All task handlers cleaned up")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_async_task_processor.py -v`
Expected: all pass.

- [ ] **Step 5: Verify lint and type**

Run: `uv run ruff check src/scietex/service/async_tasks_processor.py tests/test_async_task_processor.py`
Expected: no errors.

Run: `uv run ty check src/`
Expected: passes.

- [ ] **Step 6: Commit**

```bash
git add src/scietex/service/async_tasks_processor.py tests/test_async_task_processor.py
git commit -m "fix: shutdown cleanup is at-least-once-correct (AR-005)"
```

---

### Task 4: `ValkeyWorker` — stop ack-on-enqueue, record entry ids, ack on completion

**Files:**
- Modify: `src/scietex/service/valkey/valkey_async_worker.py` (`__init__` ~164-169, `fetch_tasks` 415-466, add `on_task_completed` override, import `TaskResult`)
- Test: `tests/test_valkey_worker.py` (extend `DummyClient`, append)

**Interfaces:**
- Consumes: `TaskResult` (add to the `from ..task_handler import ...` import at line 42), `TaskData`, `GlideClient.xack`/`xdel`/`xreadgroup`.
- Produces: `self._task_entry_ids: dict[UUID, str | bytes]` (populated by `fetch_tasks`, consumed by `on_task_completed`); `fetch_tasks()` that no longer `XACK`/`XDEL`s and instead records the entry id; `async def on_task_completed(self, task_id, task_data, task_result)` override that `XACK`s + `XDEL`s the recorded entry id and pops it from the map.

- [ ] **Step 1: Extend `DummyClient` and write the failing tests**

In `tests/test_valkey_worker.py`, replace the `DummyClient` class (lines 14-40) with a version that records `xack`/`xdel` calls and can return canned `xreadgroup` results:

```python
class DummyClient:
    """Mocking Valkey client."""

    def __init__(self, ping_ok=True, xreadgroup_result=None):
        self._ping_ok = ping_ok
        self.closed = False
        self.xreadgroup_result = xreadgroup_result
        self.acked: list = []
        self.deleted: list = []

    async def xgroup_create(self, *args, **kwargs):
        pass

    async def xadd(self, *args, **kwargs):
        pass

    async def xack(self, *args, **kwargs):
        self.acked.append(args)

    async def xdel(self, *args, **kwargs):
        self.deleted.append(args)

    async def xreadgroup(self, *args, **kwargs):
        return self.xreadgroup_result

    async def ping(self):
        return self._ping_ok

    async def close(self):
        self.closed = True
```

Append these tests:

```python
def _entry(entry_id: bytes, task_id: str, payload: bytes):
    """Build an xreadgroup/xautoclaim result mapping for one stream entry."""
    return {b"stream": {entry_id: [[task_id.encode("utf-8"), payload]]}}


@pytest.mark.asyncio
async def test_fetch_tasks_does_not_ack_on_enqueue():
    """fetch_tasks must not XACK/XDEL on enqueue; it records the entry id so
    the entry stays pending until the handler completes (AR-005)."""
    import msgspec

    from scietex.service.task_handler.schemas import TaskData

    task_data = TaskData(task="dummy", payload=b"{}")
    payload = msgspec.msgpack.encode(task_data)
    client = DummyClient(xreadgroup_result=_entry(b"1-0", "11111111-1111-1111-1111-111111111111", payload))
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    worker._client = client

    await worker.fetch_tasks()

    assert client.acked == [], "fetch_tasks must not ack on enqueue"
    assert client.deleted == [], "fetch_tasks must not delete on enqueue"
    assert not worker.task_queue.empty()
    t_id, t_data = worker.task_queue.get_nowait()
    assert t_data.task == "dummy"
    assert worker._task_entry_ids[t_id] == b"1-0"


@pytest.mark.asyncio
async def test_on_task_completed_acks_and_deletes_entry():
    """on_task_completed must XACK+XDEL the recorded entry id and clear the map (AR-005)."""
    from uuid import UUID

    client = DummyClient()
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    worker._client = client
    t_id = UUID("11111111-1111-1111-1111-111111111111")
    worker._task_entry_ids[t_id] = b"1-0"

    await worker.on_task_completed(t_id, None, None)

    assert client.acked == [(worker._task_stream_name, worker._task_group_name, [b"1-0"])]
    assert client.deleted == [(worker._task_stream_name, [b"1-0"])]
    assert t_id not in worker._task_entry_ids
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_valkey_worker.py::test_fetch_tasks_does_not_ack_on_enqueue tests/test_valkey_worker.py::test_on_task_completed_acks_and_deletes_entry -v`
Expected: `test_fetch_tasks_does_not_ack_on_enqueue` FAILS (current `fetch_tasks` calls `xack`/`xdel`). `test_on_task_completed_acks_and_deletes_entry` FAILS (`on_task_completed` does not exist yet).

- [ ] **Step 3: Add `_task_entry_ids`, import `TaskResult`, rewrite `fetch_tasks`, add `on_task_completed`**

Change the import at line 42:

```python
from ..task_handler import TaskData, TaskResult
```

In `__init__`, after `self.__encoder = msgspec.msgpack.Encoder()` (line 169), add:

```python
        # Maps a task UUID to the stream entry id it was read from, so the
        # entry can be acknowledged when the handler completes (at-least-once).
        self._task_entry_ids: dict[UUID, str | bytes] = {}
```

Replace the whole `fetch_tasks` method (lines 415-466) with:

```python
    async def fetch_tasks(self):
        """Fetch a single new task from the Valkey task stream and enqueue it.

        Reads one entry from the task stream using ``XREADGROUP`` with
        ``block_ms=1000`` and the configured consumer group. Decodes the
        msgpack payload into a :class:`TaskData` struct and puts it into
        ``self.task_queue`` as a ``(UUID, TaskData)`` tuple. The stream entry
        is NOT acknowledged here: it stays in the consumer group's pending
        list until the handler completes (see :meth:`on_task_completed`), so a
        crash after enqueue redelivers the task (at-least-once). The entry id
        is recorded in ``_task_entry_ids`` for the later acknowledgement.

        On read errors, disconnects and attempts to reconnect to Valkey.

        Returns:
            None. No-op if the Valkey client is ``None``.
        """
        if self.client is None:
            return
        try:
            res = await self.client.xreadgroup(
                {self._task_stream_name: ">"},
                self._task_group_name,
                self._consumer_name,
                StreamReadGroupOptions(count=1, block_ms=1000),
            )
            if res:
                for stream, entries in res.items():
                    for entry_id, pairs in entries.items():
                        if pairs is None:
                            continue
                        for field, payload_bytes in pairs:
                            task_id = field.decode("utf-8") if isinstance(field, bytes) else field
                            if payload_bytes is None:
                                continue
                            try:
                                task_data = msgspec.msgpack.decode(payload_bytes, type=TaskData)
                                await self.task_queue.put((UUID(task_id), task_data))
                                self._task_entry_ids[UUID(task_id)] = entry_id
                            except Exception as exc:
                                self.logger.error("Failed to decode task data: %s", exc)
                                continue
        except Exception as exc:
            self.logger.debug("Failed to fetch/parse task from Valkey stream: %s", exc)
            await self.disconnect()
            await self.connect()
```

Add this method after `fetch_tasks` (end of class):

```python
    async def on_task_completed(
        self,
        task_id: UUID,
        task_data: TaskData,
        task_result: TaskResult | None,
    ) -> None:
        """Acknowledge and delete the stream entry for a completed task.

        Called by the base ``AsyncTaskProcessor.handle_task`` when a task's
        processing terminates (success, error, or cancellation). Looks up the
        stream entry id recorded at fetch time and ``XACK``s + ``XDEL``s it, so
        the entry leaves the consumer group's pending list only after the
        handler's work on it is done (at-least-once). ``task_result`` is
        ``None`` when the task was cancelled before producing a result.

        Args:
            task_id: The unique identifier of the task.
            task_data: The task data that was processed.
            task_result: The final ``TaskResult``, or ``None`` on cancellation.
        """
        entry_id = self._task_entry_ids.pop(task_id, None)
        if entry_id is None or self.client is None:
            return
        try:
            await self.client.xack(self._task_stream_name, self._task_group_name, [entry_id])
            await self.client.xdel(self._task_stream_name, [entry_id])
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to acknowledge task %s: %s", task_id, exc)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_valkey_worker.py -v`
Expected: all pass, including the two new tests and the pre-existing connect/credentials tests.

- [ ] **Step 5: Verify lint and type**

Run: `uv run ruff check src/scietex/service/valkey/valkey_async_worker.py tests/test_valkey_worker.py`
Expected: no errors.

Run: `uv run ty check src/`
Expected: passes.

- [ ] **Step 6: Commit**

```bash
git add src/scietex/service/valkey/valkey_async_worker.py tests/test_valkey_worker.py
git commit -m "fix: ack Valkey stream entry on handler completion, not on enqueue (AR-005)"
```

---

### Task 5: `ValkeyWorker` — recover pending entries on startup via `XAUTOCLAIM`

**Files:**
- Modify: `src/scietex/service/valkey/valkey_async_worker.py` (`__init__` add `_recovered`; `fetch_tasks` trigger recovery once; add `_recover_pending_tasks`)
- Test: `tests/test_valkey_worker.py` (extend `DummyClient` with `xautoclaim`, append)

**Interfaces:**
- Consumes: `GlideClient.xautoclaim` (returns `[next_start, {entry_id: [[field, value]]}, [deleted_ids]]`), `TaskData`, `task_queue`.
- Produces: `self._recovered: bool` (once-flag); `async def _recover_pending_tasks(self) -> None` that claims every idle pending entry and enqueues it, recording entry ids; `fetch_tasks()` runs recovery once before its first `'>'` read.

- [ ] **Step 1: Extend `DummyClient` and write the failing test**

In `tests/test_valkey_worker.py`, add `xautoclaim_result` and an `xautoclaim` method to `DummyClient`:

```python
def __init__(self, ping_ok=True, xreadgroup_result=None, xautoclaim_result=None):
    self._ping_ok = ping_ok
    self.closed = False
    self.xreadgroup_result = xreadgroup_result
    self.xautoclaim_result = xautoclaim_result
    self.acked: list = []
    self.deleted: list = []


async def xautoclaim(self, *args, **kwargs):
    return self.xautoclaim_result
```

Append this test:

```python
@pytest.mark.asyncio
async def test_recover_pending_tasks_enqueues_pending_entries():
    """_recover_pending_tasks must claim idle pending entries and enqueue them,
    recording their entry ids for later ack (AR-005)."""
    import msgspec

    from scietex.service.task_handler.schemas import TaskData

    task_data = TaskData(task="dummy", payload=b"{}")
    payload = msgspec.msgpack.encode(task_data)
    # xautoclaim returns [next_start, {entry_id: [[field, value]]}, [deleted_ids]]
    client = DummyClient(
        xautoclaim_result=[
            b"0-0",
            {b"9-0": [[b"22222222-2222-2222-2222-222222222222", payload]]},
            [],
        ]
    )
    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    worker._client = client

    await worker._recover_pending_tasks()

    assert not worker.task_queue.empty()
    t_id, t_data = worker.task_queue.get_nowait()
    assert t_data.task == "dummy"
    assert worker._task_entry_ids[t_id] == b"9-0"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_valkey_worker.py::test_recover_pending_tasks_enqueues_pending_entries -v`
Expected: FAILS — `_recover_pending_tasks` does not exist yet.

- [ ] **Step 3: Add `_recovered`, `_recover_pending_tasks`, and trigger recovery in `fetch_tasks`**

In `__init__`, after the `_task_entry_ids` line added in Task 4, add:

```python
        # True once pending-entry recovery has run (start of the first
        # fetch_tasks), so a crash's unacked entries are redelivered once.
        self._recovered: bool = False
```

Add this method to the class (before `fetch_tasks`):

```python
    async def _recover_pending_tasks(self) -> None:
        """Re-enqueue stream entries left pending by a previous run.

        Uses ``XAUTOCLAIM`` to claim every entry in the consumer group's
        pending list that is idle (``min_idle_time_ms=0``) and enqueue it, so
        tasks that were read but never acknowledged before a crash are
        redelivered (at-least-once). Called once from the first
        ``fetch_tasks``, before any ``'>'`` read, when no tasks are in flight.

        Returns:
            None. No-op if the Valkey client is ``None``.
        """
        if self.client is None:
            return
        try:
            start = "0-0"
            while True:
                res = await self.client.xautoclaim(
                    self._task_stream_name,
                    self._task_group_name,
                    self._consumer_name,
                    0,
                    start,
                    count=10,
                )
                next_start = res[0]
                entries = res[1]
                for entry_id, pairs in entries.items():
                    if pairs is None:
                        continue
                    for field, payload_bytes in pairs:
                        task_id = field.decode("utf-8") if isinstance(field, bytes) else field
                        if payload_bytes is None:
                            continue
                        try:
                            task_data = msgspec.msgpack.decode(payload_bytes, type=TaskData)
                            await self.task_queue.put((UUID(task_id), task_data))
                            self._task_entry_ids[UUID(task_id)] = entry_id
                        except Exception as exc:
                            self.logger.error("Failed to decode recovered task data: %s", exc)
                if next_start == b"0-0" or next_start == "0-0":
                    break
                start = next_start
        except Exception as exc:
            self.logger.log(logging.ERROR, "Failed to recover pending tasks: %s", exc)
```

At the top of `fetch_tasks`, immediately after the `if self.client is None: return` guard, add:

```python
        if not self._recovered:
            self._recovered = True
            await self._recover_pending_tasks()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_valkey_worker.py -v`
Expected: all pass.

- [ ] **Step 5: Verify lint and type**

Run: `uv run ruff check src/scietex/service/valkey/valkey_async_worker.py tests/test_valkey_worker.py`
Expected: no errors.

Run: `uv run ty check src/`
Expected: passes.

- [ ] **Step 6: Commit**

```bash
git add src/scietex/service/valkey/valkey_async_worker.py tests/test_valkey_worker.py
git commit -m "feat: recover pending Valkey stream entries on startup (AR-005)"
```

---

### Task 6: Update `docs/architecture/data-flow.md`

**Files:**
- Modify: `docs/architecture/data-flow.md` (F2 lines 37-58, F3 lines 60-77)

**Interfaces:**
- Consumes: the behavior implemented in Tasks 1-5.

- [ ] **Step 1: Update F2 (Valkey task intake)**

Replace the F2 processing chain (lines 44-56) so it reflects ack-on-completion. The new F2 text:

```markdown
**Processing chain (`fetch_tasks`, 415):**
1. On the first call only, `_recover_pending_tasks` runs `XAUTOCLAIM` to
   re-enqueue entries left pending by a previous crash (at-least-once).
2. `XREADGROUP` on group `...:task_group`, consumer `...`, key `>`, count 1,
   `block_ms=1000`.
3. Per entry: decode field → UUID, decode value →
   `msgspec.msgpack.decode(payload, type=TaskData)`; `await
   task_queue.put((UUID(task_id), task_data))` — now flows through F1. The
   entry id is recorded in `_task_entry_ids[task_id]`.
4. Decode errors: logged, entry skipped. Read errors: `disconnect()` +
   `connect()` (reconnect).

**Transformation:** msgpack `bytes` → `TaskData` struct → typed in-memory queue
items. The stream entry is **NOT acknowledged on enqueue**; it stays in the
consumer group's pending list until `on_task_completed` acks it after the
handler's work terminates (see F1 destination note).
```

- [ ] **Step 2: Update F1 destination note and F3**

In F1 (lines 29-31), replace the destination note to mention the completion hook:

```markdown
**Destination:** the `TaskResult` is returned to `handle_task`, which logs it
at DEBUG and invokes `on_task_completed(task_id, task_data, task_result)` in
its `finally` — the transport-agnostic ack/result-sink seam. `ValkeyWorker`
overrides it to `XACK`+`XDEL` the stream entry. `running_tasks` entry is
popped and `task_queue.task_done()` called.
```

In F3 (lines 60-77), replace the path and note to reflect requeue-only-after-stop and the drain change:

```markdown
**Path:** `AsyncTaskProcessor.watchdog` (562) cancels `worker_task` when
`elapsed > task_data.timeout.timeout` (or `DEFAULT_TASK_TIMEOUT=3`), waits up
to `WORKER_TASK_CANCELLATION_TIMEOUT`, and only if the handler actually
stopped (`worker_task.done()`) calls `return_task_to_queue(task_id,
task_data)` when `timeout_action == "requeue"`. Base `return_task_to_queue`
(383) is a no-op; `ValkeyWorker` (396) does `XADD` back to the same task
stream (tail), re-entering F2/F1. A handler that ignores cancellation is not
requeued (its entry stays pending and is redelivered on restart).

**Shutdown drain** (`AsyncTaskProcessor.cleanup`, 413): queued-but-undispatched
items are dropped (their transport entries stay pending and are redelivered on
restart); in-flight running tasks are requeued through the same hook only after
their handler is confirmed stopped, when `canceled_action == "requeue"`.

**Note:** requeue via `XADD` appends to the **tail** of the stream — original
ordering is not preserved. The original entry is acknowledged by
`handle_task`'s `finally` when the handler stops, so a requeued task yields
exactly one retry copy (see §H8 for the swallowed-cancellation caveat).
```

- [ ] **Step 3: Verify the doc reads consistently**

Read the updated `docs/architecture/data-flow.md` end-to-end. Confirm F1-F3 no longer describe ack-on-enqueue or unconditional requeue.

- [ ] **Step 4: Commit**

```bash
git add docs/architecture/data-flow.md
git commit -m "docs: reflect at-least-once ack-on-completion contract (AR-005)"
```

---

### Task 7: Full verification

**Files:**
- None (verification only).

**Interfaces:**
- Consumes: all prior tasks.

- [ ] **Step 1: Run the full gate**

Run: `uv run ruff check src/`
Expected: no errors.

Run: `uv run ty check src/`
Expected: passes.

Run: `uv run pytest tests/ -q`
Expected: all tests pass (existing suite + the new AR-005 tests).

- [ ] **Step 2: Confirm the working tree is clean of unintended changes**

Run: `git status`
Expected: only the files each task committed plus this plan file (`docs/superpowers/` is untracked). No stray edits.

- [ ] **Step 3: Commit any stragglers**

If the full gate surfaced nothing new, no commit is needed here (all prior tasks committed). If a fix was required, commit it with a descriptive message.

---

## Risks

- **Sub-millisecond loss window between `finally`-ack and watchdog `XADD`.** In Task 2, the handler's `finally` acks the original entry, then the watchdog appends the retry. A crash in the tiny window between those two awaits loses the retry. This is far rarer than the duplicate it prevents and is accepted per the review's "requeue conditional on cancellation." A per-task lease/attempt-count (out of scope) is the proper fix.
- **Hung handler that ignores cancellation and never returns is not retried this run.** Task 2 drops tracking without requeueing a stuck handler. If the handler never finishes, its entry stays pending and the next restart's recovery redelivers it — no permanent loss, but no retry within the current process lifetime. This is the intended tradeoff (requeueing a still-running handler would run the task twice concurrently).
- **`_task_entry_ids` assumes one pending entry per `task_id`.** `XACK` needs the stream entry id, but `handle_task` only carries the `task_id` (the entry's field). The map is keyed by `task_id`; if a producer writes two stream entries with the same UUID field while both are pending, the second overwrites the first and one entry is never acked (redelivered on restart). A producer reusing a UUID is a producer bug; the framework assumes UUID uniqueness.
- **Recovery is startup-only and best-effort.** `_recover_pending_tasks` runs once at the first `fetch_tasks`. If it fails (logged), pending entries stay stranded until the next restart. Mid-run disconnects do not re-run recovery — in-flight entries ack normally, and a crash is recovered on the next start. This is correct because `_startup` starts managers before `initialize()` (basic_async_worker.py:770 vs 773), so recovery cannot safely live in `initialize()`.
- **`cleanup` drain no longer requeues.** Subclasses whose transport does NOT keep items pending after enqueue (unlike `ValkeyWorker`'s stream) must override `cleanup` to requeue drained items, or they will be lost on shutdown. Documented in the `cleanup` docstring.
- **`await` in `handle_task`'s `finally` during cancellation.** When a task is cancelled, `finally` awaits `on_task_completed`. The watchdog's `wait_for` does not re-cancel unless its 5 s timeout fires, so the ack completes normally. If a re-cancel did abort the ack, the entry stays pending and is redelivered on restart (at-least-once safe, just a duplicate). The `ValkeyWorker` ack is a single fast command.
- **`on_task_completed` is new public surface.** It is a no-op by default, so existing `AsyncTaskProcessor` subclasses are unaffected. `ValkeyWorker` is the only in-repo override.

## Handoff Plan

1. Task 1: in `async_tasks_processor.py` add the no-op `on_task_completed(task_id, task_data, task_result)` hook after `return_task_to_queue` (~line 393); rewrite inner `handle_task` (502-526) to capture `result` and call the hook in `finally` (wrapped in its own try/except that logs). Add `RecordingProcessor` + `test_handle_task_invokes_completion_hook`.
2. Task 2: in `async_tasks_processor.py` rewrite the `watchdog` timeout branch (579-601) to `cancel()` → `wait_for(WORKER_TASK_CANCELLATION_TIMEOUT)` → requeue only if `worker_task.done()`; always pop `running_tasks`. Add `StubbornHandler` + `test_watchdog_does_not_requeue_when_handler_ignores_cancellation` (monkeypatch `WORKER_TASK_CANCELLATION_TIMEOUT` to 0.05).
3. Task 3: in `async_tasks_processor.py` rewrite `cleanup` (413-447): drain queue with `get_nowait()`/`task_done()` without `return_task_to_queue`; cancel running tasks and requeue only if `done()` and `canceled_action == "requeue"`. Add `test_cleanup_drain_does_not_requeue_queued_tasks`.
4. Task 4: in `valkey_async_worker.py` import `TaskResult`; add `self._task_entry_ids: dict[UUID, str | bytes] = {}` in `__init__`; rewrite `fetch_tasks` (415-466) to drop `XACK`/`XDEL` and record `self._task_entry_ids[UUID(task_id)] = entry_id` after `put`; add `on_task_completed` override that pops the entry id and `XACK`s+`XDEL`s it. Extend `DummyClient` (record `acked`/`deleted`, configurable `xreadgroup_result`); add `test_fetch_tasks_does_not_ack_on_enqueue` and `test_on_task_completed_acks_and_deletes_entry`.
5. Task 5: in `valkey_async_worker.py` add `self._recovered = False` in `__init__`; add `_recover_pending_tasks` (loop `XAUTOCLAIM(..., 0, start, count=10)` until `next_start == "0-0"`, enqueue + record entry ids); trigger it once at the top of `fetch_tasks`. Extend `DummyClient` with `xautoclaim`; add `test_recover_pending_tasks_enqueues_pending_entries`.
6. Task 6: update `docs/architecture/data-flow.md` F1 destination note, F2, and F3 to the ack-on-completion + recovery + requeue-only-after-stop contract.
7. Task 7: full gate `uv run ruff check src/ && uv run ty check src/ && uv run pytest tests/ -q`.

- Risk: do not reintroduce ack-on-enqueue in `fetch_tasks`; do not requeue a still-running handler in `watchdog`/`cleanup`; do not put recovery in `initialize()` (managers start first — race); keep `_task_entry_ids` keyed by `task_id` (documented one-pending-entry-per-id assumption).
- Test: `uv run pytest tests/test_valkey_worker.py tests/test_async_task_processor.py -q` after Tasks 1-5, and the full suite in Task 7.
