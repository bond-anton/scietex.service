# Switch to scietex.logging >= 1.0 and Simplify Logging Restart

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bump `scietex.logging` to `>=1.0.0` and delete the handler-factory/replacement workaround in `BasicAsyncWorker`, because 1.0 handlers are restartable in place.

**Architecture:** In scietex.logging 0.2.x an `AsyncBaseHandler` was single-use once `stop_logging()` closed it, so `BasicAsyncWorker` recorded a factory per handler and swapped in a fresh instance on restart. In 1.0 the new base `AsyncLoggingHandler` makes `start_logging()`/`stop_logging()` restartable on the same event loop (`stop_logging()` is idempotent; a stopped handler may be started again). The factory registry and replacement logic become dead weight and are removed. Handlers are registered once as instances and restarted in place.

**Tech Stack:** Python 3.10+, asyncio, `scietex.logging>=1.0.0`, `msgspec`, optional `valkey-glide`.

**Spec:** User request: "switch to scietex.logging >= 1.0 in dependencies and simplify the logging restart because scietex.logging support handler restart." Verified against the 1.0 source at `/Users/anton/Projects/scietex.logging` (`async_logging_handler.py`).

## Global Constraints

- `scietex.logging>=1.0.0` in both `dependencies` and the `valkey` extra (was `>=0.2.0`).
- 1.0.0 is **published on PyPI** (verified: latest = 1.0.0). No local-path install needed; re-lock with `uv lock`/`uv sync`.
- `AsyncBaseHandler` and `AsyncValkeyHandler` import surface is unchanged in 1.0 (`scietex.logging/__init__.py` still exports both; `AsyncValkeyHandler` remains conditional on `glide`).
- Handler identity keys are the class name: `handler.name` is `None` (both versions extend `logging.Handler`), so the code falls back to `handler.__class__.__name__` → keys `"AsyncBaseHandler"` / `"AsyncValkeyHandler"`.
- Verification commands (from AGENTS.md): `ruff check src/`, `ty check src/`, `pytest tests/`. Order: lint → type → test.
- No new dependencies beyond the version bump. No emoji in code/comments. Match existing style.

---

## Design Decisions (recommended)

1. **`_register_logger_handler` signature → `(handler, name=None)`.** Handlers are restartable in place, so no factory is needed. Both call sites already construct the handler inline; passing the instance directly is the simplest clean signature. The `name` param stays for the Valkey case (it forces the key to `"AsyncValkeyHandler"` even though the class name already matches — kept for explicitness and to avoid churn in `valkey_async_worker.py`).

2. **Remove `__logger_handler_factories` entirely.** It exists only to re-create closed handlers. With in-place restart it is dead.

3. **Keep `__loggers_statuses` + `LoggerStatus`.** The handler exposes `logging_running_event.is_set()`, but keeping the dict gives: (a) a stable observability contract the existing test asserts on, (b) a single place that records STOPPED/RUNNING per handler name, and (c) avoids coupling `_logger_start_handlers` to the handler's internal event. It is cheap bookkeeping. **Decision: keep it.** (Removing it would break `tests/test_basic_async_service.py` and the documented observability surface — not worth the marginal simplicity.)

4. **`_logger_start_handlers()`** calls `start_logging()` on each handler whose recorded status is not RUNNING, guarded by the status dict (not the handler event) to preserve the existing contract. Keep the per-handler `asyncio.wait_for(..., timeout=self.logger_handler_timeout)` and the print-fallback error handling. **Remove** the factory-replacement block (lines 563-574) — no handler is ever removed/replaced now.

5. **`_logger_shut_down_handlers()`** calls `stop_logging()` unconditionally (it is idempotent in 1.0) and records STOPPED. Keep per-handler timeout + error handling. **Keep** the outer `loggers_timeout` wrapper in `_shutdown()` (lines 852-861): it is a cheap safety net against a pathological handler and removing it is out of scope for this refactor.

6. **Test rewrite:** `test_logging_handlers_restartable_after_shutdown` asserts the SAME handler instance is reused across restart and transitions STOPPED→RUNNING→STOPPED. `test_graceful_shutdown` is untouched.

7. **Dependency install:** 1.0.0 is on PyPI → bump `pyproject.toml`, run `uv lock` then `uv sync --extra dev --extra lint --extra test`. No local path override.

---

## File Structure

- `pyproject.toml` — version bump (2 lines) + re-lock.
- `src/scietex/service/basic_async_worker.py` — remove factory registry, simplify `_register_logger_handler`, `_logger_start_handlers`, `_logger_shut_down_handlers`.
- `src/scietex/service/valkey/valkey_async_worker.py` — update `_register_logger_handler` call to pass an instance.
- `tests/test_basic_async_service.py` — rewrite restart test to in-place semantics.
- `tests/test_async_task_processor.py` — remove dead `_logger_init_handlers` override (method does not exist); keep `_logger_shut_down_handlers` override.
- Docs (see Task 6).

---

### Task 1: Bump dependency and re-lock

**Files:**
- Modify: `pyproject.toml:18` and `pyproject.toml:25`
- Regenerate: `uv.lock`

**Interfaces:**
- Consumes: nothing.
- Produces: `scietex.logging>=1.0.0` resolvable in the venv (needed by every later task's tests).

- [ ] **Step 1: Edit `pyproject.toml`**

Change line 18:
```toml
dependencies = ["msgspec>=0.20.0", "pyaml>=26.2.1", "scietex.logging>=0.2.0"]
```
to:
```toml
dependencies = ["msgspec>=0.20.0", "pyaml>=26.2.1", "scietex.logging>=1.0.0"]
```

Change line 25:
```toml
valkey = ["scietex.logging[valkey]>=0.2.0", "valkey-glide~=2.5.0"]
```
to:
```toml
valkey = ["scietex.logging[valkey]>=1.0.0", "valkey-glide~=2.5.0"]
```

- [ ] **Step 2: Re-lock and sync**

Run: `uv lock && uv sync --extra dev --extra lint --extra test`
Expected: resolves `scietex-logging==1.0.0` (or newer 1.x) in `uv.lock`.

- [ ] **Step 3: Verify installed version**

Run: `.venv/bin/python -c "import scietex.logging as l; print(l.__version__)"`
Expected: `1.0.0` (or newer 1.x).

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: bump scietex.logging to >=1.0.0"
```

---

### Task 2: Simplify `BasicAsyncWorker` logging lifecycle

**Files:**
- Modify: `src/scietex/service/basic_async_worker.py`

**Interfaces:**
- Consumes: `scietex.logging.AsyncBaseHandler` (1.0, restartable), `LoggerStatus` from `.logging`.
- Produces: `_register_logger_handler(handler: AsyncBaseHandler, name: str | None = None) -> None`; `_logger_start_handlers()` and `_logger_shut_down_handlers()` with in-place restart semantics. `__logger_handler_factories` removed; `__loggers_statuses` retained.

- [ ] **Step 1: Remove the factory registry field and register a handler instance**

In `__init__`, replace lines 141-147:
```python
# Factories that produce fresh async handlers, keyed by handler name.
# Used to re-create handlers after a shutdown, since an
# ``AsyncBaseHandler`` is single-use once ``stop_logging()`` closes it.
self.__logger_handler_factories: dict[str, Callable[[], AsyncBaseHandler]] = {}
self._register_logger_handler(lambda: AsyncBaseHandler(service_name=self.__service_name, worker_id=self.__worker_id))
```
with:
```python
# Async handlers are restartable in place (scietex.logging >= 1.0), so a
# single instance is registered once and restarted on each start cycle.
self._register_logger_handler(AsyncBaseHandler(service_name=self.__service_name, worker_id=self.__worker_id))
```

- [ ] **Step 2: Simplify `_register_logger_handler`**

Replace lines 523-544 (the whole method) with:
```python
    def _register_logger_handler(
        self,
        handler: AsyncBaseHandler,
        name: str | None = None,
    ) -> None:
        """
        Attach an async logging handler to the logger.

        The handler is restartable in place (``start_logging``/``stop_logging``
        may be called repeatedly on the same event loop), so a single instance
        is registered once and reused across start/stop cycles.

        Args:
            handler: The ``AsyncBaseHandler`` (or subclass) to attach.
            name: Optional explicit handler name. Defaults to the handler's
                ``name`` attribute or its class name.
        """
        handler.setLevel(self.logging_level)
        handler_name = name or handler.name or handler.__class__.__name__
        self.logger.addHandler(handler)
```

- [ ] **Step 3: Simplify `_logger_start_handlers`**

Replace lines 546-593 (the whole method) with:
```python
async def _logger_start_handlers(self) -> None:
    """
    Start all async logging handlers that are not already running.

    Iterates over the logger's handlers and calls start_logging() on each
    AsyncBaseHandler whose recorded status is not RUNNING. Handlers are
    restartable in place, so no replacement is needed. Handles timeouts and
    errors gracefully, falling back to print statements if the logger is in
    an unrecoverable state.
    """
    for handler in list(self.logger.handlers):
        handler_name = handler.name or handler.__class__.__name__
        if handler_name not in self.__loggers_statuses or self.__loggers_statuses[handler_name] == LoggerStatus.STOPPED:
            if isinstance(handler, AsyncBaseHandler):
                try:
                    await asyncio.wait_for(handler.start_logging(), timeout=self.logger_handler_timeout)
                except asyncio.TimeoutError:
                    try:
                        self.logger.warning("Timeout starting logging handler %s (%s)", handler_name, handler)
                    except Exception:
                        # logger itself may be in a bad state; fallback to print
                        print(f"Timeout starting logging handler {handler_name} ({handler})")
                except Exception as e:
                    try:
                        self.logger.error(
                            "Failed to start logging handler %s (%s): %s",
                            handler_name,
                            handler,
                            e,
                        )
                    except Exception:
                        print(f"Failed to start logging handler {handler_name} ({handler}): {e}")
            self.__loggers_statuses[handler_name] = LoggerStatus.RUNNING
```

- [ ] **Step 4: Simplify `_logger_shut_down_handlers`**

Replace lines 595-622 (the whole method) with:
```python
    async def _logger_shut_down_handlers(self) -> None:
        """Cleanly shut down all async logging handlers.

        This will attempt to stop each `AsyncBaseHandler` with a per-handler
        timeout to avoid hanging shutdowns if a handler blocks. `stop_logging`
        is idempotent in scietex.logging >= 1.0, so it is safe to call on every
        handler regardless of its current state.
        """
        for handler in self.logger.handlers:
            handler_name = handler.name or handler.__class__.__name__
            if isinstance(handler, AsyncBaseHandler):
                try:
                    await asyncio.wait_for(handler.stop_logging(), timeout=self.logger_handler_timeout)
                except asyncio.TimeoutError:
                    try:
                        self.logger.warning("Timeout stopping logging handler %s (%s)", handler_name, handler)
                    except Exception:
                        # logger itself may be in a bad state; fallback to print
                        print(f"Timeout stopping logging handler {handler_name} ({handler})")
                except Exception as e:
                    try:
                        self.logger.error(
                            "Failed to shut down logging handler %s (%s): %s",
                            handler_name,
                            handler,
                            e,
                        )
                    except Exception:
                        print(f"Failed to shut down logging handler {handler_name} ({handler}): {e}")
            self.__loggers_statuses[handler_name] = LoggerStatus.STOPPED
```

- [ ] **Step 5: Remove the now-unused `Callable` import**

`Callable` is imported at line 12 (`from collections.abc import Callable, Generator`). After removing the factory dict type annotation, `Callable` is unused. Change line 12 to:
```python
from collections.abc import Generator
```

- [ ] **Step 6: Verify lint, type, and existing tests**

Run: `ruff check src/scietex/service/basic_async_worker.py`
Expected: no errors (in particular no unused `Callable` import).

Run: `ty check src/`
Expected: passes.

Run: `pytest tests/test_basic_async_service.py -q`
Expected: `test_graceful_shutdown` passes. `test_logging_handlers_restartable_after_shutdown` will FAIL at this point (it still asserts replacement semantics) — that is expected and fixed in Task 4.

- [ ] **Step 7: Commit**

```bash
git add src/scietex/service/basic_async_worker.py
git commit -m "refactor: restart async logging handlers in place (scietex.logging >= 1.0)"
```

---

### Task 3: Update `ValkeyWorker` registration call

**Files:**
- Modify: `src/scietex/service/valkey/valkey_async_worker.py:139-148`

**Interfaces:**
- Consumes: `_register_logger_handler(handler, name=None)` from Task 2.
- Produces: `ValkeyWorker` registers a single `AsyncValkeyHandler` instance.

- [ ] **Step 1: Pass a handler instance instead of a factory**

Replace lines 139-148:
```python
        self._register_logger_handler(
            lambda: AsyncValkeyHandler(
                stream_name=self._log_stream_name,
                service_name=self.service_name,
                worker_id=self.worker_id,
                valkey_config=self._client_config,
                stdout_enable=False,
            ),
            name="AsyncValkeyHandler",
        )
```
with:
```python
        self._register_logger_handler(
            AsyncValkeyHandler(
                stream_name=self._log_stream_name,
                service_name=self.service_name,
                worker_id=self.worker_id,
                valkey_config=self._client_config,
                stdout_enable=False,
            ),
            name="AsyncValkeyHandler",
        )
```

- [ ] **Step 2: Verify**

Run: `ruff check src/scietex/service/valkey/valkey_async_worker.py`
Expected: no errors.

Run: `ty check src/`
Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add src/scietex/service/valkey/valkey_async_worker.py
git commit -m "refactor: register AsyncValkeyHandler instance directly"
```

---

### Task 4: Rewrite the restart test to in-place semantics

**Files:**
- Modify: `tests/test_basic_async_service.py:45-87`

**Interfaces:**
- Consumes: `BasicAsyncWorker`, `ServiceStatus`, `LoggerStatus`; the retained `__loggers_statuses` dict.
- Produces: a test asserting the SAME handler instance is reused across restart.

- [ ] **Step 1: Replace the test body**

Replace lines 45-87 (the whole `test_logging_handlers_restartable_after_shutdown` function) with:
```python
@pytest.mark.asyncio
async def test_logging_handlers_restartable_after_shutdown():
    """After a shutdown, logging handlers must be marked STOPPED and be
    restarted in place (same instance) on the next start (scietex.logging >= 1.0)."""
    worker = BasicAsyncWorker(service_name="test_service", version="1.0.0")

    await worker.start()
    for _ in range(50):
        if worker.state == ServiceStatus.RUNNING:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.RUNNING

    statuses = worker._BasicAsyncWorker__loggers_statuses
    assert statuses.get("AsyncBaseHandler") == LoggerStatus.RUNNING
    first_handler = next(h for h in worker.logger.handlers if h.__class__.__name__ == "AsyncBaseHandler")

    await worker.stop()
    for _ in range(50):
        if worker.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.STOPPED
    # Handlers must be recorded as STOPPED after shutdown.
    assert statuses.get("AsyncBaseHandler") == LoggerStatus.STOPPED

    # Restart: the same handler instance is reused and restarted in place.
    await worker.start()
    for _ in range(50):
        if worker.state == ServiceStatus.RUNNING:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.RUNNING
    assert statuses.get("AsyncBaseHandler") == LoggerStatus.RUNNING
    second_handler = next(h for h in worker.logger.handlers if h.__class__.__name__ == "AsyncBaseHandler")
    assert second_handler is first_handler, "handler should be restarted in place, not replaced"

    await worker.stop()
    for _ in range(50):
        if worker.state == ServiceStatus.STOPPED:
            break
        await asyncio.sleep(0.05)
    assert worker.state == ServiceStatus.STOPPED
```

- [ ] **Step 2: Run the test**

Run: `pytest tests/test_basic_async_service.py -q`
Expected: both tests pass (2 passed).

- [ ] **Step 3: Commit**

```bash
git add tests/test_basic_async_service.py
git commit -m "test: assert logging handlers restart in place (scietex.logging >= 1.0)"
```

---

### Task 5: Remove dead `_logger_init_handlers` override in processor test

**Files:**
- Modify: `tests/test_async_task_processor.py:44-48`

**Interfaces:**
- Consumes: nothing (the override is dead code).
- Produces: `DemoProcessor` still disables real logging stop via `_logger_shut_down_handlers`.

**Context:** `_logger_init_handlers` does not exist anywhere in the codebase — the real startup method is `_logger_start_handlers` (AR-014 "Test mock drift"). The override at line 44 is a no-op on a nonexistent method and can be deleted. The `_logger_shut_down_handlers` override at line 47 IS real and must stay (it prevents the processor test from actually stopping the console handler).

- [ ] **Step 1: Remove the dead override**

Replace lines 44-48:
```python
async def _logger_init_handlers(self) -> None:  # disable real logging start
    return None


async def _logger_shut_down_handlers(self) -> None:  # disable real logging stop
    return None
```
with:
```python
    async def _logger_shut_down_handlers(self) -> None:  # disable real logging stop
        return None
```

- [ ] **Step 2: Run the processor tests**

Run: `pytest tests/test_async_task_processor.py -q`
Expected: passes (2 passed).

- [ ] **Step 3: Commit**

```bash
git add tests/test_async_task_processor.py
git commit -m "test: drop dead _logger_init_handlers override"
```

---

### Task 6: Update documentation

**Files:**
- Modify: `README.md:28`
- Modify: `docs/architecture/dependencies.md:90`
- Modify: `docs/architecture/components.md:218` (dependency version note)
- Modify: `docs/reviews/architecture/2026-09-05.md` AR-004 section (lines 108-132) — add a note that the factory-registry workaround is superseded by scietex.logging 1.0 in-place restart.
- Modify: `docs/architecture/lifecycle.md:118-133` (async logging handler lifecycle section) — the "closed after shutdown" analysis is obsolete.
- Modify: `docs/architecture/hotspots.md:49-60` (H4 section) — the finding is resolved by the 1.0 upgrade; mark it resolved.
- Modify: `docs/architecture/structure.md:27-28` — `logging.py` row and `basic_async_worker.py` row (remove "factory" wording if present; the row text does not mention factories, but the `logging.py` row stays accurate since `LoggerStatus` is retained).
- Modify: `docs/architecture/overview.md:58-60` — only if it describes replacement; it describes start/stop lifecycle which is unchanged, so no edit needed unless wording implies re-creation.
- Modify: `docs/reviews/architecture/2026-09-04.md` AR-004 (lines 228-259) — historical record; add a pointer that the recommendation was implemented via the 1.0 upgrade. Optional.

**Interfaces:**
- Consumes: the code changes from Tasks 1-5.
- Produces: docs consistent with in-place restart and `>=1.0.0`.

- [ ] **Step 1: Update dependency version references**

`README.md:28`:
```
**Dependencies:** `msgspec>=0.20.0`, `pyaml>=26.2.1`, `scietex.logging>=0.2.0`
```
→ change `>=0.2.0` to `>=1.0.0`.

`docs/architecture/dependencies.md:90`:
```
| `scietex.logging>=0.2.0` | core deps | ...
```
→ change `>=0.2.0` to `>=1.0.0`.

`docs/architecture/components.md:218`:
```
Installed dependency (>=0.2.0). The package embeds this framework's log sink.
```
→ change `(>=0.2.0)` to `(>=1.0.0)`.

- [ ] **Step 2: Update the AR-004 section in `2026-09-05.md`**

After the existing AR-004 "Fix (verified)" bullets (lines 114-126) and before the "Assessment" paragraph (line 128), append a note. The cleanest edit is to add a sentence to the Assessment paragraph (lines 128-132). Replace:
```
**Assessment:** This resolves the resumability gap and, together with AR-001,
makes the `STOPPED → RUNNING` restart path actually work. The factory-registry
design is clean and correctly scoped to the single-use nature of the external
handlers. The `_logger_start_handlers` mutation of `logger.handlers` while
iterating is correctly handled by iterating a `list(...)` copy.
```
with:
```
**Assessment:** This resolves the resumability gap and, together with AR-001,
makes the `STOPPED → RUNNING` restart path actually work. The factory-registry
design is clean and correctly scoped to the single-use nature of the external
handlers. The `_logger_start_handlers` mutation of `logger.handlers` while
iterating is correctly handled by iterating a `list(...)` copy.

**Superseded (2026-09-06):** scietex.logging 1.0 makes handlers restartable in
place (`AsyncLoggingHandler.start_logging`/`stop_logging`), so the factory
registry and replacement logic were removed. Handlers are now registered once
as instances and restarted in place; `__loggers_statuses` is retained.
```

- [ ] **Step 3: Update the async logging handler lifecycle section in `lifecycle.md`**

Replace lines 125-133 (the external-handler note and the obsolete analysis block):
```
- The external `scietex.logging` handlers spawn their own internal worker tasks
  on `start_logging()` and call `self.close()` at the end of `stop_logging()`
  (`basic_handler.py:235`) — the handler is **closed** after shutdown.

> *Analysis* — after one shutdown the logger handlers are closed but
> `__loggers_statuses` is left as RUNNING (`_logger_shut_down_handlers`,
> basic_async_worker.py:492), so a second `start()` on the same instance skips
> restarting them. Instance reuse across start/stop cycles appears unreliable
> (§H4).
```
with:
```
- The external `scietex.logging` handlers (>= 1.0) are restartable in place:
  `start_logging()`/`stop_logging()` may be called repeatedly on the same event
  loop. `_logger_start_handlers` starts each handler whose recorded status is
  not RUNNING; `_logger_shut_down_handlers` calls the idempotent
  `stop_logging()` and records STOPPED. `__loggers_statuses` tracks
  STOPPED/RUNNING per handler name.
```

- [ ] **Step 4: Mark H4 resolved in `hotspots.md`**

The H4 section (lines 49-60) describes the old bug (handlers recorded RUNNING after shutdown, single-use handlers). Add a resolution note at the end of the section (after line 60). Append:
```
**Resolved (2026-09-06):** `_logger_shut_down_handlers` records STOPPED, and
scietex.logging >= 1.0 handlers are restartable in place, so a second `start()`
restarts the same handler instances. See `docs/reviews/architecture/2026-09-05.md`.
```

- [ ] **Step 5: Verify docs are internally consistent**

Run: `grep -rn "0.2.0\|factory-registry\|__logger_handler_factories\|re-created\|single-use" README.md docs/`
Expected: no remaining references to the factory mechanism or `0.2.0` in active (non-historical) docs. Historical review files (`2026-09-04.md`, `2026-09-05.md`) may retain `0.2.0`/factory wording in their "Prior problem" descriptions — that is acceptable as history, but the superseded note must be present.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/
git commit -m "docs: reflect in-place logging restart and scietex.logging >= 1.0"
```

---

### Task 7: Full verification

**Files:**
- None (verification only).

**Interfaces:**
- Consumes: all prior tasks.

- [ ] **Step 1: Run the full gate**

Run: `ruff check src/`
Expected: no errors.

Run: `ty check src/`
Expected: passes.

Run: `pytest tests/ -q`
Expected: all tests pass.

- [ ] **Step 2: Confirm no stale references remain in code**

Run: `grep -rn "__logger_handler_factories\|_logger_init_handlers" src/ tests/`
Expected: no matches (both removed).

Run: `grep -rn "scietex.logging>=0.2.0\|scietex.logging\[valkey\]>=0.2.0" pyproject.toml`
Expected: no matches.

- [ ] **Step 3: Commit any stragglers**

If the full gate surfaced nothing new, no commit is needed here (all prior tasks committed). If a fix was required, commit it with a descriptive message.

---

## Risks

- **Removing `__loggers_statuses` would break the observability/test contract.** Decision: keep it. The handler's `logging_running_event` is an alternative source of truth but would couple the worker to the handler's internal event and break `tests/test_basic_async_service.py`. Keeping the dict is the lower-risk choice.
- **`AsyncValkeyHandler` import surface in 1.0:** unchanged — still exported from `scietex.logging` conditionally on `glide`. No import change needed in `valkey_async_worker.py`.
- **`handler.name` is `None`** in both versions, so handler keys fall back to class names. The `name="AsyncValkeyHandler"` argument in `valkey_async_worker.py` is redundant-but-harmless (class name already matches); kept to avoid churn.
- **`test_async_task_processor.py` dead override:** `_logger_init_handlers` never existed (real method is `_logger_start_handlers`). Removing it is safe; the `_logger_shut_down_handlers` override must stay or the processor test will attempt a real logging stop.
- **Docs line numbers are already stale** (they reference 429/465 vs actual 546/595). This plan updates only the *mechanism* descriptions and version strings, not every line number — renumbering all docs is out of scope.
- **1.0.0 is published**, so no local-path install is needed. If a future 1.x release is not yet on PyPI, fall back to `uv add scietex.logging --editable ../scietex.logging` for local testing only.

## Handoff Plan

1. Task 1: bump `pyproject.toml:18,25` to `>=1.0.0`; `uv lock && uv sync --extra dev --extra lint --extra test`; verify `l.__version__ == 1.0.0`.
2. Task 2: in `basic_async_worker.py` remove `__logger_handler_factories` (141-147), change `_register_logger_handler` to take a handler instance (523-544), strip the replacement block from `_logger_start_handlers` (546-593), simplify `_logger_shut_down_handlers` (595-622), drop unused `Callable` import (line 12).
3. Task 3: `valkey_async_worker.py:139-148` — pass `AsyncValkeyHandler(...)` instance, not a lambda.
4. Task 4: rewrite `tests/test_basic_async_service.py:45-87` to assert `second_handler is first_handler`.
5. Task 5: delete the dead `_logger_init_handlers` override in `tests/test_async_task_processor.py:44-45`.
6. Task 6: update `README.md:28`, `docs/architecture/dependencies.md:90`, `docs/architecture/components.md:218`, and add superseded/resolved notes to `2026-09-05.md` AR-004, `lifecycle.md:118-133`, `hotspots.md` H4.
7. Task 7: full gate `ruff check src/ && ty check src/ && pytest tests/ -q`.

- Risk: keep `__loggers_statuses` (observability contract); keep the `_logger_shut_down_handlers` override in the processor test; do not renumber stale doc line references.
- Test: `pytest tests/test_basic_async_service.py -q` (2 passed) and `pytest tests/ -q` (full suite green) after Task 4.
