# AR-018 Single Shared GlideClient Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix AR-018 in v3 by making `ValkeyWorker` run a single `GlideClient` lifecycle shared with the logging `AsyncValkeyHandler`, removing the two-independent-client model and its divergence machinery.

**Architecture:** Bump the `scietex.logging` floor to 1.2.0 (which adds the client-injection seam), then wire the worker's single `GlideClient` into the `AsyncValkeyHandler` via that seam. Because the seam fixes `_injected_client`/`_owns_client` at handler construction and the worker's client is created asynchronously in `connect()`, the handler is constructed lazily on the first successful `connect()` (after the client exists) and reused across restarts. The worker is the sole teardown owner; the handler never closes the shared client.

**Tech Stack:** Python 3.10+, `scietex.logging>=1.2.0`, `valkey-glide~=2.5.0`, `msgspec`, `pytest`/`pytest-asyncio`.

**Spec:** AR-018 (docs/reviews/architecture/2026-09-06.md §AR-018, H9) and the v4 roadmap entry being pulled forward (docs/ROADMAP.md "v4 — Single shared GlideClient").

## Global Constraints

- `scietex.logging` floor bumped to `>=1.2.0` in **both** pyproject dependency lines (base `dependencies` and the `valkey` extra).
- The worker is the **sole** closer of the shared `GlideClient`. The `AsyncValkeyHandler` must never close it.
- No new dependencies. No new models. Pure-config/refactor philosophy.
- Match existing style: `const`-like discipline, no emoji, comments explain WHY not WHAT, no dead code (every public method needs a caller).
- The venv currently has `scietex.logging==0.2.0` (no seam). **Re-sync the venv to 1.2.0 before running any test** (`uv sync --extra dev --extra lint --extra test --extra valkey`). The sibling repo `/home/anton/Projects/scietex.logging` is the source of 1.2.0 (commit 55c6d6e).

## Verified facts the plan relies on (do not re-derive)

- **Seam ownership semantics** (sibling repo, message_broker_handler.py): `_owns_client = client is None` (119), `_injected_client = client` (120), both fixed at construction. `_connect()` (185-188) restores `self.client = self._injected_client` when not owned; `_disconnect()` (197-199) is a no-op when not owned. The handler worker loop (`_worker`, 201-276) calls only these ownership-aware wrappers — never the raw subclass `connect()`/`disconnect()`. So an injected client is never closed by the handler and is restored on reconnect.
- **The client cannot be injected after construction.** `_injected_client` is fixed in `__init__`. Setting `handler.client` directly is fragile: after a send failure the loop nulls `self.client` (264/276) and `_connect()` restores `_injected_client` — which would be `None` if never injected.
- **The worker's client is created asynchronously** in `connect()` (valkey_async_worker.py:319) during `initialize()`, which runs *after* `__init__` and *after* `_logger_start_handlers()` in `_startup()` (basic_async_worker.py:648 then 654). `__init__` is synchronous, so the client cannot exist at handler-construction time.
- **The handler worker loop is lazy**: it only calls `_connect()` when it has a record to send and `self.client is None` (message_broker_handler.py:217). It does not eagerly connect at `start_logging()`.
- **`client_config` property** (valkey_handler.py:112-114) does `dataclasses.asdict(backend_config)`; in the injected path `backend_config` is `None` (valkey_handler.py:95), so `client_config` would crash. Tests reading it only make sense in the config-dict path.
- **Restart-in-place**: handlers are registered once and restarted each `start()` cycle (logging_lifecycle.py:47-49). The AsyncValkeyHandler must be a single instance reused across restarts, not reconstructed per cycle.
- **Worker client is replaced on reconnect**: `connect()` calls `GlideClient.create()` fresh each time (319); `disconnect()` closes it. `fetch_tasks` reconnects on read error (630-631). So the handler must track the worker's *current* client, not a stale first reference.

## Design decisions

### D1. Dependency bump
Both pyproject lines to `scietex.logging>=1.2.0` (base `dependencies` line 24 and `valkey` extra line 32). The `>=1.2.0` floor guarantees the seam exists, so the runtime `_handler_supports_client_injection()` check becomes dead and is removed.

### D2. Flag semantics: single client becomes the default and only behavior
Remove `share_glide_client` entirely. Rationale: AR-018 is a correctness issue (two unmanaged lifecycles), the seam now exists, and the user wants it fixed in v3. Keeping a two-lifecycle opt-out would preserve the very divergence machinery AR-018 exists to eliminate and contradict the roadmap's "one connection lifecycle and one teardown owner." The config-dict construction path inside the worker is internal (users never construct the handler themselves), so removing it is not a user-facing breaking change — the public `ValkeyWorker(...)` surface is unchanged except the now-removed `share_glide_client` kwarg.

**Behavioral note (flag to user):** the `AsyncValkeyHandler` is no longer present on the logger immediately after `ValkeyWorker(...)` construction; it is constructed on the first successful `connect()` (during `initialize()`). This is inherent to injecting a client that only exists after an async connect. Tests and docs that assume the handler exists post-construction must be updated.

### D3. Lifecycle wiring: construct the handler lazily on first successful `connect()`
The handler is constructed with `client=self._client` (injected) the first time `connect()` succeeds, then registered and started. On every subsequent `connect()`/`disconnect()`, the worker mirrors its live `_client` into `handler.client` so reconnects and restarts stay in sync despite the seam's fixed `_injected_client`. The handler instance is stored on the worker and reused (restart-in-place).

### D4. Obsolete machinery removed
`logging_connected`, `_log_connection_divergence`, `_handler_supports_client_injection`, and the divergence-warning calls in `connect()` are removed — with one client there is nothing to diverge. `connect()`'s return value already reflects the single client's health.

### D5. Teardown ownership: worker is sole closer
The handler is constructed with a non-None client, so `_owns_client=False` and the handler's `_disconnect()` is a no-op — it never closes the shared client. The worker's `disconnect()` closes `_client`. The handler's worker loop is stopped by the normal `_logger_shut_down_handlers()` path in `_shutdown()` (which calls `stop_logging()`, draining the queue and stopping the worker loop without closing the client).

### D6. Runtime seam check removed
`_handler_supports_client_injection()` (valkey_async_worker.py:54-63) and the `inspect` import are removed; the `>=1.2.0` floor guarantees the seam.

### D7. Docs updated
The two-lifecycle narrative in valkey_async_worker.py docstrings, docs/valkey_async_worker.md, docs/architecture/components.md, docs/architecture/hotspots.md, and the ROADMAP v4 entry are rewritten/removed to describe the single-client model.

### D8. Tests updated
Tests asserting two-lifecycle behavior (handler present post-construction, `client_config`, `logging_connected`, divergence warnings, `share_glide_client` fallback) are reworked to assert single-client behavior.

---

## Task 1: Bump the scietex.logging floor and re-sync the venv

**Files:**
- Modify: `pyproject.toml:24` and `pyproject.toml:32`

- [ ] **Step 1: Edit both dependency lines**

`pyproject.toml:24`:
```toml
  "scietex.logging>=1.2.0",
```
`pyproject.toml:32`:
```toml
valkey = ["scietex.logging[valkey]>=1.2.0", "valkey-glide~=2.5.0"]
```

- [ ] **Step 2: Re-sync the venv to 1.2.0**

Run: `uv sync --extra dev --extra lint --extra test --extra valkey`
Expected: resolves `scietex.logging` to 1.2.0 (from the sibling repo path or index).

- [ ] **Step 3: Verify the seam is importable**

Run: `.venv/bin/python -c "import inspect, scietex.logging; from scietex.logging import AsyncValkeyHandler; print('client' in inspect.signature(AsyncValkeyHandler.__init__).parameters)"`
Expected: `True`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "chore: bump scietex.logging floor to 1.2.0 for client-injection seam (AR-018)"
```

---

## Task 2: Rewire ValkeyWorker to a single shared GlideClient

**Files:**
- Modify: `src/scietex/service/valkey/valkey_async_worker.py`

**Interfaces:**
- Consumes: `AsyncValkeyHandler(stream_name=..., service_name=..., worker_id=..., client=<GlideClient>, stdout_enable=False)` — the 1.2.0 seam.
- Produces: `ValkeyWorker` with a single `_client`; `_valkey_logging_handler()` still returns the registered handler (used by tests); no `share_glide_client`, `logging_connected`, `_log_connection_divergence`, or `_handler_supports_client_injection`.

- [ ] **Step 1: Remove the runtime seam check and the `inspect` import**

Delete `_handler_supports_client_injection()` (lines 54-63) and the `import inspect` (line 10).

- [ ] **Step 2: Remove `share_glide_client` from the constructor and docstring**

Delete the `share_glide_client: bool = False` parameter (line 108), its docstring block (lines 133-138), and the fallback-warning block (lines 189-199).

- [ ] **Step 3: Replace the eager handler construction with a deferred, client-injected one**

Replace the `_register_logger_handler(AsyncValkeyHandler(...))` block (lines 200-209) with a stored reference and no registration:

```python
        # The logging handler shares the worker's single GlideClient (AR-018).
        # It cannot be constructed in __init__: the client is created
        # asynchronously in connect(), and the seam fixes ownership at
        # construction. It is built lazily on the first successful connect()
        # and reused across restarts (see _ensure_logging_handler).
        self._valkey_handler: AsyncValkeyHandler | None = None
```

Keep `self._client: GlideClient | None = None` (line 211).

- [ ] **Step 4: Add a helper to construct/register/start the handler once**

Add a private method (place it near `_valkey_logging_handler`):

```python
    def _ensure_logging_handler(self) -> AsyncValkeyHandler | None:
        """Build and register the shared-client logging handler on first connect.

        Constructed with the worker's live ``_client`` injected so the handler
        never owns or closes it (``_owns_client`` is False). Registered once and
        reused across restarts (restart-in-place). Returns the handler, or
        ``None`` if the worker has no client yet.
        """
        if self._client is None:
            return None
        if self._valkey_handler is None:
            self._valkey_handler = AsyncValkeyHandler(
                stream_name=self._log_stream_name,
                service_name=self.service_name,
                worker_id=self.worker_id,
                client=self._client,
                stdout_enable=False,
            )
            self._register_logger_handler(self._valkey_handler, name="AsyncValkeyHandler")
        else:
            # The seam fixes _injected_client at construction; keep the handler
            # on the worker's *current* client across reconnects/restarts.
            self._valkey_handler.client = self._client
        return self._valkey_handler
```

- [ ] **Step 5: Start the handler from `connect()` after the client is live**

In `connect()`, after `self._client = client` succeeds (line 326), start the handler's worker loop:

```python
            if await client.ping():
                self._client = client
                self.logger.log(logging.INFO, "Connected to Valkey")
                handler = self._ensure_logging_handler()
                if handler is not None and not handler.logging_running_event.is_set():
                    await handler.start_logging()
                return True
```

Remove the `_log_connection_divergence()` calls (lines 322, 328, 338) and the docstring paragraphs describing divergence (lines 307-310).

- [ ] **Step 6: Clear the handler's client reference on disconnect**

In `disconnect()` (341-350), before closing `_client`, null the handler's reference so it never sends on a closed client:

```python
    async def disconnect(self):
        if self._client is not None:
            if self._valkey_handler is not None:
                self._valkey_handler.client = None
            await self._client.close()
            self.logger.info("Valkey client disconnected")
            self._client = None
```

- [ ] **Step 7: Remove `logging_connected` and `_log_connection_divergence`**

Delete the `logging_connected` property (257-271) and `_log_connection_divergence()` (273-293). Keep `_valkey_logging_handler()` (250-255) — tests use it.

- [ ] **Step 8: Update the class and `__init__` docstrings**

Rewrite the class docstring's "Connection lifecycle (AR-018)" block (lines 76-93) to describe the single shared client and single teardown owner. Remove the `logging_connected` attribute entry. Update the `__init__` docstring's `share_glide_client` mention (already removed in Step 2) and the `_client` attribute note.

- [ ] **Step 9: Verify lint, type, and existing tests**

Run: `ruff check src/scietex/service/valkey/valkey_async_worker.py && ty check src/scietex/service/valkey/valkey_async_worker.py`
Expected: clean. Then run `pytest tests/test_valkey_worker.py -k "not logging_connected and not share_glide_client and not warns"` to see which tests still pass before the test rework in Task 3.

- [ ] **Step 10: Commit**

```bash
git add src/scietex/service/valkey/valkey_async_worker.py
git commit -m "feat: single shared GlideClient lifecycle for ValkeyWorker (AR-018)"
```

---

## Task 3: Rework the two-lifecycle tests to single-client assertions

**Files:**
- Modify: `tests/test_valkey_worker.py`

**Interfaces:**
- Consumes: `ValkeyWorker` with no `share_glide_client`/`logging_connected`; `_valkey_logging_handler()` returns the handler only after `connect()`.

- [ ] **Step 1: Update the credential tests to connect first**

`test_log_handler_receives_credentials` (107-114) and `test_log_handler_receives_no_credentials_by_default` (118-122) read `handler.client_config` right after construction. The handler no longer exists until `connect()`, and the injected path has no `client_config`. Replace these with a single test asserting the handler is absent before connect and present (with the worker's client) after a successful connect:

```python
@pytest.mark.asyncio
async def test_logging_handler_created_on_connect(monkeypatch):
    """The AsyncValkeyHandler is constructed on first connect with the worker's
    client injected, so worker and logging share one GlideClient (AR-018)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    import scietex.service.valkey.valkey_async_worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    assert worker._valkey_handler is None  # not built until connect

    ok = await worker.connect()
    assert ok is True
    handler = worker._valkey_handler
    assert handler is not None
    assert handler.client is worker.client  # shared, not a second client
    assert handler._owns_client is False  # worker owns teardown
```

- [ ] **Step 2: Remove the `logging_connected` tests**

Delete `test_logging_connected_reflects_handler_client` (278-289) and `test_logging_connected_false_when_handler_absent` (293-297) — the property no longer exists.

- [ ] **Step 3: Remove the divergence-warning tests**

Delete `test_connect_warns_when_logging_client_down` (301-322) and `test_connect_warns_when_worker_client_down` (326-351) — divergence warnings no longer exist.

- [ ] **Step 4: Remove the `share_glide_client` fallback test**

Delete `test_share_glide_client_seam_warns_and_falls_back` (355-366) — the flag no longer exists.

- [ ] **Step 5: Add a teardown-ownership test**

```python
@pytest.mark.asyncio
async def test_disconnect_closes_shared_client_once(monkeypatch):
    """disconnect closes the single shared client and clears the handler's
    reference; the handler never closes it (AR-018)."""

    async def create_mock(cfg):
        return DummyClient(ping_ok=True)

    import scietex.service.valkey.valkey_async_worker as mod

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    worker = ValkeyWorker(valkey_config=ValkeyConfig())
    await worker.connect()
    client = worker.client
    handler = worker._valkey_handler
    assert handler.client is client

    await worker.disconnect()
    assert client.closed is True
    assert worker.client is None
    assert handler.client is None
```

- [ ] **Step 6: Run the full test file**

Run: `pytest tests/test_valkey_worker.py -v`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add tests/test_valkey_worker.py
git commit -m "test: rework ValkeyWorker tests for single shared GlideClient (AR-018)"
```

---

## Task 4: Update documentation

**Files:**
- Modify: `docs/ROADMAP.md`
- Modify: `docs/valkey_async_worker.md`
- Modify: `docs/architecture/components.md`
- Modify: `docs/architecture/hotspots.md`

- [ ] **Step 1: Remove the AR-018 v4 roadmap entry**

Delete the "v4 — Single shared GlideClient (connection lifecycle)" section (docs/ROADMAP.md:32-42). The work is now done in v3.

- [ ] **Step 2: Update docs/valkey_async_worker.md**

Remove the `logging_connected` property row (line 128), the `share_glide_client` constructor param (line 154) and its table row (line 173). Update the constructor signature block and any prose describing two lifecycles to describe the single shared client.

- [ ] **Step 3: Update docs/architecture/components.md**

Rewrite the "Health reporting (AR-018)" block (lines 257-260) and the public-interface/constructor-kwarg notes (lines 271-272) to describe the single-client model and the removed `share_glide_client`/`logging_connected`.

- [ ] **Step 4: Update docs/architecture/hotspots.md**

Update the H9 row (line 21) and the H9 section (lines 150-167) to mark AR-018 fully resolved (single client shipped in v3), removing references to `logging_connected`, `_log_connection_divergence`, and `share_glide_client`.

- [ ] **Step 5: Commit**

```bash
git add docs/ROADMAP.md docs/valkey_async_worker.md docs/architecture/components.md docs/architecture/hotspots.md
git commit -m "docs: document single shared GlideClient lifecycle (AR-018)"
```

---

## Task 5: Full verification

- [ ] **Step 1: Run the full lint/type/test gate**

Run: `ruff check src/ && ruff format --check src/ && ty check src/ && pytest tests/`
Expected: all clean and passing.

- [ ] **Step 2: Grep for leftover references**

Run: `rg -n "share_glide_client|logging_connected|_log_connection_divergence|_handler_supports_client_injection" src/ tests/ docs/`
Expected: no matches in `src/` or `tests/`; only historical references in `docs/reviews/` (which are review records and stay).

- [ ] **Step 3: Commit any stragglers**

```bash
git add -A  # only after confirming git status shows only intended files
git commit -m "chore: final AR-018 cleanup"
```

---

## Open questions for the user before implementation

1. **Construction-contract change (D2/D3).** Making single-client the default means the `AsyncValkeyHandler` is no longer attached to the logger immediately after `ValkeyWorker(...)` — it appears on the first successful `connect()` (during `initialize()`). This is inherent to injecting a client that only exists after an async connect. Acceptable for v3, or should the worker keep a config-dict fallback path so the handler is always present post-construction (which would preserve two lifecycles for the default and contradict the AR-018 goal)? **Recommendation: accept the change; remove the fallback.**

2. **`share_glide_client` removal.** Removing the kwarg is a source-compat break for any caller that passed it (none in-repo; it was a reserved seam). Confirm it is safe to remove outright rather than deprecate-and-ignore. **Recommendation: remove outright.**

3. **Handler start timing.** In the recommended design the handler's worker loop starts inside `connect()` (Step 5 of Task 2) rather than in `_logger_start_handlers()`. This means Valkey log delivery begins slightly later in startup (after the client connects) and is not gated by the base `_logger_start_handlers()` restart path. Confirm this is acceptable; the alternative is to start it from `initialize()` after `connect()` returns, which is equivalent in practice.

## Handoff Plan
1. Execute Task 1 (pyproject bump + `uv sync` to 1.2.0) first — nothing else works until the venv has the seam.
2. Execute Task 2 (valkey_async_worker.py rewire) — the core change; verify with the targeted pytest filter in Step 9.
3. Execute Task 3 (test rework) — delete obsolete tests, add the two new single-client tests.
4. Execute Task 4 (docs) and Task 5 (full gate + grep sweep).
- Risk: the handler's `_injected_client` is fixed at first construction; the worker must re-push `handler.client` on every `connect()` (Step 4/5) or reconnects leave the handler on a stale closed client. Do not skip the mirror in `disconnect()` (Step 6).
- Risk: `client_config` crashes in the injected path (`backend_config` is None) — do not keep any test that reads it after the rework.
- Test: `pytest tests/test_valkey_worker.py -v` passes; `rg` shows no `share_glide_client`/`logging_connected`/`_log_connection_divergence` in `src/` or `tests/`.
