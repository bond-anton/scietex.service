# AGENTS.md

## Quick Start

**Install dependencies:**
```bash
uv sync --extra dev --extra lint --extra test
```

`uv sync` creates a project-local `.venv` with all dependencies.
Run all commands (linters, tests, examples) within this environment.

**Developer commands:**
- `ruff check src/` — Run ruff checks (auto-fix: `ruff check --fix`)
- `ruff format src/` — Format code
- `ty check src/` — Type check on `src`
- `pytest tests/` — Run tests
- `tox` — Run tests with coverage (testing automation)

**Order matters:** `lint -> type -> test` (lint/type must pass before merge)

## Architecture

**Package structure:**
- `src/scietex/service/` — Python package
- `tests/` — Test suite
- `examples/` — Service blueprints (see below)

**Core classes:**
- `BasicAsyncWorker` — Base async worker with signal handling, logging, heartbeat, watchdog
- `AsyncTaskProcessor` — Extends worker with task queue, concurrent processing, watchdog timeout monitoring
- `ValkeyWorker` — Extends processor with Valkey (Redis) integration via `glide` client

## Service Entry Points

Run examples with:
```bash
python -m examples.async_service           # BasicAsyncWorker
python -m examples.async_task_processor    # AsyncTaskProcessor
python -m examples.valkey_async_service    # ValkeyWorker (requires valkey-glide)
```

**Worker lifecycle:**
1. `await worker.start()` — Initialize, start managers, set state=RUNNING
2. Managers run until `await worker.exit()` (triggered by SIGINT/SIGTERM)
3. `await worker.stop()` — Graceful shutdown: stop managers, cleanup, stop loggers
4. Wait for exit: `await worker.events["exit"].wait()`

## Configuration

**Config directory precedence:**
1. `conf_dir` argument (if provided and is a directory)
2. `~/.config/scietex/`
3. `/etc/scietex/`
4. `/usr/local/etc/scietex/`
5. `./config/` (CWD)

**Valkey config:**
- Reads `valkey.yml` from config dir (YAML, uses `msgspec.yaml.decode`)
- Falls back to defaults if file missing/invalid
- Install extras: `uv sync --extra valkey` or `pip install "scietex.service[valkey]"`

## Task Handler System

**Workflow:**
1. Register handler: `processor.add_task_handler("task_type", HandlerClass)`
2. Handler `supports(task_type)` must return `True`
3. Handler `is_ready` (initialized) required before processing
4. `handle(task_data)` returns `TaskResult`

**Task schemas (msgspec.Struct):**
- `TaskData`: `task: str`, `payload: bytes`, `timeout: TaskTimeout`, `canceled_action: "requeue"|"discard"`
- `TaskResult`: `status: "success"|"error"`, `error: str`, `payload: bytes`, `processed_at: datetime`
- `TaskTimeout`: `timeout: float | None`, `timeout_action: "requeue"|"discard"`

## Testing

**Run tests:**
- All tests: `pytest tests/`
- Specific test file: `pytest tests/test_<name>.py`
- With coverage: `tox` (runs pytest with coverage reporting)

**Test helpers:**
- `pytest-asyncio` enabled
- `test_valkey_worker.py` mocks `GlideClient` — no Valkey server required for unit tests

## Quirks & Gotchas

- **Import-time errors in `scietex.service.valkey` are swallowed** — package remains importable without `valkey-glide`
- **Logging is async** — uses `AsyncBaseHandler` and `AsyncValkeyHandler`; shutdown has timeout
- **Manager restart** — fails restarts automatically on error (except `CancelledError`)
- **Valkey stream names:** `scietex:{service_name}:{worker_id}:tasks` with group `scietex:{service_name}:{worker_id}:task_group`
- **Timeout defaults:** `DEFAULT_TASK_TIMEOUT = 3s`, `DEFAULT_HEARTBEAT_INTERVAL = 10s`, `DEFAULT_WATCHDOG_INTERVAL = 1s`
- **Python 3.10+ required** (per `requires-python = ">=3.10"`)
