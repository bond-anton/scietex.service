# scietex.service Agent Guide

## Project Overview
Python microservice using `setuptools` with `uv` for dependency management. Provides async worker frameworks (`BasicAsyncWorker`, `AsyncTaskProcessor`) and optional Valkey support via `scietex.logging[valkey]`.

## Layout
- **Source**: `src/scietex/service/`
- **Tests**: `tests/`
- **Build**: `pyproject.toml` (PEP 517/518 source layout, `pythonpath = ["src"]` for pytest)
- **Version**: `src/scietex/service/version.py` (`__version__ = "1.0.7"`)

## Key Commands
- **Install deps**: `uv sync`
- **Run tests**: `tox -e py314` or `tox` (default env runs pytest with coverage)
- **Lint**: `tox -e lint` (ruff check, auto-fix: `ruff check --fix`)
- **Typecheck**: `tox -e type` (uses `ty`)
- **Format**: `tox -e format` (ruff format)

## Dependencies
- **Core**: `msgspec>=0.20.0`, `pyaml>=26.2.1`, `scietex.logging>=0.2.0`
- **Optional Valkey**: `scietex.logging[valkey]>=0.2.0`, `valkey-glide~=2.5.0` (install via `uv sync --extra valkey`)
- **Dev/test/lint**: via tox envs

## Core Components
- `basic_async_worker.py`: Base async worker with signal handlers, logging queue
- `async_tasks_processor.py`: Task queue processor with configurable concurrency
- `task_handlers/`: Abstract `TaskHandler` base class + implementations
- `valkey_async_worker.py`: Valkey-backed worker (optional, import-guarded)

## Important Details
- Requires Python ≥3.10 (`target-version = "py310"` in ruff config)
- Import-time exceptions in `scietex.service.valkey` are caught to keep package importable without Valkey deps
- Logging level defaults to `DEBUG` (`DEFAULT_LOGGING_LEVEL = logging.DEBUG`)
- Config paths: `~/.config/scietex`, `/etc/scietex`, `/usr/local/etc/scietex`, `./config`

## Known Gaps
- No `.github/workflows/` or `pre-commit-config.yaml` detected
- README only says “A micro service worker for background operation.” (minimal docs)
