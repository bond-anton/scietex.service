# Dependencies

Architectural dependency relationships and directions. These are **import /
inheritance** edges in the source; third-party packages are listed only where
they are structurally significant.

## Layer diagram

```
scietex.service (public API)                 __init__.py
   │  guarded re-export (swallow Exception)
   ▼
valkey  (valkey/valkey_async_worker.py)
   │ extends │ imports
   ▼         ▼
async_tasks_processor ──► task_handler (basic ─► schemas)
   │ extends
   ▼
basic_async_worker ──► manager
   │        │
   │        └──► utils (conf, logo)
   ▼
scietex.logging (AsyncBaseHandler / AsyncValkeyHandler)     [external]
   │
   ▼
glide (valkey-glide, optional)                              [external]
```

## Edge table

| From | To | Kind | Notes |
|---|---|---|---|
| `scietex.service/__init__` | `async_tasks_processor`, `basic_async_worker`, `manager`, `version` | import | unconditional |
| `scietex.service/__init__` | `valkey` | import | inside `try/except Exception` — optional feature |
| `basic_async_worker` | `.manager` | import | `Manager`, `ManagerStatus` |
| `basic_async_worker` | `.logging` | import | `LoggerStatus`, `parse_logging_level` |
| `basic_async_worker` | `.utils` | import | `prepare_conf_dir`, `print_scietex_logo` |
| `basic_async_worker` | `scietex.logging` | import (external) | `AsyncBaseHandler` |
| `utils.logo` | `..version` | import | `__version__` |
| `async_tasks_processor` | `basic_async_worker` | inheritance | extends |
| `async_tasks_processor` | `.manager` | import | for `@Manager` decorators |
| `async_tasks_processor` | `.task_handler` | import | `TaskData`, `TaskHandler`, `TaskResult`, `TaskTracker` |
| `task_handler.basic` | `.schemas` | import | runtime |
| `task_handler.basic` | `basic_async_worker` | TYPE_CHECKING only | no runtime edge (avoids cycle) |
| `valkey.valkey_async_worker` | `async_tasks_processor` | inheritance | `ValkeyWorker(AsyncTaskProcessor)` |
| `valkey.valkey_async_worker` | `.task_handler` | import | `TaskData` |
| `valkey.valkey_async_worker` | `.valkey_config`, `.schemas` | import | |
| `valkey.valkey_async_worker` | `scietex.logging` | import (external) | `AsyncValkeyHandler` |
| `valkey.valkey_async_worker` | `glide` | import (external, optional extra) | unguarded within module; errors surface to top-level guard |
| `valkey.valkey_config` | `glide`, `msgspec` | import | unguarded glide import; config cannot load without the extra |
| `task_handler.schemas` | `msgspec` | import | struct + serialization |

## Dependency direction analysis

- **Core → infrastructure**: `BasicAsyncWorker` and `AsyncTaskProcessor` depend
  on the external async-logging package (`scietex.logging`) and on `msgspec`
  (via task schemas). Neither depends on Valkey/glide. Valkey is infra that
  sits *under* the worker in the class hierarchy (`ValkeyWorker extends
  AsyncTaskProcessor`), so direction is **feature → core**, never core → feature.
- **Cross-module**: `task_handler` is depended on by the processor, but the
  handler ABC deliberately keeps no runtime import of the worker
  (`TYPE_CHECKING`) — a clean boundary.
- **Configuration split**: `valkey_config` is independent of
  `valkey_async_worker`; only `read_valkey_config`/`generate_glide_config`
  flow into the worker.
- **Logging has two dependency arrows** (see `scietex.logging` above): both the
  base worker (console handler) and `ValkeyWorker` (Valkey handler) attach
  external handlers. `ValkeyWorker` therefore couples to `glide` **twice** —
  directly (`self._client`) and inside the logging handler.
- **Public API re-export guard**: the only place core code tolerates a missing
  optional extra is `__init__.py`. A broken `valkey` import is silently
  swallowed, making the failure hard to observe (a design choice documented in
  its own comment, `__init__.py:24-53`).

## Circular dependencies

- **None at runtime.** The single `TYPE_CHECKING` back-reference
  (`task_handler/basic.py:9-10`) prevents what would otherwise be a
  handler ↔ worker cycle.
- Note that `task_handler` types are imported by `async_tasks_processor`,
  which is imported by `basic_async_worker`? No — `basic_async_worker` does
  not import `task_handler`; the cycle `task_handler → basic_async_worker` is
  only type-level.

## Third-party packages (dependency relevance)

| Package | Declared in | Used for | Structurally significant? |
|---|---|---|---|
| `msgspec>=0.20.0` | core deps | Struct schemas, msgpack (tasks/heartbeat), YAML (valkey config) | Yes — schemas and wire format |
| `scietex.logging>=1.1.0` | core deps | async console/Valkey log handlers | Yes — cross-package logging boundary |
| `pyaml>=26.2.1` | core deps (`pyproject.toml:18`) | **not imported anywhere in `src/`** | No — likely legacy (see §H14) |
| `valkey-glide~=2.5.0` | `[valkey]` and `[dev]` extras | Valkey client | Yes (optional) |

`UNKNOWN` — whether `pyaml` is required by `scietex.logging` transitively; no
evidence found in this repo of a direct need.

## Important dependency chains

1. **Task path (wire)**:
   `ValkeyWorker.return_task_to_queue/fetch_tasks`
   → `msgspec.msgpack.encode/decode(TaskData)`
   → `task_handler.schemas.TaskData` → `AsyncTaskProcessor.process_task` →
   `TaskHandler.handle` → `TaskResult`.
2. **Config path**: `BasicAsyncWorker.conf_dir` → `read_valkey_config`
   (`valkey.yml`, msgspec yaml) → `ValkeyConfig` → `generate_glide_config` →
   `GlideClientConfiguration` → `GlideClient.create`.
3. **Log path**: worker logger → `scietex.logging.AsyncBaseHandler.emit`
   → internal asyncio queues → console worker; or `AsyncValkeyHandler._worker`
   → its own `GlideClient.xadd` → stream.
4. **Manager runtime chain**: `@Manager`-decorated method →
   `_iter_manager_definitions` (MRO scan) → `_run_manager` (loop) →
   `_start_manager` (task) → restart path on error.
