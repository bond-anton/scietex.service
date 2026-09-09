# scietex.service

Async worker framework for building background daemon services in Python.

Provides a hierarchy of workers — from basic signal-handling daemons to
concurrent task processors with Valkey-backed distributed queues.

**Python ≥ 3.10** · **License: MIT**

## Documentation

- [Overview](docs/index.md) — Core components and architecture
- [BasicWorker](docs/basic_worker.md) — Signal handling, logging, heartbeat & watchdog managers
- [TaskProcessor](docs/task_processor.md) — Concurrent task processing, handler dispatch, timeout monitoring
- [ValkeyWorker](docs/valkey_worker.md) — Valkey stream-based task distribution
- [Task Handler](docs/task_handler.md) — Pluggable handler architecture, typed schemas

## Installation

```bash
# Core package (no Valkey)
pip install scietex.service

# With Valkey (Redis-compatible) support
pip install "scietex.service[valkey]"
```

**Dependencies:** `msgspec>=0.20.0`, `pyyaml>=6.0`, `scietex.logging>=2.0.0`

## Quick Start

### Basic Async Worker

A minimal daemon with signal handling, heartbeat, and watchdog. See the [full BasicWorker docs](docs/basic_worker.md) for lifecycle, manager system, and configuration details.

```python
import asyncio
import logging
from scietex.service import BasicWorker, WorkerConfig


class MyWorker(BasicWorker):
    async def heartbeat(self) -> None:
        self.logger.info("Worker is alive")

    async def watchdog(self) -> None:
        self.logger.debug("Running watchdog checks")

    async def cleanup(self) -> None:
        self.logger.info("Shutting down gracefully")


async def main() -> None:
    worker = MyWorker(
        WorkerConfig(
            service_name="my_service",
            version="1.0.0",
            logging_level=logging.DEBUG,
            heartbeat_interval=10,
            watchdog_interval=1,
        )
    )
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Send `SIGINT` (Ctrl+C) or `SIGTERM` to trigger graceful shutdown.

### Task Processor

Register handlers for different task types and process them concurrently. See the [full TaskProcessor docs](docs/task_processor.md) for architecture, task processing flow, and best practices.

```python
import asyncio
import logging
from scietex.service import TaskProcessor, TaskProcessorConfig
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult


class EmailHandler(TaskHandler):
    @property
    def supported_tasks(self) -> list[str]:
        return ["send_email"]

    async def initialize(self) -> bool:
        # Connect to email service, etc.
        self.logger.info("Email handler initialized")
        return True

    async def handle(self, task_data: TaskData) -> TaskResult:
        try:
            # Process task_data.payload
            self.logger.info("Sending email…")
            return TaskResult(status="success", error="")
        except Exception as exc:
            return TaskResult(status="error", error=str(exc))


class MyProcessor(TaskProcessor):
    async def fetch_tasks(self) -> bool:
        # Pull tasks from your source (DB, API, queue, etc.)
        # and enqueue them for processing:
        #     self.enqueue_task(task_id, task_data)
        # Return True when at least one task was enqueued so the
        # task_queue_manager drains a backlog back-to-back.
        return False


async def main() -> None:
    processor = MyProcessor(
        TaskProcessorConfig(
            service_name="email_worker",
            version="1.0.0",
            queue_size=100,
            max_concurrent_tasks=5,
        )
    )
    processor.add_task_handler(EmailHandler)
    await processor.start()
    await processor.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

### Valkey Worker

Distributed task processing backed by a Valkey (Redis-compatible) stream. See the [full ValkeyWorker docs](docs/valkey_worker.md) for architecture, key naming, and configuration reference.

```python
import asyncio
import logging
from scietex.service import (
    ValkeyAdvancedConfig,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyWorker,
    ValkeyWorkerConfig,
)


async def main() -> None:
    config = ValkeyConfig(
        base_config=ValkeyBaseConfig(
            nodes=[ValkeyNode(host="localhost", port=6379)],
            request_timeout=10_000,
        ),
        advanced_config=ValkeyAdvancedConfig(
            connection_timeout=10_000,
            tcp_nodelay=True,
        ),
    )
    worker = ValkeyWorker(
        ValkeyWorkerConfig(
            service_name="distributed_worker",
            version="1.0.0",
            logging_level=logging.DEBUG,
            heartbeat_interval=10,
            valkey_config=config,
            queue_size=100,
            max_concurrent_tasks=10,
        )
    )
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Tasks are stored in a Valkey stream named
`scietex:{service_name}:tasks` and consumed via a consumer
group `scietex:{service_name}:task_group`.

## Architecture

### Worker Hierarchy

<!-- markdown-link-check-disable -->
See [BasicWorker](docs/basic_worker.md), [TaskProcessor](docs/task_processor.md), and [ValkeyWorker](docs/valkey_worker.md) for detailed architecture diagrams.
<!-- markdown-link-check-enable -->

```
BasicWorker          — Signal handling, async logging, heartbeat &
                            watchdog managers, graceful shutdown
    └── TaskProcessor — Task queue, concurrent processing, handler
                            dispatch, timeout watchdog
        └── ValkeyWorker  — Valkey stream integration, connection
                            management, stream-based task fetching
```

### Manager Lifecycle

Managers are async methods decorated with `@Manager`. The worker
discovers them via the class MRO and runs each as an `asyncio.Task`:

1. **Start** — Manager loop runs the decorated method in a `while True`
   loop until cancelled.
2. **Error** — On any exception (except `CancelledError`), the error is
   recorded and the manager is automatically restarted, up to
   `manager_max_retries` consecutive failures (default 5), after which
   the manager gives up and stops restarting.
3. **Stop** — On shutdown, managers are cancelled and their optional
   `cleanup` callbacks are invoked.

### Task Handler System

See the [Task Handler docs](docs/task_handler.md) for the full handler lifecycle, schema details, and best practices.

1. **Register**: `processor.add_task_handler(HandlerClass)` —
   Registers a handler class under its class name. The processor
   creates a single handler instance on start. Dispatch is driven by
   the handler's `supported_tasks` declaration, not by a registration
   key. An optional keyword-only `name`
   (`processor.add_task_handler(HandlerClass, name="...")`) lets
   multiple instances of one class coexist under distinct keys.
   Arbitrary `**handler_kwargs` are also forwarded to the handler
   constructor on every instantiation, enabling stateful handlers —
   see `examples/stateful_handler.py`.
2. **Declare support**: `Handler.supported_tasks` property must return
   a list of task type strings this handler can process.
3. **Dispatch**: When a task arrives, the processor calls
   `handler.supports(task_type)` on each registered handler. The first
   handler returning `True` receives the task.
4. **Initialize**: `handler.start()` calls `handler.initialize()` and
   sets `handler.is_ready` to the returned value, so it is `True` only
   if `initialize()` returned `True`.
5. **Handle**: `await handler.handle(task_data)` returns a `TaskResult`
   with `status` ("success"/"error"), optional `error` message, and
   optional `payload`.
6. **Timeout**: Tasks exceeding their `timeout` (default 3s) are
   canceled and either re-queued or discarded per
   `TaskTimeout.timeout_action`.

### Task Schemas

All schemas are frozen `msgspec.Struct` instances (immutable).

| Type | Description |
|---|---|
| `TaskData` | Immutable task payload: `task` (type string), `payload` (bytes), `timeout` (`TaskTimeout`), `canceled_action` ("requeue"/"discard") |
| `TaskResult` | Handler result: `status` ("success"/"error"), `error` (message), `processed_at` (UTC datetime), `payload` (bytes), plus error-taxonomy fields `error_code`, `retryable`, `partial` |
| `TaskTimeout` | Timeout config: `timeout` (seconds, `None` for default 3s), `timeout_action` ("requeue"/"discard") |
| `TaskTracker` | Internal: tracks running `asyncio.Task`, associated `TaskData`, and monotonic start time |

## Configuration

### Config Directory Precedence

The worker searches for a config directory in this order:

1. `conf_dir` argument (if provided and is a directory)
2. `SCIETEX_CONFIG_DIR` environment variable
3. `$XDG_CONFIG_HOME/scietex/`
4. `~/.config/scietex/`
5. `/etc/scietex/`
6. `/usr/local/etc/scietex/`
7. `./config/` (current working directory)
8. `~/.config/scietex/` — created if none of the above exist

The first existing directory is used. If none exist, `~/.config/scietex/`
is created.

### Valkey Configuration

`ValkeyWorker` reads `valkey.yml` from the config directory:

```yaml
base_config:
  nodes:
    - host: localhost
      port: 6379
  user_credentials: null
  use_tls: false
  request_timeout: 5000
  database_id: null
  client_name: null
  inflight_requests_limit: null
  client_az: null
  lazy_connect: null
  read_from: PRIMARY
  backoff_strategy: null
  protocol: RESP3

advanced_config:
  connection_timeout: 10000
  tcp_nodelay: null
  tls_config:
    use_insecure_tls: false
    root_pem_cacerts: null
```

If the file is missing, it is created with default values. If the file is
present but invalid, a ``RuntimeError`` is raised and the file is left
untouched.

## API Reference

### Exported from `scietex.service`

| Symbol | Description |
|---|---|
| `BasicWorker` | Base async daemon worker |
| `TaskProcessor` | Concurrent task processor |
| `Manager` | Decorator for creating managed async loop methods |
| `ValkeyWorker` | Valkey-backed distributed worker |
| `WorkerConfig` | Immutable `msgspec.Struct` configuration for `BasicWorker` |
| `TaskProcessorConfig` | Immutable configuration for `TaskProcessor` (extends `WorkerConfig`) |
| `ValkeyWorkerConfig` | Immutable configuration for `ValkeyWorker` (extends `TaskProcessorConfig`) |
| `__version__` | Package version string |

The Valkey configuration classes (`ValkeyConfig`, `ValkeyNode`,
`ValkeyUserCredentials`, `ValkeyBackoffStrategy`, `ValkeyBaseConfig`,
`ValkeyAdvancedConfig`, `ValkeyTlsAdvancedConfiguration`, `ValkeyWorkerConfig`)
are top-level
re-exports: they are importable directly from `scietex.service` (as in the
Valkey quick-start above), not only from `scietex.service.valkey`.

### Exported from `scietex.service.task_handler`

| Symbol | Description |
|---|---|
| `TaskHandler` | Abstract base class for task handlers |
| `TaskHandlerContext` | Narrow read-only context passed to handlers (service name, instance id, logger) |
| `TaskData` | Task payload schema |
| `TaskResult` | Task result schema |
| `TaskTimeout` | Timeout configuration schema |
| `TaskTracker` | Internal task tracker schema |

### Exported from `scietex.service.valkey`

| Symbol | Description |
|---|---|
| `ValkeyConfig` | Top-level Valkey configuration |
| `ValkeyBaseConfig` | Basic connection settings |
| `ValkeyAdvancedConfig` | Advanced connection settings |
| `ValkeyNode` | Server node address |
| `ValkeyUserCredentials` | Authentication credentials |
| `ValkeyBackoffStrategy` | Reconnection backoff config |
| `ValkeyTlsAdvancedConfiguration` | TLS settings |

## Development

### Setup

```bash
# Clone the repository and install all dependencies
uv sync --all-extras

# Or install specific extras
uv sync --extra dev --extra test --extra lint
```

### Commands

| Command | Description |
|---|---|
| `uv run ruff check src/` | Lint (auto-fix: `ruff check --fix`) |
| `uv run ty check src/` | Type check |
| `uv run ruff format src/` | Format code |
| `uv run pytest tests/` | Run tests |
| `tox` | Run tests with coverage |

### Running Examples

The `examples/` directory contains runnable blueprints; see
[`examples/README.md`](examples/README.md) for what each demonstrates.

```bash
python -m examples.basic_worker
python -m examples.manager_cleanup
python -m examples.manager_collision
python -m examples.task_processor
python -m examples.named_task_handlers
python -m examples.stateful_handler
python -m examples.valkey_async_service      # requires valkey-glide
python -m examples.valkey_pubsub_worker      # requires valkey-glide
```

## License

MIT
