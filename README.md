# scietex.service

Async worker framework for building background daemon services in Python.

Provides a hierarchy of workers — from basic signal-handling daemons to
concurrent task processors with Valkey-backed distributed queues.

**Python ≥ 3.10** · **License: MIT**

## Installation

```bash
# Core package (no Valkey)
uv pip install scietex.service

# With Valkey (Redis-compatible) support
uv pip install "scietex.service[valkey]"
```

Or with pip:

```bash
pip install scietex.service
pip install "scietex.service[valkey]"
```

**Dependencies:** `msgspec`, `pyaml`, `scietex.logging`

## Quick Start

### Basic Async Worker

A minimal daemon with signal handling, heartbeat, and watchdog:

```python
import asyncio
import logging
from scietex.service import BasicAsyncWorker


class MyWorker(BasicAsyncWorker):
    async def heartbeat(self) -> None:
        self.logger.info("Worker is alive")

    async def watchdog(self) -> None:
        self.logger.debug("Running watchdog checks")

    async def cleanup(self) -> None:
        self.logger.info("Shutting down gracefully")


async def main() -> None:
    worker = MyWorker(
        service_name="my_service",
        version="1.0.0",
        logging_level=logging.DEBUG,
        heartbeat_interval=10,
        watchdog_interval=1,
    )
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Send `SIGINT` (Ctrl+C) or `SIGTERM` to trigger graceful shutdown.

### Task Processor

Register handlers for different task types and process them concurrently:

```python
import asyncio
import logging
from scietex.service import AsyncTaskProcessor
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult


class EmailHandler(TaskHandler):
    def supports(self, task_type: str) -> bool:
        return task_type == "send_email"

    async def initialize(self) -> None:
        # Connect to email service, etc.
        self._is_initialized = True

    async def handle(self, task_data: TaskData) -> TaskResult:
        try:
            # Process task_data.payload
            self.logger.info("Sending email…")
            return TaskResult(status="success", error="")
        except Exception as exc:
            return TaskResult(status="error", error=str(exc))


class MyProcessor(AsyncTaskProcessor):
    async def fetch_tasks(self) -> None:
        # Pull tasks from your source (DB, API, queue, etc.)
        # and put them into self.task_queue:
        #     await self.task_queue.put((task_id, task_data))
        pass


async def main() -> None:
    processor = MyProcessor(
        service_name="email_worker",
        version="1.0.0",
        queue_size=100,
        max_concurrent_tasks=5,
    )
    processor.add_task_handler("send_email", EmailHandler)
    await processor.start()
    await processor.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

### Valkey Worker

Distributed task processing backed by a Valkey (Redis-compatible) stream:

```python
import asyncio
import logging
from scietex.service import (
    ValkeyAdvancedConfig,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyWorker,
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
        service_name="distributed_worker",
        version="1.0.0",
        worker_id=1,
        logging_level=logging.DEBUG,
        heartbeat_interval=10,
        valkey_config=config,
        queue_size=100,
        max_concurrent_tasks=10,
    )
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Tasks are stored in a Valkey stream named
`scietex:{service_name}:{worker_id}:tasks` and consumed via a consumer
group `scietex:{service_name}:{worker_id}:task_group`.

## Architecture

### Worker Hierarchy

```
BasicAsyncWorker          — Signal handling, async logging, heartbeat &
                            watchdog managers, graceful shutdown
    └── AsyncTaskProcessor — Task queue, concurrent processing, handler
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
   recorded and the manager is automatically restarted.
3. **Stop** — On shutdown, managers are cancelled and their optional
   `cleanup` callbacks are invoked.

### Task Handler System

1. **Register**: `processor.add_task_handler("type", HandlerClass)`
2. **Support**: `handler.supports(task_type)` must return `True`
3. **Initialize**: `handler.is_ready` must be `True` (set by
   `handler.initialize()`)
4. **Handle**: `await handler.handle(task_data)` returns a `TaskResult`

### Task Schemas

| Type | Description |
|---|---|
| `TaskData` | Immutable task payload: `task`, `payload`, `timeout`, `canceled_action` |
| `TaskResult` | Handler result: `status` ("success"/"error"), `error`, `payload` |
| `TaskTimeout` | Timeout config: `timeout` (seconds), `timeout_action` ("requeue"/"discard") |
| `TaskTracker` | Internal: tracks running `asyncio.Task`, `TaskData`, and start time |

## Configuration

### Config Directory Precedence

The worker searches for a config directory in this order:

1. `conf_dir` argument (if provided and is a directory)
2. `~/.config/scietex/`
3. `/etc/scietex/`
4. `/usr/local/etc/scietex/`
5. `./config/` (current working directory)

The first existing directory is used. If none exist, `~/.config/scietex/`
is created.

### Valkey Configuration

`ValkeyWorker` reads `valkey.yml` from the config directory:

```yaml
base_config:
  nodes:
    - host: localhost
      port: 6379
  use_tls: false
  request_timeout: 5000
  read_from: PRIMARY
  protocol: RESP3
  backoff_strategy:
    num_of_retries: 5
    factor: 2
    exponent_base: 2

advanced_config:
  connection_timeout: 10000
  tcp_nodelay: true
  tls_config:
    use_insecure_tls: false
```

If the file is missing or invalid, defaults are used and the file is
created with default values.

## API Reference

### Exported from `scietex.service`

| Symbol | Description |
|---|---|
| `BasicAsyncWorker` | Base async daemon worker |
| `AsyncTaskProcessor` | Concurrent task processor |
| `ValkeyWorker` | Valkey-backed distributed worker |
| `__version__` | Package version string |

### Exported from `scietex.service.task_handler`

| Symbol | Description |
|---|---|
| `TaskHandler` | Abstract base class for task handlers |
| `TaskData` | Task payload schema |
| `TaskResult` | Task result schema |
| `TaskTimeout` | Timeout configuration schema |
| `TaskTracker` | Internal task tracker schema |
| `task_type` | TypeVar for handler registration |

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
# Install dev dependencies
uv pip install -e ".[dev,test,lint]"

# Or with pip
pip install -e ".[dev,test,lint]"
```

### Commands

| Command | Description |
|---|---|
| `uv pip run ruff check src/` | Lint (auto-fix: `ruff check --fix`) |
| `uv pip run ty check src/` | Type check |
| `uv pip run ruff format src/` | Format code |
| `uv pip run pytest tests/` | Run tests |

### Running Examples

```bash
python -m examples.async_service
python -m examples.async_task_processor
python -m examples.valkey_async_service   # requires valkey-glide
```

## License

MIT
