# BasicAsyncWorker

The `BasicAsyncWorker` is the foundation class for building asynchronous
daemon services in `scietex.service`. It provides signal handling, async
logging, heartbeat and watchdog managers, automatic manager restart on
error, and graceful shutdown support.

## Overview

```python
from scietex.service import BasicAsyncWorker
```

The worker manages three core subsystems:

- **Signal Handling** — Captures `SIGINT` and `SIGTERM` for graceful shutdown
- **Async Logging** — Uses `AsyncBaseHandler` for non-blocking log output
- **Manager Loops** — `@Manager`-decorated methods run as infinite loops
  with automatic restart on error

Subclasses should override:

| Method | Type | Description |
|---|---|---|
| `initialize()` | `async def` | Service-specific initialization logic |
| `heartbeat()` | `async def` | Periodic heartbeat behavior |
| `watchdog()` | `async def` | Periodic watchdog checks |
| `cleanup()` | `async def` | Service-specific cleanup on shutdown |

## Constants

| Constant | Default | Min | Max | Description |
|---|---|---|---|---|
| `DEFAULT_HEARTBEAT_INTERVAL` | `10` | `0.1` | `600` | Default heartbeat interval in seconds |
| `DEFAULT_WATCHDOG_INTERVAL` | `1` | `0.01` | `600` | Default watchdog check interval in seconds |
| `DEFAULT_LOGGER_HANDLER_TIMEOUT` | `2` | `1` | `10` | Default timeout for logger handler operations |
| `DEFAULT_MANAGER_SHUTDOWN_TIMEOUT` | `2` | `1` | `10` | Default timeout for manager shutdown |

## Lifecycle

```
  [STOPPED] ──► [STARTING] ──► [RUNNING] ──► [STOPPING] ──► [STOPPED]
                   │              │               │
            _startup()      initialize()     _shutdown()
                   │              │               │
                   ▼              ▼               ▼
            print logo    custom init logic   stop managers
            start loggers start managers      cleanup()
                                       start managers
                                       stop loggers
```

### Starting

```python
worker = MyWorker(service_name="my_service", version="1.0.0")
await worker.start()
```

The `start()` method creates a task that runs `_startup()`, which:

1. Waits for any previous shutdown to complete
2. Prints the service logo
3. Starts async logging handlers
4. Starts all `@Manager`-decorated methods as asyncio tasks
5. Calls `initialize()` (subclass override point)
6. Sets `start_time` and transitions to `RUNNING`

If `initialize()` returns `False`, a `RuntimeError` is raised and the
worker shuts down.

### Stopping

```python
await worker.exit()  # or await worker.stop()
```

Signals (`SIGINT`/`SIGTERM`) automatically trigger `exit()`. The
`_shutdown()` method:

1. Sets state to `STOPPING`
2. Stops all manager tasks
3. Calls `cleanup()` (subclass override point)
4. Shuts down logging handlers with a timeout
5. Clears `start_time` and transitions to `STOPPED`
6. Sets the `exit` event to signal completion

## ServiceStatus

```python
class ServiceStatus(Enum):
    STOPPED = "Stopped"
    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"
```

| Value | Description |
|---|---|
| `STOPPED` | The service is not running |
| `STARTING` | The service is in the process of starting up |
| `RUNNING` | The service is actively running and processing |
| `STOPPING` | The service is in the process of shutting down |

## Manager System

The `@Manager` decorator marks an async method as a managed loop. The
method is called repeatedly by `_run_manager()` in a `while True` loop.
On `CancelledError` the loop stops cleanly. On any other exception, the
error is recorded and the manager is automatically restarted.

Managers are discovered via the class MRO (most-derived to base classes)
and executed as named `asyncio.Task` objects.

### Creating a Manager

The decorated method performs a single iteration of work. `_run_manager()`
handles the repetition, sleep, and error recovery:

```python
from scietex.service import BasicAsyncWorker
from scietex.service.manager import Manager


class MyWorker(BasicAsyncWorker):
    @Manager(name="HealthCheck")
    async def health_check(self) -> None:
        """One iteration of the health check loop."""
        await self.check_health()
        await asyncio.sleep(30)

    async def check_health(self) -> None:
        # Custom health check logic
        pass
```

### Manager with Cleanup

The `cleanup` callable runs when the manager stops (on cancellation or
after the final restart failure):

```python
class MyWorker(BasicAsyncWorker):
    @Manager(name="ConnectionPool", cleanup=lambda worker: worker.pool.close())
    async def connection_pool_refresh(self) -> None:
        """One iteration of the pool refresh loop."""
        await self.pool.refresh()
        await asyncio.sleep(60)
```

### Built-in Managers

`BasicAsyncWorker` provides two built-in managers:

| Manager | Method | Interval | Description |
|---|---|---|---|
| `Heartbeat` | `_heartbeat_manager` | `heartbeat_interval` | Periodically calls `heartbeat()` |
| `Watchdog` | `_watchdog_manager` | `watchdog_interval` | Periodically calls `watchdog()` |

Subclasses can override `heartbeat()` and `watchdog()` to define custom
behavior.

## Properties

### Identity

| Property | Type | Description |
|---|---|---|
| `service_name` | `str` | Name of the service (read-only) |
| `worker_id` | `int` | Unique identifier for this worker (read-only) |
| `version` | `str` | Version string of the service (read-only) |

### State

| Property | Type | Description |
|---|---|---|
| `state` | `ServiceStatus` | Current lifecycle state (read-only) |
| `start_time` | `datetime \| None` | UTC timestamp when service started (read-only) |
| `events` | `dict[str, asyncio.Event]` | Lifecycle events dict (`exit_requested`, `exit`) |

### Configuration

| Property | Type | Default | Description |
|---|---|---|---|
| `heartbeat_interval` | `float` | `10` | Seconds between heartbeat calls |
| `watchdog_interval` | `float` | `1` | Seconds between watchdog checks |
| `logger_handler_timeout` | `float` | `2` | Timeout for logger handler operations |
| `manager_shutdown_timeout` | `float` | `2` | Timeout for manager shutdown |
| `conf_dir` | `Path` | *(resolved)* | Configuration directory path |
| `logging_level` | `int` | `logging.DEBUG` | Current logging level |

### Access

| Property | Type | Description |
|---|---|---|
| `logger` | `logging.Logger` | Logger instance (named `{service_name}.{worker_id}`) |

## Configuration

### Constructor

```python
BasicAsyncWorker(
    service_name: str = "service",
    version: str = "0.0.1",
    worker_id: int = 1,
    conf_dir: str | Path | None = None,
    logging_level: int | str = logging.DEBUG,
    heartbeat_interval: float | None = None,
    watchdog_interval: float | None = None,
    **kwargs,
)
```

| Parameter | Default | Description |
|---|---|---|
| `service_name` | `"service"` | Service name for logging and identification |
| `version` | `"0.0.1"` | Version string |
| `worker_id` | `1` | Unique worker identifier |
| `conf_dir` | `None` | Configuration directory (see precedence below) |
| `logging_level` | `logging.DEBUG` | Logging level as string or integer |
| `heartbeat_interval` | `None` (uses default) | Heartbeat interval in seconds |
| `watchdog_interval` | `None` (uses default) | Watchdog interval in seconds |

**kwargs** supports:

| Key | Default | Description |
|---|---|---|
| `logger_handler_timeout` | `2` | Timeout for logger handler operations |
| `manager_shutdown_timeout` | `2` | Timeout for manager shutdown |

### Config Directory Precedence

The configuration directory is resolved in this order:

1. `conf_dir` argument (if provided and is a directory)
2. `~/.config/scietex/`
3. `/etc/scietex/`
4. `/usr/local/etc/scietex/`
5. `./config/` (current working directory)

### Logging Level Strings

| Level | Accepted Strings |
|---|---|
| `DEBUG` | `'D'`, `'DBG'`, `'DEBUG'`, `logging.DEBUG` |
| `INFO` | `'I'`, `'INF'`, `'INFO'`, `'INFORMATION'`, `logging.INFO` |
| `WARNING` | `'W'`, `'WRN'`, `'WARN'`, `'WARNING'`, `logging.WARNING` |
| `ERROR` | `'E'`, `'ERR'`, `'ERROR'`, `logging.ERROR` |
| `CRITICAL` | `'C'`, `'CRT'`, `'CRIT'`, `'CRITICAL'`, `logging.CRITICAL` |
| `FATAL` | `'F'`, `'FTL'`, `'FAT'`, `'FATAL'`, `logging.FATAL` |

Invalid or `None` values default to `DEFAULT_LOGGING_LEVEL` (DEBUG).

## Example

```python
import asyncio
import logging
from scietex.service import BasicAsyncWorker


class MyService(BasicAsyncWorker):
    """A simple daemon service."""

    async def initialize(self) -> bool:
        """Connect to external services."""
        self.logger.info("Initializing MyService...")
        # self.db = await connect_database(self.conf_dir / "db.yaml")
        return True

    async def heartbeat(self) -> None:
        """Override for custom heartbeat behavior."""
        self.logger.debug("Heartbeat — all systems nominal")
        # await self.db.ping()

    async def watchdog(self) -> None:
        """Override for custom watchdog behavior."""
        self.logger.debug("Watchdog check")
        # Check disk space, memory, dependencies, etc.

    async def cleanup(self) -> None:
        """Release resources on shutdown."""
        self.logger.info("Cleaning up MyService...")
        # await self.db.close()


async def main():
    worker = MyService(
        service_name="my_daemon",
        version="1.0.0",
        worker_id=1,
        heartbeat_interval=15,
        watchdog_interval=5,
        logging_level="INFO",
    )

    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

## Best Practices

### Worker Uniqueness

Within a single process, the `(service_name, worker_id)` combination
should be unique to ensure separate logger names and avoid conflicts:

```python
# Good: separate logger names
worker_a = MyService(service_name="worker", worker_id=1)
worker_b = MyService(service_name="worker", worker_id=2)

# Avoid: duplicate names in same process
worker_a = MyService(service_name="worker", worker_id=1)
worker_b = MyService(service_name="worker", worker_id=1)  # Same logger!
```

### Manager Error Recovery

Managers automatically restart on error (except `CancelledError`). This
makes them suitable for long-running loops that should survive transient
failures:

```python
@Manager(name="MetricsCollector")
async def metrics_loop(self) -> None:
    while True:
        try:
            data = await self.collect_metrics()
            await self.push_metrics(data)
        except ConnectionError:
            self.logger.warning("Metrics push failed, will retry")
            # Not raising — the loop continues
        await asyncio.sleep(10)
```

### Graceful Shutdown

Always perform cleanup in the `cleanup()` method. The worker waits for
`cleanup()` to complete before shutting down logging handlers:

```python
async def cleanup(self) -> None:
    # Flush pending data
    await self.flush_buffer()

    # Close connections
    await self.db.close()
    await self.cache.close()

    self.logger.info("All resources released")
```

### Event Coordination

Use the `events` dictionary to coordinate with external code:

```python
await worker.start()

# Wait for full startup
await asyncio.wait_for(worker.events["exit_requested"].wait(), timeout=5.0)

# Or wait for clean shutdown
await worker.exit()
await asyncio.wait_for(worker.events["exit"].wait(), timeout=10.0)
```
