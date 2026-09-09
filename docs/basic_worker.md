# BasicWorker

The `BasicWorker` is the foundation class for building asynchronous
daemon services in `scietex.service`. It provides signal handling, async
logging, heartbeat and watchdog managers, automatic manager restart on
error, and graceful shutdown support.

## Overview

```python
from scietex.service import BasicWorker
```

The worker manages three core subsystems:

- **Signal Handling** — Captures `SIGINT` and `SIGTERM` for graceful shutdown
- **Async Logging** — Uses `ConsoleHandler` for non-blocking log output
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
| `DEFAULT_MANAGER_MAX_RETRIES` | `5` | `0` | `100` | Default max consecutive failures before a manager gives up |
| `DEFAULT_MANAGER_RESTART_BACKOFF` | `1` | `0` | `60` | Default delay in seconds between manager restart attempts |

## Lifecycle

```
  [STOPPED] ──► [STARTING] ──► [RUNNING] ──► [STOPPING] ──► [STOPPED]
                   │              │               │
            _startup()      initialize()     _shutdown()
                   │              │               │
                   ▼              ▼               ▼
            print logo    custom init logic  stop managers
            start loggers                    cleanup()
            start managers                   stop loggers
```

### Starting

```python
worker = MyWorker(WorkerConfig(service_name="my_service", version="1.0.0"))
await worker.start()
```

The `start()` method creates a task that runs `_startup()`, which:

1. Waits for any previous shutdown to complete
2. Prints the service logo
3. Starts async logging handlers
4. Calls `initialize()` (subclass override point)
5. Registers the instance via `_register_instance()`
6. Starts all `@Manager`-decorated methods as asyncio tasks
7. Sets `start_time` and transitions to `RUNNING`

If `initialize()` returns `False`, a `RuntimeError` is raised and the
worker shuts down.

### Stopping

```python
await worker.exit()
```

`exit()` sets the `exit_requested` event, then calls `stop()`. `stop()`
alone runs the shutdown sequence but does not set `exit_requested`, and
the `exit` event is set only when `exit_requested` was set, so
`await worker.stop(); await worker.events["exit"].wait()` would hang. Use
`exit()` (or a signal) to trigger the `exit` event. Signals
(`SIGINT`/`SIGTERM`) automatically trigger `exit()`. The `_shutdown()`
method:

1. Sets state to `STOPPING`
2. Stops all manager tasks
3. Unregisters the instance via `_unregister_instance()`
4. Calls `cleanup()` (subclass override point)
5. Shuts down logging handlers with a timeout
6. Clears `start_time` and transitions to `STOPPED`
7. Sets the `exit` event to signal completion

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
method is called repeatedly by `ManagerRuntime.run_manager()` in a `while True` loop.
On `CancelledError` the loop stops cleanly. On any other exception, the
error is recorded and the manager is automatically restarted after a
`manager_restart_backoff` delay. Restarts are bounded: after
`manager_max_retries` consecutive failures the manager gives up and ends in
the terminal `FAILED` state (AR-063) — the watchdog logs CRITICAL when a
manager has failed, but the worker does not auto-shutdown (the degradation
stays observable via `worker.failed_managers`).

Managers are discovered via the class MRO (most-derived to base classes)
and executed as named `asyncio.Task` objects.

Each manager is identified by its `name=` (or the decorated method name when
`name` is omitted). Discovery de-duplicates by that identity: if two managers
independently pick the same `name=`, a WARNING is logged naming the colliding
manager and the class it was found on, and only the first (most-derived)
definition runs — the later one is skipped rather than silently dropped
(AR-068).

### Creating a Manager

The decorated method performs a single iteration of work.
`ManagerRuntime.run_manager()` handles the repetition, sleep, and error recovery:

```python
from scietex.service import BasicWorker
from scietex.service.manager import Manager


class MyWorker(BasicWorker):
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
class MyWorker(BasicWorker):
    @Manager(name="ConnectionPool", cleanup=lambda worker: worker.pool.close())
    async def connection_pool_refresh(self) -> None:
        """One iteration of the pool refresh loop."""
        await self.pool.refresh()
        await asyncio.sleep(60)
```

### Built-in Managers

`BasicWorker` provides two built-in managers:

| Manager | Method | Interval | Description |
|---|---|---|---|
| `Heartbeat` | `_heartbeat_manager` | `heartbeat_interval` | Periodically calls `heartbeat()` |
| `Watchdog` | `_watchdog_manager` | `watchdog_interval` | Periodically calls `watchdog()` |

Subclasses can override `heartbeat()` and `watchdog()` to define custom
behavior.

### Manager Status Lifecycle

Each manager tracks a `ManagerStatus` (from `scietex.service.manager`),
owned by `ManagerRuntime`. The normal path is
STARTING → RUNNING → STOPPING → STOPPED; a manager that exhausts its retry
budget ends in the terminal `FAILED` state instead of stopping cleanly
(AR-063):

| Value | Description |
|---|---|
| `STARTING` | Set by `start_manager` while spawning the manager task |
| `RUNNING` | Set as soon as the manager loop starts; stays `RUNNING` through error-retry backoff (AR-057) |
| `STOPPING` | Set during the final cleanup phase |
| `STOPPED` | Terminal: the manager stopped cleanly |
| `FAILED` | Terminal: the manager gave up after exhausting its retry budget |

## Properties

### Identity

| Property | Type | Description |
|---|---|---|
| `service_name` | `str` | Name of the service (read-only) |
| `instance_id` | `str` | Unique identifier for this worker instance, auto-generated (read-only) |
| `version` | `str` | Version string of the service (read-only) |

### State

| Property | Type | Description |
|---|---|---|
| `state` | `ServiceStatus` | Current lifecycle state (read-only) |
| `start_time` | `datetime \| None` | UTC timestamp when service started (read-only) |
| `events` | `Mapping[str, asyncio.Event]` | Lifecycle events read-only view (`exit_requested`, `exit`) |

### Configuration

| Property | Type | Default | Description |
|---|---|---|---|
| `heartbeat_interval` | `float` | `10` | Seconds between heartbeat calls |
| `watchdog_interval` | `float` | `1` | Seconds between watchdog checks |
| `logger_handler_timeout` | `float` | `2` | Timeout for logger handler operations |
| `manager_shutdown_timeout` | `float` | `2` | Timeout for manager shutdown |
| `manager_max_retries` | `int` | `5` | Max consecutive failures before a manager gives up |
| `manager_restart_backoff` | `float` | `1` | Seconds between manager restart attempts |
| `conf_dir` | `Path` | *(resolved)* | Configuration directory path |
| `logging_level` | `int` | `logging.DEBUG` | Current logging level |

### Access

| Property | Type | Description |
|---|---|---|
| `logger` | `logging.Logger` | Logger instance (named `{service_name}:{instance_id}`) |
| `manager_runtime` | `ManagerRuntime` | The worker's manager runtime (read-only view): inspect manager tasks/statuses/errors and control individual managers via `start_manager`/`stop_manager` |
| `failed_managers` | `list[str]` | Names of managers that exhausted their retry budget and gave up (empty `[]` when none failed) |

## Configuration

### Constructor

`BasicWorker` takes a single immutable configuration object
(`WorkerConfig`, from `scietex.service.config`), or `None` to use the
struct defaults:

```python
import logging

from scietex.service import BasicWorker, WorkerConfig

worker = BasicWorker(
    WorkerConfig(
        service_name="service",
        version="0.0.1",
        conf_dir=None,
        logging_level=logging.DEBUG,
        heartbeat_interval=None,
        watchdog_interval=None,
        logger_handler_timeout=None,
        manager_shutdown_timeout=None,
        manager_max_retries=None,
        manager_restart_backoff=None,
    )
)
```

`WorkerConfig` is a frozen `msgspec.Struct`. Fields:

| Field | Default | Description |
|---|---|---|
| `service_name` | `"service"` | Service name for logging and identification |
| `version` | `"0.0.1"` | Version string |
| `conf_dir` | `None` | Configuration directory (`str` or `Path`; see precedence below) |
| `logging_level` | `logging.DEBUG` | Logging level as string or integer |
| `heartbeat_interval` | `None` (uses `DEFAULT_HEARTBEAT_INTERVAL`) | Heartbeat interval in seconds |
| `watchdog_interval` | `None` (uses `DEFAULT_WATCHDOG_INTERVAL`) | Watchdog interval in seconds |
| `logger_handler_timeout` | `None` (uses `DEFAULT_LOGGER_HANDLER_TIMEOUT`) | Timeout for logger handler operations |
| `manager_shutdown_timeout` | `None` (uses `DEFAULT_MANAGER_SHUTDOWN_TIMEOUT`) | Timeout for manager shutdown |
| `manager_max_retries` | `None` (uses `DEFAULT_MANAGER_MAX_RETRIES`) | Max consecutive failures before a manager gives up |
| `manager_restart_backoff` | `None` (uses `DEFAULT_MANAGER_RESTART_BACKOFF`) | Seconds between manager restart attempts |

A `None` timing/retry field resolves to its `DEFAULT_*` constant when the
worker reads it. Configuration is **immutable**: out-of-range values raise
`msgspec.ValidationError` at construction (no silent clamping), and the
worker exposes no runtime setters — all values are fixed at construction.

### Config Directory Precedence

The configuration directory is resolved in this order:

1. `conf_dir` argument (if provided and is a directory)
2. `SCIETEX_CONFIG_DIR` environment variable
3. `$XDG_CONFIG_HOME/scietex/`
4. `~/.config/scietex/`
5. `/etc/scietex/`
6. `/usr/local/etc/scietex/`
7. `./config/` (current working directory)
8. `~/.config/scietex/` — created if none of the above exist

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
from scietex.service import BasicWorker, WorkerConfig


class MyService(BasicWorker):
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
        WorkerConfig(
            service_name="my_daemon",
            version="1.0.0",
            heartbeat_interval=15,
            watchdog_interval=5,
            logging_level="INFO",
        )
    )

    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

## Best Practices

### Worker Uniqueness

Each instance auto-generates a unique `instance_id` (`uuid4().hex`) used for
logger names and (in `ValkeyWorker`) consumer/status keys, so multiple
instances of the same service can coexist in one process without any
manual id management:

```python
# Both instances get distinct logger names automatically
worker_a = MyService(WorkerConfig(service_name="worker"))
worker_b = MyService(WorkerConfig(service_name="worker"))

assert worker_a.instance_id != worker_b.instance_id
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
