# Deployment

How to package and run a service built on `scietex.service` as a container
(Docker or Podman), and how to configure it. The framework is a plain Python
library with no console-script entry point: a container runs *your* service
module, which constructs a worker and drives its lifecycle.

## Overview

A deployable image needs four things:

| Concern | What the framework expects |
|---|---|
| Entry point | A Python module with `async def main()` that constructs a worker, calls `await worker.start()`, and waits on `worker.events["exit"]` |
| Config directory | A directory the worker reads `valkey.yml`/`mqtt.yml` from — set `SCIETEX_CONFIG_DIR` and mount it |
| Persistent state | The MQTT sqlite inbox (`<conf_dir>/inbox.sqlite3`) and, if remote config is used, `<conf_dir>/config.yml` |
| Shutdown | SIGTERM (what `docker stop` sends) already triggers graceful shutdown — no extra wiring |

There is **no HTTP server and no `/health` endpoint**. A container healthcheck
must observe the broker-side heartbeat (a Valkey key or an MQTT retained topic)
or the process itself, not an HTTP probe.

## The service entry point

The framework has no `[project.scripts]` entry point. A service is launched by
running a module. The canonical pattern:

```python
# my_service/__main__.py
import asyncio

from scietex.service import TaskProcessor, TaskProcessorConfig


class MyService(TaskProcessor):
    async def initialize(self) -> bool:
        # register handlers, open resources
        return True


async def main() -> None:
    worker = MyService(TaskProcessorConfig(service_name="my_service"))
    await worker.start()
    await worker.events["exit"].wait()


if __name__ == "__main__":
    asyncio.run(main())
```

`worker.start()` spawns the startup task and returns immediately;
`worker.events["exit"].wait()` blocks until the worker has fully stopped. For a
broker-backed worker, use `ValkeyWorker`/`MqttWorker` and their config structs
instead — the lifecycle is identical.

Run it with `python -m my_service`.

## Configuration

### Config-directory resolution

The worker resolves its config directory once, in `BasicWorker.__init__`, using
this precedence (first existing directory wins):

1. The `conf_dir` constructor argument — used only if it is an existing directory
2. `SCIETEX_CONFIG_DIR` — used only if set and the path is an existing directory
3. `$XDG_CONFIG_HOME/scietex`
4. `~/.config/scietex`
5. `/etc/scietex`
6. `/usr/local/etc/scietex`
7. `./config` (current working directory)
8. `~/.config/scietex` — **created** if none of the above exist

Only step 8 creates a directory; steps 1–7 require the directory to already
exist. A set-but-nonexistent `SCIETEX_CONFIG_DIR` is silently skipped, so in a
container always create the directory before the worker starts.

The resolved path is exposed read-only as `worker.conf_dir`.

### Environment variables

The framework reads exactly two environment variables:

| Variable | Purpose | Default |
|---|---|---|
| `SCIETEX_CONFIG_DIR` | Config-directory candidate #2 (used if set and an existing dir) | unset |
| `XDG_CONFIG_HOME` | XDG base dir; candidate #3 becomes `$XDG_CONFIG_HOME/scietex` | unset → `~/.config/scietex` |

Everything else is configured through the config files or the config structs.

### Config files

All config files live in the resolved config directory and are YAML (decoded
with `msgspec.yaml`).

| File | Read by | Missing file | Present but invalid |
|---|---|---|---|
| `valkey.yml` | `ValkeyWorker` | Created with defaults, then defaults used | `RuntimeError`, file left untouched |
| `mqtt.yml` | `MqttWorker` | Created with defaults, then defaults used | `RuntimeError`, file left untouched |
| `config.yml` | Remote config (only when `remote_config_enabled=True`) | Returns `None` (write-free) | Logged as error, returns `None`; startup falls back to current/default config |

`valkey.yml` and `mqtt.yml` are auto-created on first run, so a container can
start with an empty mounted config directory and self-populate. `config.yml` is
never auto-created — it is written by `config:store` and read at startup.

There is **no logging config file**; logging is configured programmatically
(level from `WorkerConfig.logging_level`, handlers registered in code).

### Config structs

The config files cover the transport connection. Worker behaviour is set through
the config structs passed to the constructor:

```python
from scietex.service import TaskProcessorConfig

config = TaskProcessorConfig(
    service_name="my_service",
    max_concurrent_tasks=10,
    task_timeout=3.0,
)
```

`ValkeyWorkerConfig` and `MqttWorkerConfig` extend `TaskProcessorConfig` with
transport-specific fields. See the component guides for the full field tables:
[Valkey worker](valkey_worker.md), [MQTT worker](mqtt_worker.md),
[Task processor](task_processor.md).

## Persistent state

A container must mount the config directory (or at least the state files) to
survive restarts.

| State | Path | Consequence of loss |
|---|---|---|
| MQTT sqlite inbox | `<conf_dir>/inbox.sqlite3` (+ `-wal`/`-shm` sidecars) | In-flight and pending tasks are lost; at-least-once delivery is not preserved across restart |
| Remote-config snapshot | `<conf_dir>/config.yml` | The last `config:store` snapshot is lost; startup falls back to the constructor/default config |
| `valkey.yml` / `mqtt.yml` | `<conf_dir>/` | Regenerated with defaults on next start |

The MQTT inbox is the important one: with `inbox_backend="sqlite"` (the
default), every received message is persisted before processing, and the
`-wal`/`-shm` sidecar files must be preserved alongside the database. Set
`inbox_backend="memory"` (or `"none"`) to opt out of durability entirely — then
nothing needs mounting, at the cost of at-most-once delivery.

Valkey state lives in the Valkey server, not on the container filesystem.

## Signals and shutdown

`SignalHandler` registers handlers for **both SIGINT and SIGTERM**, each bound to
the worker's `_request_exit`. `docker stop` and `podman stop` send SIGTERM by
default, so a container stop produces a graceful shutdown with no extra wiring.

Shutdown is bounded per component but has **no overall process deadline**:

- Managers are stopped with `manager_shutdown_timeout` (default 2 s).
- Logging handlers are stopped with a bound of
  `len(handlers) × logger_handler_timeout + 1` (default `logger_handler_timeout`
  is 2 s).

The framework never calls `sys.exit` or force-kills. The orchestrator's own stop
grace period is the hard limit, so set it comfortably above the expected
shutdown time (the defaults imply a few seconds).

## Health and observability

There is no HTTP endpoint. A worker's liveness is observable through its
heartbeat:

| Transport | Heartbeat location | Discovery |
|---|---|---|
| Valkey | Key `scietex:{service}:{instance_id}:status` (TTL = `Heartbeat.ttl`) | `SCAN scietex:{service}:*:status` |
| MQTT | Retained topic `scietex/{service}/workers/{instance_id}` (MQTT 5 message-expiry = `Heartbeat.ttl`) | Subscribe `scietex/{service}/workers/+` |

The `Heartbeat` struct carries `status` (`"active"`/`"inactive"`),
`queue_depth`, `running_tasks`, and `tasks_per_second`, so a healthcheck can
assert both liveness and progress.

`TransportHealth` (exposed as `worker.transport_health`) tracks connection
health — `connected`/`degraded`/`last_error`/`failure_count`/`down_duration` —
and emits one CRITICAL log per sustained outage past 30 s. It is an in-process
supervisor, not a probe endpoint.

A container healthcheck therefore has two practical options:

- **Process liveness** — check the process is alive (the default for most
  orchestrators).
- **Broker heartbeat** — run a small client that reads the heartbeat key/topic
  and asserts freshness. This detects a worker that is alive but wedged.

## Dockerfile

A multi-stage build keeps the runtime image free of build tooling. The example
below installs the `valkey` extra; swap for `mqtt` or both as needed.

```dockerfile
FROM python:3.12-slim AS build

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir --prefix=/install ".[valkey]"

FROM python:3.12-slim

# Non-root runtime user
RUN useradd --create-home --uid 10001 app
COPY --from=build /install /usr/local
COPY my_service/ /app/my_service/

# Config directory the worker reads; mount a volume here
RUN mkdir -p /etc/scietex && chown app:app /etc/scietex
ENV SCIETEX_CONFIG_DIR=/etc/scietex
VOLUME ["/etc/scietex"]

USER app
WORKDIR /app

# SIGTERM (docker stop) triggers graceful shutdown
STOPSIGNAL SIGTERM

CMD ["python", "-m", "my_service"]
```

Notes:

- `SCIETEX_CONFIG_DIR=/etc/scietex` points the worker at the mounted directory.
  The directory must exist (the `RUN mkdir` guarantees it), otherwise the
  worker silently falls through to `~/.config/scietex`.
- `VOLUME ["/etc/scietex"]` preserves the MQTT sqlite inbox and any
  `config.yml` across container replacement.
- `STOPSIGNAL SIGTERM` is the default but stated explicitly; the framework
  handles it gracefully.
- Run as a non-root user — the worker needs no privileged access.

### Podman

Podman runs the same image. The only difference is the volume flag syntax:

```bash
podman build -t my-service .
podman run -d --name my-service \
  -v my-service-config:/etc/scietex \
  my-service
```

For a rootless Podman deployment, the named volume is owned by the user
namespace; ensure the container user can write to it (the `chown` in the
Dockerfile covers the image's own directory, but a bind mount may need
`--userns=keep-id` or a matching host ownership).

## Running with Docker

```bash
docker build -t my-service .

docker run -d --name my-service \
  -v my-service-config:/etc/scietex \
  --restart unless-stopped \
  my-service
```

To seed configuration before first start, mount a directory containing
`valkey.yml`/`mqtt.yml` instead of an empty volume:

```bash
docker run -d --name my-service \
  -v "$PWD/config:/etc/scietex:ro" \
  my-service
```

A read-only mount is fine for `valkey.yml`/`mqtt.yml`, but the MQTT sqlite inbox
and `config.yml` need write access — use a writable mount if either is in play.

## Compose example

```yaml
services:
  my-service:
    build: .
    restart: unless-stopped
    environment:
      SCIETEX_CONFIG_DIR: /etc/scietex
    volumes:
      - my-service-config:/etc/scietex
    stop_grace_period: 30s

volumes:
  my-service-config:
```

`stop_grace_period` should exceed the expected shutdown time; the framework's
per-component timeouts are a few seconds by default, so 30 s is a safe margin.

## Checklist

- [ ] Service module exposes `async def main()` and runs via `python -m <module>`
- [ ] `SCIETEX_CONFIG_DIR` set to a directory that exists in the image
- [ ] Config directory mounted as a volume (for the MQTT inbox and `config.yml`)
- [ ] `valkey.yml`/`mqtt.yml` seeded or allowed to self-create
- [ ] Container runs as a non-root user
- [ ] `stop_grace_period` exceeds the expected shutdown time
- [ ] Healthcheck reads the broker heartbeat (or checks process liveness)
- [ ] Broker (Valkey/MQTT) reachable from the container network
