# AGENTS.md

## Language

**Always use English.** All replies, comments, documentation, commit messages,
and subagent handoffs in this repository must be written in English, regardless
of the OS locale or the user's language. Never switch to another language unless
explicitly asked.

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
- `BasicWorker` — Base async worker with signal handling, logging, heartbeat, watchdog
- `TaskProcessor` — Extends worker with task queue, concurrent processing, watchdog timeout monitoring
- `ValkeyWorker` — Extends processor with Valkey (Redis) integration via `glide` client
- `MqttWorker` — Extends processor with MQTT 5 integration via `aiomqtt` client

**Transport layer:**
- `TaskTransport` — Protocol for the task-delivery backend (`fetch`/`requeue`/`release`/`on_started`/`ack`/`on_progress`/`on_drain`); `TaskProcessor` composes one via the keyword-only `transport=` argument
- `TaskSink` — Protocol for the enqueue surface a transport delivers into (`task_queue_full`/`enqueue_task`)
- `InMemoryTransport` — Default in-process transport (deque-backed; feed it with `submit(task_id, task_data)`)
- `ValkeyTransport` (`scietex.service.valkey`) — Valkey-stream implementation, injected automatically by `ValkeyWorker`
- `MqttTransport` (`scietex.service.mqtt`) — MQTT 5 implementation draining a durable file-backed inbox, injected automatically by `MqttWorker`
- The legacy hooks (`fetch_tasks`, `return_task_to_queue`, `on_task_started`, `on_task_completed`, `_write_task_progress`, `_on_queue_drain_task_processing`) remain on `TaskProcessor` as thin delegators to the transport

**Transport health (core, `scietex.service.health`):**
- `TransportHealth` — connection-health supervisor: aggregates failures, owns the single reconnect path, logs one CRITICAL per sustained outage; exposed via `ValkeyWorker.transport_health` and `MqttWorker.transport_health`. Hoisted from `valkey/health.py` to core in v4.4.0 and re-exported from `scietex.service.valkey.health` for back-compat.

**Valkey collaborators (internal, `scietex.service.valkey`):**
- `TaskLeaseManager` (`lease.py`) — per-entry lease store (`key`/`write`/`acquire`/`delete`/`refresh`)
- `TaskStatusStore` (`tracking.py`) — per-task status records (`record_running`/`record_terminal`/`update_progress`)
- `ValkeyWorker.__init__(config=None, *, client_factory=None)` — `client_factory` is an async `(GlideClientConfiguration) -> Awaitable[GlideClient]` used by `connect()`, defaulting to `GlideClient.create`

**MQTT collaborators (internal, `scietex.service.mqtt`):**
- `MqttInbox` (`inbox.py`) — Protocol for the durable inbox (`put`/`mark_in_flight`/`mark_terminal`/`pending`/`recover`); `FileMqttInbox` is the file-backed implementation (one JSON file per entry plus `.done` tombstones)
- `MqttTransport` (`transport.py`) — drains the inbox into the processor queue, re-publishes on `requeue`, marks entries terminal on `ack`; publishes retained `TaskStatus` messages and throttled `TaskProgress` messages to per-task topics (a status publisher, not a store — no read-back API)
- `MqttWorker.__init__(config=None, *, client_factory=None)` — `client_factory` is an async `(MqttConfig) -> Awaitable[Client]` used by `connect()`, defaulting to `aiomqtt.Client` (MQTT 5)
- `AsyncMqttHandler` — log handler that owns its own connection (no `client=`), matching `AsyncValkeyHandler`

## Service Entry Points

Run examples with:
```bash
python -m examples.basic_worker           # BasicWorker
python -m examples.manager_cleanup        # @Manager with a cleanup= callable (AR-067)
python -m examples.manager_collision      # @Manager name-collision warning (AR-068)
python -m examples.task_processor         # TaskProcessor
python -m examples.named_task_handlers    # TaskProcessor with named handler instances (AR-053)
python -m examples.stateful_handler       # TaskProcessor with a stateful handler (shared state via **handler_kwargs)
python -m examples.valkey_async_service   # ValkeyWorker (requires valkey-glide)
python -m examples.valkey_pubsub_worker   # ValkeyWorker + PubSub control channels (requires valkey-glide)
python -m examples.valkey_perf            # ValkeyWorker throughput benchmark (requires valkey-glide)
python -m examples.progress_and_cancel    # TaskProcessor + progress reporting and cancellation (requires valkey-glide)
```

**Worker lifecycle:**
1. `await worker.start()` — Initialize, start managers, set state=RUNNING
2. Managers run until `await worker.exit()` (triggered by SIGINT/SIGTERM)
3. `await worker.stop()` — Graceful shutdown: stop managers, cleanup, stop loggers
4. Wait for exit: `await worker.events["exit"].wait()`

## Configuration

**Config directory precedence:**
1. `conf_dir` argument (if provided and is a directory)
2. `SCIETEX_CONFIG_DIR` environment variable
3. `$XDG_CONFIG_HOME/scietex/`
4. `~/.config/scietex/`
5. `/etc/scietex/`
6. `/usr/local/etc/scietex/`
7. `./config/` (current working directory)
8. `~/.config/scietex/` — created if none of the above exist

The first existing directory is used; if none exist, `~/.config/scietex/`
is created.

**Valkey config:**
- Reads `valkey.yml` from config dir (YAML, uses `msgspec.yaml.decode`)
- Raises RuntimeError if the file is present but invalid; creates defaults only if missing
- Read deferred to first `connect()` (AR-066): constructing `ValkeyWorker()` with no explicit `valkey_config` does not touch the filesystem
- `ValkeyWorkerConfig.valkey_config` is `ValkeyConfig | None` (the raw-`GlideClientConfiguration` fallback was removed); PubSub listening is expressed via `ValkeyConfig.pubsub_config` (`ValkeyPubSubConfig(listening=..., parse_control_message=...)`)
- `ValkeyWorkerConfig.task_lease_ttl: int | None = None` — lease lifetime in seconds, bounds `[1, 86400]`; `None` derives `max(1, int(max(2*heartbeat_interval, 3*watchdog_interval)))`
- Install extras: `uv sync --extra valkey` or `pip install "scietex.service[valkey]"`

**MQTT config:**
- Reads `mqtt.yml` from config dir (YAML, uses `msgspec.yaml.decode`)
- Raises RuntimeError if the file is present but invalid; creates defaults only if missing
- Read deferred to first `connect()` (AR-066): constructing `MqttWorker()` with no explicit `mqtt_config` does not touch the filesystem
- `MqttWorkerConfig.mqtt_config` is `MqttConfig | None`; `MqttConfig` fields: `host`, `port`, `username`, `password`, `identifier`, `keepalive`, `clean_start`, `session_expiry_interval`, `transport`, `timeout`, `tls_insecure`, `tls_context`
- `MqttWorkerConfig` fields: `task_topic` (`scietex/{service}/tasks`), `task_qos` (default 2), `inbox_backend` (`"file"`/`"memory"`/`"none"`), `inbox_path`, `inbox_ttl`, `log_topic` (`scietex/{service}/log`), `log_qos` (default 0), `log_retain`, `status_publish_enabled` (default `True`), `status_topic_prefix` (default `scietex/{service}/tasks`), `status_qos` (default 1, range `[0, 2]`), `status_ttl` (default 86400, range `[1, 2592000]`, `None` disables expiry), `progress_qos` (default 0, range `[0, 2]`), `progress_min_interval` (default 1.0, range `[0.0, 3600.0]`), `progress_min_delta` (default 0.0, range `[0.0, 100.0]`)
- MQTT 5 only; the task id travels as the `scietex-task-id` user property (the `TaskEnvelope` wire format is untouched)
- Delivery semantics: aiomqtt v2.5.1 auto-acks at the broker when `on_message` returns, so wire QoS 2 is at-most-once at the app layer; the durable file inbox restores at-least-once by persisting every received message before processing and deduping on replay via tombstones. `inbox_backend="memory"` (or its alias `"none"`) is the explicit at-most-once opt-out, backed by `MemoryInbox`
- No status store: `MqttTransport` publishes retained `TaskStatus` messages and throttled `TaskProgress` messages to per-task topics (`scietex/{service}/tasks/{task_id}/status` default QoS 1 retained, `.../progress` default QoS 0 not retained) — a publisher with no read-back API, not a store; `status_publish_enabled=False` restores the no-op; progress also remains in-process via `TaskCapabilities`
- Registry/heartbeat use retained-message topics `scietex/{service}/workers/{instance_id}`
- Install extras: `uv sync --extra mqtt` or `pip install "scietex.service[mqtt]"`

## Task Handler System

**Workflow:**
1. Register handler: `processor.add_task_handler(HandlerClass)` — an optional keyword-only `name` (`add_task_handler(HandlerClass, name="...")`) lets multiple instances of one class coexist under distinct keys
2. Handler `supports(task_type)` must return `True`
3. Handler `is_ready` (initialized) required before processing
4. `handle(task_data, *, capabilities=...)` returns `TaskResult`; report progress via `capabilities.report_progress(value)`

**Task schemas (msgspec.Struct):**
- `TaskData`: `task: str`, `payload: bytes`, `timeout: TaskTimeout`, `canceled_action: "requeue"|"discard"`
- `TaskResult`: `status: "success"|"error"`, `error: str`, `payload: bytes`, `processed_at: datetime`, `error_code: str`, `retryable: bool`, `partial: bool`
- `TaskTimeout`: `timeout: float | None`, `timeout_action: "requeue"|"discard"`
- `TaskEnvelope`: `version: int = 1`, `data: bytes` — versioned transport envelope; encode/decode via `task_handler.wire` (`encode_task_envelope`/`decode_task_envelope`)
- `TaskStatus`: per-task tracking record — `task_id`, `service`, `task`, `status: "queued"|"running"|"completed"|"failed"|"cancelled"`, `progress: TaskProgress`, `result`, `data`, `error`, `error_code`, `created_at`/`updated_at`
- `TaskProgress`: `progress: bool = False`, `value: float = 0.0` — granular progress; `value` is meaningful only when `progress` is True
- `CancelReason`: `Literal["deliberate", "timeout", "shutdown"]` — why a task was cancelled
- `CANCEL_TASK_TYPE`: built-in `cancel_task` task-type string, served by `CancelTaskHandler`
- `CancelTaskRequest`: `target_task_id: str`, `reason: str = ""` — payload of a `cancel_task` task
- `CancelTaskResponse`: `target_task_id: str`, `outcome: str` — payload returned by a successful `cancel_task`

## Testing

**Run tests:**
- All tests: `pytest tests/`
- Specific test file: `pytest tests/test_<name>.py`, or a package module: `pytest tests/valkey/test_lease.py`, `pytest tests/task_processor/test_cancellation.py`
- With coverage: `tox` (runs pytest with coverage reporting)

**Test helpers:**
- `pytest-asyncio` enabled
- Valkey worker tests live in `tests/valkey/` and mock `GlideClient` via a shared `DummyClient` in `tests/valkey/_helpers.py` — no Valkey server required for unit tests

## Quirks & Gotchas

- **Import-time `ImportError` in `scietex.service.valkey` is swallowed** — package remains importable without `valkey-glide`; a non-`ImportError` bug (e.g. a broken glide install) propagates
- **Logging is async** — uses `ConsoleHandler` and `AsyncValkeyHandler` (both subclass `AsyncLoggingHandler`); shutdown has timeout
- **Manager restart** — fails restarts automatically on error (except `CancelledError`), up to `manager_max_retries` consecutive failures (default 5), after which it gives up
- **Valkey stream names:** `scietex:{service_name}:tasks` with group `scietex:{service_name}:task_group`
- **Transport seam:** `TaskProcessor` composes a `TaskTransport` (default `InMemoryTransport`); `ValkeyWorker` injects `ValkeyTransport`. The six legacy delivery hooks remain as thin delegators, so subclass overrides still work
- **Timeout defaults:** `task_timeout` (config `TaskProcessorConfig.task_timeout`) = 3s, `heartbeat_interval` = 10s, `watchdog_interval` = 1s
- **Python 3.10+ required** (per `requires-python = ">=3.10"`)
