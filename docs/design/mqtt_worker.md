# v4.4.0 — MQTT Worker Design

**Status:** design approved — decisions locked (§10); implementation in progress (§12)
**Target release:** v4.4.0
**Motivation:** AR-089 (`docs/reviews/architecture/2026-09-16-2.md`) — the
transport seam was built so a second transport could be added without touching
core. This document specifies that second transport: an MQTT-backed
`MqttWorker`/`MqttTransport` that reuses the core `TaskTransport` Protocol, the
transport-agnostic wire format, and the `TransportHealth` supervisor.

---

## 1. Goals and non-goals

### Goals

- A `MqttWorker` that consumes tasks from MQTT topics and processes them with
  the existing `TaskHandler` machinery, exactly as `ValkeyWorker` does for
  Valkey streams.
- Reuse of the core `TaskTransport`/`TaskSink` Protocols and the versioned
  `TaskEnvelope` wire format — no new contract, no new serialization.
- At-least-once processing via an **application-level durable inbox**, because
  the chosen broker library cannot defer acknowledgement past handler
  completion (see §3).
- The same operational surface as `ValkeyWorker`: connection health
  supervision, heartbeat, instance registry, graceful shutdown, progress
  reporting, cancellation.
- An optional `mqtt` extra so the core package stays dependency-light.

### Non-goals

- No change to the core `TaskTransport` Protocol. If MQTT needs a method the
  Protocol lacks, that is a finding to raise, not a silent extension.
- No MQTT-specific task schema. The wire format is shared.
- No broker-side exactly-once guarantee. The library cannot provide it with the
  chosen client (see §3); the durable inbox provides at-least-once with
  idempotent replay instead.
- No migration of `scietex.logging` to a newer aiomqtt. That is a separate
  repository and release.
- No new log-handler implementation. Logging to MQTT reuses the existing
  `AsyncMqttHandler` from `scietex.logging` (§2.4); the design only wires it.

---

## 2. Architecture

The design mirrors `ValkeyWorker` exactly, because that pattern is already
proven and documented (`docs/architecture/components.md` §12–13).

```
TaskProcessor (core)
  └── composes a TaskTransport (keyword-only transport=, default InMemoryTransport)
        ├── InMemoryTransport   (core, default)
        ├── ValkeyTransport     (valkey/ package)
        └── MqttTransport       (mqtt/ package)   ← new in v4.4.0

MqttWorker(TaskProcessor)
  ├── MqttTransport          (implements the 7 Protocol methods)
  ├── TransportHealth        (connection-health supervisor)
  ├── MqttInbox              (durable inbox: at-least-once + dedupe)
  └── TaskStatusStore-like   (optional: progress/status records)
```

### 2.1 Package layout

```
src/scietex/service/mqtt/
    __init__.py      # re-exports MqttWorker, MqttWorkerConfig, MqttTransport, ...
    _aiomqtt.py      # the single guarded aiomqtt import (mirrors valkey/_glide.py)
    config.py        # MqttConfig, MqttWorkerConfig, loader, converter
    transport.py     # MqttTransport — the TaskTransport implementation
    worker.py        # MqttWorker — composition + lifecycle overrides
    inbox.py         # MqttInbox — durable inbox for at-least-once
    logging.py       # logging_handler_config — MqttConfig → AsyncMqttHandler kwargs
    health.py        # (only if TransportHealth is NOT hoisted to core — see §7)
```

The `_aiomqtt.py` module is the analogue of `valkey/_glide.py`: the single
place that imports the optional dependency, raising a clear `ImportError` with
an install hint when the extra is absent (AR-048 pattern).

### 2.2 `MqttTransport`

Implements the seven `TaskTransport` methods. Mapping from MQTT semantics:

| Protocol method | MQTT behavior |
|---|---|
| `fetch(sink)` | Drain the inbox (and/or the aiomqtt message queue) into `sink.enqueue_task` until `sink.task_queue_full()`. Returns `True` if any task was enqueued. |
| `requeue(task_id, task_data)` | Re-publish the envelope to the task topic (QoS 2) and mark the inbox entry pending again. |
| `release(task_id)` | Mark the inbox entry released without re-publishing (the broker still holds the message). |
| `on_started(task_id, task_data)` | Mark the inbox entry in-flight. |
| `ack(task_id, task_data, task_result, *, cancel_reason=None)` | Mark the inbox entry terminal and remove it (or tombstone it for dedupe). |
| `on_progress(task_id, value)` | **No-op** (§10 #5): MQTT has no server-side key space for status records. Progress still works in-process via `TaskCapabilities`. |
| `on_drain(task_id, task_data)` | On shutdown, leave the inbox entry pending so it is redelivered on restart (durable) — the MQTT analogue of `ValkeyTransport.on_drain`. |

MQTT-specific extras beyond the Protocol (mirroring `ValkeyTransport`'s
`recover_pending_tasks`/`refresh_leases`):

- `recover_pending_tasks(sink)` — replay unacked inbox entries on startup.
- `refresh_leases()` — refresh inbox-entry leases (if the inbox uses leases).

### 2.3 `MqttWorker`

Composition order mirrors `ValkeyWorker.__init__`:

1. `super().__init__(config)` — builds the discarded `InMemoryTransport`.
2. Resolve `MqttWorkerConfig` from `self._config`.
3. Build `TransportHealth` first (so collaborators can receive
   `report_failure`).
4. Build `MqttInbox` (durable store).
5. Build `MqttTransport` with all collaborators injected.
6. `self._transport = self._mqtt_transport`.

Lifecycle overrides mirror `ValkeyWorker`:

| Override | Behavior |
|---|---|
| `initialize` | `super().initialize()` → connect the MQTT client → subscribe to the task topic(s) → replay the inbox. |
| `cleanup` | `super().cleanup()` → stop the message loop → stop the log handler → disconnect → flush the inbox. |
| `watchdog` | `refresh_leases()` → `health.recover()` → `super().watchdog()` → log `critical_report()`. |
| `heartbeat` | Publish a retained heartbeat message on `scietex/{service}/workers/{instance_id}` (§10 #6). |
| `_register_instance` / `_unregister_instance` | Best-effort retained-message publish/clear on the registry topic (§10 #6). |

### 2.4 Logging to MQTT

`ValkeyWorker` attaches a backend log handler so worker logs flow to the same
Valkey backend it consumes tasks from: `_ensure_logging_handler()`
(`valkey/worker.py:284-306`) lazily builds
`AsyncValkeyHandler(stream_name=self._log_stream_name, valkey_config=...)` and
registers it via `LoggingLifecycle.register_logger_handler`. `MqttWorker`
provides the same parity, using the `AsyncMqttHandler` that `scietex.logging`
already ships.

**Handler.** `AsyncMqttHandler` (`scietex/logging/handler/mqtt.py:18`),
constructor:

```python
AsyncMqttHandler(
    topic: str,
    *,
    mqtt_config: dict | None = None,
    qos: int = 0,
    retain: bool = False,
    client: aiomqtt.Client | None = None,
    error_handler: Callable[[logging.LogRecord | None, Exception], None] | None = None,
    queue_maxsize: int = 10000,
)
```

It is an `AsyncLoggingHandler`, so it plugs into the existing
`LoggingLifecycle` start/stop machinery unchanged.

**Wiring.** `MqttWorker._ensure_logging_handler()` mirrors the Valkey method:

1. Return the cached handler if already built.
2. Return `None` if `_mqtt_config` is unresolved (deferred config, AR-066).
3. Build `AsyncMqttHandler(topic=self._log_topic, mqtt_config=logging_handler_config(config), qos=..., retain=...)`.
4. `self._logging_lifecycle.register_logger_handler(handler)`.
5. Return the handler.

It is called from `initialize()` after a successful connect (mirroring
`_connect_locked`'s post-PING `start_logging()`), and the handler is stopped in
`cleanup()` before `disconnect()`.

**Connection ownership.** The handler owns its own MQTT connection by default
(no `client=` passed), matching the `AsyncValkeyHandler` pattern (AR-059/061):
the log path must not be torn down by a task-transport reconnect, and a
logging failure must never fail task processing. Passing the worker's client
via `client=` is possible but rejected as the default for exactly that reason.

**Config.** `MqttWorkerConfig` gains `log_topic: str = "scietex/{service}/log"`
(the analogue of `log_stream_name`), plus optional `log_qos: int = 0` and
`log_retain: bool = False`. The `logging_handler_config(config)` helper in
`mqtt/logging.py` reduces the typed `MqttConfig` to the scalar dict
`AsyncMqttHandler` expects — the direct analogue of
`valkey/config.py:461-489`.

---

## 3. Delivery semantics — the central constraint

### 3.1 Why broker-level "ack after handler" is not available

The chosen client is **aiomqtt v2.5.1** (see §4 for why). It wraps paho-mqtt
and does **not** expose manual acknowledgement. paho auto-acks when its
`on_message` callback returns — which happens as soon as aiomqtt enqueues the
message, *before* the handler runs. Therefore:

- The broker considers a QoS 2 message delivered as soon as it is enqueued
  locally.
- A crash mid-handler does **not** cause broker redelivery.
- Wire-level QoS 2 gives **at-most-once** processing at the application layer.

This is a property of the library, not a design choice. It is documented here
so the limitation is explicit rather than discovered in production.

### 3.2 The durable inbox

To recover at-least-once, `MqttTransport` persists every received message to a
**durable inbox** before it is handed to the processor, and dedupes on replay.

Flow:

1. The aiomqtt message loop receives a message.
2. `MqttInbox.put(task_id, task_data)` persists the envelope (and a
   `pending` marker) **before** `sink.enqueue_task` is called.
3. The processor runs the handler.
4. On terminal completion, `ack` marks the inbox entry terminal and removes it
   (or writes a tombstone for dedupe).
5. On startup, `recover_pending_tasks` replays every non-terminal inbox entry
   back into the queue.

Dedupe: a task id that is already terminal (or already in-flight) is skipped on
replay. The inbox is the source of truth for "has this task been processed".

### 3.3 Inbox backend

**Decision (§10 #3): a file-backed inbox, behind the `MqttInbox` Protocol.**

The inbox exists only to compensate for aiomqtt v2.5.1's premature broker ack.
aiomqtt v3's manual ack removes that need, so the durable backend is
transitional — a file-backed store is the smallest throwaway surface. The
`MqttInbox` Protocol (`put`/`mark_terminal`/`pending`/`recover`) keeps the v3
migration to an implementation swap.

The file-backed implementation stores entries under the config directory (an
append-only log or a small JSON store), with the same `pending`/`in-flight`/
`terminal` lifecycle as §3.2. It is **single-process**: it does not coordinate
across replicas. Multi-replica deployments would need a shared backend, which
the Protocol preserves as a future option.

The worker must refuse to start with at-least-once semantics if no inbox
backend is configured — fail loud, not silent.

---

## 4. Library choice and the ecosystem constraint

### 4.1 The two aiomqtt generations

| | aiomqtt v2.5.1 (stable) | aiomqtt v3.0.0-alpha.1 |
|---|---|---|
| Backend | paho-mqtt `>=2.1.0,<3.0.0` | `mqtt5~=0.7.0` |
| Python | `>=3.8` | `>=3.11` |
| MQTT | 5.0, 3.1.1, 3.1 | 5.0 only |
| Manual ack | **not exposed** | **default** (`puback`/`pubrec`/`pubcomp`) |
| Reconnect | caller-managed | built-in |
| TLS | `tls_context`/`tls_params` | `ssl_context` |

### 4.2 Why v2.5.1

`scietex.logging` is a **core dependency** of `scietex.service`, and its
`[mqtt]` extra already pins `aiomqtt~=2.5.0`; its `AsyncMqttHandler` is built
on the v2.5.1 API. Adopting aiomqtt v3 in `scietex.service` would make the two
packages uninstallable together. v3 is therefore off the table until
`scietex.logging` migrates (separate repo, separate release).

The cost of v2.5.1 is the manual-ack limitation in §3.1, which the durable
inbox compensates for.

### 4.3 Optional extra

```toml
[project.optional-dependencies]
mqtt = ["aiomqtt~=2.5.0"]
```

`dev` should also include it so the full development environment can run the
MQTT tests. The core `dependencies` list is unchanged.

---

## 5. Configuration

### 5.1 `MqttConfig`

A frozen `msgspec.Struct` mirroring the aiomqtt v2.5.1 scalar options, in the
same spirit as `ValkeyConfig`. MQTT 5 is the target (§10 #1), so the session
fields are the MQTT-5 ones (`clean_start`, `session_expiry_interval`) rather
than the 3.1.1 `clean_session`:

```python
class MqttConfig(msgspec.Struct, frozen=True):
    host: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    identifier: str | None = None
    keepalive: int = 60
    clean_start: bool = False
    session_expiry_interval: int = 0
    transport: Literal["tcp", "websockets", "unix"] = "tcp"
    timeout: float | None = None
    tls_insecure: bool | None = None
    # TLS material is runtime-only (not YAML-serializable)
    tls_context: ssl.SSLContext | None = None
```

### 5.2 `MqttWorkerConfig`

Extends `TaskProcessorConfig` (inheriting service identity and task-queue
fields), exactly as `ValkeyWorkerConfig` does:

```python
class MqttWorkerConfig(TaskProcessorConfig, frozen=True):
    mqtt_config: MqttConfig | None = None
    task_topic: str = "scietex/{service}/tasks"
    task_qos: int = 2
    inbox_backend: Literal["file", "none"] = "file"
    inbox_path: str | None = None
    inbox_ttl: int | None = None
    log_topic: str = "scietex/{service}/log"
    log_qos: int = 0
    log_retain: bool = False
```

`__post_init__` calls `super().__post_init__()` then `validate_range` on the
numeric fields, matching `ValkeyWorkerConfig`. `inbox_backend="none"` is the
explicit at-most-once opt-out (§10 #3); the default is the file-backed inbox.

### 5.3 Loader

`read_mqtt_config(conf_dir, *, create_default=True) -> MqttConfig` — reads
`mqtt.yml` from the config directory, mirroring `read_valkey_config`
(including the `create_default` semantics and the `RuntimeError` on an invalid
file).

---

## 6. Wire format

No new format. `MqttTransport` uses the existing transport-agnostic helpers:

- `encode_task_envelope(task_data) -> bytes` — publish payload.
- `decode_task_envelope(payload) -> TaskData | None` — consume payload.
- `decode_task_envelope_version(payload) -> int | None` — diagnostics on
  rejection (AR-098).

**Decision (§10 #2): the task id travels as an MQTT 5 user property**
(`scietex-task-id`) alongside the envelope payload. The envelope stays the pure
wire format — no wire-format change, no topic churn. This is the one place the
MQTT transport needs information the Valkey transport gets for free from the
stream entry key.

---

## 7. `TransportHealth` — hoist to core

**Decision (§10 #4): hoist now.** AR-089 states `TransportHealth` is
transport-agnostic and should be hoisted to core when a second transport is
added. This is that moment.

`TransportHealth` moves from `valkey/health.py` to
`src/scietex/service/health.py` (core), and is re-exported from
`scietex.service.valkey.health` for backward compatibility. This:

- Removes the feature→feature dependency the MQTT package would otherwise need
  (`mqtt/` importing `valkey/health.py`).
- Fulfils AR-089's stated intent.
- Is a small, mechanical move (one module + import updates + a compat
  re-export).

---

## 8. Testing plan

Mirror the Valkey test layout under `tests/mqtt/`:

- `tests/mqtt/_helpers.py` — a fake aiomqtt client (analogue of `DummyClient`),
  so no broker is required for unit tests.
- `tests/mqtt/test_transport.py` — the seven Protocol methods against the fake.
- `tests/mqtt/test_inbox.py` — put/recover/dedupe/terminal semantics.
- `tests/mqtt/test_worker.py` — composition, lifecycle overrides, heartbeat,
  registry.
- `tests/mqtt/test_logging.py` — `_ensure_logging_handler` builds and registers
  an `AsyncMqttHandler`; `logging_handler_config` translation; start/stop via
  `LoggingLifecycle`.
- `tests/mqtt/test_config.py` — `MqttWorkerConfig` validation and the loader.
- `tests/mqtt/test_health.py` — if `TransportHealth` is hoisted, the existing
  tests move to `tests/test_health.py` (core).

Integration tests against a real broker (e.g. Mosquitto in CI) are desirable
but optional; the unit tests must not require a broker.

---

## 9. Documentation updates

- `docs/mqtt_worker.md` — new usage guide, mirroring `docs/valkey_worker.md`,
  including the logging-to-MQTT section.
- `docs/architecture/structure.md` — add the `mqtt/` package layout.
- `docs/architecture/components.md` — add the MQTT component sections
  (transport, inbox, log handler).
- `docs/architecture/overview.md` — add MQTT to the transport table.
- `docs/architecture/dependencies.md` — add the `mqtt` extra and its imports.
- `docs/ROADMAP.md` — record the v4.4.0 MQTT transport.
- `README.md` — add the `mqtt` extra and the `MqttWorker` entry point.
- `AGENTS.md` — add the MQTT example to the service entry points.

---

## 10. Decisions (locked)

All design questions are resolved. These are binding for the v4.4.0
implementation.

| # | Question | Decision |
|---|---|---|
| 1 | Protocol version | **MQTT 5 only.** Single code path; user properties available. |
| 2 | Task-id carrier | **MQTT 5 user property** (`scietex-task-id`). The envelope stays untouched. |
| 3 | Inbox backend | **File-backed, behind the `MqttInbox` Protocol.** No new dependency; single-process; retired when aiomqtt v3 lands. |
| 4 | `TransportHealth` hoist | **Hoist to core now** (`src/scietex/service/health.py`), re-export from `scietex.service.valkey.health` for back-compat. |
| 5 | Status/progress store | **No status store.** `on_progress` is a no-op; progress still works in-process via `TaskCapabilities`. |
| 6 | Registry/heartbeat | **Retained-message topics** (`scietex/{service}/workers/{instance_id}`). |
| 7 | Log-handler connection | **Own connection** (no `client=`), matching the `AsyncValkeyHandler` pattern (AR-059/061). |

### Rationale notes

- **#3 (file inbox):** the durable inbox exists only to compensate for aiomqtt
  v2.5.1's premature broker ack. aiomqtt v3's manual ack removes that need, so
  the durable backend is transitional. A file-backed inbox is the smallest
  throwaway surface; the `MqttInbox` Protocol keeps the v3 migration to an
  implementation swap. Caveat: file-backed is single-process — multi-replica
  deployments would need a shared backend, which the Protocol preserves as an
  option.
- **#5 (no status store):** MQTT has no server-side key space to write status
  records into (unlike Valkey's `scietex:{service}:task:{id}` keys). Progress
  reporting remains functional in-process; it is simply not persisted.

---

## 11. Effort estimate

**Medium–Large (several days).** The transport itself is mechanical (mirroring
`ValkeyTransport`), and the logging wiring (§2.4) is a small addition that
reuses the existing `AsyncMqttHandler`. The file-backed inbox is new state with
its own correctness argument, and the `TransportHealth` hoist touches existing
code and tests. With the decisions above locked, implementation can proceed.

---

## 12. Implementation status

Branch: `feature/mqtt-worker`. Version stays 4.3.0 until the release is cut.

| # | Step | Status | Commit |
|---|---|---|---|
| 1 | Hoist `TransportHealth` to core (`src/scietex/service/health.py`), re-export from `valkey.health`, split `tests/test_health.py` | ✅ done | `e096f83` |
| 2 | `mqtt/` skeleton: `_aiomqtt.py` guarded import, `config.py` (`MqttConfig`, `MqttWorkerConfig`, `read_mqtt_config`), `logging.py` (`logging_handler_config`) | ✅ done | `d939ad8` |
| 3 | `mqtt/inbox.py` (`MqttInbox` Protocol + `FileMqttInbox`) + `tests/mqtt/test_inbox.py` | ✅ done | `a59fd38` |
| 4 | `mqtt/transport.py` (`MqttTransport`) + `tests/mqtt/test_transport.py` | ⬜ pending | — |
| 5 | `mqtt/worker.py` (`MqttWorker`) + `mqtt/__init__.py` exports + `tests/mqtt/test_worker.py` | ⬜ pending | — |
| 6 | Package guard (`MQTT_AVAILABLE`) in `src/scietex/service/__init__.py` + `pyproject.toml` `mqtt` extra | ⬜ pending | — |
| 7 | Docs updates (`docs/mqtt_worker.md`, `docs/architecture/*`, `docs/ROADMAP.md`, `README.md`, `AGENTS.md`) | ⬜ pending | — |
| 8 | Full gate: `ruff check src/ tests/`, `ty check src/`, `pytest tests/` | ⬜ pending | — |

Test count: 294 at branch start → 302 after step 3 (8 inbox tests).
