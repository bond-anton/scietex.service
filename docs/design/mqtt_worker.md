# v4.4.0 — MQTT Worker Design

**Status:** design for review (no code written yet)
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
| `on_started(task_id, task_data)` | Mark the inbox entry in-flight; optionally write a `running` status record. |
| `ack(task_id, task_data, task_result, *, cancel_reason=None)` | Mark the inbox entry terminal and delete it (or tombstone it for dedupe). |
| `on_progress(task_id, value)` | Update the status record (no-op if no status store is configured). |
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
| `cleanup` | `super().cleanup()` → stop the message loop → disconnect → flush the inbox. |
| `watchdog` | `refresh_leases()` → `health.recover()` → `super().watchdog()` → log `critical_report()`. |
| `heartbeat` | Publish a heartbeat (retained message or a heartbeat topic) with a TTL-equivalent. |
| `_register_instance` / `_unregister_instance` | Best-effort registry publish (retained message on a registry topic). |

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

The inbox needs a durable store. Options, in order of preference:

1. **Reuse the Valkey client when available** — if `valkey-glide` is installed,
   the inbox can be a Valkey key space (`scietex:{service}:inbox:{task_id}`)
   with a TTL, reusing the existing `TaskStatusStore`/`TaskLeaseManager`
   patterns. This gives durability without a new dependency.
2. **A local file/SQLite inbox** — for deployments without Valkey. Adds a
   storage dependency and a new failure mode.
3. **No inbox (at-most-once)** — acceptable only if the user explicitly opts
   out; must be a documented, deliberate choice.

**Recommendation:** make the inbox backend pluggable behind a small Protocol
(`MqttInbox` with `put`/`mark_terminal`/`pending`/`recover`), ship a Valkey-backed
implementation first (reusing the existing client), and leave a file-backed
implementation as a follow-up. The worker must refuse to start with
at-least-once semantics if no durable backend is configured — fail loud, not
silent.

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
same spirit as `ValkeyConfig`:

```python
class MqttConfig(msgspec.Struct, frozen=True):
    host: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    identifier: str | None = None
    keepalive: int = 60
    clean_session: bool | None = None
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
    inbox_backend: Literal["valkey", "file", "none"] = "valkey"
    inbox_ttl: int | None = None
    task_tracking_ttl: int | None = None
```

`__post_init__` calls `super().__post_init__()` then `validate_range` on the
numeric fields, matching `ValkeyWorkerConfig`.

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

The task id is carried as an MQTT user property (MQTT 5) or, for MQTT 3.1.1, as
a topic suffix or a header inside the envelope. **Decision needed:** the
envelope currently carries no task id (the id is the transport key). For MQTT,
the id must travel with the message. Options:

1. **MQTT 5 user property** `scietex-task-id` — clean, but MQTT-5-only.
2. **Topic suffix** `scietex/{service}/tasks/{task_id}` — works on 3.1.1, but
   makes the subscription a wildcard and complicates routing.
3. **A new envelope version** carrying the id — a wire-format change, which
   AR-064 deliberately separated from the handler contract.

**Recommendation:** option 1 (user property) with option 2 as the 3.1.1
fallback, decided by the configured protocol version. This is the one place the
MQTT transport needs information the Valkey transport gets for free from the
stream entry key.

---

## 7. `TransportHealth` — hoist to core?

AR-089 states `TransportHealth` is transport-agnostic and should be hoisted to
core when a second transport is added. This is that moment.

**Recommendation:** hoist `TransportHealth` from `valkey/health.py` to
`src/scietex/service/health.py` (core), and re-export it from
`scietex.service.valkey.health` for backward compatibility. This:

- Removes the feature→feature dependency the MQTT package would otherwise need
  (`mqtt/` importing `valkey/health.py`).
- Fulfils AR-089's stated intent.
- Is a small, mechanical move (one module + import updates + a compat
  re-export).

If the team prefers to defer the hoist, the fallback is for `mqtt/` to import
`TransportHealth` from `valkey/health.py` — acceptable but exactly the
feature→feature coupling AR-089 warns against.

---

## 8. Testing plan

Mirror the Valkey test layout under `tests/mqtt/`:

- `tests/mqtt/_helpers.py` — a fake aiomqtt client (analogue of `DummyClient`),
  so no broker is required for unit tests.
- `tests/mqtt/test_transport.py` — the seven Protocol methods against the fake.
- `tests/mqtt/test_inbox.py` — put/recover/dedupe/terminal semantics.
- `tests/mqtt/test_worker.py` — composition, lifecycle overrides, heartbeat,
  registry.
- `tests/mqtt/test_config.py` — `MqttWorkerConfig` validation and the loader.
- `tests/mqtt/test_health.py` — if `TransportHealth` is hoisted, the existing
  tests move to `tests/test_health.py` (core).

Integration tests against a real broker (e.g. Mosquitto in CI) are desirable
but optional; the unit tests must not require a broker.

---

## 9. Documentation updates

- `docs/mqtt_worker.md` — new usage guide, mirroring `docs/valkey_worker.md`.
- `docs/architecture/structure.md` — add the `mqtt/` package layout.
- `docs/architecture/components.md` — add the MQTT component sections.
- `docs/architecture/overview.md` — add MQTT to the transport table.
- `docs/architecture/dependencies.md` — add the `mqtt` extra and its imports.
- `docs/ROADMAP.md` — record the v4.4.0 MQTT transport.
- `README.md` — add the `mqtt` extra and the `MqttWorker` entry point.
- `AGENTS.md` — add the MQTT example to the service entry points.

---

## 10. Open questions

1. **Task-id transport** — user property (MQTT 5) vs topic suffix (3.1.1)?
   (§6)
2. **Inbox backend** — Valkey-backed first, or file-backed, or both? (§3.3)
3. **`TransportHealth` hoist** — do it now (recommended) or defer? (§7)
4. **Status/progress store** — does MQTT need a `TaskStatusStore` equivalent,
   or is progress reporting a no-op for MQTT? (§2.2)
5. **Registry/heartbeat** — retained-message topics, or omit for MQTT?
   (§2.3)
6. **Protocol version** — target MQTT 5 only, or support 3.1.1 too? This
   determines the task-id mechanism and the available features.

---

## 11. Effort estimate

**Medium–Large (several days).** The transport itself is mechanical (mirroring
`ValkeyTransport`), but the durable inbox is genuinely new state with its own
correctness argument, and the `TransportHealth` hoist touches existing code and
tests. The design should be reviewed and the open questions resolved before
implementation begins.
