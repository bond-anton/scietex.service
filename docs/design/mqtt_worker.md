# v4.4.0 — MQTT Worker Design

**Status:** design approved — decisions locked (§10); implemented (§12)
**Target release:** v4.4.0
**Motivation:** AR-089 (`docs/reviews/architecture/2026-09-16-2.md`) — the
transport seam was built so a second transport could be added without touching
core. This document specifies that second transport: an MQTT-backed
`MqttWorker`/`MqttTransport` that reuses the core `TaskTransport` Protocol, the
transport-agnostic wire format, and the `TransportHealth` supervisor.

> **Post-implementation note (v5.0.0).** The task-id carrier was changed after
> this design shipped: `TaskData` gained a required `task_id: str` field, so the
> task id now travels inside the encoded `TaskData` instead of the
> `scietex-task-id` MQTT 5 user property. `TASK_ID_PROPERTY` and
> `MqttWorker._extract_task_id` were removed, and `_handle_message` decodes the
> envelope first (an undecodable envelope — including a pre-v5 payload without
> a `task_id` — is skipped). This supersedes §6 and §10 #2 below;
> `docs/mqtt_worker.md` documents the current behaviour.
>
> **Post-implementation note (v5.0.0, log scoping).** The log destination was
> made per-instance after this design shipped: `log_topic` now defaults to
> `scietex/{service}/{instance_id}/log` (was `scietex/{service}/log`), and a new
> `log_message_expiry` field bounds a retained log message's life. On a graceful
> shutdown `cleanup()` clears the retained log topic when `log_retain` is
> enabled. This supersedes the `log_topic` default shown in §2.4 and §5.2;
> `docs/mqtt_worker.md` documents the current behaviour.

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
proven and documented (`docs/architecture/components.md` §13–14).

```
TaskProcessor (core)
  └── composes a TaskTransport (keyword-only transport=, default InMemoryTransport)
        ├── InMemoryTransport   (core, default)
        ├── ValkeyTransport     (valkey/ package)
        └── MqttTransport       (mqtt/ package)   ← new in v4.4.0

MqttWorker(TransportWorker)
  ├── MqttTransport          (implements the 8 Protocol methods)
  ├── TransportHealth        (connection-health supervisor)
  ├── MqttInbox              (durable inbox: at-least-once + dedupe)
  └── status publisher       (retained TaskStatus + throttled TaskProgress)
```

### 2.1 Package layout

```
src/scietex/service/mqtt/
    __init__.py      # re-exports MqttWorker, MqttWorkerConfig, MqttTransport, ...
    _aiomqtt.py      # the single guarded aiomqtt import (mirrors valkey/_glide.py)
    config.py        # MqttConfig, MqttWorkerConfig, loader, converter
    transport.py     # MqttTransport — the TaskTransport implementation
    worker.py        # MqttWorker — composition + lifecycle overrides
    inbox.py         # MqttInbox Protocol + MemoryInbox (at-most-once opt-out)
    inbox_sqlite.py  # SqliteMqttInbox — shared WAL store with cross-process claim/lease
    logging.py       # logging_handler_config — MqttConfig → AsyncMqttHandler kwargs
```

The `_aiomqtt.py` module is the analogue of `valkey/_glide.py`: the single
place that imports the optional dependency, raising a clear `ImportError` with
an install hint when the extra is absent (AR-048 pattern).

### 2.2 `MqttTransport`

Implements the eight `TaskTransport` methods. Mapping from MQTT semantics:

| Protocol method | MQTT behavior |
|---|---|
| `fetch(sink)` | Drain the inbox (and/or the aiomqtt message queue) into `sink.enqueue_task` until `sink.task_queue_full()`. Returns `True` if any task was enqueued. |
| `requeue(task_data)` | Re-publish the envelope to the task topic (QoS 2), and mark the inbox entry pending again. The task id travels inside the encoded `TaskData`; the worker's own message loop rejects any message whose envelope does not decode. |
| `on_started(task_data)` | Mark the inbox entry in-flight. |
| `ack(task_data, task_result, *, cancel_reason=None)` | Mark the inbox entry terminal and remove it (or tombstone it for dedupe). |
| `on_progress(task_id, value)` | Publish a throttled `TaskProgress` message to the per-task progress topic (addendum §13); a no-op when status publishing is disabled. Progress also stays in-process via `TaskCapabilities`. |
| `on_drain(task_data)` | On shutdown, leave the inbox entry pending so it is redelivered on restart (durable) — the MQTT analogue of `ValkeyTransport.on_drain`. No status is published. |
| `refresh_leases()` | Renew the cross-process claims on this worker's enqueued data tasks (a no-op for the memory backend; kept for parity with `ValkeyTransport`). |
| `recover_pending_tasks(sink)` | Replay non-terminal inbox entries on the first `fetch` (shared `RecoverableTransport` guard), returning `(recovery_complete, enqueued)`. |

Every lifecycle hook additionally publishes a status message when status
publishing is enabled; the table above lists delivery behavior only. See
addendum §13 for the publishing contract.

`recover_pending_tasks` and `refresh_leases` are now Protocol members
(AR-113), not MQTT-specific extras: `recover_pending_tasks(sink)` replays
non-terminal inbox entries on the first `fetch`, and `refresh_leases()` renews
the cross-process claims on the enqueued data tasks (a no-op for the
memory backend; kept for parity with `ValkeyTransport`).

### 2.3 `MqttWorker`

Composition order mirrors `ValkeyWorker.__init__`:

1. `super().__init__(config, client_factory=...)` — `TransportWorker` builds
   the `TransportHealth` and the client lock; `TaskProcessor` builds the
   discarded `InMemoryTransport`.
2. Resolve `MqttWorkerConfig` from `self._config`.
3. Build `MqttConfigSource` (retained snapshot) and the `MqttInbox` (durable
   store).
4. Build `MqttTransport` with all collaborators injected (health, publish seam).
5. `self._transport = self._mqtt_transport`.

Lifecycle overrides mirror `ValkeyWorker`:

| Override | Behavior |
|---|---|
| `_connect_locked` / `_disconnect_locked` | Build/enter and exit/tear-down the aiomqtt client (the lock is held by the base `connect()`/`disconnect()`). |
| `initialize` | `super().initialize()` → connect the MQTT client → subscribe to the task and config topics → apply local/remote config (recovery is deferred to the first `fetch`). |
| `cleanup` | `super().cleanup()` → stop the message loop → stop the log handler → disconnect → flush the inbox. |
| `_read_remote_outcome` | Await the retained config snapshot (bounded `config_startup_timeout`) and apply it. |
| `heartbeat` | Publish a retained heartbeat message on `scietex/{service}/workers/{instance_id}` (§10 #6). |
| `_register_instance` / `_unregister_instance` | Best-effort retained-message publish/clear on the registry topic (§10 #6). |

The `watchdog` (`refresh_leases()` → `health.recover()` →
`super().watchdog()` → `critical_report()`) and the `connect()`/`disconnect()`
lock wrappers are inherited from `TransportWorker` (AR-102).

In addition to the task topic, `initialize()` subscribes to the remote-config
topic (`mqtt/worker.py:458`), and `_handle_message` dispatches config-topic
messages to the config source instead of the task path (`mqtt/worker.py:736`).
See `docs/remote_config.md` (§2, §5) for the remote-configuration channel.

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
5. On the first `fetch` (shared `RecoverableTransport` guard),
   `recover_pending_tasks` replays every non-terminal inbox entry back into the
   queue.

Dedupe: a task id that is already terminal (or already in-flight) is skipped on
replay. The inbox is the source of truth for "has this task been processed".

### 3.3 Inbox backend

**Decision (§10 #3): a durable inbox behind the `MqttInbox` Protocol, backed by
a shared SQLite store (multi-process), with an in-process memory backend as the
explicit at-most-once opt-out.**

The inbox exists only to compensate for aiomqtt v2.5.1's premature broker ack.
aiomqtt v3's manual ack removes that need, so the durable backend is
transitional. The `MqttInbox` Protocol
(`put`/`mark_in_flight`/`mark_terminal`/`pending`/`recover`/`prune_expired`/
`claim`/`release`/`refresh`/`close`) keeps the v3 migration to an implementation
swap.

Two backends are selectable via `inbox_backend`:

| Backend | Store | Delivery | Multi-process |
|---|---|---|---|
| `"sqlite"` (default) | a WAL-mode SQLite database at `<conf_dir>/inbox.sqlite3` | at-least-once | **yes** — cross-process claim/lease |
| `"memory"` / `"none"` | in-process dict | at-most-once | n/a (explicit opt-out) |

The SQLite backend opens one WAL-mode database with
`check_same_thread=False` and `isolation_level=None`, serializes every access
behind an `asyncio.Lock` + `asyncio.to_thread`, and issues explicit
`BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK`. A cross-process **claim/lease** (modeled
on the Valkey `TaskLeaseManager`, AR-060) makes the drain safe for multiple
workers: `claim` wins only when the row is unclaimed or its lease has expired,
so two workers draining one store never process the same task id. A crashed
peer's entry is reclaimed once its lease lapses; `refresh` renews a live claim
over the watchdog tick, and `release` returns a rejected/requeued entry to the
pool. The lease TTL defaults to `max(1, int(max(2*heartbeat, 3*watchdog)))`
(`inbox_lease_ttl` overrides it).

Pruning is a watchdog maintenance pass, not a side effect of load:
`prune_expired()` removes tombstones older than `inbox_ttl` and, only when
`inbox_ttl` is set, entries older than `inbox_ttl`. Every worker prunes
independently on its own watchdog tick — there is no leader election, because
the prune DELETE is idempotent and indexed, so redundant maintenance across
workers sharing one store is safe and self-healing (if one worker is down, the
others still prune). The schedule is **jittered** so N workers sharing one
`inbox.sqlite3` do not all fire the same DELETE on the same tick (thundering
herd / `BEGIN IMMEDIATE` lock contention): `inbox_prune_interval` (base
seconds, default `60.0`, range `[1.0, 3600.0]`) and `inbox_prune_jitter`
(fractional, default `0.25`, range `[0.0, 1.0]`; `0.0` disables jitter) set the
next deadline to `now + interval * (1 + uniform(-jitter, +jitter))`. The first
pass is not jittered (it runs on the first watchdog tick).

The **control inbox is always in-memory and per-process** (a `MemoryInbox`),
independent of `inbox_backend`: it is never durable and never shared, because a
broadcast control command must fan out to every worker and control is event-only
— never replayed across a restart. A durable control store would only accumulate
orphaned entries no restarted worker could read, since a restart yields a fresh
`instance_id`.

The worker must refuse to start with at-least-once semantics when
`inbox_backend == "sqlite"` but no **data** inbox could be built — fail loud,
not silent.

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
mqtt = ["scietex.logging[mqtt]>=2.1.0", "aiomqtt~=2.5.0"]
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
    inbox_backend: Literal["memory", "none", "sqlite"] = "sqlite"
    inbox_path: str | None = None
    inbox_ttl: int | None = 86400
    inbox_lease_ttl: int | None = None
    inbox_prune_interval: float = 60.0
    inbox_prune_jitter: float = 0.25
    log_topic: str = "scietex/{service}/log"
    log_qos: int = 0
    log_retain: bool = False
    status_publish_enabled: bool = True
    status_topic_prefix: str = "scietex/{service}/tasks"
    status_qos: int = 1
    status_ttl: int | None = 86400
    progress_qos: int = 0
    progress_min_interval: float = 1.0
    progress_min_delta: float = 0.0
    config_topic: str = "scietex/{service}/config"
    config_qos: int = 1
    config_ttl: int | None = 86400
```

`__post_init__` calls `super().__post_init__()` then `validate_range` on the
numeric fields, matching `ValkeyWorkerConfig`. `inbox_backend="memory"` (or its
alias `"none"`) is the explicit at-most-once opt-out (§10 #3); the default is
the shared SQLite store. `inbox_lease_ttl` (`[1, 86400]`, `None` derives it)
governs the SQLite claim lease. `inbox_prune_interval` (`[1.0, 3600.0]`,
default `60.0`) and `inbox_prune_jitter` (`[0.0, 1.0]`, default `0.25`) set
the jittered cadence of the all-worker inbox prune (§3.3). The seven
status/progress fields
(`status_publish_enabled`, `status_topic_prefix`, `status_qos`, `status_ttl`,
`progress_qos`, `progress_min_interval`, `progress_min_delta`) are specified in
§13.6; the three remote-config fields (`config_topic`, `config_qos`,
`config_ttl`) in `docs/remote_config.md` §10.

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
stream entry key. **Superseded in v5.0.0** (see post-implementation note above):
the id now travels inside `TaskData.task_id`.

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
- `tests/mqtt/test_transport.py` — the eight Protocol methods against the fake.
- `tests/mqtt/test_inbox.py` — put/recover/dedupe/terminal semantics.
- `tests/mqtt/test_inbox_sqlite.py` — SQLite store: roundtrip, TTL/prune,
  conservative error handling, cross-instance claim exclusivity, stale-lease
  reclaim, refresh, concurrent writers.
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
| 2 | Task-id carrier | **MQTT 5 user property** (`scietex-task-id`). The envelope stays untouched. *(Superseded v5.0.0 — the id now travels inside `TaskData.task_id`; see note above.)* |
| 3 | Inbox backend | **Durable inbox behind the `MqttInbox` Protocol: SQLite (shared, cross-process claim/lease) or memory/none (at-most-once opt-out).** No new dependency (`sqlite3` is stdlib); retired when aiomqtt v3 lands. |
| 4 | `TransportHealth` hoist | **Hoist to core now** (`src/scietex/service/health.py`), re-export from `scietex.service.valkey.health` for back-compat. |
| 5 | Status/progress persistence | **No status store; status publisher instead** (amended, addendum §13). `MqttTransport` publishes retained `TaskStatus` messages and throttled `TaskProgress` messages to per-task topics. There is no read-back API, so this is not a store and does not reverse the original decision. |
| 6 | Registry/heartbeat | **Retained-message topics** (`scietex/{service}/workers/{instance_id}`). |
| 7 | Log-handler connection | **Own connection** (no `client=`), matching the `AsyncValkeyHandler` pattern (AR-059/061). |

### Rationale notes

- **#3 (durable inbox):** the durable inbox exists only to compensate for
  aiomqtt v2.5.1's premature broker ack. aiomqtt v3's manual ack removes that
  need, so the durable backend is transitional. The `MqttInbox` Protocol keeps
  the v3 migration to an implementation swap. The SQLite backend is a shared,
  WAL-mode store with a cross-process claim/lease so multi-replica deployments
  can share one inbox without corruption or double-processing; `sqlite3` is
  stdlib, so it adds no dependency.
- **#5 (no status store; status publisher instead):** MQTT has no server-side
  key space to write status records into (unlike Valkey's
  `scietex:{service}:task:{id}` keys), so no store is introduced. What the
  transport does instead is *publish* `TaskStatus`/`TaskProgress` messages to
  per-task topics; the broker retains the latest status message per task but
  exposes no query or read-back API. Publishing is fire-and-forget
  observability, not persistence with a read path, so the original decision
  stands. See §13.

---

## 11. Effort estimate

**Medium–Large (several days).** The transport itself is mechanical (mirroring
`ValkeyTransport`), and the logging wiring (§2.4) is a small addition that
reuses the existing `AsyncMqttHandler`. The durable SQLite inbox is new state
with its own correctness argument, and the `TransportHealth` hoist touches
existing code and tests. With the decisions above locked, implementation can proceed.

---

## 12. Implementation status

Implemented on `main`. Version bumped to 4.4.0 for the release.

| # | Step | Status | Commit |
|---|---|---|---|
| 1 | Hoist `TransportHealth` to core (`src/scietex/service/health.py`), re-export from `valkey.health`, split `tests/test_health.py` | ✅ done | `e096f83` |
| 2 | `mqtt/` skeleton: `_aiomqtt.py` guarded import, `config.py` (`MqttConfig`, `MqttWorkerConfig`, `read_mqtt_config`), `logging.py` (`logging_handler_config`) | ✅ done | `d939ad8` |
| 3 | `mqtt/inbox.py` (`MqttInbox` Protocol + `FileMqttInbox`) + `tests/mqtt/test_inbox.py` | ✅ done | `a59fd38` |
| 4 | `mqtt/transport.py` (`MqttTransport`) + `tests/mqtt/test_transport.py` | ✅ done | — |
| 5 | `mqtt/worker.py` (`MqttWorker`) + `mqtt/__init__.py` exports + `tests/mqtt/test_worker.py` | ✅ done | — |
| 6 | Package guard (`MQTT_AVAILABLE`) in `src/scietex/service/__init__.py` + `pyproject.toml` `mqtt` extra | ✅ done | — |
| 7 | Docs updates (`docs/mqtt_worker.md`, `docs/architecture/*`, `docs/ROADMAP.md`, `README.md`, `AGENTS.md`) | ✅ done | — |
| 8 | Full gate: `ruff check src/ tests/`, `ty check src/`, `pytest tests/` | ✅ done | — |

Test count: 294 at branch start → 302 after step 3 (8 inbox tests) → 316
after step 4 (14 transport tests) → 335 after step 5 (41 MQTT tests total).

---

## 13. Task status and progress publishing (addendum)

**Status:** design approved — implemented.
**Scope:** `MqttTransport`, `MqttWorkerConfig`, and the worker's `publish`
seam. No core `TaskTransport` change, no new wire format, no new dependency.

### 13.0 Relationship to §10 #5

This section amends §10 #5 (exact replacement wording in §13.8). The original
decision — no status **store** — remains in force. What this addendum adds is a
status **publisher**: `MqttTransport` publishes `TaskStatus` and `TaskProgress`
messages to per-task MQTT topics so external observers can follow a task's
lifecycle.

A publisher is not a store. A store has a read path: a caller can ask "what is
the status of task X?" and get an answer. MQTT has no such key space, and this
design adds none — there is no `get_status`, no query key, no read-back at all.
The only way to consume a status is to subscribe; the broker's retained-message
facility is delivery state, not an application data store. Nothing here reverses
§10 #5.

### 13.1 Motivation

MQTT has no server-side key space to write status records into, which is why
§10 #5 rejected a store. But observers still need to watch a task's lifecycle:
an external submitter needs to know when its task starts and finishes, and a
dashboard needs to render progress. The alternatives were rejected for concrete
reasons:

- **Status on the task topic.** `scietex/{service}/tasks` carries the versioned
  `TaskEnvelope` payload. Publishing
  status there would force every task consumer to filter messages by shape, mix
  observability traffic into the delivery path, and make the retained flag
  unusable (a retained status on the task topic would be redelivered as a bogus
  task on every new subscription).
- **A single status topic for all tasks.** `scietex/{service}/tasks/status`
  would make per-task subscription impossible: a client interested in one task
  would receive every task's updates and filter client-side. It also forces one
  retained value for the whole service, so only the most recently touched task
  would be visible to a late subscriber.
- **A per-task topic namespace.** Chosen. It allows wildcard fan-out
  (`.../+/status`), per-task subscription (`.../{id}/#`), and per-task retained
  status, at the cost of topic cardinality proportional to task throughput.

Status and progress are split onto separate topics because they have opposite
delivery profiles: status changes a handful of times per task and must survive
broker restarts for late subscribers, while progress is a high-frequency
fire-and-forget signal that must not accumulate retained state.

### 13.2 Topic scheme

At construction `MqttWorker` resolves the configured prefix once, substituting
the `{service}` placeholder exactly as it already does for `task_topic`:

```
status_prefix  = cfg.status_topic_prefix.format(service=self.service_name)
status_topic   = f"{status_prefix}/{task_id}/status"
progress_topic = f"{status_prefix}/{task_id}/progress"
```

With the defaults (`service_name="worker"`, `status_topic_prefix =
"scietex/{service}/tasks"`) the concrete topics are:

| Purpose | Topic | QoS | Retain |
|---|---|---|---|
| Task status | `scietex/worker/tasks/{task_id}/status` | 1 | **yes** |
| Task progress | `scietex/worker/tasks/{task_id}/progress` | 0 | no |

`task_id` is the string form of the task `UUID` — the same value carried in
`TaskData.task_id` and used in the Valkey store key.

Subscription examples:

| Want | Subscription |
|---|---|
| Status of every task for a service | `scietex/worker/tasks/+/status` |
| Progress of every task for a service | `scietex/worker/tasks/+/progress` |
| Everything about every task | `scietex/worker/tasks/+/#` |
| Everything about one task | `scietex/worker/tasks/{task_id}/#` |
| Status only, one task | `scietex/worker/tasks/{task_id}/status` |

The prefix is configured with `MqttWorkerConfig.status_topic_prefix` (§13.6). It
defaults to the existing task-topic root (`scietex/{service}/tasks`), so the
default namespace is a subtree of the namespace operators already know. Unlike
`task_topic`, the prefix is not itself consumed by the worker; changing it only
moves the published status/progress topics, so a subscriber and the worker must
agree on it out of band.

Retained status semantics: every status publish sets `retain=True`, so the
broker stores the latest `TaskStatus` per task. A subscriber that connects after
a task finished still receives its last status. Status messages are **not**
cleared on task completion; the final status remains observable. The retained
message also carries an MQTT 5 message-expiry interval (`status_ttl`, §13.6;
`None` disables expiry), so the broker ages out the per-task marker instead of
accumulating one retained message per task forever. Each status publish
overwrites the retained message and resets the expiry clock, so the terminal
status lives `status_ttl` from completion. Operators that want to clear status
sooner can still publish an empty payload to the status topic.

Progress messages are never retained: a late subscriber must not receive a stale
progress tick, and retained progress would leak one retained message per task.

### 13.3 Payload schemas

No new schema. Both message kinds reuse the existing task-handler structs and
the `msgspec.msgpack` codec already used by `TaskStatusStore`. Reusing the
structs keeps the MQTT and Valkey transports describing the same lifecycle with
the same field names and semantics.

- Status topic payload: `TaskStatus` (`task_handler/schemas.py:127`).
- Progress topic payload: `TaskProgress` (`task_handler/schemas.py:116`).

The task id is the topic suffix, but `TaskStatus` also carries it as a field, so
a status message is self-describing if it is copied off the wire.

**TaskStatus field population.** Every value mirrors
`TaskStatusStore.record_running`/`record_terminal` (`valkey/tracking.py:79-139`)
so the two transports describe the same lifecycle identically.

| Event | `status` | `task_id` | `service` | `task` | `data` | `result` | `error` | `error_code` | `progress` | `created_at`/`updated_at` |
|---|---|---|---|---|---|---|---|---|---|---|
| accepted into the worker queue | `queued` | `str(task_id)` | service name | `task_data.task` | `None` | `None` | `""` | `""` | `TaskProgress()` | now/now |
| handler started | `running` | `str(task_id)` | service name | `task_data.task` | `None` | `None` | `""` | `""` | `TaskProgress()` | now/now |
| success (`task_result.status == "success"`) | `completed` | `str(task_id)` | service name | `task_data.task` | `None` | `task_result.payload` | `""` | `""` | `TaskProgress()` | now/now |
| non-retryable error | `failed` | `str(task_id)` | service name | `task_data.task` | `None` | `None` | `task_result.error` | `task_result.error_code` | `TaskProgress()` | now/now |
| cancellation, `cancel_reason == "deliberate"` | `cancelled` | `str(task_id)` | service name | `task_data.task` | `task_data` | `None` | `"canceled"` | `""` | `TaskProgress()` | now/now |
| cancellation, timeout or shutdown | `failed` | `str(task_id)` | service name | `task_data.task` | `None` | `None` | `"canceled"` | `""` | `TaskProgress()` | now/now |

`created_at`/`updated_at` are both the publish instant, matching the store's
"record write time" semantics: the terminal record replaces, rather than
extends, the running one. The default `TaskProgress()` (meaning "no granular
progress") is used for every status message; granular values travel only on the
progress topic.

**TaskProgress field population.** The progress payload always sets
`progress=True`; `value` is the throttled value clamped by the reporter to
`[0.0, 100.0]` (`task_handler/capabilities.py:21-24`). The default
`TaskProgress()` (with `progress=False`) is never published on the progress
topic — publishing it would be a meaningless message.

### 13.4 Publishing points

`MqttTransport` publishes through the existing `publish` seam (same injection
pattern as `requeue`). The seam's signature is extended with a keyword-only
retain flag and an MQTT 5 `properties` argument (§13.6), because status must be
retained and carry a message-expiry interval while the envelope requeue must not:

```python
class MqttPublish(Protocol):
    async def __call__(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        *,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> None: ...
```

Hook-by-hook mapping:

| Hook | Inbox behavior (unchanged) | Status/progress behavior |
|---|---|---|
| `fetch` / `recover_pending_tasks` | Enqueue accepted tasks. | Publish `queued` (QoS 1, retained) once per task, immediately after the id is added to `_enqueued` and before `on_started` can fire. Recovery publishes it too, so a task redelivered after a restart re-advertises itself as queued. |
| `on_started` | `mark_in_flight`. | Publish `running` (QoS 1, retained). Reset the task's progress-throttle state. |
| `on_progress` | (none) | Publish a throttled `TaskProgress` (QoS 0, not retained) per §13.5. |
| `requeue` | Re-publish the envelope to the task topic; leave the entry non-terminal. | Publish `queued` (QoS 1, retained): the task has returned to the source queue. Drop the task's progress-throttle state without flushing (the task will restart; a stale progress value would be misleading). |
| `ack` (non-retryable) | Mark terminal (tombstone), drop the enqueued marker. | Flush any coalesced progress value (§13.5), then publish the terminal `completed`/`failed`/`cancelled` status (QoS 1, retained). Drop the throttle state. |
| `ack` (retryable error) | Drop the enqueued marker; **leave the entry non-terminal** (AR-077b mirror) so the retry copy is accepted. | Publish **no** terminal status — the task is not terminal. `requeue` already published `queued` immediately before this call. |
| `on_drain` | Drop the in-process claim; entry stays pending for redelivery. | Publish nothing: the task is neither terminal nor restarted, and the retained status correctly remains `queued` or `running` until redelivery. Drop the throttle state. |

Retryable-error ordering is load-bearing and mirrors the inbox path. In
`handle_task`, `requeue` runs before `ack` (`task_processor.py:1004-1046`), so for
a retryable error the sequence is: `requeue` publishes `queued`, then `ack`
returns early without publishing a terminal status. A subscriber therefore never
sees a retryable error as terminal; the task correctly transitions
`running -> queued -> running ...` until it succeeds or fails permanently.

Status publishing is gated by `status_publish_enabled` (§13.6). When disabled,
the `queued`/`running`/terminal publishes are skipped and `on_progress` is a
no-op, reproducing the pre-addendum behavior. The envelope requeue is
unaffected — it is delivery, not observability.

### 13.5 Throttling policy

Progress can be reported far faster than the broker should be asked to publish,
so progress is coalesced in the transport, below the publish seam.

**Algorithm.** `MqttTransport` keeps a per-task `_ProgressThrottle` record:

```
last_value: float        # value of the last published progress message
last_at: float           # clock() at that publish
ever_published: bool     # False until the first publish for this task
pending: float | None    # latest un-published value, coalesced
```

On `on_progress(task_id, value)` with `now = clock()`, `min_interval =
progress_min_interval`, and `min_delta = progress_min_delta`:

1. If `status_publish_enabled` is False, return.
2. Fetch or create the task's throttle record.
3. Decide whether to publish:
   - `ever_published == False` -> publish (the first tick for a task is always
     sent, so a subscriber sees progress begin).
   - `min_interval > 0` AND `now - last_at >= min_interval` -> publish.
   - `min_delta > 0` AND `abs(value - last_value) >= min_delta` -> publish.
   - `min_interval <= 0` AND `min_delta <= 0` -> publish on every call
     (throttling disabled).
   - otherwise -> do not publish.
4. On publish: send `TaskProgress(progress=True, value=value)` (QoS 0, not
   retained), set `last_value = value`, `last_at = now`,
   `ever_published = True`, `pending = None`.
5. On skip: set `pending = value` (the newest value wins; the transport never
   queues a backlog of stale ticks).

A value of `0` for `min_interval` or `min_delta` disables that threshold
explicitly; both are validated non-negative (§13.6). The "publish on every
call" case is the accurate reading of "both thresholds disabled", not a `>= 0`
comparison that would otherwise always be true.

**Where the state lives.** In `MqttTransport`, keyed by `UUID`, in a
`dict[UUID, _ProgressThrottle]`. It is in-process only; a restart loses it,
which is correct because a restart also resets the in-flight task set.

**Clock injection.** `MqttTransport.__init__` gains a keyword-only
`clock: Callable[[], float] = time.monotonic`, mirroring `TransportHealth`
(`health.py:48`), so throttling tests need no real sleeping.

**Completion flush.** On a non-retryable `ack`, if the task's record has a
`pending` value, it is published on the progress topic immediately before the
terminal status, then the record is dropped. This guarantees a completion is
preceded by the final reported value even when that value fell inside the
throttle window. On `requeue` and `on_drain` the record is dropped
**without** flushing: the task is returning to the queue or awaiting
redelivery, and a stale progress value would misrepresent a fresh run. To keep
the dict bounded, every terminal/requeue/drain path drops the task's
record; `on_started` also resets it, so a re-delivered task starts clean.

### 13.6 Configuration additions

`MqttWorkerConfig` gains seven fields and corresponding bounds. They follow the
existing style: frozen `msgspec.Struct`, module-level `MIN_`/`MAX_` constants,
`validate_range` calls in `__post_init__` after `super().__post_init__()`.

```python
MIN_STATUS_QOS: int = 0
MAX_STATUS_QOS: int = 2
MIN_STATUS_TTL: int = 1
MAX_STATUS_TTL: int = 30 * 24 * 3600
MIN_PROGRESS_QOS: int = 0
MAX_PROGRESS_QOS: int = 2
MIN_PROGRESS_MIN_INTERVAL: float = 0.0
MAX_PROGRESS_MIN_INTERVAL: float = 3600.0
MIN_PROGRESS_MIN_DELTA: float = 0.0
MAX_PROGRESS_MIN_DELTA: float = 100.0
```

| Field | Type | Default | Bounds | Meaning |
|---|---|---|---|---|
| `status_publish_enabled` | `bool` | `True` | — | Master switch for all status/progress publishing. `False` restores the pre-addendum no-op behavior. |
| `status_topic_prefix` | `str` | `"scietex/{service}/tasks"` | — | Prefix for the per-task status/progress topics. `{service}` is substituted at construction. |
| `status_qos` | `int` | `1` | `[0, 2]` | QoS for `TaskStatus` publishes. |
| `status_ttl` | `int \| None` | `86400` | `[1, 2592000]` | MQTT 5 message-expiry interval in seconds for retained `TaskStatus` publishes; `None` disables expiry. |
| `progress_qos` | `int` | `0` | `[0, 2]` | QoS for `TaskProgress` publishes. |
| `progress_min_interval` | `float` | `1.0` | `[0.0, 3600.0]` | Minimum seconds between progress publishes; `0` disables the interval threshold. |
| `progress_min_delta` | `float` | `0.0` | `[0.0, 100.0]` | Minimum absolute progress change that forces a publish; `0` disables the delta threshold. |

Validation added to `__post_init__`:

```python
validate_range(self.status_qos, "status_qos", minimum=MIN_STATUS_QOS, maximum=MAX_STATUS_QOS)
validate_range(self.status_ttl, "status_ttl", minimum=MIN_STATUS_TTL, maximum=MAX_STATUS_TTL)
validate_range(self.progress_qos, "progress_qos", minimum=MIN_PROGRESS_QOS, maximum=MAX_PROGRESS_QOS)
validate_range(
    self.progress_min_interval,
    "progress_min_interval",
    minimum=MIN_PROGRESS_MIN_INTERVAL,
    maximum=MAX_PROGRESS_MIN_INTERVAL,
)
validate_range(
    self.progress_min_delta,
    "progress_min_delta",
    minimum=MIN_PROGRESS_MIN_DELTA,
    maximum=MAX_PROGRESS_MIN_DELTA,
)
```

Retain flags are not configurable: status is always retained and progress is
always non-retained, because those are properties of the two message kinds, not
deployment choices.

### 13.7 Failure semantics

Status and progress are observability. A publish failure must never fail, block,
requeue, or slow a task, and must never corrupt inbox state.

- Every status/progress publish is wrapped so no exception escapes the hook.
  The envelope requeue in `requeue` keeps its current contract (it may raise,
  and `handle_task` logs it); only the status publish alongside it is swallowed.
- A failed **status** publish is logged at WARNING and reported to
  `TransportHealth.report_failure`, which drives the existing single reconnect
  path. A failed **progress** publish is logged at DEBUG and also reported:
  progress is high-frequency, so WARNING-level logging would be noise, but the
  health supervisor still needs to see the connection failure.
- Reporting to `TransportHealth` is safe here because `report_failure` is
  synchronous and non-blocking (`health.py:112-124`); it only marks degraded and
  requests a reconnect. The reconnect itself remains owned by
  `watchdog`/`recover`.
- Inbox mutations are performed independently of the publish. For `on_started`,
  `mark_in_flight` runs regardless of publish outcome; for `ack`, the terminal
  tombstone and the AR-077b retryable early-return are unchanged. A status
  publish failure can therefore never turn a completed task into a redelivered
  one, or vice versa.
- Only `Exception` is caught; `asyncio.CancelledError` (a `BaseException`)
  propagates so shutdown cancellation still works. `MqttTransport` already holds
  `_health` "for API parity"; with this addendum the dependency becomes
  load-bearing, and its construction comment must be updated accordingly.

### 13.8 §10 decision update (exact wording)

Replace §10 decision **#5** with:

```
| 5 | Status/progress persistence | **No status store; status publisher instead** (addendum §13). `MqttTransport` publishes retained `TaskStatus` messages and throttled `TaskProgress` messages to per-task topics. There is no read-back API, so this is not a store and does not reverse the original decision. |
```

Replace the §10 rationale note for **#5** with:

```
- **#5 (no status store; status publisher instead):** MQTT has no server-side
  key space to write status records into (unlike Valkey's
  `scietex:{service}:task:{id}` keys), so no store is introduced. What the
  transport does instead is *publish* `TaskStatus`/`TaskProgress` messages to
  per-task topics; the broker retains the latest status message per task but
  exposes no query or read-back API. Publishing is fire-and-forget
  observability, not persistence with a read path, so the original decision
  stands. See §13.
```

The distinction is intentional and should not be described in later docs as a
reversal: **store = read-back API + queryable record; publisher = write-only
messages consumed by subscription.**

### 13.9 Testing plan

All tests remain broker-free, using the existing fake inbox and a recording
publisher. Extend `_transport` in `tests/mqtt/test_transport.py` so `published`
captures `(topic, payload, qos, retain)` and so an injectable clock can be
passed.

Transport-level publish assertions (`tests/mqtt/test_transport.py`):

- `on_started` publishes a retained, QoS 1 `TaskStatus(status="running")` to
  `scietex/svc/tasks/{task_id}/status`; decode with `msgspec.msgpack.decode`.
- `ack` with a success `TaskResult` publishes `completed` with `result` set to
  the result payload.
- `ack` with a non-retryable error publishes `failed` with `error`/`error_code`.
- `ack` with `task_result=None, cancel_reason="deliberate"` publishes
  `cancelled` with `data` set; with `"timeout"`/`"shutdown"` publishes `failed`.
- `ack` with a retryable error publishes no status and leaves the inbox entry
  non-terminal (existing AR-077b assertion preserved).
- `requeue` publishes `queued` (retained, QoS 1) in addition to the envelope.
- `fetch`/recovery publishes `queued` once per accepted task and never
  republishes on a repeat poll.
- `on_drain` publishes nothing.
- A publish that raises `MqttError` does not propagate (both status and
  progress), logs, and calls `health.report_failure`.
- A retained status publish carries an MQTT 5 message-expiry property set to
  `status_ttl`; `status_ttl=None` publishes no properties; a progress tick and
  the envelope requeue publish no properties.

Throttling (`tests/mqtt/test_transport.py`, with an injected clock):

- First `on_progress` always publishes; subsequent calls inside
  `progress_min_interval` coalesce to `pending` and publish nothing.
- Advancing the clock past the interval publishes the newest pending value.
- `progress_min_delta` forces a publish on a large jump inside the interval.
- Both thresholds `0` publishes every call.
- `ack` flushes a pending value before the terminal status; `requeue`
  and `on_drain` do not flush and drop the state.

Config (`tests/mqtt/test_config.py`):

- Defaults for all seven new fields.
- `validate_range` rejects `status_qos=3`, `status_ttl=0`, `status_ttl=2592001`,
  `progress_qos=3`, `progress_min_interval=-1`, `progress_min_delta=101`.
- `status_topic_prefix` formatting is exercised by the worker test, not the
  config test.

Worker wiring (`tests/mqtt/test_worker.py`):

- The transport receives the resolved prefix from `status_topic_prefix` with
  `{service}` substituted.
- `status_publish_enabled=False` makes `on_progress` a no-op and suppresses
  status publishes.
- The worker's `_publish` forwards `retain` to `client.publish`.

### 13.10 Implementation steps

Ordered, atomic, and verifiable. Each step keeps the tree green.

| # | Step | Files | Verify |
|---|---|---|---|
| 1 | Add the ten bounds constants, the seven `MqttWorkerConfig` fields (including `status_ttl`), their `__post_init__` `validate_range` calls, and the field docstrings. | `src/scietex/service/mqtt/config.py` | `pytest tests/mqtt/test_config.py` |
| 2 | Add the new config tests (defaults, rejection cases). | `tests/mqtt/test_config.py` | `pytest tests/mqtt/test_config.py` |
| 3 | Extend the publish seam: replace the `MqttPublish` `Callable` alias with the keyword-only-retain/`properties` `Protocol`; add the `clock` constructor parameter; keep `requeue`'s call site (retain defaults to False, no properties). | `src/scietex/service/mqtt/transport.py` | `ty check src/` |
| 4 | Add `_ProgressThrottle`, the per-task dict, an encoder, `_publish_status`/`_publish_progress` helpers (try/except plus health; `_publish_status` builds the MQTT 5 message-expiry property from `status_ttl`), and wire `on_started`, `ack`, `on_progress`, `requeue`, `on_drain`, plus `queued` on `fetch`/`recover_pending_tasks`. Update module/class docstrings and the `_health` construction comment. | `src/scietex/service/mqtt/transport.py` | `ty check src/` |
| 5 | Update the worker: resolve `_status_topic_prefix`, pass the prefix and clock to `MqttTransport`, extend `_publish` with `retain` and `properties`, and pass the gating config through. | `src/scietex/service/mqtt/worker.py` | `ty check src/` |
| 6 | Extend the transport test helper (recording publisher with retain, injectable clock, config kwargs) and add the publishing/throttling/failure tests from §13.9. | `tests/mqtt/test_transport.py` | `pytest tests/mqtt/test_transport.py` |
| 7 | Add the worker wiring tests from §13.9. | `tests/mqtt/test_worker.py` | `pytest tests/mqtt/test_worker.py` |
| 8 | Full gate. | — | `ruff check src/ tests/ && ruff format --check src/ tests/ && ty check src/ && pytest tests/` |

### 13.11 Open points

- **Queued ownership.** This addendum has the worker emit `queued` on
  acceptance so a subscriber always sees a complete lifecycle. `TaskStatus`'s
  docstring and the Valkey path treat `queued` as submitter-written; an external
  submitter may also publish a retained `queued` status, and the two are
  idempotent under retained-overwrite. If parity with the Valkey split is
  preferred, drop step 4's `fetch`/recovery publish and document `queued` as
  submitter-owned.
- **Retained-status expiry — Resolved.** Retained status messages are bounded by
  an MQTT 5 message-expiry interval. `MqttWorkerConfig.status_ttl` (default
  `86400`, `None` disables) is applied to every retained status publish through
  the `properties` argument added to the `MqttPublish` seam (§13.4, §13.6). Each
  publish resets the expiry clock, so the terminal status lives `status_ttl`
  from completion; once a client has a copy, expiry does not affect it. Progress
  and the envelope requeue publish no expiry.
