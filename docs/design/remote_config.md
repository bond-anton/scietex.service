# Remote Configuration for `scietex.service`

**Status:** implemented
**Target release:** v4.5.0
**Branch:** `main` @ `74b4692`
**Motivation:** operators currently change worker behaviour by editing
`valkey.yml`/`mqtt.yml` or redeploying code. Task-processing fields (timeouts,
concurrency, cadence) are not on disk at all — they live only in the
`TaskProcessorConfig` constructor (`config.py:226-321`). This document specifies
a transport-delivered configuration channel plus three operator commands
(`config:apply`, `config:store`, `config:show`), modelled on the existing
built-in `cancel_task` control path (`task_handler/cancel.py:59-133`).

---

> **Post-implementation note (AR-117).** As originally specified, the
> persistence and inspection surface emitted *resolved* runtime values, which
> destroyed `None`-means-default and `auto_tune` intent on a store→restart
> cycle. AR-117 added a separate **declarative** view — `DeclarativeSettings` /
> `DeclarativeSections` (every field required but each may be `None`) plus
> `to_declarative(...)`. The **remote** envelope stays effective (unchanged wire
> format, no `CONFIG_ENVELOPE_VERSION` bump), while the local `config.yml` and
> the `config:show` `declarative_settings` field carry the declarative view.
> `docs/remote_config.md` documents the current behaviour.

---

## 1. Scope & non-goals

### Goals

- Deliver a **reloadable-behaviour config envelope** to a running worker over
  the transport it already uses: a durable Valkey key or an MQTT retained topic.
- Read that envelope **at startup** (if present) and on demand via a
  **`config:apply` command delivered through the existing task pipeline**.
- Persist the effective config to disk atomically (`config:store`) and inspect
  it (`config:show`), both as task types, transport-parallel.
- Let a **custom service extend the reloadable surface** with its own settings
  struct and apply hook, without the core knowing anything about those fields.
- Keep the change pure-Python, no new dependencies (stdlib
  `hmac`/`hashlib`/`os` + existing `msgspec`).
- Validate-before-swap, serialize applies, and reject replays — the config
  structs are frozen, so applying is "construct a new immutable struct, then
  swap the reference".

### Non-goals

- **No secrets hot-swap.** Credentials, TLS material, `valkey_config`/
  `mqtt_config` connection parameters are restart-required and cannot be
  expressed in the payload at all (§3).
- **No connection-config bootstrap.** The connection config cannot be delivered
  over the connection it describes (chicken-and-egg). Remote config is strictly
  *worker-behaviour* config.
- **No Valkey PubSub.** The PubSub control channel is not used for config
  delivery. The durable key is the source of truth; PubSub remains a
  fire-and-forget control channel for other purposes.
- **No multi-topic atomicity.** One envelope per source of truth, always.
- **No live reconfiguration of transport collaborators.** `ValkeyTransport`,
  `TaskStatusStore`, `TaskLeaseManager`, `MqttTransport`, and the durable inbox
  capture their config at construction (`valkey/transport.py:56-76`,
  `mqtt/transport.py:116`). Only the core `TaskProcessor` allowlist plus
  registered service settings are hot-reloadable in v1 (§3.2).
- **No writes to `valkey.yml`/`mqtt.yml`.** `config:store` writes a new
  dedicated file only (§7).
- **No new storage backend / no distributed coordination.** The remote source is
  either the Valkey key or the MQTT retained topic; the worker does not own or
  replicate it.

---

## 2. Delivery channels per transport

The two transports are deliberately kept **semantically parallel**: one durable
"desired state" location, read at startup and on `config:apply`; commands travel
as tasks.

| | Valkey | MQTT |
|---|---|---|
| Desired-state location | durable key `scietex:{service}:config` | retained topic `scietex/{service}/config` |
| Read primitive | `GET` (await, live) | subscription snapshot (retained delivered on SUBACK) |
| Write primitive (operator) | `SET` | retained `PUBLISH` |
| Command channel | task stream `scietex:{service}:tasks` | task topic `scietex/{service}/tasks` |
| Command task types | `config:apply` / `config:store` / `config:show` | same |

**Decision — Valkey source of truth is the durable key, not PubSub.** Redis/
Valkey PubSub is at-most-once and not persisted; it is a "something changed"
notice, never a source of truth. The key is durable and `GET`-able on startup
and reconnect. This mirrors `MqttWorker`'s "publish is not a store" distinction
(`mqtt_worker.md` §13.0).

**Decision — MQTT source of truth is ONE retained topic.** MQTT has no
cross-topic atomicity, so the whole envelope lives in a single retained message.
Retained = state; commands are non-retained and travel as tasks (prior art:
Home Assistant discovery, Tasmota). The retained message carries an MQTT 5
message-expiry (`config_ttl`, default `86400`, `None` disables) so a stale
marker ages out, mirroring `status_ttl` (`mqtt/config.py:118-120`).

**Naming uses transport-native separators**, matching existing keys/topics:
Valkey keys are colon-separated (`valkey/worker.py:188-192`), MQTT topics
slash-separated (`mqtt/worker.py:198-208`).

**Decision — commands are tasks, not a separate control path.** The repo already
routes a control operation (`cancel_task`) through the task pipeline
(`task_processor.py:171`) with an injected async callback
(`task_handler/cancel.py:59-133`). Reusing it gives free delivery/retry/ack/
backpressure and keeps the two transports identical. Trade-off vs a Celery-style
broadcast control channel: a `config:apply` task competes with worker tasks and
is subject to the task queue's timeout. Accepted for v1.

---

## 3. Payload schema

### 3.1 Envelope

```python
CONFIG_ENVELOPE_VERSION: int = 1

class ConfigEnvelope(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    version: int = CONFIG_ENVELOPE_VERSION
    revision: int = 0            # monotonic; replay protection
    hash: str = ""               # sha256(settings).hexdigest(); integrity only
    signature: str = ""          # hex HMAC-SHA256; empty unless signing enabled
    settings: bytes = b""        # msgpack(ReloadableSettings)
    created_at: datetime | None = None
```

`forbid_unknown_fields=True` is available in the pinned `msgspec 0.20.0`. Note
that the existing YAML loaders use `strict=True` only, which does **not** reject
unknown keys (`read_valkey_config` at `valkey/config.py:385-388`); remote config
is stricter on purpose.

Decode helpers mirror `task_handler/wire.py`: `encode_config_envelope(...) ->
bytes` and `decode_config_envelope(payload) -> ConfigEnvelope | None`, plus a
version peek for diagnostics (mirrors `decode_task_envelope_version`,
`wire.py`).

### 3.2 Reloadable allowlist

> **Post-implementation note (AR-100).** The "re-shadowed" mechanism described
> below was replaced by a single resolved snapshot: `TaskProcessor` now holds
> `self._effective: ReloadableSettings`, written only at construction and on
> core swap (adjacent to `self._config`, no `await` between), and read by the
> live properties and hot loops. There are no private reloadable shadows.

**Only core `TaskProcessor` fields that are read live or can be safely
re-resolved** are reloadable. This is deliberate: the worker resolves the
reloadable fields once into a single snapshot.

| Field | Source class | Reloadable? | Evidence |
|---|---|---|---|
| `max_concurrent_tasks` | `TaskProcessorConfig` | **yes** (re-shadow) | shadow set `task_processor.py:134-164`, read `:1071` |
| `task_manager_sleep_time` | `TaskProcessorConfig` | **yes** (live property) | `task_processor.py:469-478` |
| `task_queue_manager_sleep_time` | `TaskProcessorConfig` | **yes** (live property) | `task_processor.py:480-489` |
| `task_handler_start_timeout` | `TaskProcessorConfig` | **yes** (live property) | `task_processor.py:491-505` |
| `task_handler_stop_timeout` | `TaskProcessorConfig` | **yes** (live property) | `task_processor.py:507-521` |
| `task_timeout` | `TaskProcessorConfig` | **yes** (re-shadow) | shadow `task_processor.py:154` |
| `task_queue_fetch_timeout` | `TaskProcessorConfig` | **yes** (re-shadow) | shadow `task_processor.py:155-159` |
| `task_cancellation_timeout` | `TaskProcessorConfig` | **yes** (re-shadow) | shadow `task_processor.py:160-164` |
| `queue_size` | `TaskProcessorConfig` | no | `asyncio.Queue(maxsize=...)` fixed at `task_processor.py:166` |
| `auto_tune` | `TaskProcessorConfig` | no | read only at construction `:135-147` |
| `service_name`, `version`, `conf_dir`, `logging_level`, `heartbeat_interval`, `watchdog_interval`, `logger_handler_timeout`, `manager_*` | `WorkerConfig` | no | resolved eagerly in `basic_worker.py:104-132`; identity and manager loop cadence |
| all `ValkeyWorkerConfig` transport fields | valkey | no | transport/tracking/lease capture at construction (`valkey/transport.py:56`) |
| all `MqttWorkerConfig` transport fields | mqtt | no | transport captures config at `mqtt/transport.py:116` |
| `valkey_config` / `mqtt_config` | transport | no | connection config, secrets |

```python
RELOADABLE_FIELDS: frozenset[str] = frozenset({
    "max_concurrent_tasks",
    "task_manager_sleep_time",
    "task_queue_manager_sleep_time",
    "task_handler_start_timeout",
    "task_handler_stop_timeout",
    "task_timeout",
    "task_queue_fetch_timeout",
    "task_cancellation_timeout",
})

class ReloadableSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Complete snapshot of the hot-reloadable core fields (all required)."""
    max_concurrent_tasks: int
    task_manager_sleep_time: float
    task_queue_manager_sleep_time: float
    task_handler_start_timeout: float
    task_handler_stop_timeout: float
    task_timeout: float
    task_queue_fetch_timeout: float
    task_cancellation_timeout: float
```

**Decision — complete snapshot, not a patch.** All fields required: a partial
payload fails loudly instead of silently resetting operator-tuned values.
`forbid_unknown_fields=True` means a payload naming `queue_size` or
`valkey_config` is rejected — restart-required fields are *unrepresentable*, not
merely ignored.

### 3.3 Service extension (registered settings struct + apply hook)

A custom service built on top of the core can extend the reloadable surface
without the core knowing its fields. The service declares its own
`msgspec.Struct` and registers it with the reloader:

```python
class MyServiceSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    batch_size: int = 100
    upstream_url: str = ""

worker.register_config_settings(
    "my_service",                       # section name in the envelope
    MyServiceSettings,
    apply=self._apply_my_service_settings,   # Callable[[MyServiceSettings], None]
)
```

The envelope's `settings` payload becomes a **named-section map**:

```python
class ConfigSections(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    core: ReloadableSettings
    services: dict[str, bytes] = msgspec.field(default_factory=dict)
    # section name -> msgpack(registered struct)
```

- The core validates `core` against `ReloadableSettings` and each registered
  section against its registered struct. An unregistered section name is
  rejected (`UNKNOWN_CONFIG_SECTION`).
- Each section's `apply` hook is called **before** the core swap, in
  registration order (`config_reload.py:470-482`). A hook that raises aborts
  the whole apply with no state change (validate-before-swap: the core swap
  runs only after every hook passes).
- Registration is additive and idempotent per section name; re-registering the
  same name replaces the struct + hook.

This keeps the core transport-agnostic and validation strict: a typo in a
service field is rejected by `forbid_unknown_fields`, not silently ignored.

### 3.4 Encoding

- **Wire (task payload, Valkey key value, MQTT retained payload): msgpack.**
  Consistent with the envelope/status wire format (`task_handler/wire.py`,
  `mqtt/transport.py:135`), compact, typed, strict.
- **Disk (`config.yml`): YAML.** Consistent with `valkey.yml`/`mqtt.yml`,
  human-editable, same `msgspec.yaml` codec.
- **Never** pickle (untrusted input). JSON would lose the typed-struct
  validation; YAML on the wire would be needlessly verbose and binary-unfriendly.

---

## 4. Command surface

Three task types, registered exactly like `cancel_task`
(`task_processor.py:186-188`), with async callbacks injected at construction.
The three handlers are registered **only when `remote_config_enabled=True`**; on a
disabled worker they are absent from the dispatch table, so a `config:*` task
yields the permanent "No handler found for task type 'config:...'" result
instead of a `REMOTE_CONFIG_DISABLED` outcome (`ConfigManager.show_config`
retains its `REMOTE_CONFIG_DISABLED` guard for direct calls).

```python
CONFIG_APPLY_TASK_TYPE: str = "config:apply"
CONFIG_STORE_TASK_TYPE: str = "config:store"
CONFIG_SHOW_TASK_TYPE: str = "config:show"
```

Dispatch is opaque string matching (`_find_task_handler` at
`task_processor.py:659-676`), so the colon form is safe.

```python
class ConfigApplyRequest(msgspec.Struct, frozen=True):
    payload: bytes | None = None   # inline envelope; None => re-read source of truth
    persist: bool = False          # also write config.yml after a successful apply

class ConfigApplyResponse(msgspec.Struct, frozen=True):
    applied: bool
    revision: int = 0
    hash: str = ""
    changed: list[str] = msgspec.field(default_factory=list)
    restart_required: list[str] = msgspec.field(default_factory=list)
    error: str = ""

class ConfigStoreRequest(msgspec.Struct, frozen=True):
    target: Literal["disk", "remote", "both"] = "disk"

class ConfigStoreResponse(msgspec.Struct, frozen=True):
    stored: bool
    target: str = ""
    path: str = ""
    revision: int = 0
    hash: str = ""
    error: str = ""

class ConfigShowRequest(msgspec.Struct, frozen=True):
    include_restart_required: bool = True

class ConfigShowResponse(msgspec.Struct, frozen=True):
    settings: bytes = b""                    # msgpack(ConfigSections), never secrets
    revision: int = 0
    hash: str = ""
    source: Literal["default", "file", "remote", "inline"] = "default"
    restart_required_fields: list[str] = msgspec.field(default_factory=list)
```

Handler shape mirrors `CancelTaskHandler` (`task_handler/cancel.py:59-133`): a
`TaskHandler` subclass with injected async callbacks, decoding the request,
returning a non-retryable error on malformed payload.

- **`config:apply`** — `payload` present ⇒ apply that envelope (source
  `"inline"`); absent ⇒ `await source.load()` and apply the source of truth.
  `persist=True` additionally writes `config.yml` after a successful apply (a
  "fetch, apply, remember" one-shot).
- **`config:store`** — `disk` writes `config.yml` (§7); `remote` publishes the
  current effective settings back to the source (Valkey `SET` / MQTT retained
  `PUBLISH`); `both` does both. Default `disk`.
- **`config:show`** — returns the effective settings + metadata (§8).

Replies ride the normal task result payload (`TaskResult.payload`, msgpack),
exactly as `CancelTaskResponse` does (`cancel.py:117-120`). No reply topic/
channel is introduced.

**Error taxonomy:** malformed payload ⇒ `INVALID_CONFIG_PAYLOAD`,
`retryable=False`; validation failure ⇒ `INVALID_CONFIG`, `retryable=False`;
unknown section ⇒ `UNKNOWN_CONFIG_SECTION`, `retryable=False`; no source
attached ⇒ `CONFIG_SOURCE_NOT_CONFIGURED`, `retryable=False`; an attached
source momentarily unreachable ⇒ `CONFIG_SOURCE_UNAVAILABLE`, `retryable=True`
(opts into the framework's single retry, enforced as a per-task-id budget in
`handle_task`'s `finally` — `task_processor.py:1004-1046`; a second consecutive
retryable failure is acked terminal with `retryable=False`); remote config
disabled ⇒ the three handlers are not registered, so a `config:*` task is
answered with the permanent "No handler found" result (a direct
`ConfigManager.show_config` call still returns `REMOTE_CONFIG_DISABLED`,
`retryable=False`).

---

## 5. Startup read

### Valkey

Natural slot: `ValkeyWorker.initialize()` after `super().initialize()` and after
`connect()` succeeds and the client exists (`valkey/worker.py:492`):

```
super().initialize() -> connect() -> config_reloader.reload(ValkeyConfigSource(client)) -> xgroup_create
```

`ValkeyConfigSource.load()` is an awaited `GET scietex:{service}:config` on the
operational client (`valkey/config_source.py:34`). Absent key ⇒ `None` ⇒ fall back to
local `config.yml` if present, else defaults.

### MQTT

The retained message cannot be `GET`-ed; it arrives after SUBACK.
`MqttWorker._start_intake()` (`mqtt/worker.py:434-464`) gains a subscription to
`_config_topic`. `MqttConfigSource` records the latest payload received on that
topic as an in-memory snapshot. Startup does:

1. apply local `config.yml` (synchronous),
2. after connect/subscribe, `await source.wait_for_snapshot(timeout=cfg.config_startup_timeout)`
   (default `2.0s`, bounded), then apply the snapshot if present.

Retained delivery is immediate after SUBACK, so the bounded wait makes startup
deterministic without hanging a broker that has no retained config.
`_message_loop`/`_handle_message` (`mqtt/worker.py:699-752`) currently treats
**every** message as a task and skips messages lacking the `scietex-task-id`
user property; the source requires **topic-based dispatch** in `_handle_message`:
config-topic messages go to the source, everything else follows the existing
task path. This is a real, contained change.

### Failure policy

| Startup condition | Action |
|---|---|
| Remote absent | log DEBUG; use local file / defaults; startup succeeds |
| Remote present, valid | apply; log INFO with revision/hash |
| Remote present, invalid / bad signature / unknown field / stale revision | log ERROR (stale: DEBUG); **keep local/default; startup succeeds** |
| Source unreachable (Valkey `GET` fails) | log WARNING; report to `TransportHealth`; startup succeeds with local/default |
| Feature disabled (`remote_config_enabled=False`) | local `config.yml` ignored entirely — `_apply_local_config` early-returns (no apply, no ERROR log); the remote read logs `REMOTE_CONFIG_DISABLED` at DEBUG |

**Decision — invalid remote config never fails startup.** Availability wins; the
worker is still safe on its local config. A malformed override is an operator
error that is loudly logged, not a crash-loop.

**Staleness:** if `revision <= applied_revision` and hash differs, reject as
stale (`STALE_CONFIG`, no state change); identical hash is an idempotent
success.

---

## 6. Apply semantics

```
validate -> build candidate -> run service hooks -> swap
```

A core `ConfigReloader` (no transport knowledge) owns the whole apply path:

1. **Serialize.** `asyncio.Lock`; every apply (startup, task) takes it, so two
   concurrent `config:apply` tasks cannot interleave.
2. **Decode.** `decode_config_envelope` (rejects unknown fields, wrong schema
   version).
3. **Integrity.** `sha256(settings) == envelope.hash` (reject `HASH_MISMATCH`).
4. **Authenticity (optional).** If `config_signing_key` is set, verify
   `hmac.compare_digest` over `revision.to_bytes(8, "big") + settings` (reject
   `BAD_SIGNATURE`).
5. **Replay.** `revision >= _applied_revision` (equal only if hash matches ⇒
   idempotent).
6. **Decode sections.** `msgspec.msgpack.decode(envelope.settings, type=ConfigSections)`;
   each registered section decoded against its registered struct — unknown/
   restart-required names rejected here.
7. **Validate into a fresh full struct.** Build a shallow dict of the current
   config's fields (preserving nested structs such as `valkey_config` — do
   **not** use `msgspec.structs.asdict`, which recursively converts nested
   structs to dicts and breaks reconstruction), overlay the core settings, and
   construct `type(current)(**merged)`. This runs `__post_init__` and every
   `validate_range` (`config.py:191-227`, `:292-345`; `_validation.py:11-38`).
   `msgspec.structs.replace` must **not** be used: it was verified to bypass
   `__post_init__` in msgspec 0.20.0, silently accepting an out-of-range value.
8. **Run service hooks.** Each registered `apply` hook is called with its
   decoded struct. A raising hook aborts the apply before any swap.
9. **Swap.** Only if steps 7–8 succeeded: assign `self._config = candidate`
   and `self._effective = resolve_reloadable_settings(candidate)` adjacently
   (no `await` between them). Assignment cannot fail, so there is no rollback
   path — validation happened before any mutation.
10. **Record.** `_applied_revision = envelope.revision`, `_applied_hash`,
    `_source`.

**What "apply" swaps:** the core `_config` reference and the `_effective`
snapshot, plus whatever a registered service hook mutates. **What it does not
swap:** transport collaborators, connection clients, the internal
`asyncio.Queue`, task handlers, or any transport-specific config. This is the
honest boundary of v1 and is exactly the allowlist in §3.2.

The live-read properties and hot loops read `self._effective`; the apply path
swaps it alongside `self._config`.

Read-only observability: `worker.config_revision`, `worker.config_hash`,
`worker.config_source` properties, delegating to the reloader.

---

## 7. `config:store` semantics

**Target file:** `<conf_dir>/config.yml` — a new, dedicated file. It does **not**
touch `valkey.yml`/`mqtt.yml`, and there is no existing on-disk `WorkerConfig`
file to write (the `WorkerConfig`/`TaskProcessorConfig` structs are
code-constructed only today; only `valkey.yml`/`mqtt.yml` are loaded,
`valkey/config.py:335-388`, `mqtt/config.py:214-270`).

**Format:** YAML of `ConfigSections` via `msgspec.yaml.encode`.

**Atomic write:** encode to bytes, write to a temp file in the same directory,
`os.replace(tmp, target)` (atomic on POSIX and Windows), no partial file ever
visible. A local disk-write failure leaves the previous file intact and returns
`CONFIG_STORE_FAILED` (local-disk only); a remote store failure maps to
`CONFIG_SOURCE_UNAVAILABLE` (transient).

**Gated behind validation:** `config:store` serializes only settings that were
themselves validated (constructed through the full config struct) — it never
writes an unvalidated blob. If the request carries an inline envelope, it is
validated first (same pipeline as apply) and only then written.

**Interaction with startup:** `read_local_config(conf_dir)` reads `config.yml`
write-free (unlike `read_valkey_config(create_default=True)`, which creates
defaults — `valkey/config.py:361-381`). Missing file ⇒ `None`, no creation.
Invalid file ⇒ log ERROR, ignore, use defaults. Precedence at startup:
**constructor config < `config.yml` < remote source**. The local file is the
persisted snapshot; the remote source stays authoritative when present. This
precedence holds on **every** run of the same worker instance — each run begins
from the constructor/default baseline (the run boundary resets apply state, §9)
— and the local file is applied as a **trusted, unsigned** snapshot: signature
verification is waived for the local source only, so the unsigned `config.yml`
still applies even when `config_signing_key` is set (remote and inline
envelopes remain verified).

**Decision — `config:store` writes the reloadable snapshot only.** Storing the
full transport config (with credentials/TLS) to disk is out of scope and a
security footgun.

---

## 8. `config:show` semantics

Returns the **effective settings** as `msgpack(ConfigSections)`, plus:

- `revision`, `hash`,
- `source` ∈ `{"default","file","remote","inline"}`,
- `restart_required_fields` — the field names that exist in the concrete config
  and are not reloadable (when `include_restart_required=True`).

**Secrets are never emitted and there is no opt-out.** The reloadable surface
contains no credentials, no TLS material, no callables, no connection
parameters, so `config:show` cannot leak them by construction. Transport
connection config (`ValkeyConfig`, `MqttConfig` with `password`/`tls_context`)
is deliberately excluded. An operator who needs to audit connection config reads
it from the config directory, not over the task channel.

The reply is delivered as the task's `TaskResult.payload` (msgpack), identical
to `cancel_task` (`cancel.py:117-120`). On MQTT the same reply is additionally
observable via the status publisher (`mqtt_worker.md` §13), with no new
mechanism.

---

## 9. Security model

**What the library can enforce:**

- **Allowlist by type.** Restart-required and secret fields are unrepresentable
  in `ReloadableSettings`/registered structs; `forbid_unknown_fields=True`
  rejects a payload that names them. A remote message cannot change credentials,
  TLS, connection endpoints, `queue_size`, or anything outside the allowlist.
- **Validate-before-swap.** Every candidate is constructed through the real
  config struct and its `__post_init__`/`validate_range` bounds; a bad value is
  rejected with no state change.
- **Replay protection.** Monotonic `revision` + idempotent hash check.
- **Integrity.** `sha256` detects truncation/corruption (not authenticity).
- **Optional authenticity.** HMAC-SHA256 with `config_signing_key` (stdlib; no
  new dep) rejects unsigned/tampered envelopes when a key is configured. The key
  is runtime-supplied (constructor / environment), never read from the remote
  payload.
- **No code execution.** msgpack/YAML data only, never pickle; YAML is loaded
  with `msgspec.yaml.decode(..., type=...)`, not `yaml.load`.

**What the library cannot enforce (must be stated in docs):**

- **Broker ACLs are the primary defense.** Anyone able to write
  `scietex:{service}:config` or publish retained to `scietex/{service}/config`
  can influence worker behaviour. The library cannot authenticate the broker
  user.
- **No confidentiality on the wire.** Valkey/MQTT transport security (TLS) and
  broker ACLs are the operator's responsibility; `config:show` never emits
  secrets, but `config:apply` inputs are not encrypted by the library.
- **No enforcement of restart for restart-required changes** beyond preventing
  them from being expressed remotely. An operator who edits `valkey.yml` and
  expects a hot reload gets no such thing.
- **Replay window** is run-scoped, not persisted. The apply/replay bookkeeping
  (`_applied_revision`/`_applied_hash`/`_source`/`_section_raw`) is reset at
  each run boundary, so the monotonic revision guard is enforced within a
  single run; across runs the authoritative remote source re-seeds the window.
  There is no persisted revision, and `config.yml` does **not** seed
  `_applied_revision` across restarts.

---

## 10. Config surface

### Core (`config.py`, on `TaskProcessorConfig`)

| Field | Type | Default | Bounds | Meaning |
|---|---|---|---|---|
| `remote_config_enabled` | `bool` | `False` | — | Opt-in master switch. `False` ⇒ the three `config:*` handlers are not registered (a `config:*` task is answered "No handler found"), no startup read. |
| `config_file` | `str` | `"config.yml"` | — | Local reloadable-snapshot filename, resolved under `conf_dir`. |
| `config_signing_key` | `str \| None` | `None` | — | HMAC key. `None` disables signature enforcement. Runtime-only secret; treated as secret in docs. |
| `config_startup_timeout` | `float` | `2.0` | `[0.0, 60.0]` | MQTT bounded wait for the retained snapshot at startup; ignored by Valkey. |

New constants in `config.py`: `MIN_CONFIG_STARTUP_TIMEOUT = 0.0`,
`MAX_CONFIG_STARTUP_TIMEOUT = 60.0`, `DEFAULT_CONFIG_STARTUP_TIMEOUT = 2.0`.

### Valkey (`valkey/config.py`, on `ValkeyWorkerConfig`)

| Field | Type | Default | Bounds | Meaning |
|---|---|---|---|---|
| `config_key` | `str` | `"scietex:{service}:config"` | — | Durable desired-state key; `{service}` substituted at construction like `log_stream_name` (`valkey/worker.py:146`). |

### MQTT (`mqtt/config.py`, on `MqttWorkerConfig`)

| Field | Type | Default | Bounds | Meaning |
|---|---|---|---|---|
| `config_topic` | `str` | `"scietex/{service}/config"` | — | Retained desired-state topic. |
| `config_qos` | `int` | `1` | `[0, 2]` | QoS for config-topic publishes/subscription. |
| `config_ttl` | `int \| None` | `86400` | `[1, 2592000]` | MQTT 5 message-expiry for the retained config; `None` disables. |

New constants in `mqtt/config.py`: `MIN_CONFIG_QOS = 0`, `MAX_CONFIG_QOS = 2`,
`MIN_CONFIG_TTL = 1`, `MAX_CONFIG_TTL = 30 * 24 * 3600`.

### Task-type constants (`task_handler/schemas.py`, beside `CANCEL_TASK_TYPE` at `:15`)

`CONFIG_APPLY_TASK_TYPE`, `CONFIG_STORE_TASK_TYPE`, `CONFIG_SHOW_TASK_TYPE`,
`CONFIG_ENVELOPE_VERSION`.

---

## 11. Module layout

Core vs transport-specific split, descriptive filenames, no catch-all module.

```
src/scietex/service/
    config_reload.py            NEW  core machinery (see below)
    config.py                   EDIT add 4 TaskProcessorConfig fields + constants
    task_processor.py           EDIT build ConfigReloader, _apply_reloadable_config,
                                     register_config_settings, register the 3 handlers,
                                     config_* properties
    task_handler/
        schemas.py              EDIT config task-type constants
        config.py               NEW  handlers + request/response structs (mirrors cancel.py)
        __init__.py             EDIT re-exports
    valkey/
        config_source.py        NEW  ValkeyConfigSource (GET/SET the durable key)
        config.py               EDIT config_key field
        worker.py               EDIT build source, startup read, attach to reloader
        __init__.py             EDIT re-export ValkeyConfigSource
    mqtt/
        config_source.py        NEW  MqttConfigSource (subscribe, snapshot, publish)
        config.py               EDIT config_topic/config_qos/config_ttl
        worker.py               EDIT subscribe config topic, topic-dispatch in
                                     _handle_message, startup bounded wait, attach
        __init__.py             EDIT re-export MqttConfigSource
```

`config_reload.py` (core) contains:

```python
class ConfigSource(Protocol):
    async def load(self) -> bytes | None: ...          # envelope bytes or None
    async def store(self, envelope: bytes) -> None: ... # write desired state back

class ConfigReloader:
    # lock, _applied_revision/_hash/_source, signing key, apply callback,
    # registered sections
    def reset(self) -> None: ...   # clear run-scoped apply bookkeeping (AR-111)
    async def reload(self, source: ConfigSource) -> ConfigApplyOutcome: ...
    async def apply_envelope(self, payload: bytes, *, source: str, trusted: bool = False) -> ConfigApplyOutcome: ...
    async def store(self, ...) -> ConfigStoreOutcome: ...
    def show(self, ...) -> ConfigSections: ...
```

`reset()` (the run-boundary contract) clears the run-scoped apply bookkeeping
(`_applied_revision`/`_applied_hash`/`_source`/`_section_raw`) while preserving
registered sections and injected callbacks; it is called at the run boundary
before any startup apply. `apply_envelope(..., trusted=True)` skips signature
verification for a trusted local artifact (the persisted `config.yml`
snapshot); the replay guard still applies, and the flag must never be set for
remote or inline input.

`ConfigReloader` never imports a transport package; the `ConfigSource` Protocol
lives in core so both transports implement it without a feature→feature
dependency (the same reasoning that hoisted `TransportHealth` to core,
`mqtt_worker.md` §7). The reloader calls back into the processor through an
injected `apply: Callable[[ReloadableSettings], list[str]]` (bound to
`TaskProcessor._apply_reloadable_config`), so the effective settings stay private to
`TaskProcessor`.

---

## 12. Testing plan

Broker-free unit tests follow existing patterns; integration tests use the
`skipif` reachability guard (`tests/valkey/test_pubsub_integration.py:40-43`).

**Core — `tests/test_config_reload.py`**
- envelope encode/decode round-trip; unknown field rejected; wrong schema
  version rejected; hash mismatch rejected.
- signature: valid HMAC accepted, tampered settings / missing signature
  rejected, no key ⇒ unsigned accepted.
- revision monotonicity: equal hash idempotent, lower revision with different
  hash rejected, higher applied.
- `ReloadableSettings` requires every field; unknown (restart-required) name
  rejected.
- registered sections: unknown section rejected; registered struct validated;
  apply hook called before core swap; raising hook aborts with no state change.
- `_apply_reloadable_config`: valid values swap `_config` and all four shadows;
  out-of-range value raises and leaves state unchanged (proves `replace` is not
  used); nested `valkey_config`/`mqtt_config` object identity preserved.
- concurrency: two applies under the lock serialize (second sees updated
  revision).
- `read_local_config` write-free on missing file, rejects invalid YAML; `store`
  atomic (`os.replace`), old file preserved on encode failure.

**Task handlers — `tests/task_processor/test_config_control.py`**
- `config:apply` with inline payload applies; with `payload=None` calls
  `source.load`; source `None` ⇒ `CONFIG_SOURCE_NOT_CONFIGURED` non-retryable.
- `config:store` target disk/remote/both; `config:show` returns settings,
  source, and `restart_required_fields`; never contains connection config.
- malformed payload ⇒ `INVALID_CONFIG_PAYLOAD`, non-retryable.
- `remote_config_enabled=False` ⇒ the three handlers are absent; a `config:*`
  task is answered "No handler found" (direct `ConfigManager.show_config` calls
  still return `REMOTE_CONFIG_DISABLED`).

**Valkey — `tests/valkey/test_config_source.py`** (shared `DummyClient`,
`tests/valkey/_helpers.py`)
- `load()` GETs the resolved key; missing key ⇒ `None`; connection error ⇒
  propagates/report.
- `store()` SETs the key.
- worker startup invokes reload after connect; invalid remote does not fail
  `initialize()`.

**MQTT — `tests/mqtt/test_config_source.py`** (fake client pattern,
`tests/mqtt/`)
- config-topic message updates the snapshot; task-topic message still routes as
  a task; config message lacking `scietex-task-id` is not treated as a task.
- `wait_for_snapshot` returns on delivery and times out cleanly when absent.
- retained config publish sets `retain=True`, `config_qos`, and the `config_ttl`
  message-expiry property.

**Integration (skipif reachable)**
- Valkey: `SET` key → `ValkeyWorker` startup applies; bump revision →
  `config:apply` applies.
- MQTT (optional, if a broker is available): retained config delivered at
  subscribe.

---

## 13. Implementation steps

Each step is independently verifiable and keeps the tree green. Repo gate order:
`ruff check src/ tests/ examples/` → `ruff format --check src/ tests/ examples/`
→ `ty check src/` → `pytest tests/`.

| # | Step | Files | Verify |
|---|---|---|---|
| 1 | Add `ConfigEnvelope`, `ReloadableSettings`, `ConfigSections`, `RELOADABLE_FIELDS`, `ConfigSource`, encode/decode helpers, HMAC/hash/revision checks, `ConfigReloader`, section registry, local `read/write` (atomic). No worker wiring yet. | `src/scietex/service/config_reload.py` | `ty check src/` |
| 2 | Core tests for step 1. | `tests/test_config_reload.py` | `pytest tests/test_config_reload.py` |
| 3 | Add `TaskProcessorConfig` fields + constants + validation. | `config.py` | `pytest tests/test_config.py tests/task_processor/test_config.py` |
| 4 | Add `_apply_reloadable_config` (shadow updates, validate-then-swap), `register_config_settings`, `ConfigReloader` construction, `config_*` read-only properties on `TaskProcessor`; register the three handlers. | `task_processor.py`, `task_handler/config.py`, `task_handler/schemas.py`, `task_handler/__init__.py` | `ty check src/` |
| 5 | Handler + dispatch + apply-semantics tests. | `tests/task_processor/test_config_control.py` | `pytest tests/task_processor/test_config_control.py` |
| 6 | `ValkeyConfigSource`; `config_key` field; wire startup read in `ValkeyWorker.initialize()`. | `valkey/config_source.py`, `valkey/config.py`, `valkey/worker.py`, `valkey/__init__.py` | `pytest tests/valkey/` |
| 7 | `MqttConfigSource`; config fields; subscribe config topic; topic dispatch in `_handle_message`; bounded startup wait. | `mqtt/config_source.py`, `mqtt/config.py`, `mqtt/worker.py`, `mqtt/__init__.py` | `pytest tests/mqtt/` |
| 8 | Example blueprint + docs (`docs/remote_config.md`, architecture `structure.md`/`components.md`/`overview.md`, `AGENTS.md`, `README.md`). | `examples/remote_config.py`, docs | `ruff check src/ tests/ examples/` |
| 9 | Full gate. | — | `ruff check src/ tests/ examples/ && ruff format --check src/ tests/ examples/ && ty check src/ && pytest tests/` |

**Effort: Large (several days).** Core machinery is modest; the MQTT topic
dispatch and startup-wait are the sharp edges, and the allowlist boundary needs
the shadow-update logic verified carefully. Steps 1–5 deliver a usable core
(with no transport source) and are independently shippable.

---

## 14. Locked decisions

1. **Command channel** — task-based, reusing the `cancel_task` precedent and the
   existing task pipeline. Transport-parallel, free delivery/retry/ack.
2. **Task-type naming** — `config:apply` / `config:store` / `config:show`.
3. **`config:store` target** — new dedicated `config.yml` for reloadable
   settings only; never writes `valkey.yml`/`mqtt.yml`.
4. **Invalid remote config at startup** — ignore + log, continue on
   local/default (availability-first).
5. **Signing** — optional stdlib HMAC-SHA256 via `config_signing_key`.
6. **Payload semantics** — complete snapshot, all fields required.
7. **Valkey delivery** — durable key `scietex:{service}:config`, `GET` on demand.
   **No PubSub.**
8. **MQTT delivery** — retained topic `scietex/{service}/config`, subscribed at
   startup.
9. **Extensibility** — registered settings struct + apply hook
   (`register_config_settings`), so a custom service can add its own validated
   options without the core knowing them.
10. **Hot-reload breadth** — core allowlist (8 fields) + registered service
    sections; transport fields, credentials, `queue_size` and service identity
    stay restart-required.
