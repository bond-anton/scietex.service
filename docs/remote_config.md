# Remote Configuration

Remote configuration delivers a **reloadable-behaviour config envelope** to a
running worker over the transport it already uses — a durable Valkey key or an
MQTT retained topic — plus three operator commands (`config:apply`,
`config:store`, `config:show`) delivered as ordinary tasks. It lets an operator
tune the worker's task-processing behaviour (concurrency, timeouts, cadence)
without editing `valkey.yml`/`mqtt.yml` or redeploying code, and lets a custom
service extend the reloadable surface with its own settings without the core
knowing about them.

## Overview

```python
from scietex.service.mqtt import MqttWorker, MqttWorkerConfig

worker = MqttWorker(MqttWorkerConfig(service_name="svc", remote_config_enabled=True))
```

| Feature | Description |
|---|---|
| Transport-delivered config | One durable "desired state" location per transport: a Valkey key or an MQTT retained topic |
| Startup read | The desired state is applied at startup (availability-first: an absent or invalid config never fails startup) |
| Three commands | `config:apply`, `config:store`, `config:show` travel as tasks through the existing pipeline and reply in their `TaskResult.payload` |
| Disk snapshot | `config:store` persists the merged effective snapshot to a dedicated `config.yml`; the local file is the L2 layer |
| Extensible surface | A service registers its own settings struct + apply hook via `register_config_settings`, optionally with `defaults` (L0) and `bootstrap` (L1) |
| Security by construction | Restart-required and secret fields are *unrepresentable* in the payload; optional HMAC signing |

The feature is opt-in and disabled by default. It is pure-Python (stdlib
`hmac`/`hashlib`/`os` plus the existing `msgspec`); no new dependencies.

## File layout

The framework resolves **one** config directory (`conf_dir`) shared by every
service it runs. Each worker's snapshot file is the `config_file` field,
resolved under `conf_dir`; it defaults to `"config.yml"`, so the snapshot is
`<conf_dir>/config.yml`. A service may namespace its snapshot into a
subdirectory by setting `config_file` to a relative path — modbus uses
`"modbus/config.yml"` — so multiple services sharing one `conf_dir` do not
collide.

The snapshot holds the framework's `core:` settings plus a `services:` map of
per-service sections:

```
<conf_dir>/
├── config.yml              # default snapshot: core: + services: (shared root)
└── modbus/
    ├── config.yml          # namespaced snapshot: core: + services.modbus
    └── modbus.yml          # service-owned bootstrap (flat; framework never writes)
```

A **service-owned bootstrap** file (e.g. modbus's `modbus.yml`) is distinct from
this framework snapshot: it uses the service's own flat schema, is written once
on first run, and is never rewritten by the framework.

## Enabling

Set `remote_config_enabled=True` on the config struct (it is a
`TaskProcessorConfig` field, inherited by both worker configs):

```python
from scietex.service import TaskProcessor, TaskProcessorConfig

processor = TaskProcessor(TaskProcessorConfig(service_name="svc", remote_config_enabled=True))
```

When the switch is off (the default), the three `config:*` handlers are not
registered, so a `config:*` task is answered with the permanent "No handler
found" result and no startup read occurs.

### Config surface

All fields introduced by the feature, across the three config structs:

| Config class | Field | Type | Default | Bounds | Meaning |
|---|---|---|---|---|---|
| `TaskProcessorConfig` | `remote_config_enabled` | `bool` | `False` | — | Opt-in master switch. `False` ⇒ the three `config:*` handlers are not registered (a `config:*` task is answered "No handler found"), no startup read |
| `TaskProcessorConfig` | `config_file` | `str` | `"config.yml"` | — | Filename of the local reloadable-snapshot file, resolved under `conf_dir` |
| `TaskProcessorConfig` | `config_signing_key` | `str \| None` | `None` | — | HMAC key for envelope authenticity. `None` disables signature enforcement. Runtime-only secret, never read from the remote payload |
| `TaskProcessorConfig` | `config_startup_timeout` | `float \| None` | `None` (→ `2.0`) | `[0.0, 60.0]` | Bounded wait in seconds for the MQTT retained snapshot at startup; ignored by Valkey |
| `ValkeyWorkerConfig` | `config_key` | `str` | `"scietex:{service}:config"` | — | Durable desired-state key; `{service}` substituted at construction |
| `MqttWorkerConfig` | `config_topic` | `str` | `"scietex/{service}/config"` | — | Retained desired-state topic; `{service}` substituted at construction |
| `MqttWorkerConfig` | `config_qos` | `int` | `1` | `[0, 2]` | QoS for the config-topic subscription and publish |
| `MqttWorkerConfig` | `config_ttl` | `int \| None` | `86400` | `[1, 2592000]` | MQTT 5 message-expiry interval in seconds for the retained config; `None` disables expiry |

`config_startup_timeout` follows the usual `None`-resolves-to-default pattern:
`None` means `DEFAULT_CONFIG_STARTUP_TIMEOUT` (`2.0`), resolved at read time.
Only MQTT uses it — Valkey reads the key synchronously on `GET`, so it has no
bounded wait.

## Delivery Channels

The two transports are kept **semantically parallel**: one durable desired-state
location read at startup and on `config:apply`; commands travel as tasks.

| | Valkey | MQTT |
|---|---|---|
| Desired-state location | durable key `scietex:{service}:config` | retained topic `scietex/{service}/config` |
| Read primitive | `GET` (awaited, live) | subscription snapshot (retained message delivered on SUBACK) |
| Write primitive (operator) | `SET` | retained `PUBLISH` |
| Command channel | task stream `scietex:{service}:tasks` | task topic `scietex/{service}/tasks` |
| Command task types | `config:apply` / `config:store` / `config:show` | same |

Key names use transport-native separators, matching the existing scheme:
colon-separated for Valkey, slash-separated for MQTT.

> **Note:** the three `config:*` commands are addressed by channel. A command
> published to the **broadcast** control channel (`scietex:{service}:control`
> / `scietex/{service}/control`) reaches every worker; a command published to a
> worker's **directed** control channel reaches only that worker. Submitters
> address commands through the `ControlPublisher` protocol
> (`direct`/`broadcast`/`resolve_owner`). The durable desired state is shared, so
> a worker that misses a broadcast still converges on its own next
> startup/reload. Control is never retried, and a command published while a
> worker is down is skipped rather than replayed.

### Read semantics (`ConfigSource.load`)

`load()` is **best-effort current desired state, without waiting for delivery**:
it returns the last state the source knows, or `None`. Freshness is
transport-inherent — Valkey `GET`s the broker live on every call; MQTT returns
the most recent snapshot recorded from the retained topic. Consequently a
payload-less `config:apply` on MQTT applies the last *received* config, not a
fresh broker read. Operators needing guaranteed freshness should pass the
envelope inline in `config:apply` instead of relying on the source re-read.

The MQTT bounded startup wait (`config_startup_timeout`) is exposed as the
transport-specific `wait_for_snapshot(timeout)`; it is intentionally outside the
`ConfigSource` protocol because the apply path must never block on delivery.

### Why not PubSub (Valkey)

The Valkey source of truth is the **durable key, not the PubSub control
channel**. Valkey/Redis PubSub is at-most-once and not persisted — it is a
"something changed" notice, never a store — so it cannot answer "what is the
desired state now?" on startup or reconnect. The key is durable and `GET`-able
on demand. PubSub remains a fire-and-forget control channel for other purposes.

### Why one retained topic (MQTT)

MQTT has no cross-topic atomicity, so the whole envelope lives in a single
retained message. Retained = state, delivered to every subscriber on SUBACK;
commands are non-retained and travel as tasks (the retained message cannot be
`GET`-ed, so the worker records the latest payload received on the topic as an
in-memory snapshot). The retained marker carries an MQTT 5 message-expiry
(`config_ttl`) so a stale marker ages out of the broker instead of persisting
forever, mirroring `status_ttl`.

## Config Envelope

The on-the-wire unit is a versioned `ConfigEnvelope` (`scietex.service.config_reload`):

| Field | Type | Default | Meaning |
|---|---|---|---|
| `version` | `int` | `1` | Wire-format version |
| `revision` | `int` | `0` | Monotonic counter; replay protection |
| `hash` | `str` | `""` | `sha256(settings).hexdigest()`; integrity only |
| `signature` | `str` | `""` | Hex HMAC-SHA256 over the 8-byte big-endian `revision` + `settings`; empty unless signing is enabled |
| `settings` | `bytes` | `b""` | msgpack-encoded `ConfigSections` |
| `created_at` | `datetime \| None` | `None` | Informational creation timestamp |

`settings` decodes into `ConfigSections`, a named-section map:

| Field | Type | Meaning |
|---|---|---|
| `core` | `ReloadableSettings \| None` | The reloadable core snapshot; `None` when the producer delivers only service sections (the core is then left untouched) |
| `services` | `dict[str, bytes]` | Registered section name → msgpack(registered struct) |

`ReloadableSettings` is the complete snapshot of the hot-reloadable core
fields — all eight required, `forbid_unknown_fields=True`:

```python
class ReloadableSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    max_concurrent_tasks: int
    task_manager_sleep_time: float
    task_queue_manager_sleep_time: float
    task_handler_start_timeout: float
    task_handler_stop_timeout: float
    task_timeout: float
    task_queue_fetch_timeout: float
    task_cancellation_timeout: float
```

### Layered patches (v6)

`ReloadableSettings` is the **effective** runtime snapshot — every field is
concrete, with `None`-means-default and `auto_tune` already resolved. It is the
terminal resolver's output, not the wire type.

The wire type is a **patch dict**. Both `ConfigSections` (the L3 remote patch)
and `DeclarativeSections` (the L2 local-file patch) carry dicts:

```python
class ConfigSections(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    core: dict | None = None
    services: dict[str, dict] = msgspec.field(default_factory=dict)


class DeclarativeSections(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    core: dict | None = None
    services: dict[str, dict] = msgspec.field(default_factory=dict)
```

The config model is four layers, each a patch merged field-by-field onto the one
below:

| Layer | Source | Applied by |
|---|---|---|
| L0 | constructor defaults (concrete base struct) | `register_section(defaults=...)` |
| L1 | service bootstrap provider | `register_section(bootstrap=...)` + `seed_bootstrap()` |
| L2 | namespaced `config.yml` | `apply_declarative_sections` |
| L3 | remote source | `apply_envelope` |

The **three-state rule** governs every key at every nesting level:

- **absent** — inherit the value from the layer below;
- **`null`** — clear: fall back to the L0 default (and, for
  `max_concurrent_tasks`, the auto-tune branch);
- **value** — set.

`None` therefore means exactly one thing — *clear*. "Inherit" is *absence*, not
`None`. Each layer's patch is **replaced wholesale** on apply, so a producer must
send the complete desired patch for its layer each time.

Both `ConfigSections.core` and `DeclarativeSections.core` are optional. A
producer that does not track a worker's core settings — the API, for example —
delivers `core=None` and only the service sections; the worker then keeps its
current core settings while still running the section hooks and advancing the
revision/hash bookkeeping so the envelope is not re-applied. Only `core` may be
`None`; `services` is always a map.

`config:show`'s `declarative_settings` is the merged **patch** view — the keys
any layer explicitly set, with absence meaning inherit. It is not a
`None`-preserving settings object.

### Validation, not a complete snapshot

A core patch is **partial by design**: only the keys a layer sets are merged, and
absent keys inherit. The merged core is validated against the `RELOADABLE_FIELDS`
allowlist before any section hook runs, so a payload naming `queue_size`,
`valkey_config`, or any connection parameter is **rejected** with
`INVALID_CONFIG`, not merely ignored — restart-required fields are
*unrepresentable*, not dropped. The merged core is then resolved through the
existing `resolve_reloadable_settings` path (it is **not** converted via
`msgspec.convert`, because `ReloadableSettings` has eight required fields).

### Encoding

- **Wire** (task payload, Valkey key value, MQTT retained payload): **msgpack**,
  consistent with the task-envelope wire format, compact and typed.
- **Disk** (`config.yml`): **YAML**, consistent with `valkey.yml`/`mqtt.yml`,
  human-editable, same `msgspec.yaml` codec.
- **Never** pickle (untrusted input). JSON would lose typed-struct validation;
  YAML on the wire would be verbose and binary-unfriendly.

Envelope encode/decode is centralized in the helpers
`encode_config_envelope(sections, *, revision, signing_key=None,
created_at=None) -> bytes`, `decode_config_envelope(payload) ->
ConfigEnvelope | None`, and `peek_config_envelope_version(payload) -> int |
None` (mirroring `task_handler.wire`).

## Reloadable Allowlist

Only core `TaskProcessor` fields that are read live, or can be safely
re-resolved, are reloadable. The worker resolves the eight reloadable fields
once into a single `self._effective: ReloadableSettings` snapshot at
construction and on every apply; the live-read properties and the hot loops
read that snapshot.

| Field | Reloadable | Mechanism |
|---|---|---|
| `max_concurrent_tasks` | yes | `_effective` snapshot |
| `task_manager_sleep_time` | yes | `_effective` snapshot |
| `task_queue_manager_sleep_time` | yes | `_effective` snapshot |
| `task_handler_start_timeout` | yes | `_effective` snapshot |
| `task_handler_stop_timeout` | yes | `_effective` snapshot |
| `task_timeout` | yes | `_effective` snapshot |
| `task_queue_fetch_timeout` | yes | `_effective` snapshot |
| `task_cancellation_timeout` | yes | `_effective` snapshot |

The apply path overlays the merged core patch onto a copy of `self._config` and
re-resolves `self._effective` together with no `await` between them, so an
observer sees old-or-new, never a mix. `_current_reloadable_settings()` returns
`self._effective`.

**Restart-required** (cannot be expressed in a remote payload): `queue_size`
(the `asyncio.Queue` is sized at construction), `auto_tune`, the `WorkerConfig`
identity/cadence fields (`service_name`, `version`, `conf_dir`, `logging_level`,
`heartbeat_interval`, `watchdog_interval`, `logger_handler_timeout`,
`manager_*`), every transport field (the `ValkeyTransport`/`MqttTransport`,
tracking store, lease manager, and durable inbox capture their config at
construction), and the connection configs `valkey_config`/`mqtt_config`
(credentials and TLS material).

## Commands

Three task types, registered exactly like the built-in `task:cancel` handler
(but only when `remote_config_enabled=True`), with async callbacks injected at
construction:

```python
CONFIG_APPLY_TASK_NAME: str = "config:apply"
CONFIG_STORE_TASK_NAME: str = "config:store"
CONFIG_SHOW_TASK_NAME: str = "config:show"
```

Replies ride the normal task result payload (`TaskResult.payload`, msgpack),
exactly as `CancelTaskResponse` does. No reply topic/channel is introduced.

### `config:apply`

| Request field | Type | Default | Meaning |
|---|---|---|---|
| `payload` | `bytes \| None` | `None` | Inline envelope; `None` re-reads the source of truth |
| `persist` | `bool` | `False` | Also write `config.yml` after a successful apply |

| Response field | Type | Meaning |
|---|---|---|
| `applied` | `bool` | Whether the envelope became effective |
| `revision` / `hash` | `int` / `str` | Identity of the applied envelope |
| `changed` | `list[str]` | Core field names whose value changed |
| `restart_required` | `list[str]` | Field names present in the concrete config but not reloadable |
| `error` | `str` | Error description (empty on success) |

`payload` present ⇒ apply that envelope inline (source `"inline"`); absent ⇒
`await source.load()` and apply the source of truth (source `"remote"`).
`persist=True` additionally writes `config.yml` after a successful apply (a
"fetch, apply, remember" one-shot).

### `config:store`

| Request field | Type | Default | Meaning |
|---|---|---|---|
| `target` | `Literal["disk", "remote", "both"]` | `"disk"` | Where to persist the effective config |

| Response field | Type | Meaning |
|---|---|---|
| `stored` | `bool` | Whether the config was written |
| `target` | `str` | The requested target |
| `path` | `str` | Destination path (empty for a remote source) |
| `revision` / `hash` | `int` / `str` | Identity of the stored config |
| `error` | `str` | Error description (empty on success) |

`disk` writes the merged **patch** view to `<conf_dir>/config.yml` (the L2
layer, preserving which keys were explicitly set and any explicit `null`
clears); `remote` publishes the **effective** snapshot back to the source
(Valkey `SET` / MQTT retained `PUBLISH`); `both` does both. Only the
**reloadable surface** is written — never connection credentials or TLS
material. The disk write is atomic (`os.replace` of a temp file in the same
directory).

### `config:show`

| Request field | Type | Default | Meaning |
|---|---|---|---|
| `include_restart_required` | `bool` | `True` | Whether to list the restart-required field names |

| Response field | Type | Meaning |
|---|---|---|
| `settings` | `bytes` | msgpack-encoded effective `ConfigSections` (resolved core + merged effective service structs); never secrets |
| `declarative_settings` | `bytes` | msgpack-encoded merged patch view (`DeclarativeSections`); explicitly-set keys only, absence = inherit |
| `revision` / `hash` | `int` / `str` | Identity of the effective config |
| `source` | `"default"\|"file"\|"remote"\|"inline"` | Where the effective config came from |
| `restart_required_fields` | `list[str]` | Restart-required field names (when requested) |
| `error` / `error_code` | `str` | Empty on success |

Secrets are never emitted and there is no opt-out: the reloadable surface
contains no credentials, TLS material, callables, or connection parameters by
construction. An operator who needs to audit connection config reads it from
the config directory, not over the task channel.

### Error taxonomy and retryability

Stable error codes surface in `TaskResult.error_code` (and in the reply
structs where applicable):

| Code | Meaning | Retryable |
|---|---|---|
| `INVALID_CONFIG_PAYLOAD` | Malformed request or envelope payload | no |
| `INVALID_CONFIG` | Validation failure (out-of-range, wrong schema version, bad section) | no |
| `UNKNOWN_CONFIG_SECTION` | Envelope names an unregistered service section | no |
| `HASH_MISMATCH` | `sha256(settings)` != envelope `hash` | no |
| `BAD_SIGNATURE` | HMAC mismatch with signing enabled | no |
| `STALE_CONFIG` | Revision not newer than the applied one (with a different hash) | no |
| `CONFIG_SOURCE_NOT_CONFIGURED` | No transport source is attached (e.g. a bare `TaskProcessor`) | no |
| `CONFIG_SOURCE_UNAVAILABLE` | Attached source unreachable on read or write | **yes** |
| `CONFIG_STORE_FAILED` | Local `config.yml` write failed | no |
| `REMOTE_CONFIG_DISABLED` | Master switch off — only reachable via a direct `ConfigManager.show_config` call; a `config:*` task on a disabled worker is answered "No handler found" | no |

`RETRYABLE_ERROR_CODES` is the single source of truth for the retryable set;
it currently contains only `CONFIG_SOURCE_UNAVAILABLE`, which opts into the
framework's single retry. Every other failure is permanent — a malformed or
invalid payload, or a not-configured source, must not create a requeue loop.

## Startup Behavior

The worker applies config at startup in a fixed precedence order:

```
constructor defaults (L0)  <  service bootstrap (L1)  <  config.yml (L2)  <  remote source (L3)
```

1. The constructor defaults are the L0 base; the service bootstrap provider
   supplies L1. `seed_bootstrap()` resolves and caches L0+L1 for every
   registered section (no hooks run); services read the merged result via
   `current_settings(name)`.
2. `config.yml` (if present) is applied as a trusted, unsigned **L2 patch**
   (revision `1`, below any remote revision); signature verification is waived
   for this local file only — remote and inline envelopes are still verified.
   Because it is a patch, a `config.yml` written by `config:store` preserves
   which keys were explicitly set (and any explicit `null` clears) across a
   store→restart cycle instead of pinning the resolved values that were
   effective at store time.
3. The remote source is read and applied last as the L3 patch, so it stays
   authoritative when present.

After a successful remote apply, the effective config is persisted back to
`config.yml`, so the file becomes a durable mirror of the last-applied remote
config and is re-applied ahead of the remote read on the next start. This
persistence does not alter the precedence order — it only refreshes the local
fallback snapshot. It is best-effort: a write failure is logged and leaves
startup running (and any previous file intact). No file is written when the
remote source is unavailable or the apply is rejected.

This precedence is re-established on **every** run of the same worker instance:
each run starts from the constructor/default baseline (the run boundary resets
apply state via `ConfigReloader.reset()`), so the local file and the remote
source are re-applied from scratch rather than replaying the previous run's
revision.

**Valkey**: `ValkeyWorker.initialize()` connects, attaches a
`ValkeyConfigSource` on the operational client, applies the local snapshot,
then `GET`s the durable key and applies the remote envelope.

**MQTT**: the retained message arrives after SUBACK, so the worker subscribes
to the config topic during connect, then waits a bounded
`config_startup_timeout` (default `2.0` s) for the snapshot before applying it.
A broker with no retained config times out cleanly.

### Failure policy

| Startup condition | Action |
|---|---|
| Remote absent | log DEBUG; use local file / defaults; startup succeeds |
| Remote present, valid | apply; log INFO with revision/hash |
| Remote present, invalid / bad signature / unknown field / stale revision | log ERROR (stale: DEBUG); keep local/default; startup succeeds |
| Source unreachable (Valkey `GET` fails) | log WARNING; keep local/default; startup succeeds |

**Invalid remote config never fails startup.** Availability wins: the worker is
still safe on its local config, and a malformed override is an operator error
that is loudly logged, not a crash-loop.

## Extending with Custom Settings

A custom service extends the reloadable surface by declaring its own frozen
`msgspec.Struct` and registering it with the processor. The struct is the
concrete L0 base; an optional `bootstrap` provider supplies L1:

```python
import msgspec

from scietex.service.mqtt import MqttWorker, MqttWorkerConfig


class MyServiceSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    batch_size: int = 100
    upstream_url: str = ""


class MyWorker(MqttWorker):
    def __init__(self, config=None, *, client_factory=None):
        super().__init__(config, client_factory=client_factory)
        self._my_settings = None
        self.register_config_settings(
            "my_service",  # section name in the envelope
            MyServiceSettings,
            apply=self._apply_my_settings,  # Callable[[dict[str, object]], list[str]]
            defaults=MyServiceSettings(),  # L0 concrete base
        )

    def _apply_my_settings(self, values: dict[str, object]) -> list[str]:
        # ``values`` is the merged patch; resolve it against the L0 base.
        self._my_settings = msgspec.convert(
            {**msgspec.to_builtins(MyServiceSettings()), **values},
            MyServiceSettings,
        )
        self.logger.info("Applied batch_size=%d", self._my_settings.batch_size)
        return list(values)
```

The envelope's `settings.services` map then carries
`{"my_service": {"batch_size": 200}}` (a patch dict) alongside `core` (or on its
own, when the producer delivers service sections only). On every apply:

- The core section, when present, is a partial patch validated against the
  `RELOADABLE_FIELDS` allowlist; a `core=None` envelope leaves the current core
  settings untouched.
- Each registered section's patch is merged onto its L0 base and L1 bootstrap,
  then converted through its registered struct (`forbid_unknown_fields` rejects
  a typo in a service field, not silently ignoring it).
- An unregistered section name is rejected (`UNKNOWN_CONFIG_SECTION`).
- Each section's `apply` hook runs **before** the core swap succeeds, in
  registration order; a raising hook aborts the whole apply and leaves the
  previous config in place.
- Registration is additive and idempotent per section name; re-registering
  the same name replaces the struct + hook + defaults + bootstrap.

This keeps the core transport-agnostic and validation strict. `examples/remote_config.py`
shows the full pattern end to end (a `DemoServiceSettings` section registered
under `"demo"`).

## Security Model

**What the library enforces:**

- **Allowlist by type.** Restart-required and secret fields are unrepresentable
  in `ReloadableSettings`/registered structs; `forbid_unknown_fields=True`
  rejects a payload that names them. A remote message cannot change
  credentials, TLS, connection endpoints, `queue_size`, or anything outside
  the allowlist.
- **Validate-before-swap.** Every candidate is constructed through the real
  config struct and its `__post_init__`/`validate_range` bounds; a bad value is
  rejected with no state change.
- **Replay protection.** Monotonic `revision` plus an idempotent equal-hash
  check: a lower revision (or an equal revision with a different hash) is
  rejected as stale.
- **Integrity.** `sha256` detects truncation/corruption (not authenticity).
- **Optional authenticity.** HMAC-SHA256 with `config_signing_key` rejects
  unsigned/tampered envelopes when a key is configured. The key is
  runtime-supplied (constructor/environment), never read from the remote
  payload.
- **No code execution.** msgpack/YAML data only, never pickle; YAML is loaded
  with `msgspec.yaml.decode(..., type=...)`.

**What the library cannot enforce (operator responsibility):**

- **Broker ACLs are the primary defense.** Anyone able to write
  `scietex:{service}:config`, publish retained to `scietex/{service}/config`,
  or submit a `config:*` task can influence worker behaviour. The library
  cannot authenticate the broker user — restrict those writes to trusted
  operators at the broker.
- **No confidentiality on the wire.** TLS and broker ACLs are the operator's
  responsibility; `config:apply` inputs are not encrypted by the library.
- **No enforcement of restart for restart-required changes**, beyond
  preventing them from being expressed remotely. Editing `valkey.yml` does not
  hot-reload.

## Example

`examples/remote_config.py` is a runnable `MqttWorker` subclass that registers
a `DemoServiceSettings` section and drives the full lifecycle with an operator
client: publish a retained envelope, then `config:apply` (re-read the source),
`config:show` (inspect over the wire), and `config:store` (persist to
`config.yml` in a temp directory). It requires a running MQTT 5 broker and the
`mqtt` extra:

```bash
python -m examples.remote_config --host 127.0.0.1
```

## Operator Cheat-Sheet

The config envelope and the command replies are msgpack-encoded, so producing
them by hand on the shell is impractical. Encode an envelope in Python (as the
example does) or use a small script, then publish it:

```python
from scietex.service.config_reload import ConfigSections, encode_config_envelope

# A core patch is partial: only the keys you set are merged; absent keys
# inherit, and an explicit None clears the key back to its default.
sections = ConfigSections(
    core={"max_concurrent_tasks": 8, "task_timeout": 3.0},
    services={"my_service": {"batch_size": 200}},
)
envelope = encode_config_envelope(sections, revision=1)
```

### MQTT (`mosquitto_sub` / `mosquitto_pub`)

```bash
# Watch the retained desired-state envelope (msgpack; decode with
# msgspec.msgpack.decode(payload, type=ConfigEnvelope)).
mosquitto_sub -h localhost -p 1883 -V mqttv5 -t 'scietex/worker/config' -v

# Publish a retained envelope (encode it in Python first, then publish the
# bytes; qos=1 matches the worker's config_qos default).
mosquitto_pub -h localhost -p 1883 -V mqttv5 -t 'scietex/worker/config' \
  -q 1 -r -f envelope.bin

# Watch every config command's terminal status (retained, per task); the
# command reply is the TaskStatus.result payload (msgpack Config*Response).
mosquitto_sub -h localhost -p 1883 -V mqttv5 -t 'scietex/worker/tasks/+/status' -v
```

### Valkey (`valkey-cli`)

```bash
# Read the desired-state envelope (binary-safe; decode with msgspec).
valkey-cli GET scietex:worker:config

# Set a new envelope (raw msgpack bytes from a file).
valkey-cli -x SET scietex:worker:config < envelope.bin

# Read a config command's terminal tracking record.
valkey-cli GET scietex:worker:task:<task_id>
```
