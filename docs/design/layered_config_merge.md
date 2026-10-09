# Layered (Overlay) Configuration Merge for `scietex.service` — v6

**Status:** proposed
**Target release:** v6.0.0 (major)
**Supersedes:** the v5 section-granularity apply contract in
[`remote_config.md`](remote_config.md). v5's wire contract is frozen and is
**not** modified by this design; v6 introduces a new envelope version and a new
patch representation.

**Motivation:** today configuration precedence is last-writer-wins at *section*
granularity. A higher layer must repeat every field of a section or the omitted
fields silently fall back to the constructor defaults. A service that stores four
`ModbusSerialSettings` values in `modbus.yml` and wants to change only
`serial.baudrate` remotely has to re-state `serial.port`, `serial.parity`,
`serial.stopbits`, and `serial.timeout` as well, or lose them. This document
specifies a generic, reflection-based overlay merge so a layer patches individual
fields, with an unambiguous three-state model: **absent = inherit, `null` =
clear, value = set**.

---

## 1. Motivation and problem statement

Consider the modbus service. Its on-disk state today is:

```
<conf_dir>/modbus/
├── modbus.yml      # service bootstrap (L1, concrete, service-owned)
└── config.yml      # framework namespaced snapshot (L2, declarative)
```

A bootstrap `modbus/modbus.yml`:

```yaml
serial:
  port: /dev/ttyUSB0
  baudrate: 9600
  parity: N
  stopbits: 1
  timeout: 1.0
host: 0.0.0.0
port: 502
default_framer: RTU
devices: {}
allow_unknown_devices: false
bus_retries: 0
```

A framework snapshot `modbus/config.yml` that only wants to bump the baudrate and
add one device:

```yaml
core: null
services:
  modbus: <msgpack of the modbus section>
```

Under the current section-granularity apply, the `modbus` section decoded from
`config.yml` is constructed through `ModbusServiceSettings`. Every field the
snapshot does not name is filled by the **struct defaults**
(`ModbusSerialSettings.baudrate = DEFAULT_BAUDRATE`, `serial.port =
"/dev/ttyUSB0"`, ...), which `ModbusWorker._apply_modbus_settings`
(`modbus_worker.py:119-135`) then stores wholesale by
`self._settings = settings`. The operator's `/dev/ttyUSB0` and everything else
from `modbus.yml` are discarded unless repeated.

The same holds for the remote layer (L3) and for the core reloadable block: a
remote producer that only wants to change `task_timeout` must send all eight
`ReloadableSettings` fields, because the remote core is a complete snapshot
(`config_reload.py:58-73`).

The pain is not "a value is wrong" — it is that an override cannot express
*partial* intent. Any field the override omits is reset to the constructor
default, so operator-tuned values are lost on every partial update.

### 1.1 Why v6 and not a v5 patch

The v5 declarative contract encodes "inherit" as `None` (`DeclarativeSettings`,
`config_reload.py:76-92`). A field-level patch needs three states — inherit, set,
clear — and `None` can only carry one of them. Overloading `None` at two nesting
levels (section absent vs field inherit) is the source of the `core=None`
ambiguity that this design removes. Rather than overload `None` further, v6
redefines the field-level meaning of `None` to **clear** and represents
**inherit** by *absence*. This is a breaking change to the declarative contract,
so it ships as a major version and the v5 wire contract is left untouched.

## 2. Layer model

Four layers, from lowest to highest precedence. A layer is a **patch**: a
msgpack map (plain dict) in which each key is one of three states.

```
L0: constructor defaults                          (concrete base, terminal)
L1: <subdir>/<service>.yml (service bootstrap)    (patch)
L2: <subdir>/config.yml (namespaced snapshot)     (patch)
L3: remote source (Valkey key / MQTT retained)    (patch)
```

### 2.1 The three-state patch rule

For every key in a patch map:

| State | Encoding | Meaning |
|---|---|---|
| **inherit** | key absent from the map | no opinion; take the layer below |
| **set** | key present with a value | use this value |
| **clear** | key present with explicit `null` | drop the key; fall back to the layer below |

This is the JSON Merge Patch model (RFC 7396). It is unambiguous because
"inherit" is represented by *absence*, which is not a value at all, so it can
never collide with a legitimate value or with the explicit `null` that means
clear.

**Resolution rule.** Merge the layers bottom-up: start from the L0 base dict and
apply L1, then L2, then L3, each as a patch. A key present with `null` at a
higher layer removes the key from the accumulated result, so the field falls back
to whatever the lower layers (ultimately L0) provided. A key absent at a higher
layer leaves the accumulated value untouched. A key present with a value
overwrites it.

**Terminal.** L0 is a concrete base dict, so every field has a value after the
merge unless a higher layer explicitly cleared it *and* no lower layer set it.
Clearing a field that only L0 provided removes it from the merged dict; the
final `msgspec.convert` then fills it from the struct's own default. To keep the
declarative contract ("clear = library default") exact, L0's base dict is built
from the same concrete defaults the struct would otherwise carry, so clearing a
field resolves to the library default by construction (§4.2).

The shared root `<conf_dir>/config.yml` is **not** a layer. It is an orphan file
on the host that no service reads — every real service overrides `config_file` to
a namespaced path (`config_reload.py:463-467`; modbus sets
`config_file="modbus/config.yml"` in `run_worker.py:55`). Removing that orphan is
a host cleanup, not a code change, and this design introduces no shared-root
layer.

**Worked example** — resolve `serial.port` and `serial.baudrate`:

| Layer | `serial.port` | `serial.baudrate` |
|---|---|---|
| L0 base | `/dev/ttyUSB0` | `9600` (`DEFAULT_BAUDRATE`) |
| L1 `modbus.yml` | `/dev/ttyUSB1` | `19200` |
| L2 `config.yml` | *(absent -> inherit L1)* | *(absent -> inherit L1)* |
| L3 remote | `/dev/ttyUSB1` (set) | *(absent -> inherit L1)* |
| **resolved** | `/dev/ttyUSB1` | `19200` |

A producer that wants to touch only `serial.port` sends `{serial: {port:
/dev/ttyUSB2}}`; `baudrate` still comes from L1. A producer that wants to reset
`serial.port` to the library default sends `{serial: {port: null}}`.

### 2.2 Section-level presence

At the section level, a patch map either contains a section key or it does not:

- **section key absent** — this apply carries no patch for that section; the
  section's stored layers are left untouched.
- **section key present with a map** — the map is that section's patch for this
  layer.

There is no third state at the section level. The `core` field is typed
`dict | None`, so `core: null` decodes to `None` and is treated as absent (no
core patch). The `services` field is typed `dict[str, dict]`, so a service
section cannot be `null` — a producer that wants "no change" for a service simply
omits the key. (An explicit `null` for a service entry is a decode error, which
is the correct outcome: it is not a meaningful patch.)

This dissolves the v5 `core=None` ambiguity. In v5, `core=None` meant "leave core
untouched" and was special-cased in three places
(`config_reload.py:474`, `:575`, `:767`). In v6 there is no special case: the
absence of the `core` key *is* "no core patch", and the same rule governs every
service section. The API's service-only envelope (which omits core) keeps its
exact v5 meaning without any branch.

## 3. Merge algorithm

The merge is generic reflection over msgspec structs, operating on plain dicts.
No per-service merge code exists; a service only declares its struct schema and
L0 defaults (§4). The engine lives in a new module
`src/scietex/service/config_merge.py`.

### 3.1 Why the patch layer is a plain dict

The three-state rule cannot be expressed with msgspec structs. Verified against
msgspec 0.20.0 (the pinned version):

- A custom sentinel type cannot be a union member alongside a real type:
  `int | None | _MissingType` is rejected ("Type unions containing a custom type
  may not contain any additional types other than `None`").
- An `Enum`/`Literal` sentinel collides with `str`/`int` fields ("Type unions may
  not contain more than one str-like/int-like type").
- `omit_defaults=True` omits fields equal to their default, so it cannot
  distinguish an explicit `null` from an omitted key when the default is `None`.

A plain dict has the tri-state natively: `key in d` is set-or-clear, `d[key] is
None` is clear, `key not in d` is inherit. The wire already carries msgpack maps,
so a patch is simply the decoded map. Validation is deferred to the end of the
merge, where the merged dict is converted through the real struct type (§3.4).

### 3.2 Pseudocode

```
def merge(base: dict, patch: dict) -> dict:
    """RFC 7396 merge of `patch` onto `base`. Returns a new dict."""
    out = dict(base)
    for key, value in patch.items():
        if value is None:                       # explicit null -> clear
            out.pop(key, None)
        elif isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = merge(out[key], value)   # recurse into nested maps
        else:
            out[key] = value                    # set (scalar, list, or new map)
    return out

def resolve_section(struct_type, layers, defaults) -> Struct:
    # layers: [L3, L2, L1]  (each dict | None, high -> low)
    # defaults: the concrete L0 base dict for this section
    merged = dict(defaults)
    for layer in reversed(layers):              # low -> high: L1, L2, L3
        if layer:
            merged = merge(merged, layer)
    return msgspec.convert(merged, struct_type)  # validate + coerce
```

`resolve_section` is the only public entry point. `layers` is ordered high-to-low
for readability at the call site; the loop reverses it to apply low-to-high.

### 3.3 Field-type rules

The merge is **schema-agnostic at merge time** — it recurses whenever both the
accumulated value and the patch value are dicts. The schema only matters at the
final `msgspec.convert`, which knows which dicts are nested structs and which are
`dict[K, V]` maps. This keeps the engine simple and correct for both cases:

- **Nested struct** — the patch value is a dict, the accumulated value is a dict
  (from L0 or a lower layer), so the merge recurses field by field. Patching
  `serial.baudrate` keeps `serial.port` from a lower layer.
- **`dict[K, V]` map** — the patch value is a dict, the accumulated value is a
  dict, so the merge recurses per key. The union of keys is kept; a key mapped to
  `null` is removed (the tombstone is now just the general clear rule, not a
  special case); a key mapped to a value is set. A struct-valued map entry is
  itself a dict and recurses.
- **List / scalar** — the patch value is not a dict (or the accumulated value is
  not a dict), so it replaces. An explicit `[]` clears a list; an explicit
  `""`/`0`/`false` overrides a lower value. *Justification:* value-based list
  merge is order- and identity-sensitive (is `[a, b]` patched by `[b]` a removal
  or an append?), and index-based merge silently binds to position. Last writer
  wins is predictable and matches the existing struct overlay semantics.

**Clearing a scalar.** `{serial: {port: null}}` removes `port` from the merged
dict; `msgspec.convert` then fills it from the struct default. Because L0's base
dict carries the same value as the struct default (§4.2), the result is the
library default. This is the case v5 could not express (§10.1 in the v5 draft).

### 3.4 Validation: validate-before-swap is preserved

The merge is a pure function over plain dicts. Validation happens exactly once,
at the end, in `msgspec.convert(merged, struct_type)`. Verified against msgspec
0.20.0:

- `msgspec.convert` **rejects unknown fields at every nesting level** with
  `forbid_unknown_fields=True` (e.g. `Object contains unknown field 'baudrat' -
  at '$.serial'`). Constructing the struct with `Struct(**merged)` does **not**
  validate nested dicts and must not be used.
- `msgspec.convert` coerces nested dicts into nested structs and validates
  `__post_init__`/`validate_range` on the result.

A value from any layer that is out of range raises `msgspec.ValidationError`
while building the candidate, before any hook mutates state, so the whole apply
aborts with no state change. The section hook is invoked only with a fully-built,
validated candidate (§3.5).

For service structs, whose validation is delegated to `to_gateway_config`, the
invariant holds because the hook is the validation point and runs before any
swap; a raising hook returns `INVALID_CONFIG` and leaves the previous config in
place (`config_reload.py:523-536`).

### 3.5 Where the merge runs

The merge runs **resolve-on-apply from stored layer patches**, not as an
accumulating overlay. Rationale: an overlay onto the previous *merged* value
cannot be undone. If L2 supplies `port: 5020` and is later removed, an overlay
chain has no way to drop back to L1's `502`. Storing each layer's patch and
re-merging from L0 on every change is the only way "L2 absent -> inherit L1"
stays true.

The reloader stores, per registered section, the patch for every layer that has
been seen:

```
_section_layers: dict[str, dict[str, dict | None]]   # name -> {L1, L2, L3}
```

- **L1** is seeded once at startup from the registered `bootstrap` provider
  (§4.4).
- **L2** is replaced on every `apply_declarative_sections` from
  `sections.services[name]`.
- **L3** is replaced on every `apply_envelope` from `sections.services[name]`.

Any apply that touches a section re-merges that section from L0 upward and
invokes its hook once with the merged effective struct. The core block takes the
same path with its own layers (§5).

## 4. Schema changes

### 4.1 Service structs stay concrete

Unlike the v5 draft, service structs are **not** made optional. They keep their
concrete field types and concrete defaults, because the patch layer is a dict and
never a struct. `ModbusServiceSettings`, `ModbusSerialSettings`,
`ModbusDeviceSettings`, and `LogAggregatorSettings` are unchanged in shape.

This is a significant simplification over the v5 draft: no field becomes
`T | None`, no `forbid_unknown_fields` interaction changes, and the struct
defaults remain the single source of truth for the library defaults.

### 4.2 L0 defaults

`register_config_settings` gains a `defaults` argument: the concrete L0 instance
for the section. The engine converts it to a base dict with
`msgspec.to_builtins(defaults)` and merges the layers onto it.

```python
MODBUS_SETTINGS_DEFAULTS = ModbusServiceSettings(
    serial=ModbusSerialSettings(
        port="/dev/ttyUSB0",
        baudrate=DEFAULT_BAUDRATE,
        bytesize=DEFAULT_BYTESIZE,
        parity=DEFAULT_PARITY,
        stopbits=DEFAULT_STOPBITS,
        timeout=DEFAULT_TIMEOUT,
    ),
    host="0.0.0.0",
    port=502,
    default_framer="RTU",
    devices={},
    allow_unknown_devices=False,
    bus_retries=0,
)
```

Because the L0 base dict is built from the same values the struct carries as
defaults, clearing a field at a higher layer resolves to the library default by
construction. A section that registers no `defaults` uses `struct_type()` as its
L0 base, which is equivalent when the struct defaults are the intended library
defaults.

### 4.3 No separate patch struct

The v5 draft considered a parallel `...Patch` struct per service. v6 does not
need one: the patch is a plain dict, so there is exactly one struct per service
(the concrete one) and one L0 instance. This removes the schema-doubling
trade-off entirely.

### 4.4 Bootstrap provider

The L1 patch is supplied by the service. `register_config_settings` gains an
optional `bootstrap: Callable[[], dict | None]`:

```python
self.register_config_settings(
    MODBUS_SECTION,
    ModbusServiceSettings,
    apply=self._apply_modbus_settings,
    defaults=MODBUS_SETTINGS_DEFAULTS,
    bootstrap=lambda: read_modbus_config(self.conf_dir),
)
```

`read_modbus_config` (`config.py:88-143`) already exists and already creates the
default file on first run. In v6 it returns a **patch dict** (the parsed YAML
map), not a struct. The generated default file is written from the L0 defaults
instance (concrete) so it stays a human-useful example; reading it back yields a
patch whose values resolve to themselves.

### 4.5 `to_gateway_config` and the resolved guarantee

For a **service section**, the merge engine returns a fully-validated instance of
the concrete struct type via `msgspec.convert`, so every field is concrete —
there is no `None`-field case to guard. `to_gateway_config`
(`config.py:146-188`) is unchanged and needs no new exception contract. The v5
draft's `None`-guard and its exception-contract question are moot in v6.

The **core** block is the exception: it is not converted via `msgspec.convert`
(its struct has required fields) but routed through `_overlay_reloadable` with
`None`→resolver (§5).

## 5. Core section merge

The core reloadable block is a built-in section with the same layer walk.

| Layer | Core payload | Type |
|---|---|---|
| L0 | constructor `TaskProcessorConfig` reloadable fields | concrete base dict |
| L1 | — (see below) | — |
| L2 | `DeclarativeSections.core` | patch dict |
| L3 | `ConfigSections.core` | patch dict |

**L1 carries no core.** The bootstrap file is service-owned (`modbus.yml`,
`log_aggregator.yml`) and has a flat service schema; it has no `core:` key and
the framework never reads it as one. Core therefore participates in L0, L2, and
L3 only.

**L2 and L3 core are both patch dicts.** In v5, L3 core was a complete
`ReloadableSettings` snapshot and L2 core was a `DeclarativeSettings`. In v6 both
are patch dicts with the same three-state rule, so a remote producer can send a
partial core (`{task_timeout: 30}`) and inherit the rest. This is the
expressiveness the v5 draft deferred (§10.3 in the v5 draft); v6 delivers it
because the wire format changed anyway.

**`core` absent means no core patch.** A producer that omits the `core` key
leaves the stored core layers untouched — the v5 `core=None` semantics, now
expressed by absence rather than a special-cased `None` (§2.2).

**Core key allowlist is mandatory.** `TaskProcessorConfig` does **not** set
`forbid_unknown_fields` (verified: `forbid_unknown_fields=False`), so a core
patch carrying a valid-but-restart-required field (`queue_size`,
`valkey_config`, ...) would pass `msgspec.convert` and reach the overlay. The
core patch must therefore be validated against `RELOADABLE_FIELDS`
(`config_reload.py:42-53`) at stage time: any key outside the allowlist is
rejected with `INVALID_CONFIG`. This is a security control, not a defensive
check — it is what keeps restart-required and secret fields unrepresentable
(§8).

**Merge and terminal resolution.** The merged core is a dict, but it is **not**
converted via `msgspec.convert` into `ReloadableSettings`: that struct has eight
**required** fields (verified: no defaults), so a merged dict with a cleared
field cannot construct it. Instead the merged core dict is applied through the
existing `_overlay_reloadable` path (`task_processor.py:387-391`), which overlays
the present values onto a copy of `_config` and lets `resolve_reloadable_settings`
(`config.py:380-444`) fill every `None` field with its `DEFAULT_*` constant and
the `auto_tune` branch. The core terminal is therefore resolved by the existing
resolver, not by `msgspec.convert`.

**Core clear is a present `None`, not a popped key.** The core merge does **not**
use RFC 7396 pop semantics. A cleared core field (`null`) is kept **present** in
the merged dict with the value `None`, so `_overlay_reloadable` sets the candidate
field to `None` and `resolve_reloadable_settings` fills the default. This is the
one place the core diverges from the service-section merge: a service section
merges onto a concrete L0 base dict, so a popped key falls back to L0; the core
has no L0 base in the reloader, so a popped key would instead inherit the
*current* value and a clear would be indistinguishable from an inherit. The
`auto_tune` branch for `max_concurrent_tasks` is preserved: clearing the field
(`null`) yields a present `None`, and the resolver fills it with the auto-tune
value.

The reloader stores the L2 and L3 core patches and re-merges whenever either
changes. A present core with a field `null` falls back to L2/L0 for that field; a
`core`-absent apply leaves the L3 layer as it was.

## 6. Invariant impact analysis

Each existing invariant is listed with whether and how it changes.

### 6.1 Replay / revision guard (`_applied_revision`, `_applied_hash`, `STALE_CONFIG`)

**Unchanged in mechanism.** The guard runs on the raw envelope bytes before any
merge or overlay (`config_reload.py:435-458`): a lower revision, or an equal
revision with a different hash, is still `STALE_CONFIG`; an equal hash is still
idempotent. Merging happens strictly after the guard passes, so a stale envelope
is rejected without ever touching layer state. The hash covers the envelope
identity, not the merged result — two envelopes with different bytes but the same
merged effect are still distinct revisions, as today.

### 6.2 `config:store`

**Persist the merged effective config as a complete snapshot.** `config:store
--target disk` writes the merged result, so a store -> restart cycle reproduces
the effective config exactly. Justification: a snapshot is by definition
self-contained; an operator asking to store wants a restorable artifact, not a
delta. For `--target remote`, the effective `ConfigSections` (resolved core +
merged effective service structs) is published back unchanged in shape. The core
is the resolved effective snapshot (all eight fields), matching the `settings`
view of `config:show` (§6.3), not the merged patch.

### 6.3 `config:show`

**Both views become merged views.** Today `services` carries the raw bytes
captured at the last apply (`config_reload.py:682`). Under merging:

- `settings` (effective) is built from the merged effective section structs
  (`msgspec.msgpack.encode(merged)`), so a consumer sees the resolved values, not
  the raw layer. For the **core**, the effective view is the resolved snapshot
  from the `current` callback (`msgspec.to_builtins`), i.e. all eight reloadable
  fields with their effective values — not the merged patch.
- `declarative_settings` is built from the merged **patch** view: the union of
  keys any layer set, with their values, and no `None`-filling. This replaces the
  v5 `DeclarativeSettings` (which used `None` = library default); in v6 the
  declarative view is "what is explicitly set", and absence means inherit. For
  the **core**, this is the merged L2/L3 patch (or `None` when no layer set a
  core key).

`config_reload.show()` / `show_declarative()` gain the merged encodings; the
response field names and types are unchanged (`task_handler/config.py:101-124`).

### 6.4 `restart_required` reporting

**Unchanged.** `_restart_required_fields` (`task_processor.py:312-314`) reports
concrete config fields outside `RELOADABLE_FIELDS`. Merging does not move any
field into or out of the allowlist. The modbus hook still logs its restart
warning when it applies a merged section while the gateway is running
(`modbus_worker.py:129-133`).

**Known follow-up: the warning fires on idempotent applies.** The hook logs
"restart required" whenever it is invoked with the gateway running, regardless of
whether the merged section actually changed. Under merging, an envelope that
repeats the current values still triggers a re-merge and therefore still logs the
warning. This is pre-existing behaviour (the hook has no change detection today),
not a regression, but merging makes idempotent re-applies more common. *Deferred:*
gate the warning on a section-level change check (the same merged-vs-current diff
that §6.5 computes for core) once section deltas are reported. Not a blocker for
this release.

### 6.5 `ConfigApplyOutcome.changed`

**Changed source, same meaning.** `changed` must be the core field names whose
*effective* value changed, computed by diffing the merged core candidate against
the current config. The existing comparison in `_overlay_reloadable`
(`task_processor.py:387-391`) already compares candidate vs current, so once the
candidate is the merged result the reported delta is correct without further
work. A remote envelope that repeats the current values reports `changed=[]` even
though its raw fields are non-`None`.

Service-section deltas are not currently reported (only core field names are
returned). Leaving them out keeps `ConfigApplyResponse.changed` wire-compatible;
adding section-qualified names (`modbus.serial.baudrate`) is optional future work
(§10).

### 6.6 Declarative contract redefinition

**Changed: `None` now means clear, not library-default.** In v5,
`DeclarativeSettings` used `None` = library default. In v6, the declarative view
is a patch dict where absence = inherit and `null` = clear. This is the breaking
change that makes v6 a major version. Consequences:

- `write_local_config` writes the merged patch, not a `None`-normalized struct.
- `config:show`'s `declarative_settings` shows the explicit patch (§6.3).
- Auto-tune: `max_concurrent_tasks: null` means "clear -> resolve to auto-tune",
  which is cleaner than v5's overloaded `None`.

### 6.7 `apply_local_file` (L2) and `_reload_remote_config` (L3) startup ordering

**Ordering unchanged**, semantics re-defined. The startup sequence in
`TransportWorker._apply_local_config` then `_reload_remote_config`
(`transport_worker.py:104-132`), driven from `ValkeyWorker.initialize`
(`valkey/worker.py:496-497`) and `MqttWorker.initialize`
(`mqtt/worker.py:687-688`), still applies L2 before L3. What changes is that L2
and L3 no longer replace the section; each **records its layer** and triggers a
re-merge from L0.

**L1 seeding and the gateway-build ordering hazard.** The bootstrap file is
service-owned: `ModbusWorker.initialize` reads `modbus.yml` into `self._settings`
at `modbus_worker.py:63`, *then* calls `super().initialize()` at line 68, then
builds the gateway from `self._settings` at line 71. The framework's
`TaskProcessor.initialize` (`task_processor.py:850-874`) has no knowledge of any
service bootstrap, so it cannot itself read L1. The correct wiring is therefore:

1. The service registers a `bootstrap` provider (§4.4) that reads its own file.
2. `TaskProcessor.initialize`, after `reset()` and before handlers start, calls
   each registered section's `bootstrap` provider, stores the result as that
   section's L1 patch, and resolves L0+L1 into the section's effective struct.
3. The service must build its runtime objects from the **merged** settings, not
   from the raw bootstrap it read at line 63. Concretely, `ModbusWorker.initialize`
   changes from `self._settings = read_modbus_config(...)` to reading the merged
   result the framework resolved during `super().initialize()` (e.g. via a
   `current_settings(section)` accessor), and the `bootstrap` provider becomes the
   only place `read_modbus_config` is called.

This is the one place the design changes a service's `initialize` shape rather
than only its schema. It must be pinned down before implementation: if the
service keeps building the gateway from the raw L1 read, the gateway starts from
unmerged settings and L0 defaults are silently lost. The `bootstrap` provider is
the single source of L1; the service never reads the file directly once migrated.

### 6.8 Auto-persist-on-remote-apply

**Write the merged snapshot.** The successful-remote auto-persist
(`transport_worker.py:131-132` -> `ConfigManager.write_local`) writes the same
merged view as `config:store --target disk`, i.e. the merged result of all
layers, not the remote layer alone. Justification: the file is defined as the
framework's namespaced snapshot, and writing the merged result makes it a
faithful durable mirror that re-applies ahead of the remote read on the next
start; writing only the remote patch would make the file mean something different
than config:store writes, and would leave a restart that never saw remote unable
to reproduce the last effective config.

Consequence to state in operator docs: once a field is written explicitly into
L2, it shadows L1 for that field. This is intended — L2 is the newer authority
between the bootstrap and the live remote — but it means changing `modbus.yml`
alone will not move a field that the framework has already snapshotted. The
merged patch keeps only explicitly-set keys, so unset fields stay live.

## 7. Backward compatibility / migration

**v6 is a major version; the v5 wire contract is frozen and not modified.**

**Envelope version.** `CONFIG_ENVELOPE_VERSION` bumps to `2`. A v6 worker
**rejects** a version-1 envelope with `INVALID_CONFIG` (the existing version
check at `config_reload.py:415-421` already does this for any non-matching
version). The error message names the version so an operator can see the
mismatch. No automatic v5 -> v6 migration is performed: the v5 declarative
contract (`None` = library default) and the v6 contract (`null` = clear) are
semantically incompatible, and a silent migration would risk turning a v5
"inherit" into a v6 "clear". Producers must emit v2.

**Envelope shape (D2 = keep `ConfigSections`).** `ConfigSections` and
`DeclarativeSections` keep their field names but change value types:

```python
class ConfigSections(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    core: dict | None = None
    services: dict[str, dict] = msgspec.field(default_factory=dict)

class DeclarativeSections(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    core: dict | None = None
    services: dict[str, dict] = msgspec.field(default_factory=dict)
```

The hash and signature are still computed over `msgspec.msgpack.encode(sections)`
(`config_reload.py:422-434`), so the security pipeline is unchanged.

**`config.yml` round-trip.** A v6 `config.yml` stores `DeclarativeSections` with
patch dicts. A v5 `config.yml` is **not** safely readable by v6, and the failure
mode is not uniform (verified against msgspec 0.20.0):

- A v5 file with **service sections** (concrete-encoded struct bytes) fails to
  decode into `services: dict[str, dict]` and is treated as absent
  (`read_local_config` returns `None`, `config_reload.py:723-745`). Safe: the
  worker starts from L0/L1 and re-reads remote.
- A v5 file with **only a core block** decodes *successfully* into v6
  `DeclarativeSections` (because `core: dict` accepts any dict). This is the
  hazard: the v5 core carries fields such as `queue_size`, `heartbeat_interval`,
  and `log_level` that are **not** in `RELOADABLE_FIELDS`. Under v6 that dict
  becomes an L2 core patch, and the mandatory core key allowlist (§5) rejects it
  with `INVALID_CONFIG` — so the apply fails rather than silently applying
  restart-required fields. The worker still starts (the local apply is
  best-effort), but the operator sees an error and must regenerate the file.

Because the failure mode is not a clean "treated as absent", operators upgrading
to v6 **must delete or regenerate `config.yml`**. The design does not add a
version discriminator to the local file for this release; the core key allowlist
is the safety net that prevents a v5 core block from injecting non-reloadable
fields.

**Service struct not yet migrated.** Because v6 does not require service structs
to change shape (§4.1), a service needs only to (a) register a `defaults`
instance and (b) register a `bootstrap` provider returning a dict. A service that
registers neither still works: its L0 base is `struct_type()` and it has no L1,
so it behaves as a single-layer section. There is no non-mergeable fallback path
in v6 — every registered section is mergeable by construction, because the patch
is a dict and the struct is concrete.

## 8. Security

The merge lives strictly *after* the security pipeline and cannot weaken it.

- **Allowlist by type.** Only registered sections are decoded and merged; the
  core allowlist (`RELOADABLE_FIELDS`, `config_reload.py:42-53`) is untouched.
  Restart-required and secret fields remain unrepresentable, and
  `forbid_unknown_fields=True` still rejects a payload that names them — now at
  the final `msgspec.convert`, which validates every nesting level (§3.4).
- **Integrity and authenticity.** `sha256(settings)` and the optional HMAC are
  computed over the exact envelope bytes before decode
  (`config_reload.py:422-434`), so the merge cannot be used to smuggle bytes past
  the check.
- **Replay.** The monotonic revision guard runs before the merge (§6.1).
- **Validate-before-swap.** The merged candidate is converted through the real
  struct and validated by `__post_init__`/`validate_range`; the section hook runs
  on a fully-built candidate before any swap. A bad value at any layer aborts the
  whole apply with no state change. Because the core's value validation lives in
  the terminal overlay (which runs *after* the section hooks), the reloader takes
  an injected `validate_core` callback and calls it on the merged core **before**
  running any section hook, so an out-of-range core value cannot mutate a section
  first.
- **No new input surface.** The merge consumes already-decoded maps; it
  introduces no parser, no new encoding, and no code execution. An unknown key at
  any level is rejected by `msgspec.convert`, so a patch cannot smuggle a field
  the schema does not declare.

## 9. Test strategy

Broker-free unit tests follow the existing patterns in
`tests/config/test_config_reload.py` and
`tests/task_processor/test_config_control.py`; integration tests keep the
`skipif` reachability guard. Config tests live under `tests/config/` (the repo
has no top-level test files).

**`tests/config/test_config_merge.py` (new, generic engine)**
- Three-state rule: absent key inherits, `null` clears, value sets — for a
  scalar, a nested struct field, and a dict entry.
- Per-layer resolution: a field set only at L1, only at L2, only at L3 resolves
  to that value; a higher value beats a lower one.
- Deep nested merge: L2 patches `serial.baudrate`, L1 keeps `serial.port`; both
  survive. A layer with `serial` absent contributes no nested values.
- Per-key dict merge: L1 defines device 1 and 2; L3 patches device 1's `framer`;
  both devices survive and device 1's other fields come from L1.
- Dict clear: L3 sets device 2 to `null`; device 2 is absent, device 1 survives.
- Scalar clear: L3 sets `serial.port` to `null`; it resolves to the L0 default.
- List/scalar replace: a higher `pdus`/`source_services` replaces the lower list;
  explicit `[]` clears it; explicit `false`/`0` overrides.
- Unknown field rejected at every nesting level by `msgspec.convert`.
- L0 base: a min patch plus the defaults instance resolves to the L0 library
  defaults for every field.

**`tests/config/test_config_reload.py` (extend)**
- Replay interaction: a stale envelope is `STALE_CONFIG` and leaves all layer
  state untouched; an idempotent equal-hash envelope changes nothing.
- Layer replacement: applying L2 then L3, then re-applying a different L2,
  re-resolves from L0 rather than overlaying the previous merged value.
- `core` absent leaves the L3 core layer unchanged; a present core with a field
  `null` falls back to L2/L0 for that field only.
- Version-1 envelope rejected with `INVALID_CONFIG`.

**`tests/config/test_apply_pipeline.py` (extend)**
- `changed` reflects merged deltas: a remote that repeats current values reports
  `changed=[]`; a remote that changes one core field reports only that field.

**`tests/config/test_store.py`, `test_show.py`, `test_local_file.py` (extend)**
- `store`/`show` round-trip: `config:store --disk` then `read_local_config` then
  apply reproduces the effective config; `show` returns merged effective and
  merged patch views.

**Cross-service test (proves generic engine)**
- Register both `ModbusServiceSettings` and `LogAggregatorSettings` (with their
  defaults instances) in one test and run the same layer scenarios: nested struct
  for modbus, list fields for the aggregator, dict-per-key with clear for modbus
  devices. The same engine code path must produce both, with no service-specific
  branch.

**Consumer tests**
- `ModbusWorker`: L1 `modbus.yml` + L2 partial `config.yml` yield a gateway whose
  serial port comes from L1 and baudrate from L2.
- `LogAggregatorWorker`: L1 + remote section replaces `source_services` and
  inherits `target_stream`.

## 10. Open questions / risks

1. **`config.yml` v5 -> v6 migration.** A v5 file with service sections fails to
   decode and is treated as absent; a v5 core-only file decodes but is rejected
   by the core key allowlist (§7). *Recommendation:* accept; document that
   operators delete or regenerate `config.yml` on upgrade. A migration shim is
   possible but risks misreading v5 "inherit" as v6 "clear".

2. **Declarative view semantics.** v6's `declarative_settings` is "what is
   explicitly set", not a `None`-filled struct. *Recommendation:* accept; it is
   the honest representation of a patch and matches the wire.

3. **Partial remote core.** v6 allows a partial core patch, unlike v5's complete
   snapshot. *Recommendation:* accept; it is the expressiveness the layered model
   exists to provide, and the version bump makes the contract change explicit.

4. **L2 shadowing of L1 after auto-persist** (§6.8). *Recommendation:* accept and
   document; L2 is defined as the framework snapshot and outranks the bootstrap.
   Revisit only if operators report surprise.

5. **Bootstrap re-read.** L1 is read once at startup. If `modbus.yml` changes
   while the worker runs it is not picked up. *Recommendation:* unchanged from
   today; a restart applies it, and any live change belongs in the remote layer.

6. **Section `changed` reporting.** Only core deltas are reported today.
   *Recommendation:* defer; extend `ConfigApplyResponse` with section-qualified
   names only if a consumer needs it.

7. **`msgspec.convert` cost.** The merge converts the full merged dict once per
   apply. *Recommendation:* accept; applies are infrequent and the dict is small.
   If profiling shows a cost, cache the converted struct and invalidate on layer
   change.
