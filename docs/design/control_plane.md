# Cross-Worker Control Plane for `scietex.service`

**Status:** design (not implemented)
**Target release:** v5.0.0
**Branch:** `v5`
**Motivation:** AR-123 (`docs/reviews/architecture/2026-09-20-1.md`) — control
commands (`task:cancel`, `config:apply`, `config:store`, `config:show`) are
single-worker-scoped because they travel the same single-consumer delivery path
as data tasks. With a fleet sharing one task source, a `task:cancel` is handled
by whichever worker wins the read, not the worker running the target, and a
`config:*` command reaches exactly one worker. AR-108's in-process priority lane
solved *intra-process* starvation but left a transport-level head-of-line
residual and did nothing about routing. This document specifies AR-123's planned
"D4" fix (`docs/ROADMAP.md:36-55`): a dedicated **per-worker control channel**
and a dedicated **broadcast control channel** per transport, removing control
from the data delivery path entirely.

---

## 1. Scope & non-goals

### Goals

- Deliver every control-plane command over a channel **separate from data** on
  both transports, so a saturated data plane has no transport-level effect on
  control delivery.
- Provide exactly **two control addresses per service**:
  - one **directed** channel, owned by a single worker instance, for commands
    that must reach a specific worker (`task:cancel`);
  - one **broadcast** channel, observed by every worker instance, for commands
    that must fan out (`config:apply` / `config:store` / `config:show`).
- Keep the change pure-Python with **no new dependencies**; reuse `msgspec`,
  `glide`, and `aiomqtt` as today.
- Deliver control commands as **event-only, never-retried** messages: no lease,
  no consumer group, no recovery. A command published while a worker is down is
  intentionally dropped (tail-seek, §4.2); retention is bounded by `MAXLEN` and,
  for the directed stream, a heartbeat-refreshed TTL (§4.3).
- Make the task → owning-worker resolution possible so a submitter can address a
  `task:cancel` to the right worker (`TaskStatus.instance_id`, §3.4).
- Keep `TaskTransport` unchanged: the split is internal to each transport's
  `fetch`/`recover_pending_tasks` (§7).

### Non-goals

- **No change to `TaskData` / `TaskEnvelope`.** The address is the channel name,
  not a new struct field (§3). The envelope wire format is untouched.
- **No data-plane semantics change.** Data tasks keep their stream/topic, their
  ordering, their lease/backpressure behavior.
- **No removal of the in-process control lane.** `TaskProcessor.__control_queue`
  and `DEFAULT_CONTROL_CONCURRENCY` stay; they are concurrency isolation, not
  the routing mechanism (§6).
- **No new Valkey PubSub usage.** The existing fire-and-forget PubSub channels
  (`scietex:{service}`, `scietex:broadcast`, `valkey/config.py:418-429`) are
  unrelated and unchanged; the broadcast control channel is a **durable stream**.
- **No durable cross-worker control state.** Configuration desired-state stays
  where it is (`scietex:{service}:config` / retained `scietex/{service}/config`).
  This design changes only *how the command is triggered*, not *where state
  lives*.
- **No ordering guarantee across the two channels.** Directed and broadcast
  commands are independent; a `config:apply` and a `task:cancel` have no mutual
  ordering obligation.

---

## 2. Naming and address scheme

### 2.1 Valkey

| Purpose | Stream | Read model | Retention |
|---|---|---|---|
| Data (unchanged) | `scietex:{service}:tasks` | `XREADGROUP` on `scietex:{service}:task_group` | `XACK` + `XDEL` |
| Directed control | `scietex:{service}:control:{instance_id}` | `XREAD` from tail, in-memory cursor | `MAXLEN ~ 1000` + TTL (`active_ttl`) |
| Broadcast control | `scietex:{service}:control` | `XREAD` from tail, in-memory cursor | `MAXLEN ~ 1000` |

Rationale:

- The **directed** stream name embeds `instance_id`, so the stream itself is
  the address. It has exactly one intended reader.
- The **broadcast** stream is shared and must fan out to **every** worker. Plain
  `XREAD` gives every reader every entry, which is the fan-out mechanism; a
  consumer group would deliver each entry to only one worker.
- Neither control stream uses a consumer group, so neither has a PEL, a lease,
  or recovery. Retention is `MAXLEN` (bounded ring buffer) plus, for the directed
  stream, a TTL refreshed on the heartbeat tick (§4.3).
- `instance_id` is the same identifier already used for the heartbeat key
  (`scietex:{service}:{instance_id}:status`, `valkey/worker.py:169`) and the
  registry, so discovery is unchanged.

### 2.2 MQTT

| Purpose | Topic | QoS | Retained |
|---|---|---|---|
| Data (unchanged) | `scietex/{service}/tasks` | `task_qos` (2) | no |
| Directed control | `scietex/{service}/control/{instance_id}` | `control_qos` (1) | no |
| Broadcast control | `scietex/{service}/control` | `control_qos` (1) | no |
| Config desired state (unchanged) | `scietex/{service}/config` | `config_qos` | yes |
| Registry heartbeat (unchanged) | `scietex/{service}/workers/{instance_id}` | — | yes |

Rationale:

- The directed topic is the broadcast topic plus `/{instance_id}`, so the whole
  control plane lives in one `control` family (`scietex/{service}/control/...`)
  and a wildcard/prefix enumeration covers exactly the directed channels. It
  stays outside the registry's `workers/+` subscription (`mqtt/watch.py:41`):
  MQTT `+` matches exactly one level, so `workers/+` does **not** match
  `control/{instance_id}`. No collision, no backend change.
- The broadcast topic is a sibling of the data/config topics, distinct from both.
- Control messages are **not retained** and carry no message-expiry: a control
  command is an event, not a desired-state snapshot. A command published while a
  worker is offline is **not** replayed — the worker subscribes at startup and
  only sees messages published after SUBACK.
- QoS 1 (at-least-once at the wire) is sufficient for the live path; the durable
  inbox (§5) provides at-least-once **within a session** (a message received but
  not yet processed survives a crash). It does **not** replay commands missed
  while the worker was down — that is intentional, matching the Valkey tail-seek
  (§4.2).

---

## 3. Addressing model

### 3.1 The address is the channel, not a payload field

A control command is an ordinary `TaskData` (with its `task_id`, `task`,
`payload`, `timeout`) encoded in the existing `TaskEnvelope`. **Which channel it
is published to is the entire routing decision.** No `TaskData` field carries an
owner, a target, or a broadcast flag.

Why not an envelope field (e.g. `TaskData.target_worker`)?

- On Valkey, a stream is consumed by a group; to fan the *same* entry out to
  every worker you need **per-worker groups**, not a field a single shared group
  would read once. A field cannot create per-worker groups, so it does not solve
  broadcast at all. (This design instead uses plain `XREAD`, which fans out
  without any group — but a payload field still could not express the address.)
- On MQTT, per-worker addressing needs per-worker *topics*; a field on a shared
  subscription would still require filtering after receipt, and the broker would
  deliver every control message to every worker regardless of target.
- The channel name is already the natural address on both transports (streams /
  topics), so promoting it to the contract is the smaller, more honest design.

### 3.2 Directed vs broadcast

- **Directed** — the producer must know the target `instance_id`.
  `task:cancel` is the canonical directed command: it is meaningful only on the
  worker that currently owns the target task.
- **Broadcast** — the producer addresses the service, not a worker. `config:*`
  commands are broadcast: every worker must apply/store/show for itself.
- A producer chooses the channel. The worker does not inspect a payload to
  decide whether a command was meant for it.

### 3.3 Control-plane scope becomes fleet-wide

After this change the documented scope boundary (`AGENTS.md:50`; the
fleet note in `docs/remote_config.md:87`) is lifted:

- `task:cancel` directed to the owning worker cancels across workers.
- `config:*` broadcast reaches every worker.
- A command sent to the **wrong** address still behaves sensibly: a directed
  cancel to a non-owner returns `TASK_NOT_RUNNING`
  (`task_handler/cancel.py:128-133`), and a broadcast observed by a worker that
  already applied that config revision is rejected by existing replay
  protection (`ConfigReloader`, `config_reload.py`).

### 3.4 Task → owning-worker resolution

To address a `task:cancel`, a submitter must map `target_task_id` → owning
`instance_id`. The resolution record already exists: the tracking record
`TaskStatus` at `scietex:{service}:task:{task_id}` (Valkey,
`valkey/tracking.py:56-58`) and the retained topic
`scietex/{service}/tasks/{task_id}/status` (MQTT,
`mqtt/transport.py:143-169`). Both are written by the owning worker.

**Decision:** add `instance_id: str = ""` to `TaskStatus`
(`task_handler/schemas.py:167-188`) and set it in both status builders when the
owner writes a record. The owner identity then travels with the existing status
record — no new key, no new topic, no separate ownership map.

- `build_running_status` / `build_terminal_status` (`task_status.py:23-98`)
  gain an `instance_id` parameter.
- Valkey: `TaskStatusStore.record_running` / `record_terminal`
  (`valkey/tracking.py:80-96`) pass the worker's `instance_id`.
- MQTT: `MqttTransport` passes the worker's `instance_id` when building status
  (`mqtt/transport.py:229-241, 321, 358-360`).

A default of `""` (rather than a required field) is deliberate: `TaskStatus` is
observability and is built in tests without an owner. An empty value simply means
"owner unknown", which a router treats as "cannot direct; fall back to
broadcast or refuse".

---

## 4. Valkey design

### 4.1 Read model: `XREAD`, not `XREADGROUP`

Control streams are read with plain **`XREAD`** and an **in-memory last-seen
entry id**. No consumer group, no PEL, no lease, no local cursor file.

Why not `XREADGROUP`:

- A consumer group exists to give **competing consumers** exactly-once delivery
  of a shared stream. Control needs the opposite: the directed stream has one
  intended reader, and the broadcast stream must fan out to **every** worker.
  `XREADGROUP ">"` delivers each entry to exactly one consumer, so a shared
  group cannot fan out at all.
- Per-worker groups would solve fan-out but reintroduce server-side per-worker
  state (one group per `instance_id`) and its cleanup lifecycle.
- A group also **replays** everything since its last-delivered id, which is
  exactly the stale-command problem: a broadcast published while the worker was
  down must not be replayed.

`XREAD` with a tail-seek gives the required semantics with zero server state.

### 4.2 Startup position: seek to the tail

On startup the read position is the **stream tail**, not a persisted cursor.
Every control command published while the worker was down is **skipped** —
stale commands are structurally impossible to replay.

The tail is resolved once to a concrete entry id (`XINFO STREAM`'s
`last-generated-id`, or `0-0` when the stream does not exist yet) rather than
left as the literal `$`. `XREAD` re-resolves `$` to the live tail on every
call, so a `$` cursor would skip any command published between polls — the
cursor must pin a fixed position to advance from.

| Event | Read position |
|---|---|
| Worker startup | resolved tail — skip everything published while down |
| Running | advance from the in-memory last-seen id |
| Restart | resolved tail again — stale commands dropped |

This applies to **both** the directed and the broadcast stream. A directed
`task:cancel` published while the target worker is restarting is therefore
dropped; that is intended, because the task it targets died with the worker.

The last-seen id is held **in memory only**. `XREAD ... BLOCK` already resumes
within a session, so no local file is needed.

### 4.3 Retention: `MAXLEN` + owner-refreshed TTL

Because `XREAD` never acks, entries are not removed by consumption. Retention
mirrors the heartbeat model (`valkey/worker.py:390-394`): **self-expiring state,
refreshed by the owner's own activity**, not a cleanup job.

| Stream | `MAXLEN` | TTL | Refresh |
|---|---|---|---|
| Directed `scietex:{service}:control:{instance_id}` | `~ 1000` | yes | refreshed inside `heartbeat()` |
| Broadcast `scietex:{service}:control` | `~ 1000` | no | n/a — service-scoped |

- Every control `XADD` uses `MAXLEN ~ 1000`, so the stream is a **bounded ring
  buffer**, not an unbounded log. The `~` makes the trim approximate and cheap.
- The **directed** stream additionally carries a TTL, refreshed on the same
  heartbeat tick that refreshes the status key. A live worker keeps it alive; a
  departed worker's directed stream **expires on its own**, exactly like the
  heartbeat key. The TTL is `active_ttl` — one lifetime concept, one refresh
  tick.
- The **broadcast** stream has no single owner to refresh a TTL, so it has none.
  It is service-scoped and long-lived by design; `MAXLEN` bounds it.

Net: no groups, no leases, no reconciler, no local cursor, and no unbounded
growth. Every piece of state is either bounded (`MAXLEN`) or self-expiring (TTL).

### 4.4 Reading the control streams

`ValkeyTransport.fetch` (`valkey/transport.py:90-168`) gains two reads in
addition to the data read:

1. Flush the data deferred buffer, recover once, read the data stream
   (**unchanged**, `XREADGROUP`, block 1000 ms).
2. Read the **directed** stream with `XREAD` from the in-memory cursor
   (non-blocking).
3. Read the **broadcast** stream with `XREAD` from the in-memory cursor
   (non-blocking).

Each decoded `TaskData` is enqueued via `sink.enqueue_control_task`, so anything
read from a control stream is dispatched against the control registry regardless
of its task type (`valkey/transport.py:351`). Return `True` if any channel
enqueued.

Control reads are **non-blocking** (`block=0`) and run **sequentially after** the
data read in the existing `task_queue_manager` loop. Data keeps its 1000 ms
block; control is polled every iteration. This avoids coupling data latency to
the control read and needs **no new manager** — `XREAD ... BLOCK` is an `await`,
so it never blocks the event loop, and the existing data read already proves the
pattern.

A control entry the sink rejects (control lane full) is held in a bounded
`_control_deferred` buffer and retried on the next poll, mirroring the data
`_deferred` (`valkey/transport.py:82-88, 170-187`). `enqueue_control_task`
returning `False` is the fullness signal for the control lane.

### 4.5 Entry tracking and acknowledgement

`ValkeyWorker` keeps a parallel `_control_entry_ids: dict[UUID, tuple[str,
str | bytes]]` mapping a control task id to its `(stream_name, entry_id)`. The
existing data map `_task_entry_ids` (`valkey/worker.py:203-205`) is **unchanged**,
which means:

- `refresh_leases` (`valkey/transport.py:359-366`) iterates only data entries —
  control entries are never leased.
- `ack` (`valkey/transport.py:305-343`) first tries
  `_control_entry_ids.pop(task_id)`; if present it `XDEL`s the recorded entry on
  the recorded stream, writes the terminal status (with `instance_id`), and
  returns — **no `XACK` (no group), no lease delete**. Otherwise it takes the
  existing data path.
- `on_drain` (`valkey/transport.py:349-357`) deletes the lease for data only; a
  drained control entry needs no lease release.
- `on_started` (`valkey/transport.py:297-303`) writes the running status (with
  `instance_id`) and, for data only, writes the lease.

### 4.6 No lease for control

The lease (`valkey/lease.py`) exists to stop two replicas in the **same** data
consumer group from double-processing a recovered pending entry (AR-060). The
control plane has no such race: there is no group and no recovery, so no two
workers ever contend for the same control entry. Control entries need no lease.

### 4.7 No recovery, no retry

- `recover_pending_tasks` (`valkey/transport.py:189-275`) is **unchanged** — it
  recovers the data stream only. Control has no PEL to recover (no group), and
  tail-seek means a restart intentionally drops anything missed.
- **Control commands are never retried.** `requeue` (`valkey/transport.py:277-295`)
  detects a control task id in `_control_entry_ids` and returns without
  re-`XADD`ing. The framework's retry budget is a data-plane concept; a control
  handler returns an explicit terminal `TaskResult` (all built-ins do —
  `cancel.py:107-133`, config handlers). This is required for broadcast:
  re-publishing a broadcast on retry would re-fan-out a command to the whole
  fleet.

### 4.8 Why the transport-level head-of-line residual disappears

Today `fetch` reads data and control from one stream and scans past a blocked
data lane to reach a control command (`valkey/transport.py:104-108`). After the
split, control is on its own streams and its own read, so a data backlog cannot
delay a control read at the transport layer. The only remaining interaction is
in-process (§6), which is deliberate.

---

## 5. MQTT design

### 5.1 Partitioned durable inbox

MQTT has one intake inbox (`MqttInbox`, `mqtt/inbox.py:39-60`) that the message
loop fills and the transport drains. The split mirrors the streams: **two inbox
instances**, one for data and one for control, both file-backed by default.

- `MqttTransport.__init__` (`mqtt/transport.py:99-141`) gains a keyword-only
  `control_inbox: MqttInbox`.
- The `MqttInbox` Protocol is **unchanged** — only more instances of it.
- `fetch` (`mqtt/transport.py:202-243`) drains the data inbox with existing data
  backpressure, then drains the control inbox independently (no backpressure; a
  rejected control entry stays pending for the next poll).
- `recover_pending_tasks` (`mqtt/transport.py:245-288`) recovers both inboxes;
  only a data backpressure stop marks recovery incomplete. Recovery replays
  entries **received but not yet processed** (a crash mid-session); it does not
  resurrect commands missed while the worker was offline, because those were
  never received and never entered the inbox.
- `on_started` / `ack` / `on_drain` route to the inbox that owns the task id by
  `_control_enqueued` membership (not by task type, so a control task published
  to the legacy data topic acks in the data inbox) — `mqtt/transport.py:421,475,526`.
  This introduces no new state beyond what already exists
  (`_control_enqueued`, `mqtt/transport.py:161`).

Default paths: data inbox at the existing `inbox_path`, control inbox at a
sibling directory (e.g. `{inbox_path}/control`), so the two TTL-pruned stores
cannot collide. `inbox_backend="memory"`/`"none"` produces two `MemoryInbox`
instances, preserving the at-most-once opt-out.

### 5.2 Message routing

`MqttWorker._handle_message` (`mqtt/worker.py:695-725`) currently routes by a
single check (config topic vs task). It gains control-topic checks:

1. `_config_topic` → config source `record` (unchanged).
2. `_control_topic` (this instance's directed topic) →
   `self._control_inbox.put(...)`.
3. `_control_broadcast_topic` → `self._control_inbox.put(...)`.
4. anything else → `self._inbox.put(...)` (data, unchanged).

The directed topic is matched exactly (it embeds this worker's `instance_id`),
so a worker never receives another worker's directed command.

### 5.3 Subscriptions

`_subscribe` (`mqtt/worker.py:444-456`) additionally subscribes:

- `self._control_topic` at `control_qos`;
- `self._control_broadcast_topic` at `control_qos`.

Both are exact subscriptions (no wildcards), so they are cheap and cannot
overlap the registry's `workers/+` subscription (`mqtt/watch.py:41`).

### 5.4 Publishing control commands

A `task:cancel` is published to `scietex/{service}/control/{instance_id}`;
a `config:*` command to `scietex/{service}/control`. Because the messages are
not retained, the MQTT control plane is only meaningful while a worker is
subscribed; a command published while a worker is offline is not replayed from
the broker. This is intentional and matches the Valkey tail-seek (§4.2): control
is event-only, and a stale command must not be replayed.

---

## 6. In-process lane interaction

The in-process control lane is **kept**:

- `TaskProcessor.__control_queue` (`task_processor.py:141`) remains, but routing
  is now channel-driven: `enqueue_control_task` (`task_processor.py:402`) targets
  the control lane and `enqueue_task` targets the data lane, and a handler
  declares its lane with the `control` class attribute
  (`task_handler/basic.py`).
- `TaskExecutor` continues to admit control first on its own concurrency ceiling
  (`DEFAULT_CONTROL_CONCURRENCY = 4`, `task_executor.py:32, 95-115`), and
  `max_concurrent_tasks` continues to bound only the data plane
  (`task_processor.py:220-228`).

What changes is the **transport-level** special-casing:

- `InMemoryTransport.fetch` (`transport.py:138-164`) keeps its
  control-preference scan — it is the bare-processor default with no separate
  channel, and it is not a durable control plane.
- `ValkeyTransport.fetch`/`recover_pending_tasks` and
  `MqttTransport.fetch`/`recover_pending_tasks` **drop** the type-based
  scan-past-backpressure branches. Control arrives on its own channel, so those
  branches are dead and are removed to keep a single delivery story.

Net effect: the two lanes no longer interact at the transport layer at all;
they interact only in-process, where the interaction is exactly the intended
concurrency isolation.

---

## 7. Transport Protocol impact

**`TaskTransport` gains zero methods.** `TaskSink` gains one method,
`enqueue_control_task`, so a transport addresses a task's lane by which surface
it calls rather than by inspecting the task type.

- `fetch` owns the channel split internally; its `bool` return keeps its
  meaning ("something was enqueued").
- `recover_pending_tasks` owns control recovery internally; its
  `(recovery_complete, enqueued)` contract is unchanged.
- `ack` / `on_started` / `on_drain` / `requeue` / `refresh_leases` branch on
  control ownership internally.

This is the same shape as AR-072: a transport-specific concern stays inside the
transport and does not widen the core seam. The only core-type changes are
`TaskStatus.instance_id` (§3.4) and the new producer surface below.

### Producer surface (enabler)

A minimal core Protocol lets a submitter address control commands without
knowing the transport:

```python
class ControlPublisher(Protocol):
    async def direct(self, instance_id: str, task_data: TaskData) -> None: ...
    async def broadcast(self, task_data: TaskData) -> None: ...
    async def resolve_owner(self, task_id: str) -> str | None: ...
```

- `ValkeyControlPublisher` (`valkey/control.py`) `XADD`s the enveloped
  `TaskData` to the directed/broadcast stream with `MAXLEN ~
  control_stream_maxlen`; `resolve_owner` `GET`s the tracking key and decodes
  `TaskStatus.instance_id`.
- `MqttControlPublisher` (`mqtt/control.py`) publishes to the directed/broadcast
  topic. `resolve_owner` reads back the retained owner topic
  `scietex/{service}/tasks/{task_id}/owner` via a one-shot subscribe (§10.1).

The producer surface **ships in v5.0.0** (decided); the worker-side split is
independently valuable and testable without it (§11 step 10 is marked separable
only so it can be sequenced last).

---

## 8. Config surface

All names are always-on; there is no feature flag (consistent with the v5.0.0
hard-cut philosophy). `{service}` and `{instance_id}` are substituted at
construction, like `log_stream_name` / `config_key`
(`valkey/worker.py:141-146`) and `task_topic` (`mqtt/worker.py:193-203`).

### 8.1 `ValkeyWorkerConfig` (`valkey/config.py:265-319`)

| Field | Default | Bounds |
|---|---|---|
| `control_stream_name` | `scietex:{service}:control:{instance_id}` | — |
| `control_broadcast_stream_name` | `scietex:{service}:control` | — |
| `control_stream_maxlen` | `1000` | `[1, 100000]` |

`control_stream_maxlen` bounds both control streams (`XADD ... MAXLEN ~ N`). The
directed stream's TTL is `active_ttl` — the same value that bounds the heartbeat
key — refreshed on the heartbeat tick (§4.3). There are no group names to derive:
control uses no consumer groups.

### 8.2 `MqttWorkerConfig` (`mqtt/config.py:137-155`)

| Field | Default | Bounds |
|---|---|---|
| `control_topic` | `scietex/{service}/control/{instance_id}` | — |
| `control_broadcast_topic` | `scietex/{service}/control` | — |
| `control_qos` | `1` | `[0, 2]` |
| `control_inbox_path` | `None` (derive from `inbox_path`) | — |

`control_qos` is validated with `validate_range` like the other QoS fields
(`mqtt/config.py:159-177`).

### 8.3 Resolution helper

Both workers gain a small helper that formats a configured control name with
`service` and `instance_id` together, so a name may legitimately contain either
placeholder. A name without a placeholder passes through unchanged, matching the
existing single-placeholder behavior.

---

## 9. Migration & breaking notes

This is a **v5.0.0 hard cut**, consistent with the other two v5 wire breaks
(`TaskData.task_id`, `Heartbeat.ttl`, `docs/ROADMAP.md:7-95`). No compatibility
shims.

- **New channels only; no existing channel changes name.** Data streams/topics,
  the config key/topic, the registry, and the log channel are untouched. An
  operator who never sends control commands sees no behavior change.
- **`TaskStatus` gains `instance_id`** (default `""`). A consumer that decodes
  `TaskStatus` may ignore the field; no decode change is required because it is
  an added field with a default. This is *not* a hard break (unlike
  `TaskData.task_id`), deliberately: status is observability.
- **Control commands move off the data channel.** A submitter that currently
  publishes a `task:cancel`/`config:*` to `scietex:{service}:tasks` /
  `scietex/{service}/tasks` must switch to the directed/broadcast channel. A
  control command left on the data channel would still be handled locally by
  whichever worker reads it (the data path always worked that way), so this is a
  silent scope regression for the submitter, not a crash. Documentation and the
  control-plane scope note in `AGENTS.md` must be updated to state the new
  addresses and to remove the "single-worker-scoped" limitation.
- **Control never retried.** Any operator relying on a retryable control handler
  result is out of contract; no built-in handler produces one.
- **MQTT control is event-only.** Not retained, so it is not replayed from the
  broker to a reconnect; the inbox provides at-least-once only for messages
  already received (crash mid-session), not for commands missed while offline.
- **Cleanup:** no new keys accumulate. Control entries are bounded by
  `MAXLEN ~ control_stream_maxlen` on every `XADD`; the directed stream carries a
  TTL (`active_ttl`) refreshed on the heartbeat tick, so a departed worker's
  directed stream expires on its own, exactly like its heartbeat key. The
  broadcast stream is service-scoped and bounded by `MAXLEN`. There are no
  consumer groups, so there is no group residue to reconcile.
- **Removed transport code:** the type-based scan-past-backpressure branches in
  `ValkeyTransport.fetch`/`recover_pending_tasks` and
  `MqttTransport.fetch`/`recover_pending_tasks`, and the now-dead control-type
  import in `mqtt/transport.py:35`. `InMemoryTransport` keeps its branch.

---

## 10. Open questions & risks

1. **MQTT `resolve_owner` has no read-back.** MQTT has no `GET`; the retained
   `scietex/{service}/tasks/{task_id}/status` can only be read by subscribing
   (the `MqttConfigSource` snapshot pattern, `mqtt/config_source.py`). **Decided:**
   a dedicated retained owner topic `scietex/{service}/tasks/{task_id}/owner`,
   published alongside the status and read back via a one-shot subscribe.
2. **Directed topic subscription timing.** A worker subscribes to its directed
   topic in `_subscribe` (`mqtt/worker.py:444-456`) during `initialize`. A
   command published before SUBACK is missed (not retained). This is inherent to
   event-only control and is why a submitter should resolve ownership from the
   registry (worker already registered, `mqtt/worker.py:727-737`) before
   directing.
3. **Control lane fullness is now the only backpressure.** If the in-process
   control queue fills (`queue_size` entries), control entries defer at the
   transport. With `DEFAULT_CONTROL_CONCURRENCY = 4` and short handlers this is
   unlikely, but the `_control_deferred` buffer must be bounded and observable
   (log at DEBUG, as data does, `valkey/transport.py:163`).
4. **Replay protection must cover broadcast.** `config:*` already carries
   revision/hash replay protection (`ConfigReloader`). Confirm `task:cancel` is
   idempotent across a recovered duplicate (it is: a second cancel of an
   already-terminal target returns `TASK_NOT_RUNNING`, `cancel.py:128-133`).
5. **`instance_id` uniqueness.** The directed stream/topic keys on `instance_id`.
   Two workers sharing an `instance_id` would collide. The registry already
   assumes uniqueness (`client/__init__.py:19`); state the invariant explicitly
   in the control-plane docs.
6. **Directed commands do not survive a restart.** Tail-seek means a directed
   command published while the target worker is down is dropped. Intended for
   `task:cancel` (the target died with the worker); revisit if a future directed
   command must be durable.

---

## 11. Implementation decomposition

Ordered so each step is independently verifiable. `W` = WHERE, `Y` = WHY,
`H` = HOW, `V` = VERIFY.

1. **Owner identity on status.** `W:` `task_handler/schemas.py:167-188`,
   `task_status.py:23-98`, `valkey/tracking.py:80-96`, `mqtt/transport.py`
   status call sites. `Y:` enable task → worker resolution without a new key.
   `H:` add `instance_id: str = ""` to `TaskStatus`; add an `instance_id`
   parameter to both builders; pass the worker's id at every write. `V:`
   `pytest tests/` status tests; assert `instance_id` round-trips in a Valkey
   tracking test and an MQTT status-publish test.
2. **Valkey config fields.** `W:` `valkey/config.py:265-319`,
   `valkey/worker.py:141-225`. `Y:` addressable control streams. `H:` add
   `control_stream_name` / `control_broadcast_stream_name` /
   `control_stream_maxlen`; resolve `{service}`/`{instance_id}`. `V:`
   `ruff check src/ && ty check src/`; a config unit test for placeholder
   substitution and defaults.
3. **MQTT config fields.** `W:` `mqtt/config.py:137-178`,
   `mqtt/worker.py:193-203`. `Y:` addressable control topics. `H:` add
   `control_topic` / `control_broadcast_topic` / `control_qos` /
   `control_inbox_path`; validate `control_qos`. `V:` config unit tests,
   `ruff`/`ty`.
4. **Valkey directed control intake.** `W:` `valkey/worker.py:203-225`,
   `valkey/transport.py`. `Y:` deliver directed control off the data path.
   `H:` add `_control_entry_ids` and an in-memory directed cursor seeded to `$`
   at startup; read the directed stream with `XREAD` (non-blocking) in `fetch`;
   bounded `_control_deferred`; branch `ack`/`on_started`/`on_drain`/`requeue`
   on control ownership; no lease for control. `V:` unit test with a
   `DummyClient` that `XADD`s a `task:cancel` to the directed stream and asserts
   enqueue → ack → `XDEL` on that stream with no lease key; a second test asserts
   an entry published before startup is skipped.
5. **Valkey broadcast fan-out.** `W:` `valkey/transport.py`, `valkey/worker.py`.
   `Y:` fan config commands to every worker. `H:` read the broadcast stream with
   `XREAD` from a `$`-seeded in-memory cursor. `V:` two simulated workers with
   distinct `instance_id` each read the same broadcast entry.
6. **Valkey retention + requeue semantics.** `W:` `valkey/transport.py:189-295`,
   `valkey/worker.py` heartbeat. `Y:` bounded, self-expiring control state and no
   control retry. `H:` `XADD ... MAXLEN ~ control_stream_maxlen` on both control
   streams; refresh the directed stream's TTL (`active_ttl`) inside
   `heartbeat()`; `requeue` no-ops for control ids. `V:` a test asserts the
   directed stream TTL is refreshed on heartbeat and that a retryable control
   result is settled terminal, not re-published.
7. **MQTT partitioned inbox.** `W:` `mqtt/worker.py:217-239, 444-456, 695-725`,
   `mqtt/transport.py:99-141`. `Y:` isolate control from data backpressure.
   `H:` build `_control_inbox`; route control topics in `_handle_message`;
   subscribe the two control topics; pass `control_inbox` to `MqttTransport`.
   `V:` worker test asserts a directed/broadcast message lands in the control
   inbox and is enqueued by the control drain even with the data lane full.
8. **MQTT transport drain/ack routing.** `W:` `mqtt/transport.py:202-411`.
   `Y:` correct inbox ownership and at-least-once within a session. `H:`
   drain/recover the control inbox; route `on_started`/`ack`/`on_drain` by
   **inbox ownership** (`_control_enqueued` membership), not by
   task type — a control task published to the legacy data topic lands
   in the data inbox and must ack there, so ownership is the correct key.
   `V:` control inbox test: persist → fetch → ack writes the control tombstone;
   recovery replays a non-terminal control entry.
   *Delivered with step 7* — the partitioned inbox made the ownership routing
   necessary, so the two steps merged into one commit.
9. **Remove transport-level control special-casing.** `W:`
   `valkey/transport.py` fetch/recover, `mqtt/transport.py:35, 202-288`. `Y:`
   one delivery story. `H:` delete the type-based scan-past-backpressure
   branches and the unused import; leave `InMemoryTransport`. `V:` existing
   control-priority tests still pass; `ruff`/`ty`; grep confirms no dead import.
10. **Producer surface (separable).** `W:` new `scietex/service/control.py`,
    `valkey/control.py`, `mqtt/control.py`; exports in `service/__init__.py`.
    `Y:` submitters can address commands. `H:` implement `ControlPublisher` per
    §7; resolve MQTT owner via the retained owner topic (§10.1). `V:` a
    `task:cancel` directed to the owner cancels cross-worker; a `config:apply`
    broadcast reaches both workers.
    *Delivered:* the two publishers plus the MQTT retained owner marker
    (`MqttTransport._publish_owner`, gated on the `queued` status — the first
    ownership-establishing write). The cross-worker cancel/broadcast proof is
    step 11.
11. **Multi-worker integration test.** `W:` `tests/` (new module). `Y:` prove the
    AR-123 gaps close. `H:` two workers on a shared fake/real backend; one runs a
    task; a directed cancel from the other stops it; a broadcast config reaches
    both. `V:` the test fails on the pre-change code and passes after.
12. **Docs + scope note.** `W:` `AGENTS.md:50`, `docs/remote_config.md:87`,
    `docs/ROADMAP.md:36-55`, `docs/architecture/*`.
    `Y:` the single-worker limitation is lifted. `H:` document the two addresses
    per transport, the control-never-retried rule, the tail-seek/retention model,
    and remove the D0 boundary language. `V:` re-read each edited doc against
    §2/§9.

---

## Handoff Plan

1. **`TaskStatus.instance_id`** — `task_handler/schemas.py:167`: add
   `instance_id: str = ""`; `task_status.py:23,49`: add `instance_id` param to
   both builders and pass through; `valkey/tracking.py:80-96` and
   `mqtt/transport.py:229-241,321,358`: pass the worker `instance_id`. Verify
   with a tracking + status unit test.
2. **Valkey config** — `valkey/config.py:291-297`: add
   `control_stream_name` / `control_broadcast_stream_name` /
   `control_stream_maxlen`; resolve with `{service}`/`{instance_id}` in
   `valkey/worker.py:141-146`.
3. **MQTT config** — `mqtt/config.py:137-155`: add `control_topic`,
   `control_broadcast_topic`, `control_qos`, `control_inbox_path`; validate
   `control_qos`; resolve in `mqtt/worker.py:193-203`.
4. **Valkey transport** — `valkey/transport.py`: add `_control_entry_ids` +
   bounded `_control_deferred` + `$`-seeded in-memory cursors; read directed and
   broadcast streams with `XREAD` in `fetch`; `XADD ... MAXLEN ~ N` on publish;
   refresh the directed TTL in `heartbeat()`; branch `ack`/`on_started`/
   `on_drain`/`requeue` on control ownership; no lease for control; remove the
   type-based scan-past-backpressure branches.
5. **MQTT worker/transport** — `mqtt/worker.py`: build `_control_inbox`, route
   control topics in `_handle_message:695`, subscribe `_control_topic` /
   `_control_broadcast_topic` in `_subscribe:448`. `mqtt/transport.py:99`: accept
   `control_inbox`; drain/recover it; route ack hooks by control-inbox
   ownership (`_control_enqueued`);
   remove the type-based scan branches
   (`fetch:202`, `recover_pending_tasks:245`) .
6. **Producer surface (v5.0.0)** — new `scietex/service/control.py`
   `ControlPublisher` Protocol + `ValkeyControlPublisher` /
   `MqttControlPublisher`; export from `service/__init__.py`.
7. **Tests** — new multi-worker integration module: directed cancel reaches the
   owner; broadcast config reaches all workers; existing control-priority tests
   still pass.
8. **Docs** — update the `AGENTS.md:50` control-plane scope, the fleet note in
   `docs/remote_config.md:87`, and `docs/ROADMAP.md:36-55`.

- **Risk:** MQTT `resolve_owner` has no read-back (§10.1) — resolved via the
  retained owner topic; keep the producer surface separable (step 6) so the
  worker-side split ships regardless.
- **Risk:** a control command left on the old data channel is silently
  single-worker-scoped (§9) — update `AGENTS.md` in the same change.
- **Test:** `ruff check src/ && ty check src/ && pytest tests/`; plus the new
  multi-worker integration test, which must fail before the change and pass
  after.
