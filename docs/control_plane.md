# Control Plane

The control plane carries the commands that steer a running fleet —
`task:cancel`, `worker:start`, `worker:stop`, `worker:restart`, `worker:exit`,
`config:apply`, `config:store`, `config:show` — on channels
separate from the data lane. It is the delivery half of AR-123: the worker
registry (see [Worker Registry](worker_registry.md)) lets a client *see* the
fleet; the control plane lets it *address* one worker or all of them.

## Built-in commands

| Command | Scope | Effect |
|---|---|---|
| `task:cancel` | directed | Cancel a running or queued task on the owning worker |
| `worker:start` | directed | Start the worker if it is not already running (idempotent) |
| `worker:stop` | directed | Stop the worker |
| `worker:restart` | directed | Stop, then start, the worker |
| `worker:exit` | directed | Request worker exit (sets `exit_requested`, then stops) |
| `config:apply` | broadcast | Apply a desired-state config envelope |
| `config:store` | broadcast | Persist a desired-state config envelope |
| `config:show` | broadcast | Report the current effective config |

`task:cancel` and the `worker:*` commands are always registered. The `config:*`
commands exist only when remote config is enabled.

A `worker:*` command targets the worker executing it, so the handler schedules
the lifecycle transition as a background task and returns immediately: the
command's result is acknowledged **before** shutdown begins. The response reports
the action as *accepted*, not as completed. `worker:stop` does not set the
`exit` event — only `worker:exit` does — so a stopped worker is observed through
its state, not through `events["exit"]`.

## The channel is the address

There is no addressing field on the command. A command's destination is the
channel it is published to, so the wire format is unchanged and a misrouted
command is structurally impossible.

| Transport | Directed (one worker) | Broadcast (every worker) |
|---|---|---|
| Valkey | `scietex:{service}:control:{instance_id}` | `scietex:{service}:control` |
| MQTT | `scietex/{service}/control/{instance_id}` | `scietex/{service}/control` |

A `task:cancel` published to a worker's directed channel cancels a task running
on that worker. A `config:apply` published to the broadcast channel reaches
every worker. The two channels are read independently of the data lane, so a
saturated data queue cannot delay a control command.

## Producer surface

`ControlPublisher` (`scietex.service.control`) is the transport-agnostic
producer protocol:

| Method | Purpose |
|---|---|
| `direct(instance_id, task_data)` | Publish to one worker's directed channel |
| `broadcast(task_data)` | Publish to the service-wide broadcast channel |
| `resolve_owner(task_id)` | Return the `instance_id` owning a task, or `None` |

`ValkeyControlPublisher` (`scietex.service.valkey.control`) and
`MqttControlPublisher` (`scietex.service.mqtt.control`) implement it. A publish
failure is **raised**, never swallowed: a silently dropped command is a silently
scoped one.

`resolve_owner` reads the owner from transport-native state:

| Transport | Source |
|---|---|
| Valkey | The tracking record's `TaskStatus.instance_id` |
| MQTT | The retained owner marker `scietex/{service}/tasks/{task_id}/owner` |

The MQTT marker is published once per task delivery, when the task first enters
`queued`; `resolve_owner` reads it back with a one-shot subscription bounded by
`DEFAULT_OWNER_RESOLVE_TIMEOUT` (2.0 s).

## Read model

Control is read with plain `XREAD` (Valkey) or a subscription (MQTT) plus an
in-memory cursor seeded to the stream tail at startup. There is **no
consumer group, no PEL, no lease, and no local cursor file**.

The tail is resolved once, at startup, to a concrete entry id (`XINFO STREAM`'s
`last-generated-id`, or `0-0` for a stream that does not exist yet). The cursor
is never left as the literal `$`: `XREAD` re-resolves `$` to the live tail on
every call, so a `$` cursor would skip any command published between polls. The
read itself is non-blocking — no `BLOCK` argument is sent.

The consequence is deliberate: a command published while a worker is down is
**skipped, not replayed**. Stale replay is structurally impossible, and control
is **never retried**. A directed command does not survive a worker restart.

## Retention

Retention mirrors the heartbeat model:

- Every control `XADD` uses `MAXLEN ~ control_stream_maxlen` (default `1000`,
  bounds `[1, 100000]`).
- The **directed** stream additionally carries an `active_ttl` expiry, refreshed
  on the heartbeat tick alongside the worker's status key, so a departed
  worker's directed stream expires on its own.
- The **broadcast** stream has `MAXLEN` only — it is shared, so no single worker
  may expire it.

## MQTT inbox partitioning

`MqttTransport` drains two `MqttInbox` instances: a data inbox and a control
inbox. The data inbox is the durable store selected by `inbox_backend`; the
control inbox is always in-memory and per-process. Control entries bypass
data-lane backpressure, so a full data queue never blocks a control command.
Routing on `on_started`/`ack`/`on_drain` is by **inbox ownership**, not by task
type: a control task that arrives on the legacy data topic lands in the data
inbox and is acked there.

## In-process lane

The transport split is orthogonal to the in-process priority lane. A command
delivered on a control channel lands on `TaskProcessor.__control_queue` and is
not counted against `max_concurrent_tasks`, which bounds the data plane only.
The lane has its own ceiling, `DEFAULT_CONTROL_CONCURRENCY = 4`.

Lane routing is **channel-driven, not type-driven**. `TaskSink` exposes two
surfaces — `enqueue_control_task` for the control lane and `enqueue_task` for
the data lane — and a transport addresses a task's lane by which surface it
calls. On the handler side, a handler declares `control: ClassVar[bool]`
(default `False`); `add_task_handler` files it in either the control registry
(`control_task_handlers`) or the data registry (`task_handlers`), and
`_find_task_handler` resolves a control command only against the control
registry.

## Configuration

| Transport | Field | Default |
|---|---|---|
| Valkey | `control_stream_name` | `scietex:{service}:control:{instance_id}` |
| Valkey | `control_broadcast_stream_name` | `scietex:{service}:control` |
| Valkey | `control_stream_maxlen` | `1000` |
| MQTT | `control_topic` | `scietex/{service}/control/{instance_id}` |
| MQTT | `control_broadcast_topic` | `scietex/{service}/control` |
| MQTT | `control_qos` | `1` |

## API

| Symbol | Module | Purpose |
|---|---|---|
| `ControlPublisher` | `scietex.service.control` | The producer Protocol (`direct`/`broadcast`/`resolve_owner`) |
| `ValkeyControlPublisher` | `scietex.service.valkey.control` | Valkey-stream producer |
| `MqttControlPublisher` | `scietex.service.mqtt.control` | MQTT 5 producer |
