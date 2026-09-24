# Worker Registry

The worker registry is a **read-only client view over the worker fleet**. Each
worker publishes an expiring heartbeat through its transport; a client watches
those heartbeats and maintains a live registry of which workers exist, what
state they are in, and when each was last seen. It is the foundation for
cross-worker control routing (AR-123): before a command can be addressed to a
specific worker, a client must be able to see the fleet.

## Overview

```python
from scietex.service import WorkerWatcher
from scietex.service.valkey import PollingBackend

watcher = WorkerWatcher(PollingBackend(client, "svc"))

async for event in watcher.watch():
    print(event.kind, event.record.instance_id, event.record.status)
```

| Feature | Description |
|---|---|
| Shared heartbeat | Both transports publish the same `Heartbeat` struct, so a client sees one record shape regardless of transport |
| Expiring liveness | Every heartbeat carries a `ttl`; a record that stops being refreshed expires on its own |
| Snapshot + stream | `snapshot()` returns the current fleet; `watch()` yields `ADDED`/`UPDATED`/`EXPIRED` events |
| Swappable backend | `PollingBackend` (Valkey `SCAN`) and `SubscribeBackend` (MQTT wildcard) implement one `WatchBackend` Protocol |
| Dependency-free core | The registry, watcher, and event types import without any optional extra |

## The heartbeat

`Heartbeat` (in `scietex.service.heartbeat`) is the single liveness record both
transports publish:

| Field | Type | Meaning |
|---|---|---|
| `service` | `str` | Service name |
| `instance_id` | `str` | Unique worker instance id |
| `status` | `"active" \| "inactive"` | Liveness state |
| `heartbeat_interval` | `float` | Seconds between heartbeats |
| `start_time` | `datetime` | Worker start time |
| `ttl` | `float` | Lifetime the consumer should apply to this record |
| `queue_depth` | `int` | Tasks waiting in the producer's data-plane queue |
| `running_tasks` | `int` | Tasks the producer is currently processing |
| `tasks_per_second` | `float` | Sliding-window completion rate |
| `timestamp` | `datetime` | Time this heartbeat was produced |

`ttl` is **required** (v5.0.0). A pre-v5 heartbeat without it is rejected by the
decoder rather than defaulted, so a stale producer cannot masquerade as live.

## TTL model

A worker's record has two lifetimes, both derived from `heartbeat_interval` and
overridable on `WorkerConfig`:

| State | Default TTL | Config field |
|---|---|---|
| `active` | `2 × heartbeat_interval` | `active_ttl` |
| `inactive` | `10 × heartbeat_interval` | `inactive_ttl` |

Both are bounded `[1, 86400]` seconds, and `active_ttl` must exceed the resolved
`heartbeat_interval` (otherwise a worker would expire between its own
heartbeats). The longer `inactive` lifetime keeps a cleanly-departed worker
visible for a while, so a client can distinguish "went away" from "never
existed".

## Lifecycle

- **Startup:** the worker publishes `status="active"` with `active_ttl`.
- **Running:** each heartbeat refreshes the record and its expiry.
- **Graceful shutdown:** the worker publishes `status="inactive"` with
  `inactive_ttl` and **never deletes** the record.
- **Ungraceful disconnect (MQTT):** the broker publishes the Last Will
  (`status="inactive"`); a reconnect before `WillDelayInterval` cancels it.
- **Silent death:** the record simply expires once its TTL elapses.

## Backends

| Backend | Transport | Mechanism | Extra |
|---|---|---|---|
| `PollingBackend` | Valkey | `SCAN` over `scietex:{service}:*:status`, then `MGET` | `scietex.service[valkey]` |
| `SubscribeBackend` | MQTT | Wildcard subscription to `scietex/{service}/workers/+` | `scietex.service[mqtt]` |

Both decode the same `Heartbeat` payload, so the watcher's behaviour is
identical. `SubscribeBackend` must be `await`ed via `start()` before the first
poll; `PollingBackend` needs no start step.

## API

| Symbol | Module | Purpose |
|---|---|---|
| `WorkerWatcher` | `scietex.service.client` | Owns a registry + backend; `snapshot()`, `watch()`, `close()` |
| `WorkerRegistry` | `scietex.service.client` | In-memory registry keyed by `instance_id` |
| `WorkerRecord` | `scietex.service.client` | A worker's last-known state (`heartbeat`, `received_at`) |
| `WorkerEvent` | `scietex.service.client` | A single observed change (`kind`, `record`) |
| `WorkerEventKind` | `scietex.service.client` | `ADDED` / `UPDATED` / `EXPIRED` |
| `WatchBackend` | `scietex.service.client` | The delivery-mechanism Protocol (`poll()`, `close()`) |
