# Roadmap

Planned work for future major versions. Items here are **not** committed to a
release date; they are tracked so architectural decisions made in earlier
versions are not lost. Each entry cites the review finding that motivated it.

## v4 — Multi-replica / shared-queue topology

**Motivation:** AR-023 (docs/reviews/architecture/2026-09-06.md). In v3 a
service runs a **single worker**; the Valkey stream/group/consumer key space
embeds `worker_id`, so horizontal scale-out would require replicas to share a
`worker_id`, defeating identity. This is a deliberate v3 constraint, not a bug.

**Planned change:** separate the key space into two namespaces so multiple
replicas can consume one shared queue:

- stream: `scietex:{service}:tasks` (service-scoped, shared across replicas)
- group: `scietex:{service}:task_group` (service-scoped)
- consumer: `scietex:{service}:{worker_id}` (worker-scoped)
- status/heartbeat key: `scietex:{service}:{worker_id}:status` (unchanged)

**Breaking:** existing deployed streams/groups under the old per-`worker_id`
names will be orphaned. Consumers must drain/ack old streams before deploying,
or accept redelivery from the old group. Requires a major-version bump.

**Open questions to resolve before design:**
- Delivery semantics across replicas (at-least-once already holds; confirm
  ordering guarantees are not required across consumers).
- Whether `worker_id` remains a meaningful identity when replicas share a queue,
  or whether a separate replica/instance id is needed for status keys.

## v4 — Single shared GlideClient (connection lifecycle)

**Motivation:** AR-018 (docs/reviews/architecture/2026-09-06.md). `ValkeyWorker`
currently runs two independent `GlideClient` lifecycles: its own task client and
the external `scietex.logging` `AsyncValkeyHandler`'s logging client. True
unification is blocked on the external package gaining a client-injection seam.

**Planned change:** once `scietex.logging` accepts an injected `GlideClient`,
flip the `share_glide_client` feature-flag seam (added in v3.x) to pass the
worker's client through to the handler, giving one connection lifecycle and one
teardown owner.

## v4 — Task registration reconciled with task types

**Motivation:** AR-022 (docs/reviews/architecture/2026-09-06.md). Registration
keys passed to `add_task_handler` are unrelated to the task types a handler
declares via `supported_tasks`; dispatch is first-match over `supports()`. The
key-based registration API is deprecated in v3.x in favor of type-based
registration; remove the deprecated key-based path in v4.
