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
- consumer: `scietex:{service}:{instance_id}` (worker-scoped)
- status/heartbeat key: `scietex:{service}:{instance_id}:status` (worker-scoped)
- worker registry: `scietex:{service}:workers` (service-scoped set; SADD on
  startup, SREM on shutdown; liveness is the status-key TTL, not set membership)
- `XAUTOCLAIM` recovery floor raised to `DEFAULT_CLAIM_MIN_IDLE_MS = 1000` so a
  replica's startup recovery does not claim entries a slow-but-alive handler on
  another replica is still processing.

**Breaking:** existing deployed streams/groups under the old per-`worker_id`
names will be orphaned. Consumers must drain/ack old streams before deploying,
or accept redelivery from the old group. Requires a major-version bump.

**Open questions to resolve before design:**
- Delivery semantics across replicas (at-least-once already holds; confirm
  ordering guarantees are not required across consumers).
- Whether `worker_id` remains a meaningful identity when replicas share a queue,
  or whether a separate replica/instance id is needed for status keys.

**Status: implemented** in v4.0.0 (commits `2ebc58e`, `b7b58fc`).

## v4 — Task registration reconciled with task types

**Motivation:** AR-022 (docs/reviews/architecture/2026-09-06.md). Registration
keys passed to `add_task_handler` are unrelated to the task types a handler
declares via `supported_tasks`; dispatch is first-match over `supports()`. The
key is a lifecycle handle, not a dispatch key.

**Decision (v4):** handlers are stateless by design — a handler class is
registered once per worker and holds no per-instance configuration. The
user-supplied `handler_name` key and the `supported_tasks` override argument
are removed: `add_task_handler` takes only the handler class. The lifecycle key
is derived from `handler_class.__name__` (single instance per class); a
duplicate class name raises. The class-level `supported_tasks` declaration is
kept — it is the dispatch contract (`_find_task_handler` routes by
`supports()` membership), not an argument. This drops the multi-key /
multi-instance-per-class capability, which was never exercised (all handlers
are stateless) and which the stateless model does not need.

**Breaking:** the `add_task_handler(handler_name, handler_class, supported_tasks=None)`
signature becomes `add_task_handler(handler_class)`; code that registered the
same class under multiple keys, relied on a custom lifecycle name, or passed a
`supported_tasks` override must adapt. Requires a major-version bump.

**Status: implemented** in v4.0.0 (commits `3400420`, `9c7689f`).

## v4 — Error-policy enforcement on task results

**Motivation:** AR-022 (docs/reviews/architecture/2026-09-06.md). The v3 error
taxonomy on `TaskResult` (`retryable`, `requeue`, `retry_count`, `partial`,
`error_code`) is inert: `process_task` produces it, but `handle_task` and the
watchdog ignore it, so the framework cannot act on a handler's retry intent.
The watchdog docstring flags error-path requeue as future work gated on result
availability.

**Decision (v4):** the framework executes a retry **once** per task. When a
task fails (`status="error"`) with `retryable=True`, `handle_task` requeues it
via `return_task_to_queue` before acking the transport entry (XADD then XACK,
preserving at-least-once without duplication). Permanent failures
(`retryable=False`) are acked and dropped. The worker owns only the *execution*
of the requeue; the handler owns the *intent* (only it knows transient vs
permanent). No retry count, cap, or backoff in v4 — retry policy beyond the
single retry is left to the transport/handler.

**Taxonomy simplification (breaking):** under retry-once, `retryable` is the
single retry signal. The `requeue` and `retry_count` fields are redundant
(second ways to express the same intent, nothing reads/writes them) and are
dropped from `TaskResult`. `partial` and `error_code` are kept — they are
orthogonal progress/error-reporting fields, not retry fields.

**Raise-path change (breaking):** the default that marks a handler which
*raises* as `retryable=True` is removed. A handler that raises is treated as
permanent (`retryable=False`) unless it explicitly returns a `retryable=True`
result. This removes the infinite-requeue hazard an unhandled exception would
otherwise create under retry-once.

**Breaking:** `TaskResult` schema change (drop `requeue`, `retry_count`) and the
raise-path retryability flip. Requires a major-version bump.

**Status: implemented** in v4.0.0 (commits `3a49b9c`, `fa6a8cc`, `45923d8`).
