# Roadmap

Planned work for future major versions. Items here are **not** committed to a
release date; they are tracked so architectural decisions made in earlier
versions are not lost. Each entry cites the review finding that motivated it.

## v4.5.0 — Remote configuration

**Motivation:** operators change worker behaviour by editing `valkey.yml`/
`mqtt.yml` or redeploying code; task-processing fields (timeouts, concurrency,
cadence) live only in the `TaskProcessorConfig` constructor. A
transport-delivered configuration channel plus three operator commands
(`config:apply`, `config:store`, `config:show`) lets a running worker be
reconfigured without a restart.

**Decision (v4.5.0):** add a transport-agnostic `ConfigReloader` (core) owning
the validate-before-swap apply/reload/store/show pipeline, with one durable
desired-state location per transport — Valkey key `scietex:{service}:config`
(`GET`/`SET`) and MQTT retained topic `scietex/{service}/config`. Only the eight
core fields in `RELOADABLE_FIELDS` are hot-reloadable; everything else is
restart-required. Opt-in via `TaskProcessorConfig.remote_config_enabled`
(default `False`). Precedence at startup: constructor config < `config.yml` <
remote source; an invalid remote config never fails startup
(availability-first). `register_config_settings(name, struct_type, apply=...)`
is the service-side extension point.

**Status: implemented** (v4.5.0, merged to `main`).
See [docs/design/remote_config.md](design/remote_config.md) and
[docs/remote_config.md](remote_config.md).

**Follow-up (v4.5.0):** AR-100 — the eight reloadable fields now resolve through
a single pure `resolve_reloadable_settings` into one `self._effective` snapshot
on `TaskProcessor`; the private reloadable shadows and the duplicated
`None`/`auto_tune` resolution are removed. No public API change.

**Follow-up (v4.5.0):** AR-102 + AR-113 — the shared `TransportWorker` base
(`src/scietex/service/transport_worker.py`) now owns the client-lock/reconnect
pattern, `TransportHealth` construction, the startup config-apply pipeline, and
the watchdog glue; `ValkeyWorker`/`MqttWorker` extend it and keep only
broker-specific methods. The `TaskTransport` Protocol is reconciled to 8
methods: the dead `release` method is removed and `refresh_leases()`/
`recover_pending_tasks(sink)` are promoted to required Protocol members, so
workers stay protocol-typed with no concrete down-cast.

**Follow-up (v4.5.0):** AR-114 — the per-task `TaskStatus` field-population
matrix now lives once in the core module `src/scietex/service/task_status.py`
(`build_running_status`/`build_terminal_status`); `TaskStatusStore` and
`MqttTransport` both delegate to it, so the two transports can no longer drift.
A parametrized cross-transport equivalence test pins the invariant.

**Follow-up (v4.5.0):** AR-105 — a `ConfigManager` collaborator
(`src/scietex/service/config_manager.py`, internal, not exported) now owns the
`ConfigReloader`, the local `config.yml` path/reads/writes, the attached
`ConfigSource`, and the three `config:*` handler callbacks; `TaskProcessor`
delegates the config lifecycle to it. The three `config:*` handlers are
registered only when `remote_config_enabled=True` — on a disabled worker a
`config:*` task is answered with the permanent "No handler found" result.

**Follow-up (v4.5.0):** AR-104 — bounded timeout-driven requeue: a new
restart-required `TaskProcessorConfig.max_timeout_requeues` ceiling (default 1)
caps the watchdog's timeout requeue loop through a distinct
`TaskExecutor._timeout_requeues` budget; on reaching the ceiling the entry is
acked terminal instead of redelivered indefinitely.

**Follow-up (v4.5.0):** AR-111 — reloader state now resets per run: the new
`ConfigReloader.reset()` clears the run-scoped apply bookkeeping,
`apply_envelope(trusted=True)` skips signature verification for the trusted
local artifact, and `ConfigManager.reset()` (delegating to the reloader) is
called from `TaskProcessor.initialize()` at the run boundary, with
`MqttConfigSource.reset()` clearing the MQTT snapshot — so a reused worker
instance no longer drops the persisted `config.yml` on a second `start()`.

**Follow-up (v4.5.0):** AR-106 — the worker lifecycle state machine is now
guarded: `WorkerLifecycle.state` is no longer settable, and state moves only
through `transition(new_state)` (validated against an allowed-edge table;
illegal edges raise `InvalidStateTransition`) or the unguarded `force_stopped()`
terminal escape. `_wait_until_stopped()` awaits the `_stopped` event (set iff
STOPPED) instead of the 100 ms poll. `ManagerRuntime.stop_managers(reverse=True)` and
`BasicWorker._stop_managers_best_effort()` (called from both cancellation
handlers) unwind managers in reverse start order before forcing STOPPED, so a
cancelled orchestrator no longer strands running managers under STOPPED.

**Follow-up (v4.5.0):** AR-107 — manager registration now converges on one
path. The descriptor-free `ManagerDefinition` value (`__slots__ = ("name",
"method", "cleanup", "owner", "attribute_name")`) is the only type stored in
`MANAGER_REGISTRY_ATTR`, produced by the single private `_record_definition`
primitive that both `Manager.__set_name__` and `register_manager` funnel
through; `Manager` keeps its decorator + descriptor roles and
`ManagerRuntime.iter_manager_definitions()` yields `(name, ManagerDefinition)`.
`BasicWorker`'s built-in Heartbeat/Watchdog managers are now `@Manager`-decorated
methods (`_heartbeat_manager`/`_watchdog_manager`) on the class, and
`register_manager` is a compatibility shim with no in-tree production caller.

**Follow-up (v4.5.0):** AR-116 — the config error taxonomy is now symmetric: a
new permanent `CONFIG_SOURCE_NOT_CONFIGURED` code represents "no source
attached", so the transient "transport down / no config yet" condition is
carried solely by `CONFIG_SOURCE_UNAVAILABLE`; the retryable set is centralized
as `RETRYABLE_ERROR_CODES = frozenset({CONFIG_SOURCE_UNAVAILABLE})`, and
`ConfigReloader.store` now maps a remote store failure to
`CONFIG_SOURCE_UNAVAILABLE` (retryable during a network blip) while
`CONFIG_STORE_FAILED` is local-disk-only.

## v4.4.0 — MQTT transport

**Motivation:** the framework ships a Valkey transport but no broker-agnostic
alternative. MQTT 5 is a natural second backend: it is widely deployed, has
native user properties (so the task id can travel without touching the
`TaskEnvelope` wire format), and needs no server-side key space.

**Decision (v4.4.0):** add `MqttWorker`/`MqttTransport` mirroring the
`ValkeyWorker`/`ValkeyTransport` split. The transport reuses the core
`TaskTransport`/`TaskSink` protocols, the versioned `TaskEnvelope` wire format,
and the core `TransportHealth` supervisor (hoisted from `valkey/health.py` to
`src/scietex/service/health.py` and re-exported for back-compat). MQTT 5 only;
the task id travels as the `scietex-task-id` user property. Because aiomqtt
v2.5.1 auto-acks at the broker when `on_message` returns, wire QoS 2 is
at-most-once at the application layer; a durable file-backed inbox
(`FileMqttInbox`, behind the `MqttInbox` Protocol) restores at-least-once by
persisting every received message before handing it to the processor and
deduping on replay via tombstones. `inbox_backend="memory"` (or its alias
`"none"`) is the explicit at-most-once opt-out, backed by `MemoryInbox`. There
is no status store: `MqttTransport` publishes retained `TaskStatus` messages and
throttled `TaskProgress` messages to per-task topics (a publisher, not a store —
no read-back API), gated by `status_publish_enabled`; progress also remains
in-process via `TaskCapabilities`. Registry/heartbeat use
retained-message topics (`scietex/{service}/workers/{instance_id}`), and the
log handler owns its own connection, matching `AsyncValkeyHandler`.

**Status: implemented** (v4.4.0, merged to `main`).
See [docs/design/mqtt_worker.md](design/mqtt_worker.md) and
[docs/mqtt_worker.md](mqtt_worker.md).

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
- `XAUTOCLAIM` recovery floor raised to `claim_min_idle_ms` (default 1000 ms) so a
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

> **Follow-up (AR-053):** the optional keyword-only `name` was later
> re-introduced. The current signature is
> `add_task_handler(handler_class, *, name: str | None = None)` — the
> lifecycle key is the resolved name (`name` if given, otherwise
> `handler_class.__name__`), re-enabling multiple named instances of one
> class. The class-level `supported_tasks` declaration remains the dispatch
> contract (selection is by `supports()` membership, not the key).

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

> **Correction (retry cap now enforced).** The original decision above left the
> "single retry" as an intent with no enforcement: a handler that kept returning
> `retryable=True` was requeued forever. The cap is now enforced in
> `handle_task` as an in-memory per-task-id budget
> (`_MAX_TASK_RETRIES = 1`, `self._retry_attempts`). A second consecutive
> retryable failure is acked as terminal with `retryable=False` (so a durable
> transport does not leave the entry pending for a retry that never comes). The
> budget adds no schema field and is per-execution: a durable transport
> redelivering a previously-requeued task after a restart starts a fresh budget.
> The watchdog's timeout-driven requeue is a separate axis, not gated by this
> budget.

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

## v4 — logging and manager modules grouped into subpackages

**Motivation:** the logging and manager subsystems each lived as two loose
top-level modules (`logging.py` + `logging_lifecycle.py`, `manager.py` +
`manager_runtime.py`), splitting one component's code across unrelated files
and obscuring ownership.

**Decision (v4):** group each component into its own subpackage —
`LoggingLifecycle` moves to `log_handlers/lifecycle.py` and `ManagerRuntime` to
`manager/runtime.py` — with each `__init__.py` re-exporting its sibling
module's public API. Pure move, no logic changes.

**Status: implemented** in v4.0.0 (commit `91b0afe`).

> **Follow-up (AR-065):** the `__init__.py` re-exports this grouping introduced
> (`ManagerRuntime` from `manager/__init__.py`, `LoggingLifecycle` from
> `log_handlers/__init__.py`) caused circular imports and were removed. These
> symbols are now importable only from their real homes
> (`scietex.service.manager.runtime.ManagerRuntime`,
> `scietex.service.log_handlers.lifecycle.LoggingLifecycle`).

## v4 — migrate to scietex.logging 2.0.0 API

**Motivation:** upstream `scietex.logging` released a 2.0.0 API that removed
the `AsyncBaseHandler` class the worker's async logging was built on.

**Decision (v4):** replace the removed `AsyncBaseHandler` with
`ConsoleHandler`/`AsyncLoggingHandler`, drop `service_name`/`worker_id`/
`stdout_enable` from `AsyncValkeyHandler`, and bump the `scietex.logging`
floor to `>=2.0.0` across pyproject, source, tests, and docs.

**Status: implemented** in v4.0.0 (commit `03f1f9a`).
