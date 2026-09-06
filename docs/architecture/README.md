# scietex.service — Architecture Map

Structural documentation for the `scietex.service` package (v3.1.0, commit
`5e2f079`). This map describes the system **as it currently exists**. It is a
factual baseline for a later architectural review; it deliberately does not
propose changes.

## Project in one paragraph

`scietex.service` is an **asyncio-based Python framework** for building
background daemon/worker services. It provides a three-level class hierarchy:

```
BasicAsyncWorker          (foundation: signals, async logging, manager runtime)
 └── AsyncTaskProcessor   (in-process task queue, handler dispatch, timeouts)
      └── ValkeyWorker    (Valkey/Redis stream transport via glide)
```

All runtime behavior is single-process, single-threaded `asyncio`. Work is
structured as **managers**: infinite async loops (e.g. heartbeat, watchdog,
task intake, task dispatch) that are discovered via class inspection and run as
named `asyncio.Task`s. A secondary subsystem (`task_handler`) defines a
pluggable handler contract plus typed `msgspec.Struct` schemas. The Valkey
layer is optional: import-time errors inside `scietex.service.valkey` are
swallowed so the core package imports without `valkey-glide`.

## Architecture documents

| Document | Scope |
|---|---|
| [`overview.md`](./overview.md) | Major subsystems, responsibilities, interaction, entry points, runtime processes |
| [`structure.md`](./structure.md) | Repository/package layout, module responsibilities, boundaries |
| [`components.md`](./components.md) | Per-component purpose, classes, interfaces, dependency relations |
| [`dependencies.md`](./dependencies.md) | Architectural dependency graph and directions |
| [`data-flow.md`](./data-flow.md) | Important data flows (tasks, logs, config, heartbeats) |
| [`lifecycle.md`](./lifecycle.md) | Startup, runtime, shutdown, cleanup, ownership |
| [`hotspots.md`](./hotspots.md) | Areas needing deeper architectural investigation |

## Related material

- Top-level usage documentation and public API reference: [`README.md`](../../README.md)
- Component usage guides (may drift from code; see [`hotspots.md`](./hotspots.md) §H12): [`docs/`](../index.md)
- Developer/build conventions: [`AGENTS.md`](../../AGENTS.md)
