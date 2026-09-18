# scietex.service — Architecture Map

Structural documentation for the `scietex.service` package (v4.4.0). This map
describes the system **as it currently exists**. It is a factual baseline for
a later architectural review; it deliberately does not propose changes.

## Project in one paragraph

`scietex.service` is an **asyncio-based Python framework** for building
background daemon/worker services. It provides a three-level class hierarchy:

```
BasicWorker          (foundation: signals, async logging, manager runtime)
 └── TaskProcessor   (in-process task queue, handler dispatch, timeouts)
      ├── ValkeyWorker    (Valkey/Redis stream transport via glide)
      └── MqttWorker      (MQTT 5 topic transport via aiomqtt)
```

All runtime behavior is single-process, single-threaded `asyncio`. Work is
structured as **managers**: infinite async loops (e.g. heartbeat, watchdog,
task intake, task dispatch) that are discovered via class inspection and run as
named `asyncio.Task`s. A secondary subsystem (`task_handler`) defines a
pluggable handler contract plus typed `msgspec.Struct` schemas. Remote
configuration (`config_reload.py`) is a core subsystem that both transports
feed through `ConfigSource`. The Valkey and MQTT layers are optional:
import-time errors inside `scietex.service.valkey` and `scietex.service.mqtt`
are swallowed so the core package imports without `valkey-glide` or `aiomqtt`.

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
