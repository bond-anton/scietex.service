# scietex.service Documentation

## Core Components

- [BasicAsyncWorker](./basic_async_worker.md) — Foundation async daemon worker with signal handling, logging, heartbeat and watchdog managers
- [AsyncTaskProcessor](./async_task_processor.md) — Concurrent task processing framework built on BasicAsyncWorker with task queue and handler dispatch
- [ValkeyWorker](./valkey_async_worker.md) — Valkey-backed task processor with stream-based task distribution and heartbeat publishing

## Task System

- [Task Handler](./task_handler.md) — Pluggable task handler architecture with typed schemas for task data, results, and timeouts
