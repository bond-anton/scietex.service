"""Single-process Valkey task-consumption throughput benchmark.

Preloads ``N`` tasks into the Valkey task stream with a producer client, then
starts a ``ValkeyWorker`` and times only the drain: the period from
``worker.start()`` until every preloaded task has been acknowledged and deleted.
Requires a running Valkey server and the ``valkey`` extra:

    pip install "scietex.service[valkey]"

Run with ``python -m examples.valkey_perf --tasks 10000``.
"""

import argparse
import asyncio
import logging
import time
from uuid import UUID, uuid4

from glide import GlideClient

from scietex.service import (
    ValkeyAdvancedConfig,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyWorker,
    ValkeyWorkerConfig,
)
from scietex.service.task_handler import TaskData, TaskHandler, TaskResult, encode_task_envelope
from scietex.service.valkey.config import generate_glide_config


class PerfHandler(TaskHandler):
    """No-op handler that returns an instant success for every ``perf`` task."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["perf"]

    async def handle(self, task_data: TaskData) -> TaskResult:
        return TaskResult(status="success")


class PerfWorker(ValkeyWorker):
    """Valkey worker that signals drain completion and records steady-state rates."""

    def __init__(self, config: ValkeyWorkerConfig, total: int) -> None:
        super().__init__(config)
        self._total = total
        self._completed = 0
        # Fires once every preloaded task has been acked and deleted, so the
        # stream and pending list are empty when the caller resumes.
        self.all_done = asyncio.Event()
        # Sample the drain every ~5% so a median steady-state rate can
        # discount startup ramp-up and the tail-end drain.
        self._sample_step = max(1, total // 20)
        self._samples: list[tuple[float, int]] = []

    @property
    def samples(self) -> list[tuple[float, int]]:
        """``(monotonic seconds, completed count)`` samples recorded during the drain."""
        return self._samples

    async def on_task_completed(
        self,
        task_id: UUID,
        task_data: TaskData,
        task_result: TaskResult | None,
    ) -> None:
        await super().on_task_completed(task_id, task_data, task_result)
        # Single event loop, no await between increment and check: the count is
        # always accurate when the completion event is inspected.
        self._completed += 1
        if self._completed % self._sample_step == 0:
            self._samples.append((time.monotonic(), self._completed))
        if self._completed >= self._total:
            self.all_done.set()


def build_valkey_config(host: str, port: int) -> ValkeyConfig:
    """Build a programmatic ``ValkeyConfig`` for the given host/port."""
    return ValkeyConfig(
        base_config=ValkeyBaseConfig(
            nodes=[ValkeyNode(host=host, port=port)],
            request_timeout=10_000,
        ),
        advanced_config=ValkeyAdvancedConfig(
            connection_timeout=10_000,
            tcp_nodelay=True,
        ),
    )


async def load_tasks(client: GlideClient, stream_name: str, n: int) -> int:
    """Preload ``n`` tasks as one XADD per entry, then return the stream length.

    Each task is its own stream entry (field = task UUID, value = encoded
    envelope) so the worker acknowledges and deletes exactly one entry per
    completed task. Packing multiple field/value pairs into a single XADD would
    share one entry id and corrupt the worker's per-entry ack/delete bookkeeping.
    """
    for _ in range(n):
        task_id = str(uuid4()).encode("utf-8")
        await client.xadd(stream_name, [(task_id, encode_task_envelope(TaskData(task="perf")))])
    return await client.xlen(stream_name)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the benchmark."""
    parser = argparse.ArgumentParser(
        description="Preload N tasks into a Valkey stream, then drain them with a single ValkeyWorker."
    )
    parser.add_argument("-n", "--tasks", type=int, default=10_000, help="Total tasks preloaded then drained")
    parser.add_argument("--host", default="localhost", help="Valkey host")
    parser.add_argument("--port", type=int, default=6379, help="Valkey port")
    parser.add_argument("--service-name", default="ValKeyPerfService", help="Derives the stream/group key")
    parser.add_argument("--max-concurrent-tasks", type=int, default=100, help="max_concurrent_tasks")
    parser.add_argument(
        "--queue-size",
        type=int,
        default=None,
        help="queue_size; defaults to --tasks so the whole backlog buffers without back-pressure",
    )
    parser.add_argument("--task-fetch-batch-size", type=int, default=100, help="XREADGROUP count")
    parser.add_argument("--task-timeout", type=float, default=300.0, help="Per-task timeout in seconds")
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=600.0,
        help="Heartbeat interval in seconds (capped at the config max of 600)",
    )
    parser.add_argument("--keep-stream", action="store_true", help="Skip the pre-load stream flush")
    return parser.parse_args(argv)


def _median_steady_state_rate(samples: list[tuple[float, int]]) -> float | None:
    """Median per-window completion rate from ``(time, count)`` samples."""
    if len(samples) < 2:
        return None
    rates = sorted((count2 - count1) / (t2 - t1) for (t1, count1), (t2, count2) in zip(samples, samples[1:]))
    mid = len(rates) // 2
    if len(rates) % 2:
        return rates[mid]
    return (rates[mid - 1] + rates[mid]) / 2


async def run(args: argparse.Namespace) -> None:
    """Preload the stream, drain it, and report the timed drain throughput."""
    valkey_config = build_valkey_config(args.host, args.port)
    queue_size = args.queue_size if args.queue_size is not None else args.tasks
    config = ValkeyWorkerConfig(
        service_name=args.service_name,
        version="0.0.1",
        logging_level=logging.WARNING,
        heartbeat_interval=args.heartbeat_interval,
        valkey_config=valkey_config,
        queue_size=queue_size,
        max_concurrent_tasks=args.max_concurrent_tasks,
        task_fetch_batch_size=args.task_fetch_batch_size,
        task_timeout=args.task_timeout,
    )
    stream_name = f"scietex:{args.service_name}:tasks"

    producer = await GlideClient.create(
        generate_glide_config(
            valkey_config,
            service_name=args.service_name,
            worker_id="perf-producer",
            listening=False,
        )
    )
    if not args.keep_stream:
        # Flush only this benchmark's stream so stale unacked pending entries
        # from an interrupted prior run do not skew the drain count.
        await producer.delete([stream_name])
    loaded = await load_tasks(producer, stream_name, args.tasks)
    if loaded != args.tasks:
        raise RuntimeError(f"Preload mismatch: loaded {loaded} of {args.tasks} tasks")
    print(f"Preloaded {loaded} tasks into {stream_name}")
    await producer.close()

    worker = PerfWorker(config, total=args.tasks)
    worker.add_task_handler(PerfHandler)

    t0 = time.perf_counter()
    await worker.start()
    await worker.all_done.wait()
    t1 = time.perf_counter()

    total_seconds = t1 - t0
    print(f"Drained {args.tasks} tasks in {total_seconds:.3f}s")
    print(f"Throughput: {args.tasks / total_seconds:.1f} tasks/sec")
    median = _median_steady_state_rate(worker.samples)
    if median is not None:
        print(f"Steady-state throughput (median): {median:.1f} tasks/sec")
    print(
        "Config: "
        f"max_concurrent_tasks={args.max_concurrent_tasks}, "
        f"queue_size={queue_size}, "
        f"task_fetch_batch_size={args.task_fetch_batch_size}, "
        f"task_timeout={args.task_timeout}s, "
        f"heartbeat_interval={args.heartbeat_interval}s"
    )

    await worker.exit()
    await worker.events["exit"].wait()


def main(argv: list[str] | None = None) -> None:
    asyncio.run(run(parse_args(argv)))


if __name__ == "__main__":
    main()
