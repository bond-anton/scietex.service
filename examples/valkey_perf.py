"""Valkey task-consumption throughput benchmark with an optional separate-process producer.

Preloads ``N`` tasks into the Valkey task stream with a producer client, then
starts a ``ValkeyWorker`` and drains the stream. Requires a running Valkey
server and the ``valkey`` extra:

    pip install "scietex.service[valkey]"

Run with ``python -m examples.valkey_perf --tasks 10000``.

Two producer modes (``--producer-mode``, default ``process``):

- ``process`` -- the producer runs in a separate OS process (this script
  re-invoked with a hidden ``--producer`` flag), so its publish cost does not
  contend with the worker's event loop. A ready/go handshake over the child's
  stdin/stdout excludes child startup and connect from the timed window, and
  the parent waits on the producer and the drain concurrently, so the wall
  window is ``max(producer, drain)``. The stream is flushed first, then the
  worker starts and the producer publishes concurrently. This redefines the
  metric as publish + drain, making it comparable to ``mqtt_perf.py``, which
  cannot preload tasks before the worker subscribes.
- ``inline`` -- the producer preloads the whole stream in this process before
  the worker starts, reproducing the historical single-process behavior. The
  timed window is drain-only: from ``worker.start()`` until every preloaded
  task has been acknowledged and deleted.

Two timing windows are reported: the producer's own publish/preload loop
(``Producer publish``) and the timed wall window (``Drained`` / ``Throughput``).
"""

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
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
from scietex.service.task_handler import (
    CancelReason,
    TaskCapabilities,
    TaskData,
    TaskHandler,
    TaskResult,
    encode_task_envelope,
)
from scietex.service.valkey.config import generate_glide_config

_SCRIPT = str(Path(__file__).resolve())
_READY = "PERF_PRODUCER_READY"
_RESULT_PREFIX = "PERF_PRODUCER_RESULT"


@dataclass
class ProducerResult:
    """Publish result reported by the producer child over stdout."""

    count: int
    seconds: float


class PerfHandler(TaskHandler):
    """No-op handler that returns an instant success for every ``perf`` task."""

    @property
    def supported_tasks(self) -> list[str]:
        return ["perf"]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
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
        *,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        await super().on_task_completed(task_id, task_data, task_result, cancel_reason=cancel_reason)
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


async def _flush_stream(valkey_config: ValkeyConfig, stream_name: str, service_name: str) -> None:
    """Delete the benchmark stream so stale entries from a prior run are cleared."""
    client = await GlideClient.create(
        generate_glide_config(
            valkey_config,
            service_name=service_name,
        )
    )
    await client.delete([stream_name])
    await client.close()


async def run_producer(args: argparse.Namespace) -> None:
    """Child entry point: connect, preload, and report a single result line.

    Stdout carries only the handshake and result lines; logging stays on the
    inherited stderr so the parent's protocol channel is never polluted. The
    child never flushes the stream -- it only XADDs tasks.
    """
    if args.tasks <= 0:
        print(f"{_RESULT_PREFIX} count=0 seconds=0", flush=True)
        return
    valkey_config = build_valkey_config(args.host, args.port)
    client = await GlideClient.create(
        generate_glide_config(
            valkey_config,
            service_name=args.service_name,
        )
    )
    stream_name = f"scietex:{args.service_name}:tasks"
    print(_READY, flush=True)
    # Block until the parent sends the go byte, so startup and connect stay
    # out of the timed window.
    await asyncio.to_thread(sys.stdin.readline)
    t0 = time.perf_counter()
    await load_tasks(client, stream_name, args.tasks)
    seconds = time.perf_counter() - t0
    await client.close()
    print(f"{_RESULT_PREFIX} count={args.tasks} seconds={seconds:.6f}", flush=True)


async def _start_producer(args: argparse.Namespace) -> tuple[asyncio.subprocess.Process, float]:
    """Spawn the producer child, handshake, and return ``(proc, t0)``.

    ``t0`` is captured after the child signals readiness and before the go byte
    is sent, so child startup and connect are excluded from the timed window.
    """
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        _SCRIPT,
        "--producer",
        "--tasks",
        str(args.tasks),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--service-name",
        args.service_name,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        # stderr is inherited so child logs never pollute the protocol channel.
    )
    assert proc.stdout is not None
    assert proc.stdin is not None
    try:
        ready_line = await asyncio.wait_for(proc.stdout.readline(), timeout=args.handshake_timeout)
    except asyncio.TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"producer did not signal readiness within {args.handshake_timeout:.1f}s") from exc
    if not ready_line:
        await proc.wait()
        raise RuntimeError(f"producer exited before signaling readiness (status {proc.returncode})")
    if ready_line.decode().strip() != _READY:
        raise RuntimeError(f"unexpected producer handshake line: {ready_line.decode().strip()!r}")
    t0 = time.perf_counter()
    proc.stdin.write(b"\n")
    await proc.stdin.drain()
    proc.stdin.close()
    return proc, t0


async def _collect_producer(proc: asyncio.subprocess.Process) -> ProducerResult:
    """Read the producer's result line from stdout and reap the process."""
    assert proc.stdout is not None
    stdout = await proc.stdout.read()
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"producer exited with status {proc.returncode}")
    for raw in stdout.decode().splitlines():
        line = raw.strip()
        if not line.startswith(_RESULT_PREFIX):
            continue
        payload = line[len(_RESULT_PREFIX) :].strip()
        fields = dict(pair.split("=", 1) for pair in payload.split())
        try:
            count = int(fields["count"])
            seconds = float(fields["seconds"])
        except (KeyError, ValueError) as exc:
            raise RuntimeError(f"malformed producer result: {line!r}") from exc
        return ProducerResult(count=count, seconds=seconds)
    raise RuntimeError("producer did not report a result")


async def _terminate_producer(proc: asyncio.subprocess.Process | None) -> None:
    """Terminate and reap the child if it is still running."""
    if proc is None or proc.returncode is not None:
        return
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


async def _wait_drained(worker: PerfWorker, timeout: float | None) -> None:
    """Wait for the drain to complete, bounded by ``timeout`` (``None`` = forever)."""
    if timeout is None:
        await worker.all_done.wait()
        return
    await asyncio.wait_for(worker.all_done.wait(), timeout=timeout)


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
    parser.add_argument(
        "--producer-mode",
        choices=["inline", "process"],
        default="process",
        help="Run the producer in this process (inline) or a separate OS process (process, default)",
    )
    parser.add_argument(
        "--handshake-timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for the child producer to signal readiness (process mode)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=None,
        help="Seconds to wait for the drain to complete; None waits forever",
    )
    parser.add_argument("--producer", action="store_true", help=argparse.SUPPRESS)
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
    """Publish the tasks, drain them, and report the timed throughput."""
    if args.tasks <= 0:
        print("Nothing to do: --tasks is zero.")
        return

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

    # Flush before the worker starts so stale unacked entries from an
    # interrupted prior run do not skew the drain count. The child producer
    # never flushes; only XADDs.
    if not args.keep_stream:
        await _flush_stream(valkey_config, stream_name, args.service_name)

    worker = PerfWorker(config, total=args.tasks)
    worker.add_task_handler(PerfHandler)

    proc: asyncio.subprocess.Process | None = None
    producer_count = args.tasks
    producer_seconds = 0.0
    try:
        if args.producer_mode == "process":
            # The producer runs concurrently with the worker: start the worker,
            # then publish while it drains. The timed window covers the full
            # publish + drain pipeline.
            await worker.start()
            proc, t0 = await _start_producer(args)
            producer_result, _ = await asyncio.gather(
                _collect_producer(proc),
                _wait_drained(worker, args.drain_timeout),
            )
            producer_count = producer_result.count
            producer_seconds = producer_result.seconds
        else:
            # Historical single-process behavior: preload the whole stream,
            # then drain it. Only the drain is timed.
            producer = await GlideClient.create(
                generate_glide_config(
                    valkey_config,
                    service_name=args.service_name,
                )
            )
            preload_start = time.perf_counter()
            loaded = await load_tasks(producer, stream_name, args.tasks)
            producer_seconds = time.perf_counter() - preload_start
            await producer.close()
            if loaded != args.tasks:
                raise RuntimeError(f"Preload mismatch: loaded {loaded} of {args.tasks} tasks")
            print(f"Preloaded {loaded} tasks into {stream_name}")
            t0 = time.perf_counter()
            await worker.start()
            await _wait_drained(worker, args.drain_timeout)
        t1 = time.perf_counter()
    except asyncio.TimeoutError:
        print(f"Drain timed out after {args.drain_timeout:.1f}s; benchmark inconclusive.")
        return
    finally:
        await _terminate_producer(proc)
        await worker.exit()
        await worker.events["exit"].wait()

    total_seconds = t1 - t0
    producer_rate = producer_count / producer_seconds if producer_seconds > 0 else 0.0
    print(f"Drained {args.tasks} tasks in {total_seconds:.3f}s")
    print(f"Producer publish: {producer_count} tasks in {producer_seconds:.3f}s ({producer_rate:.1f}/s)")
    throughput_line = f"Throughput: {args.tasks / total_seconds:.1f} tasks/sec"
    if producer_seconds >= 0.95 * total_seconds:
        throughput_line += " (producer-bound)"
    print(throughput_line)
    median = _median_steady_state_rate(worker.samples)
    if median is not None:
        print(f"Steady-state throughput (median): {median:.1f} tasks/sec")
    print(
        "Config: "
        f"max_concurrent_tasks={args.max_concurrent_tasks}, "
        f"queue_size={queue_size}, "
        f"task_fetch_batch_size={args.task_fetch_batch_size}, "
        f"task_timeout={args.task_timeout}s, "
        f"heartbeat_interval={args.heartbeat_interval}s, "
        f"producer_mode={args.producer_mode}"
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.producer:
        asyncio.run(run_producer(args))
        return
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
