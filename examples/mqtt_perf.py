"""MQTT task-consumption throughput benchmark with an optional separate-process producer.

Runs an ``MqttWorker``, waits for it to subscribe, then publishes ``N`` tasks to
the task topic and times the drain: the period from the first publish until
every task has been acknowledged. Requires a running MQTT 5 broker and the
``mqtt`` extra:

    pip install "scietex.service[mqtt]"

Run with ``python -m examples.mqtt_perf --tasks 10000``.

Two producer modes (``--producer-mode``, default ``process``):

- ``process`` -- the producer runs in a separate OS process (this script
  re-invoked with a hidden ``--producer`` flag), so its publish cost does not
  contend with the worker's event loop. A ready/go handshake over the child's
  stdin/stdout excludes child startup and broker connect from the timed window,
  and the parent waits on the producer and the drain concurrently, so the wall
  window is ``max(producer, drain)``.
- ``inline`` -- the producer publishes in this process, preserving the original
  single-process behavior.

Two timing windows are reported: the producer's own publish loop (``Producer
publish``) and the full wall window from the go signal to drain completion
(``Drained`` / ``Throughput``).

Unlike the Valkey benchmark, tasks cannot be preloaded before the worker
starts: with ``clean_start=False`` and ``session_expiry_interval=0`` the broker
discards the session on disconnect, so messages published to an offline
subscriber are dropped rather than queued. The producer therefore publishes
after the worker has subscribed, and the timed window covers the full
push -> inbox -> pull -> handler pipeline, which is what makes the poll
interval's contribution measurable.

The dominant cost is the durable inbox, not the poll interval. On a local
broker the sqlite-backed inbox sustains roughly 120-160 tasks/sec, while the
default in-memory backend (at-most-once, no disk) reaches roughly 4800-5000
tasks/sec -- a ~35x difference (inline-mode figures). The benchmark defaults to
the in-memory backend so it measures the transport and handler pipeline in
isolation; pass ``--inbox-backend sqlite`` to measure the durability cost.
"""

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import aiomqtt

from scietex.service import MqttConfig, MqttWorker, MqttWorkerConfig
from scietex.service.task_handler import (
    CancelReason,
    TaskCapabilities,
    TaskData,
    TaskHandler,
    TaskResult,
    encode_task_envelope,
)

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


class PerfWorker(MqttWorker):
    """MQTT worker that signals drain completion and records steady-state rates."""

    def __init__(self, config: MqttWorkerConfig, total: int) -> None:
        super().__init__(config)
        self._total = total
        self._completed = 0
        # Fires once every preloaded task has been acked, so the inbox is empty
        # when the caller resumes.
        self.all_done = asyncio.Event()
        # Fires once the worker has subscribed to the task topic, so the
        # producer can publish without racing the subscription.
        self.subscribed = asyncio.Event()
        # Sample the drain every ~5% so a median steady-state rate can
        # discount startup ramp-up and the tail-end drain.
        self._sample_step = max(1, total // 20)
        self._samples: list[tuple[float, int]] = []

    @property
    def samples(self) -> list[tuple[float, int]]:
        """``(monotonic seconds, completed count)`` samples recorded during the drain."""
        return self._samples

    async def _start_intake(self) -> bool:
        """Subscribe and start the message loop, then signal readiness."""
        started = await super()._start_intake()
        if started:
            self.subscribed.set()
        return started

    async def on_task_completed(
        self,
        task_data: TaskData,
        task_result: TaskResult | None,
        *,
        cancel_reason: CancelReason | None = None,
    ) -> None:
        await super().on_task_completed(task_data, task_result, cancel_reason=cancel_reason)
        # Single event loop, no await between increment and check: the count is
        # always accurate when the completion event is inspected.
        self._completed += 1
        if self._completed % self._sample_step == 0:
            self._samples.append((time.monotonic(), self._completed))
        if self._completed >= self._total:
            self.all_done.set()


async def load_tasks(client: aiomqtt.Client, topic: str, n: int, qos: int) -> None:
    """Publish ``n`` task envelopes to ``topic``, one per message.

    Each task carries its id inside the encoded ``TaskData``, so the worker
    acknowledges exactly one inbox entry per completed task.
    """
    for _ in range(n):
        await client.publish(
            topic,
            encode_task_envelope(TaskData(task_id=str(uuid4()), task="perf")),
            qos=qos,
        )


async def run_producer(args: argparse.Namespace) -> None:
    """Child entry point: connect, publish, and report a single result line.

    Stdout carries only the handshake and result lines; logging stays on the
    inherited stderr so the parent's protocol channel is never polluted.
    """
    if args.tasks <= 0:
        print(f"{_RESULT_PREFIX} count=0 seconds=0", flush=True)
        return
    async with aiomqtt.Client(
        hostname=args.host,
        port=args.port,
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        topic = f"scietex/{args.service_name}/tasks"
        print(_READY, flush=True)
        # Block until the parent sends the go byte, so startup and broker
        # connect stay out of the timed window.
        await asyncio.to_thread(sys.stdin.readline)
        t0 = time.perf_counter()
        await load_tasks(client, topic, args.tasks, args.task_qos)
        seconds = time.perf_counter() - t0
    print(f"{_RESULT_PREFIX} count={args.tasks} seconds={seconds:.6f}", flush=True)


async def _start_producer(args: argparse.Namespace) -> tuple[asyncio.subprocess.Process, float]:
    """Spawn the producer child, handshake, and return ``(proc, t0)``.

    ``t0`` is captured after the child signals readiness and before the go byte
    is sent, so child startup and broker connect are excluded from the timed
    window.
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
        "--task-qos",
        str(args.task_qos),
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
        description="Publish N tasks to an MQTT topic, then drain them with a single MqttWorker."
    )
    parser.add_argument("-n", "--tasks", type=int, default=10_000, help="Total tasks published then drained")
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--service-name", default="MqttPerfService", help="Derives the task/status topics")
    parser.add_argument("--max-concurrent-tasks", type=int, default=100, help="max_concurrent_tasks")
    parser.add_argument(
        "--queue-size",
        type=int,
        default=None,
        help="queue_size; defaults to --tasks so the whole backlog buffers without back-pressure",
    )
    parser.add_argument("--task-qos", type=int, default=2, help="QoS for task publishes")
    parser.add_argument(
        "--inbox-backend",
        choices=["sqlite", "memory", "none"],
        default="memory",
        help="Inbox backend; 'memory' (default) is the at-most-once opt-out (no disk I/O), 'sqlite' is durable",
    )
    parser.add_argument(
        "--task-queue-manager-sleep-time",
        type=float,
        default=None,
        help="Poll interval in seconds; defaults to the library default (0.01)",
    )
    parser.add_argument("--task-timeout", type=float, default=300.0, help="Per-task timeout in seconds")
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=600.0,
        help="Heartbeat interval in seconds (capped at the config max of 600)",
    )
    parser.add_argument(
        "--status-publish",
        action="store_true",
        help="Enable status/progress publishing (off by default so it does not skew the drain)",
    )
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
    """Publish the tasks, drain them, and report the timed drain throughput."""
    if args.tasks <= 0:
        print("Nothing to do: --tasks is zero.")
        return

    queue_size = args.queue_size if args.queue_size is not None else args.tasks
    task_topic = f"scietex/{args.service_name}/tasks"
    config = MqttWorkerConfig(
        service_name=args.service_name,
        version="0.0.1",
        logging_level=logging.WARNING,
        heartbeat_interval=args.heartbeat_interval,
        mqtt_config=MqttConfig(host=args.host, port=args.port),
        queue_size=queue_size,
        max_concurrent_tasks=args.max_concurrent_tasks,
        task_qos=args.task_qos,
        inbox_backend=args.inbox_backend,
        task_timeout=args.task_timeout,
        task_queue_manager_sleep_time=args.task_queue_manager_sleep_time,
        # Status publishing is off by default: it adds a publish per lifecycle
        # event and would measure the publisher, not the intake pipeline.
        status_publish_enabled=args.status_publish,
    )

    worker = PerfWorker(config, total=args.tasks)
    worker.add_task_handler(PerfHandler)

    # The worker must be subscribed before publishing: the broker does not
    # queue messages for an offline subscriber (see module docstring).
    await worker.start()
    await worker.subscribed.wait()

    proc: asyncio.subprocess.Process | None = None
    producer_count = args.tasks
    producer_seconds = 0.0
    try:
        if args.producer_mode == "process":
            proc, t0 = await _start_producer(args)
            producer_result, _ = await asyncio.gather(
                _collect_producer(proc),
                _wait_drained(worker, args.drain_timeout),
            )
            producer_count = producer_result.count
            producer_seconds = producer_result.seconds
        else:
            async with aiomqtt.Client(
                hostname=args.host,
                port=args.port,
                protocol=aiomqtt.ProtocolVersion.V5,
            ) as producer:
                t0 = time.perf_counter()
                await load_tasks(producer, task_topic, args.tasks, args.task_qos)
                producer_seconds = time.perf_counter() - t0
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
        f"task_qos={args.task_qos}, "
        f"inbox_backend={args.inbox_backend}, "
        f"task_queue_manager_sleep_time={args.task_queue_manager_sleep_time or 'default (0.01)'}, "
        f"task_timeout={args.task_timeout}s, "
        f"heartbeat_interval={args.heartbeat_interval}s, "
        f"status_publish={args.status_publish}, "
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
