"""Single-process MQTT task-consumption throughput benchmark.

Starts an ``MqttWorker``, waits for it to subscribe, then publishes ``N`` tasks
to the task topic and times only the drain: the period from the first publish
until every task has been acknowledged. Requires a running MQTT 5 broker and
the ``mqtt`` extra:

    pip install "scietex.service[mqtt]"

Run with ``python -m examples.mqtt_perf --tasks 10000``.

Unlike the Valkey benchmark, tasks cannot be preloaded before the worker
starts: with ``clean_start=False`` and ``session_expiry_interval=0`` the broker
discards the session on disconnect, so messages published to an offline
subscriber are dropped rather than queued. The producer therefore publishes
after the worker has subscribed, and the timed window covers the full
push -> inbox -> pull -> handler pipeline, which is what makes the poll
interval's contribution measurable.

The dominant cost is the durable inbox, not the poll interval. On a local
broker the default file-backed inbox sustains roughly 120-160 tasks/sec, while
``--inbox-backend none`` (at-most-once, no disk) reaches roughly 4800-5000
tasks/sec -- a ~35x difference. Use ``--inbox-backend none`` to measure the
transport and handler pipeline in isolation, and the default to measure the
durability cost.
"""

import argparse
import asyncio
import logging
import time
from uuid import UUID, uuid4

import aiomqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from scietex.service import MqttConfig, MqttWorker, MqttWorkerConfig
from scietex.service.task_handler import (
    CancelReason,
    TaskCapabilities,
    TaskData,
    TaskHandler,
    TaskResult,
    encode_task_envelope,
)

TASK_ID_PROPERTY = "scietex-task-id"


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


async def load_tasks(client: aiomqtt.Client, topic: str, n: int, qos: int) -> None:
    """Publish ``n`` task envelopes to ``topic``, one per message.

    Each task carries its own id in the ``scietex-task-id`` user property, so
    the worker acknowledges exactly one inbox entry per completed task.
    """
    for _ in range(n):
        task_id = uuid4()
        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = [(TASK_ID_PROPERTY, str(task_id))]
        await client.publish(
            topic,
            encode_task_envelope(TaskData(task="perf")),
            qos=qos,
            properties=props,
        )


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
        choices=["file", "none"],
        default="file",
        help="Durable inbox backend; 'none' is the at-most-once opt-out (no disk I/O)",
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

    async with aiomqtt.Client(
        hostname=args.host,
        port=args.port,
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as producer:
        # The worker must be subscribed before publishing: the broker does not
        # queue messages for an offline subscriber (see module docstring).
        await worker.start()
        await worker.subscribed.wait()

        t0 = time.perf_counter()
        await load_tasks(producer, task_topic, args.tasks, args.task_qos)
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
        f"task_qos={args.task_qos}, "
        f"inbox_backend={args.inbox_backend}, "
        f"task_queue_manager_sleep_time={args.task_queue_manager_sleep_time or 'default (0.01)'}, "
        f"task_timeout={args.task_timeout}s, "
        f"heartbeat_interval={args.heartbeat_interval}s, "
        f"status_publish={args.status_publish}"
    )

    await worker.exit()
    await worker.events["exit"].wait()


def main(argv: list[str] | None = None) -> None:
    asyncio.run(run(parse_args(argv)))


if __name__ == "__main__":
    main()
