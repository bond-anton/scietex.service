"""Example: an ``MqttWorker`` consuming tasks from a local MQTT broker.

A self-contained demo against a broker on ``localhost:1883``. It shows the
full MQTT round trip:

1. The worker connects, subscribes to ``scietex/{service}/tasks`` (QoS 2), and
   replays any non-terminal entries from its durable file inbox.
2. A producer publishes a ``TaskEnvelope`` to that topic with the task id in
   the MQTT 5 user property ``scietex-task-id``.
3. The worker persists the message to its inbox, drains it into the processor
   queue, and runs the matching handler.
4. The handler reports progress; the worker publishes retained ``TaskStatus``
   messages to ``scietex/{service}/tasks/{task_id}/status`` and throttled
   ``TaskProgress`` messages to ``.../progress``.
5. The producer subscribes to the per-task status topic and prints the
   lifecycle: ``queued`` -> ``running`` -> ``completed``.

Requires a running MQTT 5 broker and the ``mqtt`` extra:

    pip install "scietex.service[mqtt]"

The producer uses the public ``aiomqtt`` API. The one paho import is for the
MQTT 5 ``Properties`` type, which aiomqtt v2.5.1 does not re-export but which
is required to attach the ``scietex-task-id`` user property.

Run with ``python -m examples.mqtt_worker``. Override the broker with
``--host``/``--port``.
"""

import argparse
import asyncio
import logging
from uuid import UUID, uuid4

import aiomqtt
import msgspec
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from scietex.service import MqttConfig, MqttWorker, MqttWorkerConfig
from scietex.service.task_handler import (
    TaskCapabilities,
    TaskData,
    TaskHandler,
    TaskResult,
    TaskStatus,
    encode_task_envelope,
)

SERVICE_NAME = "MqttDemo"
TASK_TOPIC = f"scietex/{SERVICE_NAME}/tasks"
STATUS_TOPIC_PREFIX = f"scietex/{SERVICE_NAME}/tasks"
TASK_ID_PROPERTY = "scietex-task-id"

# Number of progress steps the long job performs before finishing.
TOTAL_STEPS = 10
STEP_DELAY = 0.3


# ── Handler ──────────────────────────────────────────────────────────────


class LongJobHandler(TaskHandler):
    """A long-running task that reports granular progress.

    Progress is reported through the per-call ``capabilities`` object passed to
    ``handle``; the worker's transport turns each call into a throttled
    ``TaskProgress`` publish on the task's progress topic.
    """

    @property
    def supported_tasks(self) -> list[str]:
        return ["long_job"]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        for step in range(1, TOTAL_STEPS + 1):
            await asyncio.sleep(STEP_DELAY)
            await capabilities.report_progress(step / TOTAL_STEPS * 100.0)
            self.logger.info("long_job progress: %d/%d", step, TOTAL_STEPS)
        return TaskResult(status="success", payload=b"finished")


# ── Producer helpers ─────────────────────────────────────────────────────


async def submit(client: aiomqtt.Client, task_data: TaskData) -> UUID:
    """Publish one task envelope and return its id.

    The task id travels as the MQTT 5 user property ``scietex-task-id``; the
    envelope wire format itself is unchanged.
    """
    task_id = uuid4()
    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = [(TASK_ID_PROPERTY, str(task_id))]
    await client.publish(
        TASK_TOPIC,
        encode_task_envelope(task_data),
        qos=2,
        properties=props,
    )
    return task_id


async def watch_status(client: aiomqtt.Client, task_id: UUID, *, timeout: float = 15.0) -> None:
    """Subscribe to the task's status topic and print lifecycle transitions."""
    topic = f"{STATUS_TOPIC_PREFIX}/{task_id}/status"
    await client.subscribe(topic, qos=1)
    print(f"Watching {topic}")

    seen: set[str] = set()
    deadline = asyncio.get_running_loop().time() + timeout
    async for message in client.messages:
        if asyncio.get_running_loop().time() > deadline:
            print("Timed out waiting for terminal status")
            return
        status = msgspec.msgpack.decode(message.payload, type=TaskStatus)
        if status.status in seen:
            continue
        seen.add(status.status)
        print(f"  status={status.status} progress={status.progress.value:.0f}%")
        if status.status in {"completed", "failed", "cancelled"}:
            if status.result is not None:
                print(f"  result={status.result!r}")
            return


# ── Main ─────────────────────────────────────────────────────────────────


async def run(host: str, port: int) -> None:
    worker = MqttWorker(
        MqttWorkerConfig(
            service_name=SERVICE_NAME,
            version="0.0.1",
            logging_level=logging.INFO,
            heartbeat_interval=4,
            mqtt_config=MqttConfig(host=host, port=port),
            queue_size=100,
            max_concurrent_tasks=4,
            # The long job runs TOTAL_STEPS * STEP_DELAY seconds; the default
            # task_timeout (3s) would cancel it just before it finishes.
            task_timeout=TOTAL_STEPS * STEP_DELAY + 5.0,
            # Publish progress at most once per second (the default), and
            # always on a >=10% jump.
            progress_min_interval=1.0,
            progress_min_delta=10.0,
        )
    )
    worker.add_task_handler(LongJobHandler)

    await worker.start()

    # A separate client acts as the producer and status subscriber.
    async with aiomqtt.Client(hostname=host, port=port, protocol=aiomqtt.ProtocolVersion.V5) as producer:
        job_id = await submit(producer, TaskData(task="long_job"))
        print(f"Submitted long_job {job_id}")
        await watch_status(producer, job_id)

    await worker.exit()
    await worker.events["exit"].wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the MQTT worker example.")
    parser.add_argument("--host", default="localhost", help="MQTT broker host (default: localhost)")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    args = parser.parse_args()
    asyncio.run(run(host=args.host, port=args.port))


if __name__ == "__main__":
    main()
