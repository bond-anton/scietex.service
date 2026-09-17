"""Example: a progress-reporting task that is cancelled mid-flight.

A ``ValkeyWorker`` runs a long ``long_job`` handler that reports granular
progress, while a producer client submits the job and then a ``cancel_task``
request for it. The example shows the full round trip:

1. The producer ``XADD``s a ``long_job`` task to ``scietex:{service}:tasks``.
2. The worker picks it up and the handler calls ``report_progress`` on every
   step. ``ValkeyWorker`` writes each value into the task's tracking record
   (``scietex:{service}:task:{task_id}``), so the producer can poll it.
3. After a few progress updates the producer submits a ``cancel_task`` task
   whose payload is a msgpack ``CancelTaskRequest`` naming the target id.
4. The built-in ``CancelTaskHandler`` cancels the running target; the target's
   terminal status becomes ``cancelled`` and embeds the original ``TaskData``
   so an external process can modify and resubmit it under a new id.

``report_progress`` is exposed as a per-call capability: the processor passes a
``TaskCapabilities`` object to ``handle``, whose ``report_progress`` method
routes to the transport's progress hook for that specific task. The handler
never touches processor internals.

Requires a running Valkey server and the ``valkey`` extra:

    pip install "scietex.service[valkey]"

Run with ``python -m examples.progress_and_cancel``.
"""

import asyncio
import logging
from uuid import UUID, uuid4

import msgspec
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
    CANCEL_TASK_TYPE,
    CancelTaskRequest,
    CancelTaskResponse,
    TaskCapabilities,
    TaskData,
    TaskHandler,
    TaskHandlerContext,
    TaskResult,
    TaskStatus,
    encode_task_envelope,
)
from scietex.service.valkey.config import generate_glide_config

SERVICE_NAME = "ProgressCancelDemo"
STREAM_NAME = f"scietex:{SERVICE_NAME}:tasks"
TRACKING_KEY_PREFIX = f"scietex:{SERVICE_NAME}:task:"

# Number of progress steps the long job performs before finishing on its own.
TOTAL_STEPS = 20
STEP_DELAY = 0.25


# ── Handler ──────────────────────────────────────────────────────────────


class LongJobHandler(TaskHandler):
    """A long-running task that reports progress and honours cancellation.

    Progress is reported through the per-call ``capabilities`` object passed to
    ``handle``, which routes to the transport's progress hook for this specific
    task. The handler never touches processor internals.
    """

    def __init__(self, name: str, context: TaskHandlerContext) -> None:
        super().__init__(name, context)

    @property
    def supported_tasks(self) -> list[str]:
        return ["long_job"]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        for step in range(1, TOTAL_STEPS + 1):
            # A cancellation request cancels this worker task; the sleep is the
            # cancellation point. CancelledError propagates and the processor
            # records the terminal `cancelled` status.
            await asyncio.sleep(STEP_DELAY)
            await capabilities.report_progress(step / TOTAL_STEPS * 100.0)
            self.logger.info("long_job progress: %d/%d", step, TOTAL_STEPS)
        return TaskResult(status="success", payload=b"finished")


# ── Producer helpers ─────────────────────────────────────────────────────


async def submit(client: GlideClient, task_data: TaskData) -> UUID:
    """Submit one task as its own stream entry and return its id."""
    task_id = uuid4()
    await client.xadd(STREAM_NAME, [(str(task_id).encode("utf-8"), encode_task_envelope(task_data))])
    return task_id


async def submit_cancel(client: GlideClient, target_id: UUID, reason: str) -> UUID:
    """Submit a ``cancel_task`` request targeting ``target_id``."""
    payload = msgspec.msgpack.encode(CancelTaskRequest(target_task_id=str(target_id), reason=reason))
    return await submit(client, TaskData(task=CANCEL_TASK_TYPE, payload=payload))


async def read_status(client: GlideClient, task_id: UUID) -> TaskStatus | None:
    """Read and decode the tracking record for ``task_id``, if present."""
    raw = await client.get(f"{TRACKING_KEY_PREFIX}{task_id}")
    if raw is None:
        return None
    return msgspec.msgpack.decode(raw, type=TaskStatus)


async def wait_for_status(
    client: GlideClient,
    task_id: UUID,
    wanted: set[str],
    *,
    timeout: float = 10.0,
) -> TaskStatus | None:
    """Poll the tracking record until its status is in ``wanted``."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        status = await read_status(client, task_id)
        if status is not None and status.status in wanted:
            return status
        await asyncio.sleep(0.1)
    return None


# ── Main ─────────────────────────────────────────────────────────────────


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


async def run(host: str, port: int) -> None:
    valkey_config = build_valkey_config(host, port)
    worker = ValkeyWorker(
        ValkeyWorkerConfig(
            service_name=SERVICE_NAME,
            version="0.0.1",
            logging_level=logging.INFO,
            heartbeat_interval=4,
            valkey_config=valkey_config,
            queue_size=100,
            # Cancellation needs a free slot for the cancel task itself: with
            # max_concurrent_tasks == 1 the cancel request would queue behind
            # its target and degrade to `not_running`.
            max_concurrent_tasks=4,
        )
    )
    # The handler reports progress through the per-call capabilities object.
    worker.add_task_handler(LongJobHandler)

    producer = await GlideClient.create(
        generate_glide_config(
            valkey_config,
            service_name=SERVICE_NAME,
            worker_id="progress-cancel-producer",
        )
    )

    await worker.start()

    job_id = await submit(producer, TaskData(task="long_job"))
    print(f"Submitted long_job {job_id}")

    # Watch progress climb while the job runs.
    for _ in range(3):
        await asyncio.sleep(0.6)
        status = await read_status(producer, job_id)
        if status is not None:
            print(f"  status={status.status} progress={status.progress.value:.0f}%")

    cancel_id = await submit_cancel(producer, job_id, reason="operator requested")
    print(f"Submitted cancel_task {cancel_id} for {job_id}")

    cancel_status = await wait_for_status(producer, cancel_id, {"completed", "failed"})
    if cancel_status is not None and cancel_status.result is not None:
        response = msgspec.msgpack.decode(cancel_status.result, type=CancelTaskResponse)
        print(f"Cancel outcome: {response.outcome}")

    final = await wait_for_status(producer, job_id, {"cancelled", "completed", "failed"})
    if final is None:
        print("Target status not observed before timeout")
    else:
        print(f"Target final status: {final.status} (error={final.error!r})")
        if final.data is not None:
            # The cancelled record embeds the original request so an external
            # process can modify and resubmit it under a NEW id.
            print(f"Embedded TaskData: task={final.data.task!r} payload={final.data.payload!r}")
            resubmitted = await submit(producer, final.data)
            print(f"Resubmitted under new id {resubmitted}")

    await producer.close()
    await worker.exit()
    await worker.events["exit"].wait()


def main() -> None:
    asyncio.run(run(host="localhost", port=6379))


if __name__ == "__main__":
    main()
