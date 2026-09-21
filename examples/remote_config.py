"""Example: remote configuration over MQTT with a custom settings section.

Remote config delivers a reloadable-behaviour snapshot to a running worker
over the transport it already uses. Over MQTT this uses two channels:

1. A retained desired-state message on ``scietex/{service}/config`` — the
   source of truth, read at startup and re-read by ``config:apply``.
2. Three commands delivered as ordinary tasks through
   ``scietex/{service}/tasks``: ``config:apply`` applies the source of truth,
   ``config:store`` persists the effective config, and ``config:show``
   inspects it. Each command's reply rides its ``TaskResult.payload``, which
   the transport surfaces as a retained ``TaskStatus`` on the per-task status
   topic.

A custom service extends the reloadable surface by registering its own
``msgspec.Struct`` section: :class:`ConfigDemoWorker` registers
``DemoServiceSettings`` under the ``"demo"`` section name, and its apply hook
stores and logs the decoded settings whenever a config applies. The core
``TaskProcessor`` fields (concurrency, timeouts) remain reloadable alongside
it.

Security: broker ACLs are the primary defense. Anyone able to publish a
retained message to the config topic — or a task to the task topic — can
influence worker behaviour, so the broker must restrict those writes to
trusted operators. The reloadable surface carries no credentials or TLS
material by construction; those fields are restart-required and cannot be
expressed in a remote payload at all.

Requires a running MQTT 5 broker and the ``mqtt`` extra:

    pip install "scietex.service[mqtt]"

Run with ``python -m examples.remote_config``. Override the broker with
``--host``/``--port`` and the service name with ``--service-name``. The
default host is ``127.0.0.1`` (not ``localhost``) because on hosts where
``localhost`` resolves to IPv6 first, an IPv4-only broker is unreachable.
"""

import argparse
import asyncio
import logging
import tempfile
from uuid import uuid4

import aiomqtt
import msgspec

from scietex.service import MqttConfig, MqttWorker, MqttWorkerConfig
from scietex.service.config_reload import ConfigSections, ReloadableSettings, encode_config_envelope
from scietex.service.task_handler import (
    CONFIG_APPLY_TASK_NAME,
    CONFIG_SHOW_TASK_NAME,
    CONFIG_STORE_TASK_NAME,
    ConfigApplyRequest,
    ConfigApplyResponse,
    ConfigShowRequest,
    ConfigShowResponse,
    ConfigStoreRequest,
    ConfigStoreResponse,
    TaskData,
    TaskStatus,
    encode_task_envelope,
)

#: Section name the demo settings are registered under.
DEMO_SECTION = "demo"


class DemoServiceSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Custom reloadable settings section for this demo service.

    Registered under the ``"demo"`` section name; the core validates and
    applies it without knowing its fields. A typo or unknown field in a remote
    payload is rejected by ``forbid_unknown_fields`` rather than ignored.
    """

    batch_size: int = 100
    greeting: str = "hello"


class ConfigDemoWorker(MqttWorker):
    """An ``MqttWorker`` with a registered ``demo`` settings section.

    Registers ``DemoServiceSettings`` with the processor's remote-config
    reloader in ``__init__`` (before startup applies anything), so a remote
    envelope carrying a ``services["demo"]`` entry is decoded against it and
    handed to :meth:`_apply_demo_settings` on every successful apply.
    """

    def __init__(self, config: MqttWorkerConfig | None = None, *, client_factory=None) -> None:
        super().__init__(config, client_factory=client_factory)
        self._demo_settings: DemoServiceSettings | None = None
        #: Set once the worker has subscribed to the task and config topics.
        #: ``BasicWorker.start()`` returns before startup completes, so an
        #: operator must wait on this before publishing non-retained tasks.
        self.subscribed = asyncio.Event()
        self.register_config_settings(DEMO_SECTION, DemoServiceSettings, apply=self._apply_demo_settings)

    async def _start_intake(self) -> bool:
        started = await super()._start_intake()
        if started:
            self.subscribed.set()
        return started

    @property
    def demo_settings(self) -> DemoServiceSettings | None:
        """The most recently applied demo settings, or ``None`` before the first apply."""
        return self._demo_settings

    def _apply_demo_settings(self, settings: DemoServiceSettings) -> None:
        """Store and log the decoded demo settings (the section apply hook)."""
        self._demo_settings = settings
        self.logger.info("Applied demo settings: batch_size=%d greeting=%r", settings.batch_size, settings.greeting)


async def submit_command(
    client: aiomqtt.Client,
    task_type: str,
    request: msgspec.Struct,
    *,
    task_topic: str,
    status_topic_prefix: str,
    timeout: float = 15.0,
) -> bytes | None:
    """Publish one ``config:*`` command task and return its reply payload.

    Subscribes to the task's retained status topic before publishing, so the
    terminal ``completed`` status is delivered whether the task is still
    running or has already finished (status messages are retained). The reply
    struct travels as the task's ``TaskResult.payload``, which the transport
    surfaces as ``TaskStatus.result``. Returns ``None`` on a failed/cancelled
    task or a timeout.
    """
    task_id = uuid4()
    await client.subscribe(f"{status_topic_prefix}/{task_id}/status", qos=1)
    await client.publish(
        task_topic,
        encode_task_envelope(TaskData(task_id=str(task_id), task=task_type, payload=msgspec.msgpack.encode(request))),
        qos=2,
    )

    async def _await_reply() -> bytes | None:
        async for message in client.messages:
            try:
                status = msgspec.msgpack.decode(message.payload, type=TaskStatus)
            except msgspec.DecodeError:
                continue
            if status.task_id != str(task_id):
                continue
            if status.status == "completed":
                return status.result
            if status.status in {"failed", "cancelled"}:
                print(f"{task_type} -> {status.status}: {status.error or status.error_code}")
                return None
        return None

    try:
        return await asyncio.wait_for(_await_reply(), timeout=timeout)
    except TimeoutError:
        print(f"Timed out waiting for {task_type} reply ({task_id})")
        return None


def _print_worker_state(worker: ConfigDemoWorker) -> None:
    """Print the worker's applied config revision and source."""
    print(f"  worker: revision={worker.config_revision} source={worker.config_source!r}")


async def run(host: str, port: int, service_name: str) -> None:
    config_topic = f"scietex/{service_name}/config"
    task_topic = f"scietex/{service_name}/tasks"
    status_topic_prefix = f"scietex/{service_name}/tasks"

    # A temp conf_dir so config:store (target="disk") has a writable home and
    # the durable inbox does not touch a real config directory.
    with tempfile.TemporaryDirectory(prefix="remote-config-demo-") as tmpdir:
        worker = ConfigDemoWorker(
            MqttWorkerConfig(
                service_name=service_name,
                version="0.0.1",
                logging_level=logging.INFO,
                heartbeat_interval=4,
                conf_dir=tmpdir,
                remote_config_enabled=True,
                mqtt_config=MqttConfig(host=host, port=port),
                queue_size=100,
                max_concurrent_tasks=4,
                task_manager_sleep_time=0.01,
                task_queue_manager_sleep_time=0.01,
                task_handler_start_timeout=5.0,
                task_handler_stop_timeout=5.0,
                task_timeout=3.0,
                task_queue_fetch_timeout=1.0,
                task_cancellation_timeout=5.0,
            )
        )
        await worker.start()
        # start() returns before startup completes; wait until the worker has
        # subscribed, otherwise the non-retained command tasks below are
        # published to a topic with no subscriber and the broker drops them.
        await worker.subscribed.wait()

        # A separate client acts as the operator: it publishes the desired
        # state and the three commands, and reads each command's reply from its
        # retained per-task status topic.
        async with aiomqtt.Client(hostname=host, port=port, protocol=aiomqtt.ProtocolVersion.V5) as operator:
            # (a) Desired state: a retained config envelope on the config topic.
            # The core snapshot is complete (all fields required); the "demo"
            # section carries this service's registered settings.
            sections = ConfigSections(
                core=ReloadableSettings(
                    max_concurrent_tasks=8,  # bumped from the worker's 4: a core field reload
                    task_manager_sleep_time=0.01,
                    task_queue_manager_sleep_time=0.01,
                    task_handler_start_timeout=5.0,
                    task_handler_stop_timeout=5.0,
                    task_timeout=3.0,
                    task_queue_fetch_timeout=1.0,
                    task_cancellation_timeout=5.0,
                ),
                services={
                    DEMO_SECTION: msgspec.msgpack.encode(DemoServiceSettings(batch_size=42, greeting="howdy")),
                },
            )
            envelope = encode_config_envelope(sections, revision=1)
            # qos=1 matches the worker's config_qos default. Publishing alone
            # does not apply: the retained message only seeds the source of
            # truth that config:apply (or a restart) later reads.
            await operator.publish(config_topic, envelope, qos=1, retain=True)
            print(f"Published retained config to {config_topic} (revision 1)")
            _print_worker_state(worker)

            # (b) config:apply — no inline payload, so the worker re-reads the
            # retained source of truth. The retained publish above is delivered
            # on the worker's existing subscription, so the snapshot is already
            # recorded by the time this command is processed.
            payload = await submit_command(
                operator,
                CONFIG_APPLY_TASK_NAME,
                ConfigApplyRequest(),
                task_topic=task_topic,
                status_topic_prefix=status_topic_prefix,
            )
            if payload is not None:
                response = msgspec.msgpack.decode(payload, type=ConfigApplyResponse)
                print(
                    f"config:apply -> applied={response.applied} revision={response.revision} "
                    f"changed={response.changed}"
                )
            _print_worker_state(worker)
            print(f"  worker demo settings: {worker.demo_settings}")

            # (c) config:show — inspect the effective config over the wire.
            payload = await submit_command(
                operator,
                CONFIG_SHOW_TASK_NAME,
                ConfigShowRequest(),
                task_topic=task_topic,
                status_topic_prefix=status_topic_prefix,
            )
            if payload is not None:
                response = msgspec.msgpack.decode(payload, type=ConfigShowResponse)
                shown = msgspec.msgpack.decode(response.settings, type=ConfigSections)
                demo = msgspec.msgpack.decode(shown.services[DEMO_SECTION], type=DemoServiceSettings)
                print(
                    f"config:show -> revision={response.revision} source={response.source!r} "
                    f"max_concurrent_tasks={shown.core.max_concurrent_tasks}"
                )
                print(f"  demo section: {demo}")
                print(f"  restart_required_fields={response.restart_required_fields}")
            _print_worker_state(worker)

            # (d) config:store — persist the effective config to config.yml.
            payload = await submit_command(
                operator,
                CONFIG_STORE_TASK_NAME,
                ConfigStoreRequest(target="disk"),
                task_topic=task_topic,
                status_topic_prefix=status_topic_prefix,
            )
            if payload is not None:
                response = msgspec.msgpack.decode(payload, type=ConfigStoreResponse)
                print(f"config:store -> stored={response.stored} target={response.target} path={response.path}")
            _print_worker_state(worker)

        await worker.exit()
        await worker.events["exit"].wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the remote-config MQTT worker example.")
    parser.add_argument("--host", default="127.0.0.1", help="MQTT broker host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    parser.add_argument("--service-name", default="ConfigDemo", help="Service name (default: ConfigDemo)")
    args = parser.parse_args()
    asyncio.run(run(host=args.host, port=args.port, service_name=args.service_name))


if __name__ == "__main__":
    main()
