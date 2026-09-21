"""MQTT producer surface for the control plane (AR-123 §7/§9).

``MqttControlPublisher`` writes control commands to the MQTT control topics a
:class:`~scietex.service.mqtt.worker.MqttWorker` reads, and resolves a task's
owning worker from its retained owner marker.

The channel layout mirrors the worker's: a *direct* command goes to the
addressed worker's dedicated topic ``{control_topic}`` (which carries the
``{instance_id}`` placeholder and is formatted per call), a *broadcast* goes to
the shared ``{control_broadcast_topic}``, and ownership is read from the
retained owner topic ``{status_topic_prefix}/{task_id}/owner`` published by
:class:`~scietex.service.mqtt.transport.MqttTransport`. ``service_name`` is
folded into the pre-resolved topic names and ``status_topic_prefix``, so it is
not a separate parameter.

The publisher is transport-only: it addresses and encodes. It takes an
already-built :class:`~scietex.service.task_handler.schemas.TaskData` and never
inspects its type or payload. Control is never retried (design §4.7/§9), so a
failed publish is raised rather than swallowed — the submitter must learn that
the command was not sent.
"""

import asyncio
import logging

from ..task_handler import TaskData
from ..task_handler.wire import encode_task_envelope
from ._aiomqtt import Client

#: Seconds to wait for the retained owner marker after subscribing. MQTT has no
#: ``GET``, so the owner is read back by subscribing and awaiting the retained
#: message; the wait is bounded so a broker holding no owner marker returns
#: ``None`` instead of hanging (mirrors ``MqttConfigSource.wait_for_snapshot``).
DEFAULT_OWNER_RESOLVE_TIMEOUT: float = 2.0

#: Subscription QoS for the owner read. Subscription QoS is the maximum the
#: subscriber accepts, so 1 safely receives a retained marker published at any
#: ``status_qos`` (0, 1, or 2) — a higher publish QoS is downgraded, never lost.
_OWNER_SUBSCRIBE_QOS: int = 1


class MqttControlPublisher:
    """Publish control commands to the MQTT control topics.

    Constructed with an already-connected ``aiomqtt.Client`` and the same
    control-topic names and status-topic prefix the workers use, so a submitter
    can address commands from any process that can reach the broker.

    Args:
        client: The connected ``aiomqtt.Client`` used for all commands. Passed
            by reference; the publisher never owns or closes it.
        control_topic: The per-worker control topic template. It carries an
            ``{instance_id}`` placeholder formatted per ``direct`` call and has
            ``{service}`` already substituted (it is the resolved
            ``MqttWorkerConfig.control_topic``).
        control_broadcast_topic: The shared broadcast topic, with ``{service}``
            already substituted.
        control_qos: QoS for control publishes.
        status_topic_prefix: The resolved ``{service}``-substituted status
            prefix (``scietex/{service}/tasks``) the worker's owner markers
            nest under, so ``resolve_owner`` subscribes to the same topic the
            transport publishes.
        resolve_timeout: Seconds ``resolve_owner`` waits for the retained owner
            marker before returning ``None``. Defaults to
            :data:`DEFAULT_OWNER_RESOLVE_TIMEOUT`.
        logger: Logger for diagnostics.
    """

    def __init__(
        self,
        *,
        client: Client,
        control_topic: str,
        control_broadcast_topic: str,
        control_qos: int,
        status_topic_prefix: str,
        logger: logging.Logger,
        resolve_timeout: float = DEFAULT_OWNER_RESOLVE_TIMEOUT,
    ) -> None:
        self._client = client
        self._control_topic = control_topic
        self._control_broadcast_topic = control_broadcast_topic
        self._control_qos = control_qos
        self._status_topic_prefix = status_topic_prefix
        self._logger = logger
        self._resolve_timeout = resolve_timeout

    async def direct(self, instance_id: str, task_data: TaskData) -> None:
        """Publish ``task_data`` to the addressed worker's control topic.

        The command is encoded into a versioned transport envelope and published
        to the worker's dedicated topic (``{instance_id}`` substituted). Control
        is event-only (never retained, design §9), and a failed publish is
        raised so the submitter learns the command was not sent (AR-123 §4.7).
        """
        await self._publish(self._control_topic.format(instance_id=instance_id), task_data)

    async def broadcast(self, task_data: TaskData) -> None:
        """Publish ``task_data`` to the shared broadcast topic.

        Every worker subscribes to this topic, so the command reaches the whole
        fleet. Event-only and raised on failure, as in :meth:`direct`.
        """
        await self._publish(self._control_broadcast_topic, task_data)

    async def resolve_owner(self, task_id: str) -> str | None:
        """Return the ``instance_id`` of the worker owning ``task_id``, if any.

        Reads the retained owner marker ``{status_topic_prefix}/{task_id}/owner``
        via a one-shot subscribe: the retained message is delivered on SUBACK,
        so this awaits it for a bounded ``DEFAULT_OWNER_RESOLVE_TIMEOUT`` and
        decodes its payload (the raw UTF-8 ``instance_id``). Returns ``None`` on
        timeout or an empty payload — the task has no owner, e.g. it is not
        running anywhere (design §10.1).
        """

        async def _read_owner() -> str | None:
            owner_topic = f"{self._status_topic_prefix}/{task_id}/owner"
            await self._client.subscribe(owner_topic, qos=_OWNER_SUBSCRIBE_QOS)
            async for message in self._client.messages:
                if str(message.topic) == owner_topic:
                    return message.payload.decode() or None

        try:
            return await asyncio.wait_for(_read_owner(), timeout=self._resolve_timeout)
        except asyncio.TimeoutError:
            return None

    async def _publish(self, topic: str, task_data: TaskData) -> None:
        await self._client.publish(topic, encode_task_envelope(task_data), qos=self._control_qos, retain=False)
