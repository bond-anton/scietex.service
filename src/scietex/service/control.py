"""Transport-agnostic producer surface for the control plane (AR-123 §7).

The control plane travels on channels separate from the data plane on every
transport, so a submitter that wants to address a control command
(``task:cancel``, ``worker:*``, ``config:*``) must publish to the *control* channel, not the
data channel. This module defines the minimal producer contract that lets a
submitter do that without knowing which transport is in use.

Two rules shape the surface:

- **The channel is the address** (design §3.1). A control command is an
  ordinary :class:`~scietex.service.task_handler.schemas.TaskData`; the caller
  owns the task type and payload. The publisher only addresses it (which
  stream/topic) and encodes it (the versioned ``TaskEnvelope``). No payload
  field carries a target or a broadcast flag.
- **Control is never retried** (design §4.7/§9). A publish failure is raised,
  not swallowed and retried: the submitter must learn that the command was not
  sent, because a silently dropped command is a silently scoped one.

Concrete publishers live in the transport packages:
``scietex.service.valkey.control.ValkeyControlPublisher`` and
``scietex.service.mqtt.control.MqttControlPublisher``.
"""

from typing import Protocol

from .task_handler.schemas import TaskData


class ControlPublisher(Protocol):
    """Address control commands without knowing the transport.

    ``direct`` targets a single worker by its ``instance_id`` (the address is
    that worker's dedicated control channel); ``broadcast`` targets every
    worker; and ``resolve_owner`` maps a ``task_id`` to the ``instance_id`` of
    the worker that currently owns it, so a submitter can direct a
    ``task:cancel`` to the right worker (design §3.4). A publish failure is
    raised, never swallowed or retried: a silently dropped command is a
    silently scoped one.
    """

    async def direct(self, instance_id: str, task_data: TaskData) -> None:
        """Publish ``task_data`` to ``instance_id``'s dedicated control channel."""
        ...

    async def broadcast(self, task_data: TaskData) -> None:
        """Publish ``task_data`` to every worker's control channel."""
        ...

    async def resolve_owner(self, task_id: str) -> str | None:
        """Return the ``instance_id`` of the worker owning ``task_id``, or ``None``."""
        ...
