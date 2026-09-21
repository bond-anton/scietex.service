"""Valkey producer surface for the control plane (AR-123 §7/§9).

``ValkeyControlPublisher`` writes control commands to the Valkey control
streams a :class:`~scietex.service.valkey.worker.ValkeyWorker` reads, and
resolves a task's owning worker from its tracking record.

The channel layout mirrors the worker's: a *direct* command goes to the
addressed worker's dedicated stream ``{control_stream_name}`` (which carries
the ``{instance_id}`` placeholder and is formatted per call), a *broadcast*
goes to the shared ``{control_broadcast_stream_name}``, and ownership is read
from the tracking key ``{status_key_prefix}:{task_id}`` written by
:class:`~scietex.service.valkey.tracking.TaskStatusStore`. ``service_name`` is
folded into the pre-resolved ``status_key_prefix`` (``scietex:{service}:task``)
and the ``{service}``-substituted stream names, so it is not a separate
parameter.

The publisher is transport-only: it addresses and encodes. It takes an
already-built :class:`~scietex.service.task_handler.schemas.TaskData` and never
inspects its type or payload. Control is never retried (design §4.7/§9), so a
failed ``XADD`` is raised rather than swallowed — the submitter must learn that
the command was not sent.
"""

import logging

import msgspec

from ..task_handler import TaskData, TaskStatus
from ..task_handler.wire import encode_task_envelope
from ._glide import GlideClient, StreamAddOptions, TrimByMaxLen
from .transport import TASK_FIELD


class ValkeyControlPublisher:
    """Publish control commands to the Valkey control streams.

    Constructed with an already-connected ``GlideClient`` and the same
    control-stream names and status-key prefix the workers use, so a submitter
    can address commands from any process that can reach Valkey.
    """

    def __init__(
        self,
        *,
        client: GlideClient,
        control_stream_name: str,
        control_broadcast_stream_name: str,
        control_stream_maxlen: int,
        status_key_prefix: str,
        logger: logging.Logger,
    ) -> None:
        """Initialize the publisher.

        Args:
            client: The ``GlideClient`` used for all commands. Passed by
                reference; the publisher never owns or closes it.
            control_stream_name: The per-worker control stream name template.
                It carries an ``{instance_id}`` placeholder formatted per
                ``direct`` call and has ``{service}`` already substituted (it is
                the resolved ``ValkeyWorkerConfig.control_stream_name``).
            control_broadcast_stream_name: The shared broadcast stream name,
                with ``{service}`` already substituted.
            control_stream_maxlen: Maximum retained entries per control stream,
                applied as an approximate ``MAXLEN ~ N`` trim on every ``XADD``.
            status_key_prefix: The resolved tracking-key prefix
                (``scietex:{service}:task``) that ``TaskStatusStore.key()``
                builds on, so ``resolve_owner`` reads the same key the worker
                writes.
            logger: Logger for diagnostics.
        """
        self._client = client
        self._control_stream_name = control_stream_name
        self._control_broadcast_stream_name = control_broadcast_stream_name
        self._control_stream_maxlen = control_stream_maxlen
        self._status_key_prefix = status_key_prefix
        self._logger = logger
        self._status_decoder = msgspec.msgpack.Decoder(TaskStatus)

    async def direct(self, instance_id: str, task_data: TaskData) -> None:
        """Publish ``task_data`` to the addressed worker's control stream.

        The command is encoded into a versioned transport envelope and appended
        under the worker's dedicated stream (``{instance_id}`` substituted).
        A failed ``XADD`` is raised: the submitter must learn the command was
        not sent (AR-123 §4.7).
        """
        await self._xadd(self._control_stream_name.format(instance_id=instance_id), task_data)

    async def broadcast(self, task_data: TaskData) -> None:
        """Publish ``task_data`` to the shared broadcast stream.

        Every worker reads this stream, so the command reaches the whole fleet.
        A failed ``XADD`` is raised (AR-123 §4.7).
        """
        await self._xadd(self._control_broadcast_stream_name, task_data)

    async def resolve_owner(self, task_id: str) -> str | None:
        """Return the ``instance_id`` of the worker owning ``task_id``, if any.

        Reads the tracking record the worker writes at ``{status_key_prefix}:{task_id}``
        and returns its ``instance_id``; ``None`` when the key is absent (the
        task has no owner, e.g. already terminal or not yet running). A missing
        key is not an error — it means the task is not running anywhere.
        """
        raw = await self._client.get(f"{self._status_key_prefix}:{task_id}")
        if raw is None:
            return None
        status = self._status_decoder.decode(raw)
        return status.instance_id or None

    async def _xadd(self, stream_name: str, task_data: TaskData) -> None:
        packed = encode_task_envelope(task_data)
        options = StreamAddOptions(trim=TrimByMaxLen(exact=False, threshold=self._control_stream_maxlen))
        await self._client.xadd(stream_name, [(TASK_FIELD, packed)], options)
