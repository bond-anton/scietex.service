"""Built-in handlers for the ``config:*`` remote-configuration task names.

Three handlers mirror the ``task:cancel`` control path: each decodes a
request struct from the task payload and delegates the actual work to a
callback injected by the owning processor. The processor owns the
:class:`~scietex.service.config_reload.ConfigReloader` and the transport
source, so the handlers never reach into processor internals.
"""

from collections.abc import Awaitable, Callable
from typing import ClassVar, Literal

import msgspec

from ..config_reload import (
    INVALID_CONFIG,
    INVALID_CONFIG_PAYLOAD,
    RETRYABLE_ERROR_CODES,
    ConfigApplyOutcome,
    ConfigStoreOutcome,
)
from .basic import TaskHandler
from .capabilities import TaskCapabilities
from .context import TaskHandlerContext
from .schemas import CONFIG_APPLY_TASK_NAME, CONFIG_SHOW_TASK_NAME, CONFIG_STORE_TASK_NAME, TaskData, TaskResult

#: Label recorded on ``ConfigShowResponse.source`` naming where the effective
#: config came from.
ConfigSourceLabel = Literal["default", "file", "remote", "inline"]


class ConfigApplyRequest(msgspec.Struct, frozen=True):
    """Payload of a ``config:apply`` task.

    Args:
        payload: Inline envelope bytes; ``None`` re-reads the source of truth.
        persist: Also write ``config.yml`` after a successful apply.
    """

    payload: bytes | None = None
    persist: bool = False


class ConfigApplyResponse(msgspec.Struct, frozen=True):
    """Payload returned by a successful ``config:apply`` task.

    Args:
        applied: Whether the envelope became effective.
        revision: Revision of the applied envelope.
        hash: Hash of the applied envelope.
        changed: Core field names that changed as a result of the apply.
        restart_required: Field names that exist in the concrete config but
            are not reloadable.
        error: Error description (empty on success).
    """

    applied: bool
    revision: int = 0
    hash: str = ""
    changed: list[str] = msgspec.field(default_factory=list)
    restart_required: list[str] = msgspec.field(default_factory=list)
    error: str = ""


class ConfigStoreRequest(msgspec.Struct, frozen=True):
    """Payload of a ``config:store`` task.

    Args:
        target: Where to persist the effective config: ``"disk"`` (local
            ``config.yml``), ``"remote"`` (the transport source), or
            ``"both"``.
    """

    target: Literal["disk", "remote", "both"] = "disk"


class ConfigStoreResponse(msgspec.Struct, frozen=True):
    """Payload returned by a successful ``config:store`` task.

    Args:
        stored: Whether the config was written.
        target: Which target was requested.
        path: Destination path (empty for a remote source).
        revision: Revision of the stored config.
        hash: Hash of the stored config.
        error: Error description (empty on success).
    """

    stored: bool
    target: str = ""
    path: str = ""
    revision: int = 0
    hash: str = ""
    error: str = ""


class ConfigShowRequest(msgspec.Struct, frozen=True):
    """Payload of a ``config:show`` task.

    Args:
        include_restart_required: Whether to list the restart-required field
            names.
    """

    include_restart_required: bool = True


class ConfigShowResponse(msgspec.Struct, frozen=True):
    """Payload returned by a ``config:show`` task.

    Args:
        settings: msgpack-encoded effective ``ConfigSections``; never secrets.
        declarative_settings: msgpack-encoded declarative ``DeclarativeSections``
            (AR-117); preserves ``None``-means-default and ``auto_tune`` intent.
        revision: Revision of the effective config.
        hash: Hash of the effective config.
        source: Where the effective config came from.
        restart_required_fields: Field names that exist in the concrete config
            but are not reloadable.
        error: Error description (empty on success).
        error_code: Stable outcome code (empty on success).
    """

    settings: bytes = b""
    declarative_settings: bytes = b""
    revision: int = 0
    hash: str = ""
    source: ConfigSourceLabel = "default"
    restart_required_fields: list[str] = msgspec.field(default_factory=list)
    error: str = ""
    error_code: str = ""


#: Async callback injected by the processor to apply a config envelope.
#: ``payload`` is the inline envelope (``None`` re-reads the source of truth);
#: ``persist`` requests a local-file write after a successful apply.
ConfigApplyCallback = Callable[[bytes | None, bool], Awaitable[ConfigApplyOutcome]]

#: Async callback injected by the processor to persist the effective config.
ConfigStoreCallback = Callable[[str], Awaitable[ConfigStoreOutcome]]

#: Synchronous callback injected by the processor to inspect the effective
#: config.
ConfigShowCallback = Callable[[bool], ConfigShowResponse]


class ConfigApplyHandler(TaskHandler):
    """Handler for the built-in ``config:apply`` task name.

    Decodes a :class:`ConfigApplyRequest` and calls the injected ``apply``
    callback. A malformed payload yields a non-retryable error result rather
    than raising, so a bad request never crashes the task loop.
    """

    #: A config command is control-plane: it arrives on a control channel and is
    #: served from the control registry, never the data registry.
    control: ClassVar[bool] = True

    def __init__(
        self,
        name: str,
        context: TaskHandlerContext,
        *,
        apply: ConfigApplyCallback,
    ) -> None:
        """Initialize the handler.

        Args:
            name: Human-readable name for this handler instance.
            context: Narrow context provided by the owning processor.
            apply: Async callback that applies the envelope and returns the
                outcome.
        """
        super().__init__(name, context)
        self._apply: ConfigApplyCallback = apply

    @property
    def supported_tasks(self) -> list[str]:
        """Task types handled by this handler."""
        return [CONFIG_APPLY_TASK_NAME]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        """Apply the config envelope named in the payload.

        Args:
            task_data: Task data whose ``payload`` is a msgpack-encoded
                :class:`ConfigApplyRequest`.
            capabilities: Keyword-only per-call capabilities (unused here; the
                handler only needs the injected ``apply`` callback).

        Returns:
            A ``TaskResult``: ``success`` with a msgpack-encoded
            :class:`ConfigApplyResponse` when the envelope applied, or an
            ``error`` otherwise. A transient source-unavailable outcome is
            retryable; every other failure (including a not-configured source)
            is non-retryable.
        """
        try:
            request = msgspec.msgpack.decode(task_data.payload, type=ConfigApplyRequest)
        except msgspec.DecodeError as exc:
            return TaskResult(
                status="error",
                error=f"invalid config:apply payload: {exc}",
                error_code=INVALID_CONFIG_PAYLOAD,
                retryable=False,
            )

        try:
            outcome = await self._apply(request.payload, request.persist)
        except Exception as exc:
            return TaskResult(
                status="error",
                error=str(exc),
                error_code=INVALID_CONFIG,
                retryable=False,
            )

        if outcome.applied:
            return TaskResult(
                status="success",
                payload=msgspec.msgpack.encode(
                    ConfigApplyResponse(
                        applied=True,
                        revision=outcome.revision,
                        hash=outcome.hash,
                        changed=outcome.changed,
                        restart_required=outcome.restart_required,
                    )
                ),
            )
        return TaskResult(
            status="error",
            error=outcome.error,
            error_code=outcome.error_code,
            retryable=outcome.error_code in RETRYABLE_ERROR_CODES,
        )


class ConfigStoreHandler(TaskHandler):
    """Handler for the built-in ``config:store`` task name.

    Decodes a :class:`ConfigStoreRequest` and calls the injected ``store``
    callback. A malformed payload yields a non-retryable error result rather
    than raising, so a bad request never crashes the task loop.
    """

    #: A config command is control-plane: it arrives on a control channel and is
    #: served from the control registry, never the data registry.
    control: ClassVar[bool] = True

    def __init__(
        self,
        name: str,
        context: TaskHandlerContext,
        *,
        store: ConfigStoreCallback,
    ) -> None:
        """Initialize the handler.

        Args:
            name: Human-readable name for this handler instance.
            context: Narrow context provided by the owning processor.
            store: Async callback that persists the effective config and
                returns the outcome.
        """
        super().__init__(name, context)
        self._store: ConfigStoreCallback = store

    @property
    def supported_tasks(self) -> list[str]:
        """Task types handled by this handler."""
        return [CONFIG_STORE_TASK_NAME]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        """Persist the effective config to the target named in the payload.

        Args:
            task_data: Task data whose ``payload`` is a msgpack-encoded
                :class:`ConfigStoreRequest`.
            capabilities: Keyword-only per-call capabilities (unused here; the
                handler only needs the injected ``store`` callback).

        Returns:
            A ``TaskResult``: ``success`` with a msgpack-encoded
            :class:`ConfigStoreResponse` when the config was stored, or an
            ``error`` otherwise. A transient source-unavailable outcome is
            retryable; every other failure (including a not-configured source)
            is non-retryable.
        """
        try:
            request = msgspec.msgpack.decode(task_data.payload, type=ConfigStoreRequest)
        except msgspec.DecodeError as exc:
            return TaskResult(
                status="error",
                error=f"invalid config:store payload: {exc}",
                error_code=INVALID_CONFIG_PAYLOAD,
                retryable=False,
            )

        try:
            outcome = await self._store(request.target)
        except Exception as exc:
            return TaskResult(
                status="error",
                error=str(exc),
                error_code=INVALID_CONFIG,
                retryable=False,
            )

        if outcome.stored:
            return TaskResult(
                status="success",
                payload=msgspec.msgpack.encode(
                    ConfigStoreResponse(
                        stored=True,
                        target=outcome.target,
                        path=outcome.path,
                        revision=outcome.revision,
                        hash=outcome.hash,
                    )
                ),
            )
        return TaskResult(
            status="error",
            error=outcome.error,
            error_code=outcome.error_code,
            retryable=outcome.error_code in RETRYABLE_ERROR_CODES,
        )


class ConfigShowHandler(TaskHandler):
    """Handler for the built-in ``config:show`` task name.

    Decodes a :class:`ConfigShowRequest` and calls the injected ``show``
    callback. A malformed payload yields a non-retryable error result rather
    than raising, so a bad request never crashes the task loop.
    """

    #: A config command is control-plane: it arrives on a control channel and is
    #: served from the control registry, never the data registry.
    control: ClassVar[bool] = True

    def __init__(
        self,
        name: str,
        context: TaskHandlerContext,
        *,
        show: ConfigShowCallback,
    ) -> None:
        """Initialize the handler.

        Args:
            name: Human-readable name for this handler instance.
            context: Narrow context provided by the owning processor.
            show: Synchronous callback that inspects the effective config and
                returns the response.
        """
        super().__init__(name, context)
        self._show: ConfigShowCallback = show

    @property
    def supported_tasks(self) -> list[str]:
        """Task types handled by this handler."""
        return [CONFIG_SHOW_TASK_NAME]

    async def handle(self, task_data: TaskData, *, capabilities: TaskCapabilities) -> TaskResult:
        """Return the effective config named in the payload.

        Args:
            task_data: Task data whose ``payload`` is a msgpack-encoded
                :class:`ConfigShowRequest`.
            capabilities: Keyword-only per-call capabilities (unused here; the
                handler only needs the injected ``show`` callback).

        Returns:
            A ``TaskResult``: ``success`` with a msgpack-encoded
            :class:`ConfigShowResponse` when the config is inspectable, or an
            ``error`` on a malformed payload, a raising callback, or a
            response carrying a non-empty ``error_code``. A transient
            source-unavailable outcome is retryable; every other failure
            (including a not-configured source) is non-retryable.
        """
        try:
            request = msgspec.msgpack.decode(task_data.payload, type=ConfigShowRequest)
        except msgspec.DecodeError as exc:
            return TaskResult(
                status="error",
                error=f"invalid config:show payload: {exc}",
                error_code=INVALID_CONFIG_PAYLOAD,
                retryable=False,
            )

        try:
            response = self._show(request.include_restart_required)
        except Exception as exc:
            return TaskResult(
                status="error",
                error=str(exc),
                error_code=INVALID_CONFIG,
                retryable=False,
            )

        if response.error_code:
            return TaskResult(
                status="error",
                error=response.error,
                error_code=response.error_code,
                retryable=response.error_code in RETRYABLE_ERROR_CODES,
            )

        return TaskResult(
            status="success",
            payload=msgspec.msgpack.encode(response),
        )
