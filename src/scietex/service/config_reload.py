"""Transport-agnostic remote configuration machinery for ``scietex.service``.

Delivers a reloadable-behaviour config envelope to a running worker over the
transport it already uses (a durable Valkey key or an MQTT retained topic)
without this module knowing which transport that is. The envelope
(:class:`ConfigEnvelope`) wraps a versioned, hash-checked, optionally
HMAC-signed snapshot of the hot-reloadable core settings
(:class:`ReloadableSettings`) plus optional named service sections
(:class:`ConfigSections`).

Only the core ``TaskProcessor`` fields on the ``RELOADABLE_FIELDS`` allowlist
(plus explicitly registered service sections) are reloadable. The structs are
declared with ``forbid_unknown_fields=True``, so a payload naming
``queue_size``, credentials, TLS material, or any connection parameter is
rejected rather than silently ignored — restart-required fields are
*unrepresentable*, not merely dropped.

:class:`ConfigReloader` owns the apply pipeline and enforces
validate-before-swap. Every candidate is fully decoded first (including each
registered section against its own struct), then every section apply hook is
run, and only then is the core settings snapshot swapped through the injected
``apply`` callback. A raising hook aborts the apply before any state changes,
so a partial candidate never becomes effective. Applies are serialized behind
an ``asyncio.Lock`` and protected against replay by a monotonic ``revision``.

This module deliberately imports no transport package and no processor type:
transports implement the :class:`ConfigSource` protocol, and the reloader
calls back into the processor through injected callables, so the private
shadows stay private to ``TaskProcessor``.
"""

import asyncio
import hashlib
import hmac
import logging
import os
import tempfile
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import msgspec

#: Current wire-format version of the config envelope.
CONFIG_ENVELOPE_VERSION: int = 1

# Outcome taxonomy. These exact strings are surfaced in ``ConfigApplyOutcome``
# and ``ConfigStoreOutcome`` so callers (task handlers, transports) can branch
# on a stable code instead of parsing free-text error messages. Exactly one
# code (``CONFIG_SOURCE_UNAVAILABLE``, see ``RETRYABLE_ERROR_CODES``) describes
# a transient condition that may succeed on retry; every other code is a
# permanent condition a retry cannot fix.
INVALID_CONFIG_PAYLOAD: str = "INVALID_CONFIG_PAYLOAD"
INVALID_CONFIG: str = "INVALID_CONFIG"
UNKNOWN_CONFIG_SECTION: str = "UNKNOWN_CONFIG_SECTION"
HASH_MISMATCH: str = "HASH_MISMATCH"
BAD_SIGNATURE: str = "BAD_SIGNATURE"
STALE_CONFIG: str = "STALE_CONFIG"
CONFIG_SOURCE_NOT_CONFIGURED: str = "CONFIG_SOURCE_NOT_CONFIGURED"
CONFIG_SOURCE_UNAVAILABLE: str = "CONFIG_SOURCE_UNAVAILABLE"
CONFIG_STORE_FAILED: str = "CONFIG_STORE_FAILED"
REMOTE_CONFIG_DISABLED: str = "REMOTE_CONFIG_DISABLED"

#: Outcome codes describing a transient condition that may succeed on retry:
#: an *attached* source that is momentarily unreachable on read or write.
#: ``CONFIG_SOURCE_NOT_CONFIGURED`` (no source attached) is permanent, as is
#: every validation/hash/signature/disabled outcome.
RETRYABLE_ERROR_CODES: frozenset[str] = frozenset({CONFIG_SOURCE_UNAVAILABLE})

RELOADABLE_FIELDS: frozenset[str] = frozenset(
    {
        "max_concurrent_tasks",
        "task_manager_sleep_time",
        "task_queue_manager_sleep_time",
        "task_handler_start_timeout",
        "task_handler_stop_timeout",
        "task_timeout",
        "task_queue_fetch_timeout",
        "task_cancellation_timeout",
    }
)

_logger = logging.getLogger(__name__)


class ReloadableSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Complete snapshot of the hot-reloadable core fields (all required).

    A partial payload fails loudly instead of silently resetting
    operator-tuned values: every field is required, and any name outside this
    allowlist is rejected by ``forbid_unknown_fields``.
    """

    max_concurrent_tasks: int
    task_manager_sleep_time: float
    task_queue_manager_sleep_time: float
    task_handler_start_timeout: float
    task_handler_stop_timeout: float
    task_timeout: float
    task_queue_fetch_timeout: float
    task_cancellation_timeout: float


class ConfigSections(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Named-section payload carried inside a :class:`ConfigEnvelope`.

    ``core`` holds the reloadable core settings; ``services`` maps a
    registered section name to its msgpack-encoded struct bytes so a custom
    service can extend the reloadable surface without the core knowing its
    fields.
    """

    core: ReloadableSettings
    services: dict[str, bytes] = msgspec.field(default_factory=dict)


class ConfigEnvelope(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Versioned transport envelope for a remote config snapshot.

    Args:
        version: Wire-format version. ``1`` wraps a msgpack-encoded
            :class:`ConfigSections` in ``settings``.
        revision: Monotonic counter used for replay protection.
        hash: ``sha256(settings).hexdigest()``; integrity only.
        signature: Hex HMAC-SHA256 over ``revision`` + ``settings``; empty
            unless signing is enabled.
        settings: msgpack-encoded :class:`ConfigSections`.
        created_at: Optional creation timestamp (informational).
    """

    version: int = CONFIG_ENVELOPE_VERSION
    revision: int = 0
    hash: str = ""
    signature: str = ""
    settings: bytes = b""
    created_at: datetime | None = None


def encode_config_envelope(
    sections: ConfigSections,
    *,
    revision: int,
    signing_key: str | None = None,
    created_at: datetime | None = None,
) -> bytes:
    """Encode a ``ConfigSections`` snapshot into a signed, hashed envelope.

    Args:
        sections: The :class:`ConfigSections` snapshot to wrap.
        revision: Monotonic revision number for replay protection.
        signing_key: Optional HMAC key. When set, the envelope is signed with
            HMAC-SHA256 over ``revision`` + ``settings``; ``None`` leaves the
            signature empty.
        created_at: Optional creation timestamp recorded on the envelope.

    Returns:
        The msgpack-encoded :class:`ConfigEnvelope` bytes.
    """
    settings = msgspec.msgpack.encode(sections)
    digest = hashlib.sha256(settings).hexdigest()
    signature = ""
    if signing_key is not None:
        signature = _compute_signature(signing_key, revision, settings)
    envelope = ConfigEnvelope(
        version=CONFIG_ENVELOPE_VERSION,
        revision=revision,
        hash=digest,
        signature=signature,
        settings=settings,
        created_at=created_at,
    )
    return msgspec.msgpack.encode(envelope)


def decode_config_envelope(payload: bytes) -> ConfigEnvelope | None:
    """Decode a versioned envelope, returning ``None`` on any decode failure.

    Unlike the apply pipeline, this helper performs no version, hash, or
    signature checks — it only turns bytes into a :class:`ConfigEnvelope`
    structure (rejecting unknown fields and malformed msgpack). Callers run
    the security and replay checks separately.

    Args:
        payload: The msgpack-encoded envelope bytes read from the transport.

    Returns:
        The decoded :class:`ConfigEnvelope`, or ``None`` when the payload is
        not a valid envelope.
    """
    try:
        return msgspec.msgpack.decode(payload, type=ConfigEnvelope)
    except msgspec.DecodeError as exc:
        _logger.debug("Failed to decode config envelope: %s", exc)
        return None


def peek_config_envelope_version(payload: bytes) -> int | None:
    """Return the wire-format version of an envelope, or ``None`` if malformed.

    Used by callers to distinguish "unsupported version" from "corrupt" when
    :func:`decode_config_envelope` returns ``None``.

    Args:
        payload: The msgpack-encoded envelope bytes read from the transport.

    Returns:
        The envelope's version, or ``None`` when the payload is not a valid
        envelope.
    """
    try:
        envelope = msgspec.msgpack.decode(payload, type=ConfigEnvelope)
        return envelope.version
    except msgspec.DecodeError:
        return None


class ConfigSource(Protocol):
    """Delivery backend a :class:`ConfigReloader` reads and writes through.

    A transport implements ``load`` to read the desired-state envelope (a
    durable Valkey ``GET`` or an MQTT retained snapshot) and ``store`` to
    write the current effective config back. Keeping this protocol in core
    lets both transports implement it without a feature-to-feature
    dependency.
    """

    async def load(self) -> bytes | None: ...

    async def store(self, envelope: bytes) -> None: ...


class ConfigApplyOutcome(msgspec.Struct, frozen=True):
    """Result of an envelope apply attempt.

    Args:
        applied: Whether the envelope became effective (or was already the
            effective state, idempotently).
        revision: Revision of the applied envelope.
        hash: Hash of the applied envelope.
        changed: Core field names that changed as a result of the apply.
        restart_required: Field names that exist in the concrete config but
            are not reloadable.
        error: Free-text error description when ``applied`` is ``False``.
        error_code: Stable outcome code (one of the module error constants).
    """

    applied: bool
    revision: int = 0
    hash: str = ""
    changed: list[str] = msgspec.field(default_factory=list)
    restart_required: list[str] = msgspec.field(default_factory=list)
    error: str = ""
    error_code: str = ""


class ConfigStoreOutcome(msgspec.Struct, frozen=True):
    """Result of a config store (persist-back) attempt.

    Args:
        stored: Whether the envelope was written to the source.
        target: Which target was requested (e.g. ``"remote"``).
        path: Destination path (empty for a remote source).
        revision: Revision of the stored envelope.
        hash: Hash of the stored envelope.
        error: Free-text error description when ``stored`` is ``False``.
        error_code: Stable outcome code (one of the module error constants).
    """

    stored: bool
    target: str = ""
    path: str = ""
    revision: int = 0
    hash: str = ""
    error: str = ""
    error_code: str = ""


class ConfigReloader:
    """Owns the remote-config apply, reload, store, and show pipeline.

    The reloader is transport-agnostic: it reads and writes envelopes through
    a :class:`ConfigSource` and mutates the processor through injected
    callables. Apply semantics are validate-before-swap — every candidate is
    fully decoded and validated (including each registered section against its
    own struct) and every section hook is run *before* the core settings are
    swapped, so a raising hook aborts the apply with no state change.
    Applies are serialized behind an ``asyncio.Lock``.

    Args:
        apply: Callback that validates and swaps the core settings, returning
            the changed field names. It must validate before mutating and
            raise on an invalid candidate so the reloader can leave state
            unchanged.
        current: Callback returning the current effective core settings.
        restart_required: Callback returning the field names that exist in the
            concrete config but are not reloadable.
        logger: Logger for apply/reload/store diagnostics.
        signing_key: Optional HMAC key for envelope authenticity. ``None``
            disables signature enforcement.
        enabled: Master switch. When ``False``, ``apply_envelope``,
            ``reload``, and ``store`` short-circuit with
            ``REMOTE_CONFIG_DISABLED``.
    """

    def __init__(
        self,
        *,
        apply: Callable[[ReloadableSettings], list[str]],
        current: Callable[[], ReloadableSettings],
        restart_required: Callable[[], list[str]],
        logger: logging.Logger,
        signing_key: str | None = None,
        enabled: bool = True,
    ) -> None:
        self._apply = apply
        self._current = current
        self._restart_required = restart_required
        self._logger = logger
        self._signing_key = signing_key
        self._enabled = enabled

        self._lock = asyncio.Lock()
        self._applied_revision: int = 0
        self._applied_hash: str = ""
        self._source: str = "default"
        # section name -> (struct type, apply hook)
        self._sections: dict[str, tuple[type[msgspec.Struct], Callable[[Any], None]]] = {}
        # section name -> last-applied raw bytes, captured for store/show.
        self._section_raw: dict[str, bytes] = {}

    def register_section(
        self,
        name: str,
        struct_type: type[msgspec.Struct],
        apply: Callable[[Any], None],
    ) -> None:
        """Register a service settings struct and its apply hook.

        Additive and idempotent per section name: re-registering the same name
        replaces the struct and hook. Each registered section is decoded
        against its struct and its hook invoked (in registration order) during
        an apply; an unregistered section name in an envelope is rejected.

        Args:
            name: Section name used as the key in ``ConfigSections.services``.
            struct_type: The ``msgspec.Struct`` type to decode the section
                bytes against.
            apply: Hook called with the decoded struct during an apply.
        """
        self._sections[name] = (struct_type, apply)

    def reset(self) -> None:
        """Clear run-scoped apply bookkeeping for a fresh run start (AR-111).

        ``_applied_revision``/``_applied_hash``/``_source``/``_section_raw`` are
        instance-lifetime otherwise, so a second ``start()`` of the same worker
        would reject the revision-1 local snapshot as ``STALE_CONFIG`` and keep
        the previous run's shadow config. Registered sections (configuration)
        and the injected callbacks are intentionally left untouched, and the
        replay guard still applies to every envelope within a run.

        Must be called at the run boundary, before any startup apply.
        """
        self._applied_revision = 0
        self._applied_hash = ""
        self._source = "default"
        self._section_raw = {}

    async def apply_envelope(self, payload: bytes, *, source: str, trusted: bool = False) -> ConfigApplyOutcome:
        """Apply a remote config envelope under the reloader's lock.

        Runs the full pipeline: decode, version, hash, optional signature,
        replay, decode sections, run section hooks, then swap the core
        settings. Returns a structured :class:`ConfigApplyOutcome` for every
        terminal state instead of raising.

        Args:
            payload: The msgpack-encoded :class:`ConfigEnvelope` bytes.
            source: Label recorded on success (e.g. ``"remote"`` or
                ``"inline"``).
            trusted: When ``True``, signature verification is skipped because
                the envelope is a trusted local artifact (the persisted
                ``config.yml`` snapshot) rather than input read off a
                transport. The replay guard still applies. Must never be set
                for remote or inline input, which is untrusted.

        Returns:
            A :class:`ConfigApplyOutcome` describing the result.
        """
        if not self._enabled:
            return ConfigApplyOutcome(applied=False, error_code=REMOTE_CONFIG_DISABLED)
        async with self._lock:
            envelope = decode_config_envelope(payload)
            if envelope is None:
                self._logger.error("Config apply rejected: invalid envelope payload")
                return ConfigApplyOutcome(applied=False, error_code=INVALID_CONFIG_PAYLOAD)
            if envelope.version != CONFIG_ENVELOPE_VERSION:
                self._logger.error(
                    "Config apply rejected: unsupported version %d (expected %d)",
                    envelope.version,
                    CONFIG_ENVELOPE_VERSION,
                )
                return ConfigApplyOutcome(applied=False, error_code=INVALID_CONFIG)
            if hashlib.sha256(envelope.settings).hexdigest() != envelope.hash:
                self._logger.error("Config apply rejected: settings hash mismatch")
                return ConfigApplyOutcome(applied=False, error_code=HASH_MISMATCH)
            if (
                not trusted
                and self._signing_key is not None
                and not hmac.compare_digest(
                    _compute_signature(self._signing_key, envelope.revision, envelope.settings),
                    envelope.signature,
                )
            ):
                self._logger.error("Config apply rejected: bad signature")
                return ConfigApplyOutcome(applied=False, error_code=BAD_SIGNATURE)
            if envelope.revision < self._applied_revision:
                self._logger.debug(
                    "Config apply skipped: stale revision %d < %d",
                    envelope.revision,
                    self._applied_revision,
                )
                return ConfigApplyOutcome(applied=False, error_code=STALE_CONFIG)
            if envelope.revision == self._applied_revision:
                if envelope.hash != self._applied_hash:
                    self._logger.debug(
                        "Config apply skipped: stale hash at revision %d",
                        envelope.revision,
                    )
                    return ConfigApplyOutcome(applied=False, error_code=STALE_CONFIG)
                self._logger.debug(
                    "Config apply idempotent: revision %d already applied",
                    envelope.revision,
                )
                return ConfigApplyOutcome(
                    applied=True,
                    revision=envelope.revision,
                    hash=envelope.hash,
                    changed=[],
                )

            try:
                sections = msgspec.msgpack.decode(envelope.settings, type=ConfigSections)
            except msgspec.DecodeError as exc:
                self._logger.error("Config apply rejected: invalid settings payload: %s", exc)
                return ConfigApplyOutcome(applied=False, error_code=INVALID_CONFIG, error=str(exc))

            decoded: list[tuple[str, msgspec.Struct]] = []
            for name, raw in sections.services.items():
                entry = self._sections.get(name)
                if entry is None:
                    self._logger.error("Config apply rejected: unknown section %r", name)
                    return ConfigApplyOutcome(
                        applied=False,
                        error_code=UNKNOWN_CONFIG_SECTION,
                        error=f"unknown config section {name!r}",
                    )
                struct_type, _ = entry
                try:
                    decoded.append((name, msgspec.msgpack.decode(raw, type=struct_type)))
                except msgspec.DecodeError as exc:
                    self._logger.error("Config apply rejected: section %r invalid: %s", name, exc)
                    return ConfigApplyOutcome(
                        applied=False,
                        error_code=INVALID_CONFIG,
                        error=f"section {name!r}: {exc}",
                    )

            # Section hooks run before the core swap so a raising hook aborts
            # the apply with no state change (validate-before-swap).
            for name, value in decoded:
                _, hook = self._sections[name]
                try:
                    hook(value)
                except Exception as exc:
                    self._logger.error("Config apply rejected: section %r hook failed: %s", name, exc)
                    return ConfigApplyOutcome(
                        applied=False,
                        error_code=INVALID_CONFIG,
                        error=f"section {name!r}: {exc}",
                    )

            try:
                changed = self._apply(sections.core)
            except Exception as exc:
                self._logger.error("Config apply rejected: core settings invalid: %s", exc)
                return ConfigApplyOutcome(applied=False, error_code=INVALID_CONFIG, error=str(exc))

            self._applied_revision = envelope.revision
            self._applied_hash = envelope.hash
            self._source = source
            self._section_raw = dict(sections.services)
            self._logger.info(
                "Applied config revision %d from %s (%d fields changed)",
                envelope.revision,
                source,
                len(changed),
            )
            return ConfigApplyOutcome(
                applied=True,
                revision=envelope.revision,
                hash=envelope.hash,
                changed=changed,
                restart_required=self._restart_required(),
            )

    async def reload(self, source: ConfigSource) -> ConfigApplyOutcome:
        """Load the desired-state envelope from ``source`` and apply it.

        A ``None`` payload (source has no config) and any ``load`` exception
        both map to ``CONFIG_SOURCE_UNAVAILABLE``; the worker keeps its
        current config either way.

        Args:
            source: The :class:`ConfigSource` to read the envelope from.

        Returns:
            A :class:`ConfigApplyOutcome` describing the result.
        """
        if not self._enabled:
            return ConfigApplyOutcome(applied=False, error_code=REMOTE_CONFIG_DISABLED)
        try:
            payload = await source.load()
        except Exception as exc:
            self._logger.warning("Config source load failed: %s", exc)
            return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_UNAVAILABLE)
        if payload is None:
            return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_UNAVAILABLE)
        return await self.apply_envelope(payload, source="remote")

    async def store(self, source: ConfigSource, *, target: str = "remote") -> ConfigStoreOutcome:
        """Persist the current effective config back to ``source``.

        Builds a :class:`ConfigSections` snapshot from the current core
        settings plus the last-applied raw section bytes, wraps it in a
        signed envelope at the current revision, and hands it to
        ``source.store``. A source store failure maps to
        ``CONFIG_SOURCE_UNAVAILABLE`` (transient); a local disk write failure
        is ``CONFIG_STORE_FAILED`` (`ConfigManager.write_local`).

        Args:
            source: The :class:`ConfigSource` to write the envelope to.
            target: Label recorded on the outcome (e.g. ``"remote"``).

        Returns:
            A :class:`ConfigStoreOutcome` describing the result.
        """
        if not self._enabled:
            return ConfigStoreOutcome(
                stored=False,
                error_code=REMOTE_CONFIG_DISABLED,
                error="remote config is disabled",
            )
        sections = ConfigSections(core=self._current(), services=dict(self._section_raw))
        settings = msgspec.msgpack.encode(sections)
        digest = hashlib.sha256(settings).hexdigest()
        signature = ""
        if self._signing_key is not None:
            signature = _compute_signature(self._signing_key, self._applied_revision, settings)
        envelope = ConfigEnvelope(
            version=CONFIG_ENVELOPE_VERSION,
            revision=self._applied_revision,
            hash=digest,
            signature=signature,
            settings=settings,
        )
        try:
            await source.store(msgspec.msgpack.encode(envelope))
        except Exception as exc:
            self._logger.error("Config source store failed: %s", exc)
            return ConfigStoreOutcome(
                stored=False,
                target=target,
                revision=self._applied_revision,
                hash=digest,
                error=str(exc),
                error_code=CONFIG_SOURCE_UNAVAILABLE,
            )
        return ConfigStoreOutcome(
            stored=True,
            target=target,
            revision=self._applied_revision,
            hash=digest,
        )

    def show(self) -> ConfigSections:
        """Return the current effective sections snapshot.

        ``core`` comes from the ``current`` callback; ``services`` holds the
        raw bytes captured at the last successful apply.

        Returns:
            The current effective :class:`ConfigSections`.
        """
        return ConfigSections(core=self._current(), services=dict(self._section_raw))

    @property
    def enabled(self) -> bool:
        """Whether remote config is enabled (the master switch)."""
        return self._enabled

    @property
    def revision(self) -> int:
        """The revision of the last successfully applied envelope."""
        return self._applied_revision

    @property
    def hash(self) -> str:
        """The hash of the last successfully applied envelope."""
        return self._applied_hash

    @property
    def source(self) -> str:
        """The source label of the last successfully applied envelope."""
        return self._source


def _compute_signature(signing_key: str, revision: int, settings: bytes) -> str:
    """Return the hex HMAC-SHA256 over ``revision`` + ``settings``."""
    return hmac.new(
        signing_key.encode(),
        revision.to_bytes(8, "big") + settings,
        hashlib.sha256,
    ).hexdigest()


def read_local_config(path: Path) -> ConfigSections | None:
    """Read a local config snapshot from ``path``, or ``None`` when absent.

    Write-free: a missing file returns ``None`` without creating anything
    (unlike the ``valkey.yml`` loader). An unreadable or invalid file is
    logged as an error and also returns ``None``, so startup falls back to
    the current/default config.

    Args:
        path: Path to the YAML snapshot (``config.yml``).

    Returns:
        The decoded :class:`ConfigSections`, or ``None`` when the file is
        missing or invalid.
    """
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        _logger.error("Failed to read local config %s: %s", path, exc)
        return None
    try:
        return msgspec.yaml.decode(data, type=ConfigSections, strict=True)
    except msgspec.DecodeError as exc:
        _logger.error("Invalid local config %s: %s", path, exc)
        return None


def write_local_config(path: Path, sections: ConfigSections) -> None:
    """Atomically write ``sections`` to ``path`` as YAML.

    Encodes the sections with ``msgspec.yaml.encode``, writes to a temporary
    file in the same directory, then ``os.replace``-s it into place so a
    reader never sees a partial file. The parent directory is created if
    missing, and the temporary file is removed if the write fails.

    Args:
        path: Destination file path (``config.yml``).
        sections: The validated :class:`ConfigSections` snapshot to persist.

    Raises:
        OSError: If the temporary file cannot be created, written, or moved.
    """
    data = msgspec.yaml.encode(sections)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(data)
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


__all__ = [
    "BAD_SIGNATURE",
    "CONFIG_ENVELOPE_VERSION",
    "CONFIG_SOURCE_NOT_CONFIGURED",
    "CONFIG_SOURCE_UNAVAILABLE",
    "CONFIG_STORE_FAILED",
    "ConfigApplyOutcome",
    "ConfigEnvelope",
    "ConfigReloader",
    "ConfigSections",
    "ConfigSource",
    "ConfigStoreOutcome",
    "HASH_MISMATCH",
    "INVALID_CONFIG",
    "INVALID_CONFIG_PAYLOAD",
    "RELOADABLE_FIELDS",
    "REMOTE_CONFIG_DISABLED",
    "RETRYABLE_ERROR_CODES",
    "ReloadableSettings",
    "STALE_CONFIG",
    "UNKNOWN_CONFIG_SECTION",
    "decode_config_envelope",
    "encode_config_envelope",
    "peek_config_envelope_version",
    "read_local_config",
    "write_local_config",
]
