"""Transport-agnostic remote configuration: envelope, source, and reloader."""

import asyncio
import hashlib
import hmac
import logging
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import msgspec

from .config_merge import merge_all, resolve_section

#: Current wire-format version of the config envelope.
CONFIG_ENVELOPE_VERSION: int = 2

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

    Retained for the core apply path (the resolver still produces this concrete
    snapshot); it is no longer the wire type, which is a patch dict (§5).
    """

    max_concurrent_tasks: int
    task_manager_sleep_time: float
    task_queue_manager_sleep_time: float
    task_handler_start_timeout: float
    task_handler_stop_timeout: float
    task_timeout: float
    task_queue_fetch_timeout: float
    task_cancellation_timeout: float


class DeclarativeSections(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Local ``config.yml`` artifact: declarative core patch + service patches.

    ``core`` is the merged L2 core patch (``dict``) or ``None`` when no core
    key is explicitly set; ``services`` maps a registered section name to its
    patch dict. A patch dict follows the three-state rule: a key absent means
    "inherit the layer below", a key present with ``null`` means "clear", and a
    key present with a value means "set" (§2.1). ``core`` absent means "no core
    patch" — the same rule that governs every service section.
    """

    core: dict | None = None
    services: dict[str, dict] = msgspec.field(default_factory=dict)


class ConfigSections(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Named-section payload carried inside a :class:`ConfigEnvelope`.

    ``core`` is the remote (L3) core patch dict, or ``None`` when a producer
    delivers only service sections (e.g. the API, which does not track a
    worker's core settings); the core layers are then left untouched.
    ``services`` maps a registered section name to its L3 patch dict, so a
    custom service can extend the reloadable surface without the core knowing
    its fields.
    """

    core: dict | None = None
    services: dict[str, dict] = msgspec.field(default_factory=dict)


class ConfigEnvelope(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Versioned transport envelope for a remote config snapshot.

    Args:
        version: Wire-format version. ``2`` wraps a msgpack-encoded
            :class:`ConfigSections` of patch dicts in ``settings``.
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
    """
    try:
        envelope = msgspec.msgpack.decode(payload, type=ConfigEnvelope)
        return envelope.version
    except msgspec.DecodeError:
        return None


class ConfigSource(Protocol):
    """Delivery backend a :class:`ConfigReloader` reads and writes through.

    ``load`` returns the desired-state envelope as currently known to the
    source, without waiting for transport delivery, or ``None`` when no desired
    state is known. Freshness is transport-inherent: an on-demand backend reads
    live on each call while a push-only backend returns the last snapshot it
    recorded, so callers must treat it as best-effort current state, never as a
    guarantee of broker-live state. ``store`` writes an envelope back. A
    transport whose state only arrives asynchronously exposes a
    transport-specific ``wait_for_snapshot(timeout)`` outside this protocol, so
    the reloader's apply path never blocks on delivery.
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


@dataclass(frozen=True, slots=True)
class _RegisteredSection:
    """Registration record for one named config section.

    Args:
        struct_type: The concrete ``msgspec.Struct`` the merged patch is
            converted through (validates + coerces every level).
        hook: Called with the merged struct on each apply that touches the
            section; the hook is the validation point for services whose
            struct defers validation to ``to_gateway_config``.
        defaults: The concrete L0 base instance; ``None`` uses
            ``struct_type()`` as the L0 base.
        bootstrap: Optional L1 provider returning a patch dict; ``None`` means
            the section has no L1 (single-layer).
    """

    struct_type: type[msgspec.Struct]
    hook: Callable[[Any], None]
    defaults: msgspec.Struct | None = None
    bootstrap: Callable[[], dict | None] | None = None


class ConfigReloader:
    """Owns the remote-config apply, reload, store, and show pipeline.

    Transport-agnostic: it reads and writes envelopes through a
    :class:`ConfigSource` and mutates the processor through injected callables.
    Configuration precedence is a four-layer overlay resolved from stored layer
    patches on every apply (§3.5): L0 constructor defaults, L1 service
    bootstrap, L2 declarative ``config.yml``, L3 remote envelope. Each layer is
    a patch dict with the three-state rule (absent = inherit, ``null`` = clear,
    value = set); the reloader stores per-section layer patches and re-merges
    them from L0 upward, converting the merged dict through the section's
    concrete struct (validate-before-swap). The core block follows the same
    path with L2/L3 only — the reloader stores the L2/L3 core patches, merges
    them, and hands the merged patch dict to the injected ``apply`` callable,
    which owns the L0 base and terminal resolution. Applies are serialized
    behind an ``asyncio.Lock`` and protected against replay by a monotonic
    ``revision``.
    """

    def __init__(
        self,
        *,
        apply: Callable[[dict[str, object]], list[str]],
        current: Callable[[], ReloadableSettings],
        restart_required: Callable[[], list[str]],
        logger: logging.Logger,
        signing_key: str | None = None,
        enabled: bool = True,
        validate_core: Callable[[dict[str, object]], None] | None = None,
        apply_declarative: Callable[[dict[str, object]], list[str]] | None = None,
    ) -> None:
        """Initialize the reloader.

        Args:
            apply: Callback that overlays the merged core patch onto the
                current config, validates it, and swaps it in, returning the
                changed core field names. It receives the merged L2+L3 core
                patch dict (keys present only; ``null`` preserved as "clear")
                and performs the terminal resolution (``DEFAULT_*`` fallback
                and auto-tune) itself. It must validate before mutating and
                raise on an invalid candidate so the reloader can leave state
                unchanged.
            current: Callback returning the current effective core settings,
                used to render the effective ``show`` view (§6.3).
            restart_required: Callback returning the field names that exist in
                the concrete config but are not reloadable.
            logger: Logger for apply/reload/store diagnostics.
            signing_key: Optional HMAC key for envelope authenticity. ``None``
                disables signature enforcement.
            enabled: Master switch. When ``False``, ``apply_envelope``,
                ``reload``, and ``store`` short-circuit with
                ``REMOTE_CONFIG_DISABLED``.
            validate_core: Optional callback that validates the merged core
                patch (including cleared fields) before any section hook runs,
                so an out-of-range core value aborts the apply with no state
                change (validate-before-swap, §3.4). ``None`` skips the
                pre-hook check; the terminal ``apply`` still validates.
            apply_declarative: Optional callback that overlays the merged core
                patch from the declarative (L2) view, returning the changed
                core field names. ``None`` makes ``apply_declarative_sections``
                reject with ``INVALID_CONFIG``.
        """
        self._apply = apply
        self._current = current
        self._restart_required = restart_required
        self._logger = logger
        self._signing_key = signing_key
        self._enabled = enabled
        self._validate_core = validate_core
        self._apply_declarative = apply_declarative

        self._lock = asyncio.Lock()
        self._applied_revision: int = 0
        self._applied_hash: str = ""
        self._source: str = "default"
        # section name -> registration record (struct type, hook, defaults, bootstrap)
        self._sections: dict[str, _RegisteredSection] = {}
        # section name -> {"L1": patch, "L2": patch, "L3": patch} (each dict | None)
        self._section_layers: dict[str, dict[str, dict | None]] = {}
        # section name -> last resolved effective struct (the merge cache)
        self._resolved: dict[str, msgspec.Struct] = {}
        # Core patch layers. The reloader owns only L2 (declarative) and L3
        # (remote); L0 lives in the processor and L1 carries no core (§5).
        self._core_layers: dict[str, dict | None] = {"L2": None, "L3": None}

    def register_section(
        self,
        name: str,
        struct_type: type[msgspec.Struct],
        apply: Callable[[Any], None],
        *,
        defaults: msgspec.Struct | None = None,
        bootstrap: Callable[[], dict | None] | None = None,
    ) -> None:
        """Register a service settings struct and its apply hook.

        Additive and idempotent per section name: re-registering the same name
        replaces the struct, hook, defaults, and bootstrap provider. Each
        registered section's merged patch is converted through its struct and
        its hook invoked (in registration order) during an apply; an
        unregistered section name in an envelope is rejected.

        Args:
            name: Section name used as the key in ``ConfigSections.services``.
            struct_type: The ``msgspec.Struct`` type the merged patch is
                converted through (validates + coerces every level).
            apply: Hook called with the merged struct during an apply.
            defaults: Concrete L0 base instance; ``None`` uses ``struct_type()``
                as the L0 base.
            bootstrap: Optional L1 provider returning a patch dict; ``None``
                means the section has no L1 (single-layer).
        """
        self._sections[name] = _RegisteredSection(struct_type, apply, defaults, bootstrap)
        self._section_layers[name] = {"L1": None, "L2": None, "L3": None}
        self._resolved.pop(name, None)

    def reset(self) -> None:
        """Clear run-scoped apply bookkeeping for a fresh run start (AR-111).

        ``_applied_revision``/``_applied_hash``/``_source``, every section's
        layer patches, the resolved-struct cache, and the core layers are
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
        for name in self._section_layers:
            self._section_layers[name] = {"L1": None, "L2": None, "L3": None}
        self._resolved = {}
        self._core_layers = {"L2": None, "L3": None}

    def seed_bootstrap(self) -> None:
        """Seed each registered section's L1 patch from its bootstrap provider.

        For each registered section with a ``bootstrap`` provider, call it,
        store the result as that section's L1 patch, and resolve L0+L1 into the
        section's effective struct (cached, so ``current_settings`` returns it).
        Sections without a provider stay single-layer (no L1) and resolve on
        first apply.

        A provider that raises, or returns a patch that fails validation, is
        logged and leaves that section unresolved: a bad bootstrap must not
        abort the whole run start, and the section's own apply path re-validates
        on the next envelope.
        """
        for name, entry in self._sections.items():
            if entry.bootstrap is None:
                continue
            try:
                self._section_layers[name]["L1"] = entry.bootstrap()
                self._resolve_section(name)
            except Exception as exc:
                self._logger.error("Failed to seed bootstrap for section %r: %s", name, exc)

    def current_settings(self, name: str) -> msgspec.Struct | None:
        """Return the last resolved effective struct for ``name``.

        ``None`` for an unregistered or never-resolved section. Services use
        this to build runtime objects from the merged settings (§6.7).
        """
        return self._resolved.get(name)

    def _resolve_section(self, name: str) -> msgspec.Struct:
        """Resolve ``name``'s effective struct from its stored layers and cache it."""
        resolved = self._candidate(name, self._section_layers[name]["L3"], layer="L3")
        self._resolved[name] = resolved
        return resolved

    def _candidate(self, name: str, patch: dict | None, *, layer: str) -> msgspec.Struct:
        """Resolve a section with ``patch`` staged at ``layer`` ("L2" or "L3").

        Pure: builds the candidate from the staged patch without committing it,
        so a validation failure leaves all layer state untouched.
        """
        entry = self._sections[name]
        layers = self._section_layers[name]
        if layer == "L3":
            ordered: list[dict | None] = [patch, layers["L2"], layers["L1"]]
        else:
            ordered = [layers["L3"], patch, layers["L1"]]
        return resolve_section(entry.struct_type, ordered, defaults=entry.defaults)

    def _merge_core(self, patch: dict | None, *, layer: str) -> dict[str, object]:
        """Merge the staged core ``patch`` at ``layer`` with the other layer.

        ``patch=None`` keeps the layer's stored value (used when the apply
        carries no core). Returns the merged core patch dict, preserving an
        explicit ``None`` as "clear" (the terminal resolver fills it with the
        ``DEFAULT_*`` constant or auto-tune). Unlike the RFC 7396 section merge,
        a cleared key is kept present so ``_overlay_reloadable`` can distinguish
        "clear" (reset to default) from "absent" (inherit current).
        """
        l2 = self._core_layers["L2"]
        l3 = self._core_layers["L3"]
        if layer == "L3":
            l3 = patch if patch is not None else l3
        else:
            l2 = patch if patch is not None else l2
        # Last-writer-wins over the two flat patch dicts: L2 then L3, so L3
        # wins on a key both set. ``None`` is a first-class value here, not a
        # tombstone — the terminal resolver is the only clear-handling point.
        merged: dict[str, object] = {}
        if l2:
            merged.update(l2)
        if l3:
            merged.update(l3)
        return merged

    def _resolve_and_validate(
        self,
        services: dict[str, dict],
        core: dict | None,
        *,
        layer: str,
    ) -> tuple[list[tuple[str, _RegisteredSection, msgspec.Struct]], dict[str, object]] | ConfigApplyOutcome:
        """Resolve + validate every touched section and the core candidate.

        Pure with respect to layer state: builds candidate structs from the
        staged ``layer`` patches without committing anything, so a validation
        failure leaves all layers, the resolved cache, and bookkeeping
        untouched (validate-before-swap). Returns ``(resolved, merged_core)``
        on success or an error outcome.
        """
        resolved: list[tuple[str, _RegisteredSection, msgspec.Struct]] = []
        for name, patch in services.items():
            entry = self._sections.get(name)
            if entry is None:
                self._logger.error("Config apply rejected: unknown section %r", name)
                return ConfigApplyOutcome(
                    applied=False,
                    error_code=UNKNOWN_CONFIG_SECTION,
                    error=f"unknown config section {name!r}",
                )
            try:
                candidate = self._candidate(name, patch, layer=layer)
            except msgspec.ValidationError as exc:
                self._logger.error("Config apply rejected: section %r invalid: %s", name, exc)
                return ConfigApplyOutcome(
                    applied=False,
                    error_code=INVALID_CONFIG,
                    error=f"section {name!r}: {exc}",
                )
            resolved.append((name, entry, candidate))

        if core is not None:
            # Mandatory core key allowlist (§5): the reloader's core patch is a
            # plain dict, so an unknown key would otherwise reach the overlay.
            unknown = sorted(set(core) - RELOADABLE_FIELDS)
            if unknown:
                self._logger.error("Config apply rejected: core field(s) not reloadable: %s", ", ".join(unknown))
                return ConfigApplyOutcome(
                    applied=False,
                    error_code=INVALID_CONFIG,
                    error=f"core field(s) not reloadable: {', '.join(unknown)}",
                )

        merged_core = self._merge_core(core, layer=layer)

        # Validate the merged core candidate before any section hook runs, so an
        # out-of-range core value aborts the apply with no state change
        # (validate-before-swap). The terminal ``apply`` is the backstop, but it
        # runs after the hooks, so the pre-hook check is what keeps a bad core
        # from mutating a section first.
        if core is not None and self._validate_core is not None:
            try:
                self._validate_core(merged_core)
            except Exception as exc:
                self._logger.error("Config apply rejected: core settings invalid: %s", exc)
                return ConfigApplyOutcome(applied=False, error_code=INVALID_CONFIG, error=str(exc))

        return resolved, merged_core

    async def apply_envelope(self, payload: bytes, *, source: str, trusted: bool = False) -> ConfigApplyOutcome:
        """Apply a remote config envelope under the reloader's lock.

        Runs the full pipeline: decode, version, hash, optional signature,
        replay, decode sections, resolve each touched section to its merged
        struct (validate-before-swap), run section hooks, then apply the merged
        core patch through the injected ``apply``. Returns a structured
        :class:`ConfigApplyOutcome` for every terminal state instead of raising.

        Args:
            payload: The msgpack-encoded :class:`ConfigEnvelope` bytes.
            source: Label recorded on success (e.g. ``"remote"`` or
                ``"inline"``).
            trusted: When ``True``, signature verification is skipped because
                the envelope is a trusted local artifact (the persisted
                ``config.yml`` snapshot) rather than input read off a
                transport. The replay guard still applies. Must never be set
                for remote or inline input, which is untrusted.
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

            prepared = self._resolve_and_validate(sections.services, sections.core, layer="L3")
            if isinstance(prepared, ConfigApplyOutcome):
                return prepared
            resolved, merged_core = prepared

            # Section hooks run before the core swap so a raising hook aborts
            # the apply with no state change (validate-before-swap).
            for name, entry, candidate in resolved:
                try:
                    entry.hook(candidate)
                except Exception as exc:
                    self._logger.error("Config apply rejected: section %r hook failed: %s", name, exc)
                    return ConfigApplyOutcome(
                        applied=False,
                        error_code=INVALID_CONFIG,
                        error=f"section {name!r}: {exc}",
                    )

            # A producer may deliver only service sections (core absent); the
            # core layers are then left untouched and the merged core is not
            # re-applied. The revision/hash bookkeeping still advances so the
            # envelope is not re-applied.
            changed: list[str] = []
            if sections.core is not None:
                try:
                    changed = self._apply(merged_core)
                except Exception as exc:
                    self._logger.error("Config apply rejected: core settings invalid: %s", exc)
                    return ConfigApplyOutcome(applied=False, error_code=INVALID_CONFIG, error=str(exc))

            # Commit layer state + bookkeeping only after every hook and the
            # core apply succeeded (validate-before-swap).
            for name, _, candidate in resolved:
                self._section_layers[name]["L3"] = sections.services[name]
                self._resolved[name] = candidate
            if sections.core is not None:
                self._core_layers["L3"] = sections.core

            self._applied_revision = envelope.revision
            self._applied_hash = envelope.hash
            self._source = source
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

    async def apply_declarative_sections(
        self, sections: DeclarativeSections, *, source: str, trusted: bool = False
    ) -> ConfigApplyOutcome:
        """Apply a declarative sections snapshot (the local ``config.yml`` artifact).

        Mirrors ``apply_envelope`` for the declarative persistence view: each
        service section's patch is staged as its L2 layer and re-resolved, and
        the core patch is applied through ``apply_declarative``. The local
        artifact is revision 1 by contract, so success records revision 1 and
        the hash of the msgpack-encoded declarative sections.

        The declarative path is inherently trusted (a local artifact, not
        transport input), so ``trusted`` is accepted only for signature
        symmetry with ``apply_envelope`` and no signature verification is
        performed regardless of its value.

        Args:
            sections: The :class:`DeclarativeSections` snapshot to apply.
            source: Label recorded on success (e.g. ``"file"``).
            trusted: Accepted for signature symmetry with ``apply_envelope``;
                unused, as the declarative path never verifies signatures.
        """
        if not self._enabled:
            return ConfigApplyOutcome(applied=False, error_code=REMOTE_CONFIG_DISABLED)
        if self._apply_declarative is None:
            return ConfigApplyOutcome(
                applied=False,
                error_code=INVALID_CONFIG,
                error="declarative apply not configured",
            )
        async with self._lock:
            prepared = self._resolve_and_validate(sections.services, sections.core, layer="L2")
            if isinstance(prepared, ConfigApplyOutcome):
                return prepared
            resolved, merged_core = prepared

            for name, entry, candidate in resolved:
                try:
                    entry.hook(candidate)
                except Exception as exc:
                    self._logger.error("Declarative apply rejected: section %r hook failed: %s", name, exc)
                    return ConfigApplyOutcome(
                        applied=False,
                        error_code=INVALID_CONFIG,
                        error=f"section {name!r}: {exc}",
                    )

            # A service-only artifact (core absent) leaves the core layers
            # untouched.
            changed: list[str] = []
            if sections.core is not None:
                try:
                    changed = self._apply_declarative(merged_core)
                except Exception as exc:
                    self._logger.error("Declarative apply rejected: core settings invalid: %s", exc)
                    return ConfigApplyOutcome(applied=False, error_code=INVALID_CONFIG, error=str(exc))

            for name, _, candidate in resolved:
                self._section_layers[name]["L2"] = sections.services[name]
                self._resolved[name] = candidate
            if sections.core is not None:
                self._core_layers["L2"] = sections.core

            self._applied_revision = 1
            self._applied_hash = hashlib.sha256(msgspec.msgpack.encode(sections)).hexdigest()
            self._source = source
            self._logger.info(
                "Applied declarative config from %s (%d fields changed)",
                source,
                len(changed),
            )
            return ConfigApplyOutcome(
                applied=True,
                revision=1,
                hash=self._applied_hash,
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

        Builds a :class:`ConfigSections` snapshot from the resolved effective
        core (the ``current`` snapshot as a dict) plus the merged effective
        service structs (encoded with ``msgspec.to_builtins``), wraps it in a
        signed envelope at the current revision, and hands it to
        ``source.store`` (§6.2). The remote desired-state envelope stays
        concrete (effective) by design. The declarative view is exposed
        separately through :meth:`show_declarative` and the local ``config.yml``
        artifact. A source store failure maps to ``CONFIG_SOURCE_UNAVAILABLE``
        (transient); a local disk write failure is ``CONFIG_STORE_FAILED``
        (`ConfigManager.write_local`).

        Args:
            source: The :class:`ConfigSource` to write the envelope to.
            target: Label recorded on the outcome (e.g. ``"remote"``).
        """
        if not self._enabled:
            return ConfigStoreOutcome(
                stored=False,
                error_code=REMOTE_CONFIG_DISABLED,
                error="remote config is disabled",
            )
        sections = self.show()
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

        ``core`` is the resolved effective core (the ``current`` snapshot as a
        dict); ``services`` holds the merged effective service structs as dicts
        (``msgspec.to_builtins``). The declarative patch view is exposed
        separately through :meth:`show_declarative` (§6.3).
        """
        return ConfigSections(
            core=msgspec.to_builtins(self._current()),
            services={name: msgspec.to_builtins(struct) for name, struct in self._resolved.items()},
        )

    def _merged_core_patch(self) -> dict | None:
        """Return the merged core patch (union of L2/L3 keys), or ``None`` if empty."""
        merged = merge_all({}, [self._core_layers["L2"], self._core_layers["L3"]])
        return merged if merged else None

    def show_declarative(self) -> DeclarativeSections:
        """Return the current declarative (merged patch) sections snapshot.

        ``core`` is the merged core patch; ``services`` is the merged patch
        view: the union of keys any layer set, with their values, and no
        ``None``-filling (§6.3). Absence means "inherit".
        """
        return DeclarativeSections(core=self._merged_core_patch(), services=self._merged_patch_view())

    def _merged_patch_view(self) -> dict[str, dict]:
        """Return each section's merged patch (union of explicitly-set keys)."""
        view: dict[str, dict] = {}
        for name, layers in self._section_layers.items():
            merged = merge_all({}, [layers["L1"], layers["L2"], layers["L3"]])
            if merged:
                view[name] = merged
        return view

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


def read_local_config(path: Path) -> DeclarativeSections | None:
    """Read a local config snapshot from ``path``, or ``None`` when absent.

    Write-free: a missing file returns ``None`` without creating anything
    (unlike the ``valkey.yml`` loader). An unreadable or invalid file is
    logged as an error and also returns ``None``, so startup falls back to
    the current/default config. A v5 file with service sections fails to
    decode into ``services: dict[str, dict]`` and is also treated as absent.

    Args:
        path: Path to the YAML snapshot (``config.yml``).
    """
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        _logger.error("Failed to read local config %s: %s", path, exc)
        return None
    try:
        return msgspec.yaml.decode(data, type=DeclarativeSections, strict=True)
    except msgspec.DecodeError as exc:
        _logger.error("Invalid local config %s: %s", path, exc)
        return None


def write_local_config(path: Path, sections: ConfigSections | DeclarativeSections) -> None:
    """Atomically write ``sections`` to ``path`` as YAML.

    Encodes the sections with ``msgspec.yaml.encode``, writes to a temporary
    file in the same directory, then ``os.replace``-s it into place so a
    reader never sees a partial file. The parent directory is created if
    missing, and the temporary file is removed if the write fails. A concrete
    :class:`ConfigSections` snapshot is re-wrapped as a
    :class:`DeclarativeSections` (identical shape in v6) so the local artifact
    always carries the declarative type; no field is normalised.

    Args:
        path: Destination file path (``config.yml``).
        sections: The validated :class:`ConfigSections` or
            :class:`DeclarativeSections` snapshot to persist.

    Raises:
        OSError: If the temporary file cannot be created, written, or moved.
    """
    if isinstance(sections, ConfigSections):
        sections = DeclarativeSections(core=sections.core, services=sections.services)
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
    "DeclarativeSections",
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
