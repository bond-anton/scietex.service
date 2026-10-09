"""Remote-configuration lifecycle owner: reloader, local file, and source."""

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import msgspec

from .config_reload import (
    CONFIG_SOURCE_NOT_CONFIGURED,
    CONFIG_STORE_FAILED,
    REMOTE_CONFIG_DISABLED,
    ConfigApplyOutcome,
    ConfigReloader,
    ConfigSource,
    ConfigStoreOutcome,
    ReloadableSettings,
    read_local_config,
    write_local_config,
)
from .task_handler import (
    ConfigApplyHandler,
    ConfigShowHandler,
    ConfigShowResponse,
    ConfigSourceLabel,
    ConfigStoreHandler,
)


class ConfigManager:
    """Owns the reloader, the local ``config.yml``, the source, and the handlers.

    A transport-agnostic collaborator extracted from ``TaskProcessor`` (AR-105).
    It wires a :class:`ConfigReloader` to the processor's injected callables,
    tracks the local snapshot path, holds the attached ``ConfigSource``, and
    exposes the three ``config:*`` handler callbacks (``apply_config``,
    ``store_config``, ``show_config``).
    """

    def __init__(
        self,
        *,
        conf_dir: Path,
        config_file: str,
        apply: Callable[[dict[str, object]], list[str]],
        current: Callable[[], ReloadableSettings],
        restart_required: Callable[[], list[str]],
        logger: logging.Logger,
        signing_key: str | None = None,
        enabled: bool = False,
        validate_core: Callable[[dict[str, object]], None] | None = None,
        apply_declarative: Callable[[dict[str, object]], list[str]] | None = None,
    ) -> None:
        """Initialize the manager.

        Args:
            conf_dir: Directory holding the local ``config_file`` snapshot.
            config_file: Filename of the local reloadable snapshot, resolved
                under ``conf_dir``.
            apply: Callback that receives the merged core patch dict, overlays
                it, validates it, and swaps the core settings.
            current: Callback returning the current effective core settings.
            restart_required: Callback returning the restart-required field
                names.
            logger: Logger for configuration diagnostics.
            signing_key: Optional HMAC key; ``None`` disables signature
                enforcement.
            enabled: Master switch for remote config (default ``False``).
            validate_core: Optional callback that validates the merged core
                patch before any section hook runs.
            apply_declarative: Optional callback that receives the merged core
                patch dict, overlays it, validates it, and swaps the
                declarative core settings.
        """
        self._conf_dir = conf_dir
        self._config_file = config_file
        self._logger = logger
        self._restart_required = restart_required
        self._reloader = ConfigReloader(
            apply=apply,
            current=current,
            restart_required=restart_required,
            logger=logger,
            signing_key=signing_key,
            enabled=enabled,
            validate_core=validate_core,
            apply_declarative=apply_declarative,
        )
        self._source: ConfigSource | None = None

    @property
    def enabled(self) -> bool:
        """Whether remote config is enabled (the master switch)."""
        return self._reloader.enabled

    @property
    def revision(self) -> int:
        """The revision of the last successfully applied envelope."""
        return self._reloader.revision

    @property
    def hash(self) -> str:
        """The hash of the last successfully applied envelope."""
        return self._reloader.hash

    @property
    def source(self) -> str:
        """The source label of the last successfully applied envelope."""
        return self._reloader.source

    def register_section(
        self,
        name: str,
        struct_type: type[msgspec.Struct],
        *,
        apply: Callable[[Any], None],
        defaults: msgspec.Struct | None = None,
        bootstrap: Callable[[], dict | None] | None = None,
    ) -> None:
        """Register a service settings struct and its apply hook with the reloader.

        Forwards ``defaults`` (the concrete L0 base; ``None`` uses
        ``struct_type()``) and ``bootstrap`` (an optional L1 provider; ``None``
        means single-layer) so a section can declare its full layer stack.
        """
        self._reloader.register_section(name, struct_type, apply, defaults=defaults, bootstrap=bootstrap)

    def reset(self) -> None:
        """Reset run-scoped replay state for a fresh run start (AR-111)."""
        self._reloader.reset()

    def seed_bootstrap(self) -> None:
        """Seed each registered section's L1 patch from its bootstrap provider.

        Resolves L0+L1 into each section's effective struct. Called at run
        start before handlers start.
        """
        self._reloader.seed_bootstrap()

    def current_settings(self, name: str) -> msgspec.Struct | None:
        """Return the last resolved effective struct for a registered section.

        Returns ``None`` for an unregistered or never-resolved section.
        """
        return self._reloader.current_settings(name)

    def attach_source(self, source: ConfigSource | None) -> None:
        """Attach (or clear) the transport's desired-state ``ConfigSource``."""
        self._source = source

    def register_handlers(self, add_handler: Callable[..., None]) -> None:
        """Register the three ``config:*`` handlers with the owning processor."""
        add_handler(ConfigApplyHandler, apply=self.apply_config)
        add_handler(ConfigStoreHandler, store=self.store_config)
        add_handler(ConfigShowHandler, show=self.show_config)

    async def apply_config(self, payload: bytes | None, persist: bool) -> ConfigApplyOutcome:
        """Apply a config envelope (inline or from the source of truth).

        Injected into ``ConfigApplyHandler``. A present ``payload`` is applied
        inline; ``payload=None`` re-reads the transport source, which requires
        a source to be attached (``CONFIG_SOURCE_NOT_CONFIGURED`` otherwise).
        ``persist`` additionally writes the local snapshot after a successful
        apply.
        """
        if payload is not None:
            outcome = await self._reloader.apply_envelope(payload, source="inline")
        else:
            source = self._source
            if source is None:
                return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_NOT_CONFIGURED)
            outcome = await self._reloader.reload(source)
        if persist and outcome.applied:
            self.write_local()
        return outcome

    async def store_config(self, target: str) -> ConfigStoreOutcome:
        """Persist the effective config to the requested target.

        Injected into ``ConfigStoreHandler``. ``disk`` writes the local
        snapshot; ``remote`` publishes back to the transport source; ``both``
        does both. A remote target without an attached source is
        ``CONFIG_SOURCE_NOT_CONFIGURED``.
        """
        if target == "disk":
            return self.write_local()
        source = self._source
        if source is None:
            return ConfigStoreOutcome(stored=False, target=target, error_code=CONFIG_SOURCE_NOT_CONFIGURED)
        if target == "both":
            disk_outcome = self.write_local()
            if not disk_outcome.stored:
                return disk_outcome
        return await self._reloader.store(source, target=target)

    def show_config(self, include_restart_required: bool) -> ConfigShowResponse:
        """Build the effective-config inspection response.

        Injected into ``ConfigShowHandler``. ``settings`` is the msgpack
        encoding of the reloader's effective ``ConfigSections`` (never
        secrets); ``declarative_settings`` is the msgpack encoding of the
        merged patch view — the keys any layer explicitly set, where absence
        means inherit (not ``None``-means-default). ``restart_required_fields``
        is only populated when requested. When the master switch is off, the
        response carries ``REMOTE_CONFIG_DISABLED`` instead of the settings.
        """
        if not self._reloader.enabled:
            return ConfigShowResponse(
                error_code=REMOTE_CONFIG_DISABLED,
                error="remote config is disabled",
            )
        return ConfigShowResponse(
            settings=msgspec.msgpack.encode(self._reloader.show()),
            declarative_settings=msgspec.msgpack.encode(self._reloader.show_declarative()),
            revision=self._reloader.revision,
            hash=self._reloader.hash,
            source=cast(ConfigSourceLabel, self._reloader.source),
            restart_required_fields=self._restart_required() if include_restart_required else [],
        )

    def write_local(self) -> ConfigStoreOutcome:
        """Write the merged patch view to ``<conf_dir>/<config_file>``.

        Writes the reloader's merged patch view so a store→restart cycle
        preserves which keys were explicitly set (and any explicit ``null``
        clears) rather than pinning resolved values. Uses the reloader's atomic
        ``write_local_config``; a failure returns ``CONFIG_STORE_FAILED`` and
        leaves any previous file intact.
        """
        path = self._conf_dir / self._config_file
        try:
            write_local_config(path, self._reloader.show_declarative())
        except Exception as exc:
            self._logger.error("Failed to write local config %s: %s", path, exc)
            return ConfigStoreOutcome(
                stored=False,
                target="disk",
                path=str(path),
                revision=self._reloader.revision,
                hash=self._reloader.hash,
                error=str(exc),
                error_code=CONFIG_STORE_FAILED,
            )
        return ConfigStoreOutcome(
            stored=True,
            target="disk",
            path=str(path),
            revision=self._reloader.revision,
            hash=self._reloader.hash,
        )

    async def apply_local_file(self) -> ConfigApplyOutcome | None:
        """Apply the persisted ``config.yml`` snapshot as a trusted local artifact.

        Applied ahead of the remote read as a trusted, unsigned L2 patch: the
        merged patch view preserves which keys were explicitly set (and any
        explicit ``null`` clears), so a store→restart cycle does not pin
        resolved values. The remote source stays authoritative. Returns ``None``
        when the feature is disabled, the file is absent, or the apply fails;
        otherwise returns the apply outcome for the caller to log.
        """
        if not self._reloader.enabled:
            return None
        sections = read_local_config(self._conf_dir / self._config_file)
        if sections is None:
            return None
        try:
            outcome = await self._reloader.apply_declarative_sections(sections, source="file", trusted=True)
        except Exception as exc:
            self._logger.error("Failed to apply local config: %s", exc)
            return None
        return outcome

    async def reload_remote(self) -> ConfigApplyOutcome:
        """Load and apply the desired-state envelope from the attached source.

        A missing source maps to ``CONFIG_SOURCE_NOT_CONFIGURED``; otherwise
        the reloader reads and applies the source's envelope.
        """
        if self._source is None:
            return ConfigApplyOutcome(applied=False, error_code=CONFIG_SOURCE_NOT_CONFIGURED)
        return await self._reloader.reload(self._source)

    async def apply_envelope(self, payload: bytes, *, source: str) -> ConfigApplyOutcome:
        """Apply a remote config envelope under the reloader's lock."""
        return await self._reloader.apply_envelope(payload, source=source)
