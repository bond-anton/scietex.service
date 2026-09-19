"""Shared fakes and factory helpers for the isolated ConfigManager suite (AR-105)."""

import logging
from pathlib import Path

from scietex.service.config_manager import ConfigManager
from scietex.service.config_reload import (
    ConfigSections,
    ReloadableSettings,
    encode_config_envelope,
)

logger = logging.getLogger("scietex.service.config_manager.tests")

_CORE_DEFAULTS: dict[str, float | int] = {
    "max_concurrent_tasks": 10,
    "task_manager_sleep_time": 0.1,
    "task_queue_manager_sleep_time": 0.1,
    "task_handler_start_timeout": 10.0,
    "task_handler_stop_timeout": 10.0,
    "task_timeout": 3.0,
    "task_queue_fetch_timeout": 0.5,
    "task_cancellation_timeout": 2.0,
}


def make_settings(**overrides) -> ReloadableSettings:
    """Build a complete, in-bounds ``ReloadableSettings`` snapshot with overrides."""
    values = dict(_CORE_DEFAULTS)
    values.update(overrides)
    return ReloadableSettings(**values)


def make_envelope(sections: ConfigSections | None = None, *, revision: int = 1) -> bytes:
    """Encode a valid, unsigned envelope around ``sections`` (defaults to the
    base core settings)."""
    if sections is None:
        sections = ConfigSections(core=make_settings())
    return encode_config_envelope(sections, revision=revision)


class FakeConfigSource:
    """A ``ConfigSource`` double with configurable load/store behaviour."""

    def __init__(
        self,
        *,
        payload: bytes | None = None,
        load_error: Exception | None = None,
        store_error: Exception | None = None,
    ) -> None:
        self.payload = payload
        self.load_error = load_error
        self.store_error = store_error
        self.load_calls = 0
        self.stored: list[bytes] = []

    async def load(self) -> bytes | None:
        self.load_calls += 1
        if self.load_error is not None:
            raise self.load_error
        return self.payload

    async def store(self, envelope: bytes) -> None:
        self.stored.append(envelope)
        if self.store_error is not None:
            raise self.store_error


def build_manager(
    conf_dir: Path,
    *,
    apply=None,
    current=None,
    restart_required=None,
    signing_key: str | None = None,
    enabled: bool = True,
) -> ConfigManager:
    """Build a ``ConfigManager`` with a recording in-memory ``apply``/``current``
    double (mirrors the ``_reloader`` factory in ``test_config_reload.py``)."""
    state = {"current": make_settings()}
    apply_calls: list[ReloadableSettings] = []

    def _apply(settings: ReloadableSettings) -> list[str]:
        apply_calls.append(settings)
        previous = state["current"]
        changed = [
            field
            for field in ReloadableSettings.__struct_fields__
            if getattr(previous, field) != getattr(settings, field)
        ]
        state["current"] = settings
        return changed

    def _current() -> ReloadableSettings:
        return state["current"]

    def _restart_required() -> list[str]:
        return ["queue_size", "auto_tune"]

    manager = ConfigManager(
        conf_dir=conf_dir,
        config_file="config.yml",
        apply=_apply if apply is None else apply,
        current=_current if current is None else current,
        restart_required=_restart_required if restart_required is None else restart_required,
        logger=logger,
        signing_key=signing_key,
        enabled=enabled,
    )
    manager.apply_calls = apply_calls
    return manager
