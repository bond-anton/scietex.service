"""ConfigManager section-registration tests (AR-105)."""

import msgspec
import pytest

from scietex.service.config_reload import ConfigSections

from ._helpers import build_manager, make_envelope, make_settings


class MyServiceSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    batch_size: int = 100


@pytest.mark.asyncio
async def test_register_section_passthrough_invokes_hook(tmp_path):
    """A registered service section decodes against its struct and invokes the
    registered apply hook."""
    manager = build_manager(tmp_path, enabled=True)
    applied: list[MyServiceSettings] = []
    manager.register_section("my_service", MyServiceSettings, apply=applied.append)

    sections = ConfigSections(
        core=make_settings(),
        services={"my_service": msgspec.msgpack.encode(MyServiceSettings(batch_size=42))},
    )
    outcome = await manager.apply_config(make_envelope(sections, revision=1), False)

    assert outcome.applied is True
    assert len(applied) == 1
    assert applied[0].batch_size == 42


@pytest.mark.asyncio
async def test_reregister_section_replaces_hook(tmp_path):
    """Re-registering a name replaces the struct and hook (idempotent)."""
    manager = build_manager(tmp_path, enabled=True)
    first: list[object] = []
    second: list[object] = []
    manager.register_section("svc", MyServiceSettings, apply=first.append)
    manager.register_section("svc", MyServiceSettings, apply=second.append)

    sections = ConfigSections(
        core=make_settings(),
        services={"svc": msgspec.msgpack.encode(MyServiceSettings(batch_size=7))},
    )
    outcome = await manager.apply_config(make_envelope(sections, revision=1), False)

    assert outcome.applied is True
    assert first == []
    assert second == [MyServiceSettings(batch_size=7)]
