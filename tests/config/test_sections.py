"""ConfigManager section-registration tests (AR-105)."""

import msgspec
import pytest

from scietex.service.config_reload import ConfigSections

from ._helpers import build_manager, make_core, make_envelope


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
        core=make_core(),
        services={"my_service": {"batch_size": 42}},
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
        core=make_core(),
        services={"svc": {"batch_size": 7}},
    )
    outcome = await manager.apply_config(make_envelope(sections, revision=1), False)

    assert outcome.applied is True
    assert first == []
    assert second == [MyServiceSettings(batch_size=7)]


@pytest.mark.asyncio
async def test_core_none_applies_sections_without_touching_core(tmp_path):
    """A service-only envelope runs section hooks but leaves core settings alone.

    The API does not track a worker's core settings, so it delivers
    ``core=None``; the worker must keep its operator-tuned core values while
    still applying the service section and advancing the revision.
    """
    manager = build_manager(tmp_path, enabled=True)
    applied: list[MyServiceSettings] = []
    manager.register_section("svc", MyServiceSettings, apply=applied.append)

    sections = ConfigSections(
        core=None,
        services={"svc": {"batch_size": 9}},
    )
    outcome = await manager.apply_config(make_envelope(sections, revision=5), False)

    assert outcome.applied is True
    assert outcome.revision == 5
    assert outcome.changed == []
    assert applied == [MyServiceSettings(batch_size=9)]
    assert manager.apply_calls == []


@pytest.mark.asyncio
async def test_core_none_advances_revision_for_replay_guard(tmp_path):
    """A service-only apply advances the revision so a replay is rejected."""
    manager = build_manager(tmp_path, enabled=True)
    manager.register_section("svc", MyServiceSettings, apply=lambda _: None)

    sections = ConfigSections(
        core=None,
        services={"svc": {"batch_size": 1}},
    )
    payload = make_envelope(sections, revision=5)
    first = await manager.apply_config(payload, False)
    second = await manager.apply_config(payload, False)

    assert first.applied is True
    assert second.applied is True
    assert second.changed == []
