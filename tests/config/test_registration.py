"""ConfigManager handler-registration tests (AR-105)."""

import logging

import msgspec
import pytest

from scietex.service.config_reload import (
    ConfigReloader,
    ConfigSections,
    encode_config_envelope,
)
from scietex.service.task_handler.config import (
    ConfigApplyHandler,
    ConfigShowHandler,
    ConfigStoreHandler,
)

from ._helpers import build_manager, make_settings


def test_register_handlers_binds_exactly_three_handler_classes(tmp_path):
    """``register_handlers`` binds exactly the three config handler classes,
    each with the manager's own callback."""
    manager = build_manager(tmp_path, enabled=True)
    registrations: list[tuple[type, dict]] = []

    def add_handler(handler_class, **kwargs):
        registrations.append((handler_class, kwargs))

    manager.register_handlers(add_handler)

    assert [cls for cls, _ in registrations] == [
        ConfigApplyHandler,
        ConfigStoreHandler,
        ConfigShowHandler,
    ]
    apply_reg, store_reg, show_reg = registrations
    assert apply_reg[1] == {"apply": manager.apply_config}
    assert store_reg[1] == {"store": manager.store_config}
    assert show_reg[1] == {"show": manager.show_config}


def test_register_handlers_is_unconditional_on_enabled(tmp_path):
    """Handler registration stays unconditional in step 1 (gating is a later
    step), so a disabled manager still registers all three handlers."""
    manager = build_manager(tmp_path, enabled=False)
    registered: list[type] = []

    manager.register_handlers(lambda handler_class, **kwargs: registered.append(handler_class))

    assert registered == [ConfigApplyHandler, ConfigStoreHandler, ConfigShowHandler]


# --- reloader-level registration (defaults / bootstrap / seed / current) -----
#
# These exercise the raw :class:`ConfigReloader` directly. The ``ConfigManager``
# forwarding of ``defaults``/``bootstrap``/``seed_bootstrap``/``current_settings``
# is covered in ``test_layered_integration.py``.


class MyServiceSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    batch_size: int = 100
    retries: int = 3


def _bare_reloader() -> ConfigReloader:
    """A bare reloader with no-op injected callables (core path is unused here)."""
    return ConfigReloader(
        apply=lambda patch: [],
        current=make_settings,
        restart_required=lambda: [],
        logger=logging.getLogger("test_registration"),
    )


@pytest.mark.asyncio
async def test_register_section_defaults_supplies_l0_base(tmp_path):
    """A registered ``defaults`` instance is the L0 base: a partial patch
    resolves every omitted field to the defaults value, not the struct default."""
    reloader = _bare_reloader()
    received: list[MyServiceSettings] = []
    reloader.register_section(
        "svc",
        MyServiceSettings,
        received.append,
        defaults=MyServiceSettings(batch_size=500, retries=9),
    )

    sections = ConfigSections(core=None, services={"svc": {"retries": 2}})
    outcome = await reloader.apply_envelope(encode_config_envelope(sections, revision=1), source="remote")

    assert outcome.applied is True
    assert received == [MyServiceSettings(batch_size=500, retries=2)]


def test_seed_bootstrap_seeds_l1_and_resolves(tmp_path):
    """``seed_bootstrap`` stores the provider's patch as L1 and resolves L0+L1,
    so ``current_settings`` returns the merged struct before any apply."""
    reloader = _bare_reloader()
    reloader.register_section(
        "svc",
        MyServiceSettings,
        lambda value: None,
        defaults=MyServiceSettings(batch_size=500, retries=9),
        bootstrap=lambda: {"batch_size": 777},
    )

    assert reloader.current_settings("svc") is None

    reloader.seed_bootstrap()

    assert reloader.current_settings("svc") == MyServiceSettings(batch_size=777, retries=9)


def test_seed_bootstrap_invalid_patch_leaves_section_unresolved(tmp_path):
    """A bootstrap patch that fails validation is logged and leaves the section
    unresolved instead of aborting the whole run start."""
    reloader = _bare_reloader()
    reloader.register_section(
        "svc",
        MyServiceSettings,
        lambda value: None,
        bootstrap=lambda: {"batch_size": "not-an-int"},
    )

    reloader.seed_bootstrap()

    assert reloader.current_settings("svc") is None


def test_section_without_bootstrap_resolves_on_apply_not_seed(tmp_path):
    """A section without a ``bootstrap`` provider is single-layer: ``seed_bootstrap``
    leaves it unresolved, and it resolves on the first apply."""
    reloader = _bare_reloader()
    reloader.register_section("svc", MyServiceSettings, lambda value: None)

    reloader.seed_bootstrap()

    assert reloader.current_settings("svc") is None
