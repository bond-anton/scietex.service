"""ConfigManager handler-registration tests (AR-105)."""

from scietex.service.task_handler.config import (
    ConfigApplyHandler,
    ConfigShowHandler,
    ConfigStoreHandler,
)

from ._helpers import build_manager


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
