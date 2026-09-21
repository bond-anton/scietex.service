"""ConfigManager show tests (AR-105)."""

import msgspec

from scietex.service.config_reload import (
    RELOADABLE_FIELDS,
    REMOTE_CONFIG_DISABLED,
    ConfigSections,
)

from ._helpers import build_manager

#: Connection-config field names that must never leak through ``config:show``.
_SECRET_FIELDS: tuple[str, ...] = (
    "password",
    "tls_context",
    "tls_insecure",
    "host",
    "port",
    "username",
    "identifier",
    "keepalive",
    "valkey_config",
    "mqtt_config",
)


def test_show_disabled_reports_disabled(tmp_path):
    """With the master switch off, ``config:show`` reports
    REMOTE_CONFIG_DISABLED rather than the effective settings."""
    manager = build_manager(tmp_path, enabled=False)

    response = manager.show_config(True)

    assert response.error_code == REMOTE_CONFIG_DISABLED
    assert response.error == "remote config is disabled"
    assert response.settings == b""
    assert response.revision == 0
    assert response.hash == ""
    assert response.source == "default"


def test_show_restart_required_fields_gated_on_request(tmp_path):
    """``restart_required_fields`` is populated only when requested."""
    manager = build_manager(tmp_path, enabled=True)

    assert manager.show_config(True).restart_required_fields
    assert manager.show_config(False).restart_required_fields == []


def test_show_never_contains_secrets(tmp_path):
    """The show payload exposes only core + registered services and never a
    connection-config field name."""
    manager = build_manager(tmp_path, enabled=True)

    response = manager.show_config(True)

    sections = msgspec.msgpack.decode(response.settings, type=ConfigSections)
    assert sections.services == {}
    core_fields = {f.name for f in msgspec.structs.fields(type(sections.core))}
    assert core_fields == set(RELOADABLE_FIELDS)

    encoded = msgspec.msgpack.encode(response)
    for field in _SECRET_FIELDS:
        assert field.encode() not in encoded
