"""Tests for the generic RFC 7396 layered-config merge engine in ``config_merge``."""

import msgspec
import pytest

from scietex.service.config_merge import merge, merge_all, resolve_section


class Serial(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    port: str = "/dev/ttyUSB0"
    baudrate: int = 9600
    parity: str = "N"


class Device(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    address: int = 1
    framer: str = "RTU"


class Settings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    serial: Serial = msgspec.field(default_factory=Serial)
    host: str = "0.0.0.0"
    port: int = 502
    enabled: bool = True
    pdus: list[str] = msgspec.field(default_factory=list)
    devices: dict[int, Device] = msgspec.field(default_factory=dict)


# --- merge: three-state rule -------------------------------------------------


def test_merge_three_state_scalar():
    base = {"host": "0.0.0.0", "port": 502}
    # value sets
    assert merge(base, {"host": "1.2.3.4"}) == {"host": "1.2.3.4", "port": 502}
    # null clears
    assert merge(base, {"host": None}) == {"port": 502}
    # absent inherits
    assert merge(base, {"port": 6000}) == {"host": "0.0.0.0", "port": 6000}


def test_merge_three_state_nested():
    base = {"serial": {"port": "/dev/ttyUSB0", "baudrate": 9600}}
    # value sets a single nested field
    assert merge(base, {"serial": {"baudrate": 19200}}) == {"serial": {"port": "/dev/ttyUSB0", "baudrate": 19200}}
    # null clears the whole nested key
    assert merge(base, {"serial": None}) == {}
    # absent inherits
    assert merge(base, {}) == base


def test_merge_three_state_dict_entry():
    base = {"devices": {1: {"address": 1, "framer": "RTU"}}}
    # value sets a new entry alongside the existing one
    assert merge(base, {"devices": {2: {"address": 2, "framer": "ASCII"}}}) == {
        "devices": {
            1: {"address": 1, "framer": "RTU"},
            2: {"address": 2, "framer": "ASCII"},
        }
    }
    # null clears an entry
    assert merge(base, {"devices": {1: None}}) == {"devices": {}}
    # absent inherits
    assert merge(base, {"devices": {}}) == base


# --- merge: non-mutation -----------------------------------------------------


def test_merge_does_not_mutate_inputs():
    base = {"serial": {"port": "/dev/ttyUSB0"}, "host": "0.0.0.0", "pdus": ["a"]}
    patch = {"serial": {"baudrate": 19200}, "pdus": ["b"], "host": None}
    result = merge(base, patch)
    assert base == {"serial": {"port": "/dev/ttyUSB0"}, "host": "0.0.0.0", "pdus": ["a"]}
    assert patch == {"serial": {"baudrate": 19200}, "pdus": ["b"], "host": None}
    # the changed nested dict is copied, not shared with base
    assert result is not base
    assert result["serial"] is not base["serial"]


# --- merge_all ---------------------------------------------------------------


def test_merge_all_skips_none_and_empty():
    base = {"host": "0.0.0.0"}
    assert merge_all(base, [None, {}, {"host": "x"}]) == {"host": "x"}
    assert merge_all(base, [None, {}]) == {"host": "0.0.0.0"}


def test_merge_all_folds_low_to_high():
    base = {"host": "base"}
    assert merge_all(base, [{"host": "low"}, {"host": "high"}]) == {"host": "high"}


# --- resolve_section: per-layer resolution -----------------------------------


def test_per_layer_resolution():
    l1 = {"port": 1000}
    l2 = {"serial": {"baudrate": 19200}}
    l3 = {"host": "l3-host"}
    resolved = resolve_section(Settings, [l3, l2, l1])
    assert resolved.port == 1000  # only L1 sets it
    assert resolved.serial.baudrate == 19200  # only L2 sets it
    assert resolved.host == "l3-host"  # only L3 sets it
    # untouched fields fall back to the struct defaults
    assert resolved.serial.port == "/dev/ttyUSB0"
    assert resolved.serial.parity == "N"
    assert resolved.enabled is True
    assert resolved.pdus == []
    assert resolved.devices == {}


def test_higher_layer_beats_lower():
    l1 = {"host": "l1-host"}
    l2 = {"host": "l2-host"}
    l3 = {"host": "l3-host"}
    assert resolve_section(Settings, [l3, l2, l1]).host == "l3-host"


# --- resolve_section: nested / map merging -----------------------------------


def test_deep_nested_merge():
    l1 = {"serial": {"port": "/dev/ttyUSB1"}}
    l2 = {"serial": {"baudrate": 19200}}
    resolved = resolve_section(Settings, [l2, l1])
    assert resolved.serial.port == "/dev/ttyUSB1"  # from L1
    assert resolved.serial.baudrate == 19200  # from L2


def test_absent_nested_contributes_nothing():
    l1 = {"serial": {"port": "/dev/ttyUSB1"}}
    l2 = {"host": "h"}  # serial absent: no nested opinion
    resolved = resolve_section(Settings, [l2, l1])
    assert resolved.serial.port == "/dev/ttyUSB1"
    assert resolved.serial.baudrate == 9600


def test_per_key_dict_merge():
    l1 = {"devices": {1: {"address": 1, "framer": "RTU"}, 2: {"address": 2, "framer": "RTU"}}}
    l3 = {"devices": {1: {"framer": "ASCII"}}}
    resolved = resolve_section(Settings, [l3, l1])
    assert resolved.devices[1].framer == "ASCII"  # patched by L3
    assert resolved.devices[1].address == 1  # other fields come from L1
    assert resolved.devices[2].framer == "RTU"  # untouched
    assert resolved.devices[2].address == 2


def test_dict_clear():
    l1 = {"devices": {1: {"address": 1}, 2: {"address": 2}}}
    l3 = {"devices": {2: None}}
    resolved = resolve_section(Settings, [l3, l1])
    assert list(resolved.devices) == [1]
    assert resolved.devices[1].address == 1


# --- resolve_section: clear / replace ----------------------------------------


def test_scalar_clear_resolves_to_default():
    l1 = {"serial": {"port": "/dev/ttyUSB1"}}
    l3 = {"serial": {"port": None}}
    resolved = resolve_section(Settings, [l3, l1])
    assert resolved.serial.port == "/dev/ttyUSB0"  # the L0/struct default


def test_list_replace_and_clear():
    l1 = {"pdus": ["pdu-a", "pdu-b"]}
    assert resolve_section(Settings, [{"pdus": ["pdu-c"]}, l1]).pdus == ["pdu-c"]
    assert resolve_section(Settings, [{"pdus": []}, l1]).pdus == []


def test_falsy_scalar_override():
    # An explicit ``false``/``0`` is a value, not a clear, so it overrides.
    l1 = {"port": 502, "enabled": True}
    l2 = {"port": 0, "enabled": False}
    resolved = resolve_section(Settings, [l2, l1])
    assert resolved.port == 0
    assert resolved.enabled is False


# --- resolve_section: validation ---------------------------------------------


def test_unknown_field_rejected_top_level():
    with pytest.raises(msgspec.ValidationError):
        resolve_section(Settings, [{"bogus": 1}])


def test_unknown_field_rejected_nested():
    with pytest.raises(msgspec.ValidationError):
        resolve_section(Settings, [{"serial": {"baudrat": 19200}}])


def test_unknown_field_rejected_dict_entry():
    with pytest.raises(msgspec.ValidationError):
        resolve_section(Settings, [{"devices": {1: {"address": 1, "framer": "RTU", "bogus": True}}}])


# --- resolve_section: L0 defaults --------------------------------------------


def test_empty_layers_resolve_to_struct_defaults():
    assert resolve_section(Settings, []) == Settings()


def test_empty_layers_resolve_to_explicit_defaults():
    defaults = Settings(
        serial=Serial(port="/dev/custom", baudrate=115200),
        host="10.0.0.1",
        port=7000,
        enabled=False,
        pdus=["pdu-a"],
        devices={1: Device(address=1)},
    )
    assert resolve_section(Settings, [], defaults=defaults) == defaults


def test_minimal_patch_over_defaults_keeps_defaults():
    defaults = Settings(serial=Serial(port="/dev/custom"), host="10.0.0.1")
    resolved = resolve_section(Settings, [{"port": 8000}], defaults=defaults)
    assert resolved.port == 8000  # patched
    assert resolved.host == "10.0.0.1"  # L0 default preserved
    assert resolved.serial.port == "/dev/custom"  # L0 nested default preserved
    assert resolved.serial.baudrate == 9600  # struct default preserved
