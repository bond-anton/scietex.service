"""Tests for the transport-agnostic remote-config machinery in ``config_reload``."""

import asyncio
import hashlib
import logging
from pathlib import Path

import msgspec
import pytest

from scietex.service.config_reload import (
    BAD_SIGNATURE,
    CONFIG_ENVELOPE_VERSION,
    CONFIG_SOURCE_UNAVAILABLE,
    HASH_MISMATCH,
    INVALID_CONFIG,
    INVALID_CONFIG_PAYLOAD,
    REMOTE_CONFIG_DISABLED,
    RETRYABLE_ERROR_CODES,
    STALE_CONFIG,
    UNKNOWN_CONFIG_SECTION,
    ConfigEnvelope,
    ConfigReloader,
    ConfigSections,
    ConfigSource,
    ReloadableSettings,
    decode_config_envelope,
    encode_config_envelope,
    peek_config_envelope_version,
    read_local_config,
    write_local_config,
)

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


def _settings(**overrides) -> ReloadableSettings:
    values = dict(_CORE_DEFAULTS)
    values.update(overrides)
    return ReloadableSettings(**values)


def _sections(
    core: ReloadableSettings | None = None,
    services: dict[str, bytes] | None = None,
) -> ConfigSections:
    return ConfigSections(
        core=_settings() if core is None else core,
        services={} if services is None else services,
    )


class _ServiceA(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    a: int = 0


class _ServiceB(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    b: int = 0


class _FakeSource:
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
        self.store_calls: list[bytes] = []

    async def load(self) -> bytes | None:
        self.load_calls += 1
        if self.load_error is not None:
            raise self.load_error
        return self.payload

    async def store(self, envelope: bytes) -> None:
        self.store_calls.append(envelope)
        if self.store_error is not None:
            raise self.store_error


def _reloader(*, apply=None, current=None, **kwargs) -> ConfigReloader:
    """Build a ``ConfigReloader`` with a recording core-apply callback.

    The default ``apply`` swaps the tracked current settings and returns the
    changed field names; ``apply`` and ``current`` can be overridden to simulate
    validation failures or a fixed current snapshot.
    """
    state = {"current": _settings()}
    calls: list[ReloadableSettings] = []

    def _apply(settings: ReloadableSettings) -> list[str]:
        calls.append(settings)
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

    reloader = ConfigReloader(
        apply=_apply if apply is None else apply,
        current=_current if current is None else current,
        restart_required=_restart_required,
        logger=logging.getLogger("test_config_reload"),
        **kwargs,
    )
    reloader.apply_calls = calls
    return reloader


# --- envelope encode/decode -------------------------------------------------


def test_envelope_round_trip():
    sections = _sections(core=_settings(task_timeout=7.5))
    payload = encode_config_envelope(sections, revision=42)

    envelope = decode_config_envelope(payload)
    assert envelope is not None
    assert envelope.version == CONFIG_ENVELOPE_VERSION
    assert envelope.revision == 42
    assert envelope.hash == hashlib.sha256(envelope.settings).hexdigest()
    assert msgspec.msgpack.decode(envelope.settings, type=ConfigSections) == sections
    assert peek_config_envelope_version(payload) == CONFIG_ENVELOPE_VERSION


def test_decode_garbage_returns_none():
    assert decode_config_envelope(b"not msgpack") is None


def test_peek_garbage_returns_none():
    assert peek_config_envelope_version(b"garbage") is None


def test_decode_rejects_unknown_envelope_field():
    raw = msgspec.msgpack.encode(
        {
            "version": 1,
            "revision": 1,
            "hash": "",
            "signature": "",
            "settings": b"",
            "created_at": None,
            "extra": True,
        }
    )
    assert decode_config_envelope(raw) is None


@pytest.mark.asyncio
async def test_wrong_schema_version_rejected():
    reloader = _reloader()
    payload = msgspec.msgpack.encode(ConfigEnvelope(version=99, revision=1, settings=b""))
    outcome = await reloader.apply_envelope(payload, source="remote")
    assert outcome.applied is False
    assert outcome.error_code == INVALID_CONFIG


@pytest.mark.asyncio
async def test_invalid_envelope_payload_rejected():
    reloader = _reloader()
    outcome = await reloader.apply_envelope(b"not msgpack", source="remote")
    assert outcome.applied is False
    assert outcome.error_code == INVALID_CONFIG_PAYLOAD


# --- hash / signature -------------------------------------------------------


@pytest.mark.asyncio
async def test_hash_mismatch_rejected():
    reloader = _reloader()
    envelope = decode_config_envelope(encode_config_envelope(_sections(), revision=1))
    tampered = msgspec.msgpack.encode(
        ConfigEnvelope(
            version=1,
            revision=envelope.revision,
            hash=envelope.hash,
            settings=envelope.settings + b"\x00",
        )
    )
    outcome = await reloader.apply_envelope(tampered, source="remote")
    assert outcome.applied is False
    assert outcome.error_code == HASH_MISMATCH


@pytest.mark.asyncio
async def test_valid_signature_accepted():
    reloader = _reloader(signing_key="secret")
    payload = encode_config_envelope(_sections(), revision=1, signing_key="secret")
    outcome = await reloader.apply_envelope(payload, source="remote")
    assert outcome.applied is True
    assert reloader.revision == 1


@pytest.mark.asyncio
async def test_tampered_settings_with_valid_looking_signature_rejected():
    key = "secret"
    original = decode_config_envelope(
        encode_config_envelope(_sections(core=_settings(task_timeout=1.0)), revision=1, signing_key=key)
    )
    # The signature was computed over the original settings, so different
    # settings carrying the same signature no longer verify.
    other_settings = msgspec.msgpack.encode(_sections(core=_settings(task_timeout=2.0)))
    tampered = msgspec.msgpack.encode(
        ConfigEnvelope(
            version=1,
            revision=1,
            hash=hashlib.sha256(other_settings).hexdigest(),
            signature=original.signature,
            settings=other_settings,
        )
    )
    reloader = _reloader(signing_key=key)
    outcome = await reloader.apply_envelope(tampered, source="remote")
    assert outcome.applied is False
    assert outcome.error_code == BAD_SIGNATURE


@pytest.mark.asyncio
async def test_missing_signature_with_key_rejected():
    reloader = _reloader(signing_key="secret")
    payload = encode_config_envelope(_sections(), revision=1)
    outcome = await reloader.apply_envelope(payload, source="remote")
    assert outcome.applied is False
    assert outcome.error_code == BAD_SIGNATURE


@pytest.mark.asyncio
async def test_unsigned_envelope_accepted_without_key():
    reloader = _reloader()
    payload = encode_config_envelope(_sections(), revision=1)
    outcome = await reloader.apply_envelope(payload, source="remote")
    assert outcome.applied is True


@pytest.mark.asyncio
async def test_trusted_skips_signature_but_not_replay():
    reloader = _reloader(signing_key="secret")

    # A trusted unsigned envelope bypasses signature verification.
    outcome = await reloader.apply_envelope(
        encode_config_envelope(_sections(), revision=1), source="file", trusted=True
    )
    assert outcome.applied is True
    assert reloader.revision == 1

    # The same unsigned envelope over the untrusted path is rejected.
    outcome = await reloader.apply_envelope(encode_config_envelope(_sections(), revision=2), source="remote")
    assert outcome.applied is False
    assert outcome.error_code == BAD_SIGNATURE

    # Advance the applied revision so a stale replay can be detected.
    outcome = await reloader.apply_envelope(
        encode_config_envelope(_sections(), revision=3, signing_key="secret"),
        source="remote",
    )
    assert outcome.applied is True
    assert reloader.revision == 3

    # The replay guard still applies to trusted input.
    outcome = await reloader.apply_envelope(
        encode_config_envelope(_sections(), revision=2), source="file", trusted=True
    )
    assert outcome.applied is False
    assert outcome.error_code == STALE_CONFIG


# --- revision / replay ------------------------------------------------------


@pytest.mark.asyncio
async def test_higher_revision_applied():
    reloader = _reloader()
    await reloader.apply_envelope(encode_config_envelope(_sections(), revision=1), source="remote")
    outcome = await reloader.apply_envelope(
        encode_config_envelope(_sections(core=_settings(task_timeout=9.0)), revision=2),
        source="remote",
    )
    assert outcome.applied is True
    assert reloader.revision == 2
    assert outcome.changed == ["task_timeout"]


@pytest.mark.asyncio
async def test_lower_revision_with_different_hash_stale():
    reloader = _reloader()
    await reloader.apply_envelope(encode_config_envelope(_sections(), revision=5), source="remote")
    outcome = await reloader.apply_envelope(
        encode_config_envelope(_sections(core=_settings(task_timeout=9.0)), revision=4),
        source="remote",
    )
    assert outcome.applied is False
    assert outcome.error_code == STALE_CONFIG
    assert reloader.revision == 5


@pytest.mark.asyncio
async def test_equal_revision_equal_hash_idempotent():
    reloader = _reloader()
    payload = encode_config_envelope(_sections(core=_settings(task_timeout=9.0)), revision=3)
    first = await reloader.apply_envelope(payload, source="remote")
    second = await reloader.apply_envelope(payload, source="remote")
    assert first.applied is True
    assert second.applied is True
    assert second.changed == []
    assert reloader.revision == 3


@pytest.mark.asyncio
async def test_equal_revision_different_hash_stale():
    reloader = _reloader()
    await reloader.apply_envelope(
        encode_config_envelope(_sections(core=_settings(task_timeout=1.0)), revision=3),
        source="remote",
    )
    outcome = await reloader.apply_envelope(
        encode_config_envelope(_sections(core=_settings(task_timeout=2.0)), revision=3),
        source="remote",
    )
    assert outcome.applied is False
    assert outcome.error_code == STALE_CONFIG
    assert reloader.revision == 3


@pytest.mark.asyncio
async def test_reset_restores_initial_replay_state():
    reloader = _reloader()
    reloader.register_section("svc", _ServiceA, lambda value: None)
    section = _ServiceA(a=1)
    sections = _sections(services={"svc": msgspec.msgpack.encode(section)})
    outcome = await reloader.apply_envelope(encode_config_envelope(sections, revision=5), source="remote")
    assert outcome.applied is True
    assert reloader.revision == 5
    assert reloader.source == "remote"
    assert reloader.show().services == {"svc": msgspec.msgpack.encode(section)}

    reloader.reset()

    assert reloader.revision == 0
    assert reloader.hash == ""
    assert reloader.source == "default"
    assert reloader.show().services == {}

    outcome = await reloader.apply_envelope(encode_config_envelope(_sections(), revision=1), source="remote")
    assert outcome.applied is True


# --- settings validation ----------------------------------------------------


@pytest.mark.parametrize("missing", list(ReloadableSettings.__struct_fields__))
def test_reloadable_settings_requires_every_field(missing):
    values = {field: _CORE_DEFAULTS[field] for field in ReloadableSettings.__struct_fields__ if field != missing}
    with pytest.raises(msgspec.ValidationError):
        msgspec.msgpack.decode(msgspec.msgpack.encode(values), type=ReloadableSettings)


@pytest.mark.asyncio
async def test_unknown_core_field_rejected():
    reloader = _reloader()
    settings = msgspec.msgpack.encode({"core": dict(_CORE_DEFAULTS, queue_size=100), "services": {}})
    payload = msgspec.msgpack.encode(
        ConfigEnvelope(
            version=1,
            revision=1,
            hash=hashlib.sha256(settings).hexdigest(),
            settings=settings,
        )
    )
    outcome = await reloader.apply_envelope(payload, source="remote")
    assert outcome.applied is False
    assert outcome.error_code == INVALID_CONFIG


@pytest.mark.asyncio
async def test_apply_callback_raising_leaves_state_unchanged():
    def failing_apply(settings):
        raise ValueError("out-of-range value")

    reloader = _reloader(apply=failing_apply)
    outcome = await reloader.apply_envelope(
        encode_config_envelope(_sections(core=_settings(task_timeout=999.0)), revision=7),
        source="remote",
    )
    assert outcome.applied is False
    assert outcome.error_code == INVALID_CONFIG
    assert reloader.revision == 0
    assert reloader.hash == ""
    assert reloader.source == "default"


# --- registered sections ----------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_section_rejected():
    reloader = _reloader()
    sections = _sections(services={"bogus": msgspec.msgpack.encode(_ServiceA())})
    outcome = await reloader.apply_envelope(encode_config_envelope(sections, revision=1), source="remote")
    assert outcome.applied is False
    assert outcome.error_code == UNKNOWN_CONFIG_SECTION


@pytest.mark.asyncio
async def test_registered_section_bad_payload_rejected():
    reloader = _reloader()
    reloader.register_section("svc", _ServiceA, lambda value: None)
    sections = _sections(services={"svc": b"garbage"})
    outcome = await reloader.apply_envelope(encode_config_envelope(sections, revision=1), source="remote")
    assert outcome.applied is False
    assert outcome.error_code == INVALID_CONFIG


@pytest.mark.asyncio
async def test_section_hook_receives_decoded_struct():
    received = []
    reloader = _reloader()
    reloader.register_section("svc", _ServiceA, received.append)
    section = _ServiceA(a=7)
    sections = _sections(services={"svc": msgspec.msgpack.encode(section)})
    outcome = await reloader.apply_envelope(encode_config_envelope(sections, revision=1), source="remote")
    assert outcome.applied is True
    assert received == [section]


@pytest.mark.asyncio
async def test_raising_section_hook_aborts_before_core_swap():
    def failing_hook(value):
        raise RuntimeError("hook failed")

    reloader = _reloader()
    reloader.register_section("svc", _ServiceA, failing_hook)
    sections = _sections(
        core=_settings(task_timeout=42.0),
        services={"svc": msgspec.msgpack.encode(_ServiceA())},
    )
    outcome = await reloader.apply_envelope(encode_config_envelope(sections, revision=1), source="remote")
    assert outcome.applied is False
    assert outcome.error_code == INVALID_CONFIG
    assert reloader.apply_calls == []
    assert reloader.revision == 0


@pytest.mark.asyncio
async def test_reregistering_section_replaces_struct_and_hook():
    first, second = [], []
    reloader = _reloader()
    reloader.register_section("svc", _ServiceA, first.append)
    reloader.register_section("svc", _ServiceB, second.append)
    # The payload only decodes against the second struct (_ServiceB).
    sections = _sections(services={"svc": msgspec.msgpack.encode(_ServiceB(b=5))})
    outcome = await reloader.apply_envelope(encode_config_envelope(sections, revision=1), source="remote")
    assert outcome.applied is True
    assert first == []
    assert second == [_ServiceB(b=5)]


# --- reload / store / show --------------------------------------------------


@pytest.mark.asyncio
async def test_reload_source_returns_none():
    reloader = _reloader()
    outcome = await reloader.reload(_FakeSource(payload=None))
    assert outcome.applied is False
    assert outcome.error_code == CONFIG_SOURCE_UNAVAILABLE


@pytest.mark.asyncio
async def test_reload_source_raises():
    reloader = _reloader()
    outcome = await reloader.reload(_FakeSource(load_error=RuntimeError("boom")))
    assert outcome.applied is False
    assert outcome.error_code == CONFIG_SOURCE_UNAVAILABLE


@pytest.mark.asyncio
async def test_reload_valid_payload_applies():
    reloader = _reloader()
    source = _FakeSource(payload=encode_config_envelope(_sections(core=_settings(task_timeout=7.0)), revision=2))
    outcome = await reloader.reload(source)
    assert outcome.applied is True
    assert reloader.source == "remote"
    assert reloader.revision == 2
    assert source.load_calls == 1


@pytest.mark.asyncio
async def test_store_writes_decodable_envelope():
    reloader = _reloader()
    await reloader.apply_envelope(
        encode_config_envelope(_sections(core=_settings(task_timeout=6.0)), revision=2),
        source="remote",
    )
    source = _FakeSource()
    outcome = await reloader.store(source)
    assert outcome.stored is True
    assert outcome.target == "remote"
    assert len(source.store_calls) == 1
    stored = decode_config_envelope(source.store_calls[0])
    assert stored is not None
    assert stored.revision == reloader.revision
    assert stored.hash == reloader.hash
    assert msgspec.msgpack.decode(stored.settings, type=ConfigSections).core == _settings(task_timeout=6.0)


@pytest.mark.asyncio
async def test_store_source_raises():
    """A source store failure maps to CONFIG_SOURCE_UNAVAILABLE (transient)."""
    reloader = _reloader()
    outcome = await reloader.store(_FakeSource(store_error=RuntimeError("boom")))
    assert outcome.stored is False
    assert outcome.error_code == CONFIG_SOURCE_UNAVAILABLE


def test_retryable_error_codes_contains_only_source_unavailable():
    """RETRYABLE_ERROR_CODES is exactly {CONFIG_SOURCE_UNAVAILABLE}."""
    assert RETRYABLE_ERROR_CODES == {CONFIG_SOURCE_UNAVAILABLE}


def test_show_returns_current_core_settings():
    reloader = _reloader()
    assert reloader.show().core == _settings()


@pytest.mark.asyncio
async def test_disabled_reloader_short_circuits():
    reloader = _reloader(enabled=False)
    source = _FakeSource(payload=encode_config_envelope(_sections(), revision=1))
    apply_outcome = await reloader.apply_envelope(encode_config_envelope(_sections(), revision=1), source="remote")
    reload_outcome = await reloader.reload(source)
    assert apply_outcome.error_code == REMOTE_CONFIG_DISABLED
    assert reload_outcome.error_code == REMOTE_CONFIG_DISABLED
    assert source.load_calls == 0
    assert reloader.revision == 0


@pytest.mark.asyncio
async def test_store_disabled_returns_disabled_and_skips_source():
    reloader = _reloader(enabled=False)
    source = _FakeSource()
    outcome = await reloader.store(source)
    assert outcome.stored is False
    assert outcome.error_code == REMOTE_CONFIG_DISABLED
    assert source.store_calls == []


# --- local file I/O ---------------------------------------------------------


def test_read_local_config_missing_file_returns_none(tmp_path: Path):
    path = tmp_path / "config.yml"
    assert read_local_config(path) is None
    assert not path.exists()


def test_read_local_config_invalid_yaml_returns_none(tmp_path: Path):
    path = tmp_path / "config.yml"
    path.write_bytes(b"not: [valid: yaml\n  base_config: broken")
    assert read_local_config(path) is None


def test_write_then_read_local_config_round_trips(tmp_path: Path):
    path = tmp_path / "config.yml"
    sections = _sections(
        core=_settings(task_timeout=4.5),
        services={"svc": msgspec.msgpack.encode(_ServiceA(a=3))},
    )
    write_local_config(path, sections)
    assert read_local_config(path) == sections


def test_write_local_config_is_atomic(tmp_path: Path):
    path = tmp_path / "config.yml"
    write_local_config(path, _sections())
    assert [entry.name for entry in tmp_path.iterdir()] == ["config.yml"]


def test_write_local_config_creates_parent_directory(tmp_path: Path):
    path = tmp_path / "missing" / "sub" / "config.yml"
    write_local_config(path, _sections())
    assert path.exists()


# --- concurrency ------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_applies_serialize():
    reloader = _reloader()
    lower = encode_config_envelope(_sections(core=_settings(task_timeout=1.0)), revision=2)
    higher = encode_config_envelope(_sections(core=_settings(task_timeout=2.0)), revision=3)
    outcomes = await asyncio.gather(
        reloader.apply_envelope(lower, source="remote"),
        reloader.apply_envelope(higher, source="remote"),
    )
    assert reloader.revision == 3
    assert any(outcome.applied and outcome.revision == 3 for outcome in outcomes)
    assert all(outcome.applied or outcome.error_code == STALE_CONFIG for outcome in outcomes)


def test_config_source_protocol_surface_is_load_and_store():
    """The ConfigSource protocol stays exactly {load, store} (AR-110).

    Widening it with a required member (e.g. wait_for_snapshot) would break
    every external structural implementer, so the surface is pinned here.
    """
    members = {name for name in vars(ConfigSource) if not name.startswith("_")}
    assert members == {"load", "store"}
