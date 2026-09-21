"""Remote-config control path tests: handler dispatch, processor wiring, and
apply semantics for the ``config:apply`` / ``config:store`` / ``config:show``
task types (design `docs/design/remote_config.md` §4, §12)."""

import asyncio
import logging
from typing import Any, cast
from uuid import uuid4

import msgspec
import pytest

from scietex.service.config import TaskProcessorConfig
from scietex.service.config_reload import (
    CONFIG_SOURCE_NOT_CONFIGURED,
    CONFIG_SOURCE_UNAVAILABLE,
    CONFIG_STORE_FAILED,
    INVALID_CONFIG,
    INVALID_CONFIG_PAYLOAD,
    RELOADABLE_FIELDS,
    REMOTE_CONFIG_DISABLED,
    UNKNOWN_CONFIG_SECTION,
    ConfigApplyOutcome,
    ConfigSections,
    ConfigStoreOutcome,
    DeclarativeSections,
    ReloadableSettings,
    encode_config_envelope,
)
from scietex.service.task_handler.capabilities import TaskCapabilities
from scietex.service.task_handler.config import (
    ConfigApplyHandler,
    ConfigApplyRequest,
    ConfigApplyResponse,
    ConfigShowHandler,
    ConfigShowRequest,
    ConfigShowResponse,
    ConfigStoreHandler,
    ConfigStoreRequest,
    ConfigStoreResponse,
)
from scietex.service.task_handler.context import TaskHandlerContext
from scietex.service.task_handler.schemas import (
    CONFIG_APPLY_TASK_NAME,
    CONFIG_SHOW_TASK_NAME,
    CONFIG_STORE_TASK_NAME,
    TaskData,
)
from scietex.service.task_processor import TaskProcessor

# A complete, in-bounds snapshot of the eight reloadable core fields. Used both
# to build the processor's initial explicit config (so the `changed` list names
# only fields that actually moved) and to build valid envelopes.
_BASE_SETTINGS: dict[str, float | int] = {
    "max_concurrent_tasks": 4,
    "task_manager_sleep_time": 0.02,
    "task_queue_manager_sleep_time": 0.02,
    "task_handler_start_timeout": 6.0,
    "task_handler_stop_timeout": 6.0,
    "task_timeout": 4.0,
    "task_queue_fetch_timeout": 2.0,
    "task_cancellation_timeout": 6.0,
}

#: Connection-config field names from ``ValkeyConfig``/``MqttConfig`` that must
#: never leak through the ``config:show`` channel.
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


class NestedSettings(msgspec.Struct, frozen=True):
    """A nested config struct carried on a config subclass, to prove
    ``_apply_reloadable_config`` preserves it by reference."""

    value: int = 1


class ConfigWithNested(TaskProcessorConfig, frozen=True):
    """Concrete config carrying a nested struct field (mirrors a transport
    config such as ``valkey_config`` on ``ValkeyWorkerConfig``)."""

    nested: NestedSettings = NestedSettings()


class FakeConfigSource:
    """ConfigSource double: returns a canned payload and records stores."""

    def __init__(self, payload: bytes | None = None) -> None:
        self._payload = payload
        self.stored: list[bytes] = []

    async def load(self) -> bytes | None:
        return self._payload

    async def store(self, envelope: bytes) -> None:
        self.stored.append(envelope)


def make_settings(**overrides: object) -> ReloadableSettings:
    """Build a complete ``ReloadableSettings`` snapshot with overrides."""
    merged: dict[str, object] = dict(_BASE_SETTINGS)
    merged.update(overrides)
    return ReloadableSettings(**cast(Any, merged))


def make_envelope(sections: ConfigSections | None = None, *, revision: int = 1) -> bytes:
    """Encode a valid, unsigned envelope around ``sections`` (defaults to the
    base core settings)."""
    if sections is None:
        sections = ConfigSections(core=make_settings())
    return encode_config_envelope(sections, revision=revision)


def make_processor(tmp_path, **config_kwargs: object) -> TaskProcessor:
    """Build a processor rooted at ``tmp_path`` with explicit base settings."""
    cfg_kwargs: dict[str, object] = dict(_BASE_SETTINGS)
    cfg_kwargs.update(config_kwargs)
    cfg_kwargs.setdefault("conf_dir", tmp_path)
    return TaskProcessor(TaskProcessorConfig(**cast(Any, cfg_kwargs)))


def make_context() -> TaskHandlerContext:
    return TaskHandlerContext(
        service_name="test",
        instance_id=uuid4().hex,
        logger=logging.getLogger("scietex.service.tests.config_control"),
    )


async def _noop_write_progress(task_id, value) -> None:
    return None


def make_capabilities() -> TaskCapabilities:
    return TaskCapabilities(task_id=uuid4(), _write_progress=_noop_write_progress)


def _task(task_type: str, payload: bytes) -> TaskData:
    return TaskData(task_id=str(uuid4()), task=task_type, payload=payload)


# --------------------------------------------------------------------------- #
# Handler dispatch: drive the handlers directly, not through a full worker run.
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_apply_handler_success_calls_callback_and_returns_response():
    """A valid ConfigApplyRequest calls the injected callback with the right
    args and returns a success result whose payload decodes to a response."""
    calls: list[tuple[bytes | None, bool]] = []

    async def apply(payload: bytes | None, persist: bool) -> ConfigApplyOutcome:
        calls.append((payload, persist))
        return ConfigApplyOutcome(
            applied=True,
            revision=7,
            hash="abc",
            changed=["task_timeout"],
            restart_required=["queue_size"],
        )

    handler = ConfigApplyHandler("apply", make_context(), apply=apply)
    envelope = make_envelope()
    result = await handler.handle(
        _task(CONFIG_APPLY_TASK_NAME, msgspec.msgpack.encode(ConfigApplyRequest(payload=envelope, persist=True))),
        capabilities=make_capabilities(),
    )

    assert calls == [(envelope, True)]
    assert result.status == "success"
    response = msgspec.msgpack.decode(result.payload, type=ConfigApplyResponse)
    assert response.applied is True
    assert response.revision == 7
    assert response.hash == "abc"
    assert response.changed == ["task_timeout"]
    assert response.restart_required == ["queue_size"]


@pytest.mark.asyncio
async def test_apply_handler_malformed_payload_is_not_retryable():
    """A payload that is not a ConfigApplyRequest yields INVALID_CONFIG_PAYLOAD
    and never invokes the callback."""
    calls: list[tuple[bytes | None, bool]] = []

    async def apply(payload: bytes | None, persist: bool) -> ConfigApplyOutcome:
        calls.append((payload, persist))
        return ConfigApplyOutcome(applied=True)

    handler = ConfigApplyHandler("apply", make_context(), apply=apply)
    result = await handler.handle(
        _task(CONFIG_APPLY_TASK_NAME, b"not-msgpack"),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == INVALID_CONFIG_PAYLOAD
    assert result.retryable is False
    assert calls == []


@pytest.mark.asyncio
async def test_apply_handler_source_unavailable_is_retryable():
    """A source-unavailable outcome opts into the framework's single retry."""

    async def apply(payload: bytes | None, persist: bool) -> ConfigApplyOutcome:
        return ConfigApplyOutcome(applied=False, error="down", error_code=CONFIG_SOURCE_UNAVAILABLE)

    handler = ConfigApplyHandler("apply", make_context(), apply=apply)
    result = await handler.handle(
        _task(CONFIG_APPLY_TASK_NAME, msgspec.msgpack.encode(ConfigApplyRequest(payload=None))),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == CONFIG_SOURCE_UNAVAILABLE
    assert result.retryable is True


@pytest.mark.asyncio
async def test_apply_handler_not_configured_is_not_retryable():
    """A not-configured-source outcome is permanent, not retryable."""

    async def apply(payload: bytes | None, persist: bool) -> ConfigApplyOutcome:
        return ConfigApplyOutcome(applied=False, error="no source", error_code=CONFIG_SOURCE_NOT_CONFIGURED)

    handler = ConfigApplyHandler("apply", make_context(), apply=apply)
    result = await handler.handle(
        _task(CONFIG_APPLY_TASK_NAME, msgspec.msgpack.encode(ConfigApplyRequest(payload=None))),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == CONFIG_SOURCE_NOT_CONFIGURED
    assert result.retryable is False


@pytest.mark.asyncio
async def test_apply_handler_invalid_config_is_not_retryable():
    """A validation failure is permanent, not retryable."""

    async def apply(payload: bytes | None, persist: bool) -> ConfigApplyOutcome:
        return ConfigApplyOutcome(applied=False, error="bad value", error_code=INVALID_CONFIG)

    handler = ConfigApplyHandler("apply", make_context(), apply=apply)
    result = await handler.handle(
        _task(CONFIG_APPLY_TASK_NAME, msgspec.msgpack.encode(ConfigApplyRequest(payload=b"x"))),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == INVALID_CONFIG
    assert result.retryable is False


@pytest.mark.asyncio
async def test_apply_handler_callback_raises_is_not_retryable():
    """A raising callback is unclassified: INVALID_CONFIG, retryable=False."""

    async def apply(payload: bytes | None, persist: bool) -> ConfigApplyOutcome:
        raise RuntimeError("boom")

    handler = ConfigApplyHandler("apply", make_context(), apply=apply)
    result = await handler.handle(
        _task(CONFIG_APPLY_TASK_NAME, msgspec.msgpack.encode(ConfigApplyRequest(payload=b"x"))),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error == "boom"
    assert result.error_code == INVALID_CONFIG
    assert result.retryable is False


@pytest.mark.asyncio
async def test_store_handler_success_calls_callback_and_returns_response():
    """A valid ConfigStoreRequest calls the injected callback and returns a
    success result whose payload decodes to a response."""
    calls: list[str] = []

    async def store(target: str) -> ConfigStoreOutcome:
        calls.append(target)
        return ConfigStoreOutcome(stored=True, target=target, path="/x/config.yml", revision=3, hash="h")

    handler = ConfigStoreHandler("store", make_context(), store=store)
    result = await handler.handle(
        _task(CONFIG_STORE_TASK_NAME, msgspec.msgpack.encode(ConfigStoreRequest(target="disk"))),
        capabilities=make_capabilities(),
    )

    assert calls == ["disk"]
    assert result.status == "success"
    response = msgspec.msgpack.decode(result.payload, type=ConfigStoreResponse)
    assert response.stored is True
    assert response.target == "disk"
    assert response.path == "/x/config.yml"
    assert response.revision == 3


@pytest.mark.asyncio
async def test_store_handler_malformed_payload_is_not_retryable():
    """A payload that is not a ConfigStoreRequest yields INVALID_CONFIG_PAYLOAD."""
    calls: list[str] = []

    async def store(target: str) -> ConfigStoreOutcome:
        calls.append(target)
        return ConfigStoreOutcome(stored=True)

    handler = ConfigStoreHandler("store", make_context(), store=store)
    result = await handler.handle(
        _task(CONFIG_STORE_TASK_NAME, b"not-msgpack"),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == INVALID_CONFIG_PAYLOAD
    assert result.retryable is False
    assert calls == []


@pytest.mark.asyncio
async def test_store_handler_store_failed_is_not_retryable():
    """A store failure (CONFIG_STORE_FAILED) is permanent, not retryable."""

    async def store(target: str) -> ConfigStoreOutcome:
        return ConfigStoreOutcome(stored=False, target=target, error="disk full", error_code=CONFIG_STORE_FAILED)

    handler = ConfigStoreHandler("store", make_context(), store=store)
    result = await handler.handle(
        _task(CONFIG_STORE_TASK_NAME, msgspec.msgpack.encode(ConfigStoreRequest(target="disk"))),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == CONFIG_STORE_FAILED
    assert result.retryable is False


@pytest.mark.asyncio
async def test_store_handler_remote_source_failure_is_retryable():
    """A remote store source-unavailable outcome opts into the single retry."""

    async def store(target: str) -> ConfigStoreOutcome:
        return ConfigStoreOutcome(stored=False, target=target, error_code=CONFIG_SOURCE_UNAVAILABLE)

    handler = ConfigStoreHandler("store", make_context(), store=store)
    result = await handler.handle(
        _task(CONFIG_STORE_TASK_NAME, msgspec.msgpack.encode(ConfigStoreRequest(target="remote"))),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == CONFIG_SOURCE_UNAVAILABLE
    assert result.retryable is True


@pytest.mark.asyncio
async def test_store_handler_callback_raises_is_not_retryable():
    """A raising store callback is unclassified: INVALID_CONFIG, retryable=False."""

    async def store(target: str) -> ConfigStoreOutcome:
        raise RuntimeError("boom")

    handler = ConfigStoreHandler("store", make_context(), store=store)
    result = await handler.handle(
        _task(CONFIG_STORE_TASK_NAME, msgspec.msgpack.encode(ConfigStoreRequest(target="disk"))),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == INVALID_CONFIG
    assert result.retryable is False


@pytest.mark.asyncio
async def test_show_handler_success_calls_callback_and_returns_response():
    """A valid ConfigShowRequest calls the injected show callback and returns
    its response as the result payload."""
    calls: list[bool] = []

    def show(include_restart_required: bool) -> ConfigShowResponse:
        calls.append(include_restart_required)
        return ConfigShowResponse(settings=b"x", revision=5, hash="h", source="remote", restart_required_fields=["a"])

    handler = ConfigShowHandler("show", make_context(), show=show)
    result = await handler.handle(
        _task(CONFIG_SHOW_TASK_NAME, msgspec.msgpack.encode(ConfigShowRequest(include_restart_required=False))),
        capabilities=make_capabilities(),
    )

    assert calls == [False]
    assert result.status == "success"
    response = msgspec.msgpack.decode(result.payload, type=ConfigShowResponse)
    assert response.settings == b"x"
    assert response.revision == 5
    assert response.source == "remote"
    assert response.restart_required_fields == ["a"]


@pytest.mark.asyncio
async def test_show_handler_malformed_payload_is_not_retryable():
    """A payload that is not a ConfigShowRequest yields INVALID_CONFIG_PAYLOAD."""
    calls: list[bool] = []

    def show(include_restart_required: bool) -> ConfigShowResponse:
        calls.append(include_restart_required)
        return ConfigShowResponse()

    handler = ConfigShowHandler("show", make_context(), show=show)
    result = await handler.handle(
        _task(CONFIG_SHOW_TASK_NAME, b"not-msgpack"),
        capabilities=make_capabilities(),
    )

    assert result.status == "error"
    assert result.error_code == INVALID_CONFIG_PAYLOAD
    assert result.retryable is False
    assert calls == []


@pytest.mark.asyncio
async def test_show_handler_disabled_response_is_not_retryable():
    """A show response carrying a non-empty error_code maps to a non-retryable
    error result."""
    calls: list[bool] = []

    def show(include_restart_required: bool) -> ConfigShowResponse:
        calls.append(include_restart_required)
        return ConfigShowResponse(error_code=REMOTE_CONFIG_DISABLED, error="remote config is disabled")

    handler = ConfigShowHandler("show", make_context(), show=show)
    result = await handler.handle(
        _task(CONFIG_SHOW_TASK_NAME, msgspec.msgpack.encode(ConfigShowRequest())),
        capabilities=make_capabilities(),
    )

    assert calls == [True]
    assert result.status == "error"
    assert result.error_code == REMOTE_CONFIG_DISABLED
    assert result.error == "remote config is disabled"
    assert result.retryable is False


@pytest.mark.parametrize(
    ("handler_cls", "task_type", "kwarg"),
    [
        (ConfigApplyHandler, CONFIG_APPLY_TASK_NAME, "apply"),
        (ConfigStoreHandler, CONFIG_STORE_TASK_NAME, "store"),
        (ConfigShowHandler, CONFIG_SHOW_TASK_NAME, "show"),
    ],
)
def test_config_handlers_supported_tasks_is_single(handler_cls, task_type, kwarg):
    """Each config handler declares exactly its own one task type."""
    if kwarg == "show":
        handler = handler_cls("x", make_context(), show=lambda _: ConfigShowResponse())
    else:

        async def _async_stub(*args, **kwargs):  # pragma: no cover - unused
            raise AssertionError("unused")

        handler = handler_cls("x", make_context(), **{kwarg: _async_stub})

    assert handler.supported_tasks == [task_type]


# --------------------------------------------------------------------------- #
# TaskProcessor integration.
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_enabled_processor_registers_config_handlers():
    """An enabled processor registers apply/store/show handlers that dispatch on
    their task types."""
    proc = TaskProcessor(TaskProcessorConfig(remote_config_enabled=True))
    await proc._start_task_handler("ConfigApplyHandler")
    await proc._start_task_handler("ConfigStoreHandler")
    await proc._start_task_handler("ConfigShowHandler")

    assert isinstance(proc._find_task_handler(CONFIG_APPLY_TASK_NAME, control=True), ConfigApplyHandler)
    assert isinstance(proc._find_task_handler(CONFIG_STORE_TASK_NAME, control=True), ConfigStoreHandler)
    assert isinstance(proc._find_task_handler(CONFIG_SHOW_TASK_NAME, control=True), ConfigShowHandler)


@pytest.mark.asyncio
async def test_disabled_processor_does_not_register_config_handlers():
    """A disabled processor registers no config:* handlers, so none of the three
    task types resolves to an active handler."""
    proc = TaskProcessor()
    await proc._start_task_handler("ConfigApplyHandler")
    await proc._start_task_handler("ConfigStoreHandler")
    await proc._start_task_handler("ConfigShowHandler")

    assert proc._find_task_handler(CONFIG_APPLY_TASK_NAME, control=True) is None
    assert proc._find_task_handler(CONFIG_STORE_TASK_NAME, control=True) is None
    assert proc._find_task_handler(CONFIG_SHOW_TASK_NAME, control=True) is None


@pytest.mark.asyncio
async def test_remote_config_disabled_apply_returns_disabled(tmp_path):
    """With the master switch off, an inline apply is rejected with
    REMOTE_CONFIG_DISABLED."""
    proc = make_processor(tmp_path)
    outcome = await proc._config_manager.apply_config(make_envelope(revision=1), False)

    assert outcome.applied is False
    assert outcome.error_code == REMOTE_CONFIG_DISABLED


@pytest.mark.asyncio
async def test_remote_config_disabled_show_reports_disabled(tmp_path):
    """With the master switch off, ``config:show`` reports
    REMOTE_CONFIG_DISABLED rather than the effective settings."""
    proc = make_processor(tmp_path)
    response = proc._config_manager.show_config(True)

    assert response.error_code == REMOTE_CONFIG_DISABLED
    assert response.error == "remote config is disabled"
    assert response.settings == b""
    assert response.revision == 0
    assert response.hash == ""
    assert response.source == "default"


@pytest.mark.asyncio
async def test_remote_config_disabled_store_remote_is_gated(tmp_path):
    """With the master switch off, a remote store must be rejected with
    REMOTE_CONFIG_DISABLED rather than publishing to the source."""
    proc = make_processor(tmp_path)
    source = FakeConfigSource()
    proc._config_manager.attach_source(source)

    outcome = await proc._config_manager.store_config("remote")

    assert outcome.stored is False
    assert outcome.error_code == REMOTE_CONFIG_DISABLED
    assert source.stored == []


@pytest.mark.asyncio
async def test_apply_with_no_source_and_none_payload_returns_source_not_configured(tmp_path):
    """``payload=None`` with no attached source of truth is
    CONFIG_SOURCE_NOT_CONFIGURED."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    outcome = await proc._config_manager.apply_config(None, False)

    assert outcome.applied is False
    assert outcome.error_code == CONFIG_SOURCE_NOT_CONFIGURED


@pytest.mark.asyncio
async def test_apply_reload_from_source_updates_revision_and_source(tmp_path):
    """``payload=None`` with an attached source reloads and records the applied
    revision and ``remote`` source label."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    proc._config_manager.attach_source(FakeConfigSource(payload=make_envelope(revision=5)))

    outcome = await proc._config_manager.apply_config(None, False)

    assert outcome.applied is True
    assert outcome.revision == 5
    assert proc.config_revision == 5
    assert proc.config_source == "remote"


@pytest.mark.asyncio
async def test_apply_inline_payload_sets_source_inline(tmp_path):
    """An inline envelope applies with the ``inline`` source label."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    outcome = await proc._config_manager.apply_config(make_envelope(revision=1), False)

    assert outcome.applied is True
    assert proc.config_source == "inline"


@pytest.mark.asyncio
async def test_apply_persist_writes_config_yml(tmp_path):
    """``persist=True`` writes the local snapshot after a successful apply."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    outcome = await proc._config_manager.apply_config(make_envelope(revision=1), True)

    assert outcome.applied is True
    assert (tmp_path / "config.yml").exists()


@pytest.mark.asyncio
async def test_store_disk_writes_file(tmp_path):
    """``config:store`` to disk writes ``<conf_dir>/config.yml``."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    outcome = await proc._config_manager.store_config("disk")

    assert outcome.stored is True
    assert (tmp_path / "config.yml").exists()


@pytest.mark.asyncio
async def test_store_remote_calls_source_store(tmp_path):
    """``config:store`` to remote publishes to the attached source."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    source = FakeConfigSource()
    proc._config_manager.attach_source(source)

    outcome = await proc._config_manager.store_config("remote")

    assert outcome.stored is True
    assert len(source.stored) == 1


@pytest.mark.asyncio
async def test_store_both_writes_disk_and_source(tmp_path):
    """``config:store`` to both writes the file and publishes to the source."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    source = FakeConfigSource()
    proc._config_manager.attach_source(source)

    outcome = await proc._config_manager.store_config("both")

    assert outcome.stored is True
    assert (tmp_path / "config.yml").exists()
    assert len(source.stored) == 1


def test_show_restart_required_fields_gated_on_request(tmp_path):
    """``restart_required_fields`` is populated only when requested."""
    proc = make_processor(tmp_path, remote_config_enabled=True)

    assert proc._config_manager.show_config(True).restart_required_fields
    assert proc._config_manager.show_config(False).restart_required_fields == []


def test_show_never_contains_connection_config(tmp_path):
    """The show payload exposes only core + registered services and never a
    connection-config field name."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    response = proc._config_manager.show_config(True)

    sections = msgspec.msgpack.decode(response.settings, type=ConfigSections)
    assert sections.services == {}
    core_fields = {f.name for f in msgspec.structs.fields(type(sections.core))}
    assert core_fields == set(RELOADABLE_FIELDS)

    encoded = msgspec.msgpack.encode(response)
    for field in _SECRET_FIELDS:
        assert field.encode() not in encoded


# --------------------------------------------------------------------------- #
# Apply semantics.
# --------------------------------------------------------------------------- #


def test_apply_reloadable_config_swaps_config_and_effective(tmp_path):
    """Valid settings swap ``_config`` and ``_effective`` together; the changed
    list names only the fields that actually moved."""
    proc = make_processor(tmp_path)
    changed = proc._apply_reloadable_config(make_settings(max_concurrent_tasks=7, task_timeout=9.0))
    cfg = cast(TaskProcessorConfig, proc._config)

    assert set(changed) == {"max_concurrent_tasks", "task_timeout"}
    assert cfg.max_concurrent_tasks == 7
    assert proc.max_concurrent_tasks == 7
    assert cfg.task_timeout == 9.0
    assert proc.task_handler_start_timeout == 6.0


def test_apply_reloadable_config_out_of_range_raises_and_preserves_state(tmp_path):
    """An out-of-range value is rejected before any mutation, leaving ``_config``
    and the effective settings untouched."""
    proc = make_processor(tmp_path)
    before = proc._config
    before_effective = proc._effective

    with pytest.raises(msgspec.ValidationError):
        proc._apply_reloadable_config(make_settings(max_concurrent_tasks=0))

    assert proc._config is before
    assert proc._effective is before_effective
    assert proc._current_reloadable_settings() == before_effective
    assert cast(TaskProcessorConfig, proc._config).max_concurrent_tasks == 4
    assert proc.max_concurrent_tasks == 4


def test_apply_reloadable_config_preserves_nested_struct_identity(tmp_path):
    """A nested struct on the concrete config survives a core apply by
    reference — never replaced or deep-copied."""
    nested = NestedSettings(value=42)
    proc = TaskProcessor(ConfigWithNested(conf_dir=tmp_path, nested=nested, **cast(Any, _BASE_SETTINGS)))

    proc._apply_reloadable_config(make_settings(max_concurrent_tasks=9))
    cfg = cast(ConfigWithNested, proc._config)

    assert type(cfg) is ConfigWithNested
    assert cfg.nested is nested
    assert cfg.nested.value == 42
    assert cfg.max_concurrent_tasks == 9


@pytest.mark.asyncio
async def test_register_config_settings_registered_section_calls_hook(tmp_path):
    """An envelope carrying a registered service section decodes it and calls
    the registered apply hook."""

    class MyServiceSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
        batch_size: int = 100

    proc = make_processor(tmp_path, remote_config_enabled=True)
    applied: list[object] = []
    proc.register_config_settings("my_service", MyServiceSettings, apply=applied.append)

    sections = ConfigSections(
        core=make_settings(),
        services={"my_service": msgspec.msgpack.encode(MyServiceSettings(batch_size=42))},
    )
    outcome = await proc._config_manager.apply_config(make_envelope(sections, revision=1), False)

    assert outcome.applied is True
    assert len(applied) == 1
    assert cast(MyServiceSettings, applied[0]).batch_size == 42


@pytest.mark.asyncio
async def test_apply_unknown_section_rejected(tmp_path):
    """An envelope naming an unregistered service section is rejected with
    UNKNOWN_CONFIG_SECTION."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    sections = ConfigSections(core=make_settings(), services={"unknown": msgspec.msgpack.encode({"x": 1})})

    outcome = await proc._config_manager.apply_config(make_envelope(sections, revision=1), False)

    assert outcome.applied is False
    assert outcome.error_code == UNKNOWN_CONFIG_SECTION


# --------------------------------------------------------------------------- #
# Effective-config collapse (AR-100).
# --------------------------------------------------------------------------- #


def test_reload_updates_every_read_path_atomically(tmp_path):
    """A distinct apply updates ``_effective``, the five public properties, and
    the decoded ``config:show`` core in one shot — every read path agrees."""
    proc = make_processor(tmp_path, remote_config_enabled=True)
    distinct = make_settings(
        max_concurrent_tasks=11,
        task_manager_sleep_time=0.5,
        task_queue_manager_sleep_time=0.6,
        task_handler_start_timeout=7.0,
        task_handler_stop_timeout=8.0,
        task_timeout=9.0,
        task_queue_fetch_timeout=3.0,
        task_cancellation_timeout=10.0,
    )

    changed = proc._apply_reloadable_config(distinct)

    assert set(changed) == set(RELOADABLE_FIELDS)
    eff = proc._effective
    assert eff.max_concurrent_tasks == proc.max_concurrent_tasks
    assert eff.task_manager_sleep_time == proc.task_manager_sleep_time
    assert eff.task_queue_manager_sleep_time == proc.task_queue_manager_sleep_time
    assert eff.task_handler_start_timeout == proc.task_handler_start_timeout
    assert eff.task_handler_stop_timeout == proc.task_handler_stop_timeout

    decoded = msgspec.msgpack.decode(proc._config_manager.show_config(False).settings, type=ConfigSections)
    assert decoded.core == proc._current_reloadable_settings()
    assert decoded.core == eff


def test_auto_tune_effective_matches_show(tmp_path):
    """auto_tune concurrency agrees across the property, the effective snapshot,
    and ``config:show`` while ``_config`` keeps its declarative ``None``."""
    proc = make_processor(
        tmp_path,
        remote_config_enabled=True,
        auto_tune=True,
        max_concurrent_tasks=None,
    )
    decoded = msgspec.msgpack.decode(proc._config_manager.show_config(False).settings, type=ConfigSections)

    assert proc.max_concurrent_tasks == proc._current_reloadable_settings().max_concurrent_tasks
    assert proc._current_reloadable_settings().max_concurrent_tasks == decoded.core.max_concurrent_tasks
    assert cast(TaskProcessorConfig, proc._config).max_concurrent_tasks is None


def test_show_declarative_settings_preserve_none_and_auto_tune(tmp_path):
    """``config:show.declarative_settings`` carries the raw declarative values
    (``None``/auto_tune intent) while ``settings`` stays effective (AR-117)."""
    proc = make_processor(
        tmp_path,
        remote_config_enabled=True,
        auto_tune=True,
        max_concurrent_tasks=None,
        task_timeout=None,
    )
    response = proc._config_manager.show_config(False)

    declarative = msgspec.msgpack.decode(response.declarative_settings, type=DeclarativeSections)
    assert declarative.core.max_concurrent_tasks is None
    assert declarative.core.task_timeout is None

    effective = msgspec.msgpack.decode(response.settings, type=ConfigSections)
    assert effective.core.max_concurrent_tasks == proc.max_concurrent_tasks
    assert effective.core.task_timeout == proc._current_reloadable_settings().task_timeout


def test_store_then_restart_preserves_declarative_intent(tmp_path):
    """A store→restart cycle keeps ``None``/auto_tune intent: the local file is
    written declaratively and re-applied without pinning resolved values (AR-117)."""
    proc = make_processor(
        tmp_path,
        remote_config_enabled=True,
        auto_tune=True,
        max_concurrent_tasks=None,
        task_timeout=None,
    )
    assert proc._config_manager.write_local().stored is True

    # A fresh processor over the same conf_dir applies the persisted file.
    restarted = make_processor(
        tmp_path,
        remote_config_enabled=True,
        auto_tune=True,
        max_concurrent_tasks=None,
        task_timeout=None,
    )
    outcome = asyncio.run(restarted._config_manager.apply_local_file())

    assert outcome is not None and outcome.applied is True
    assert cast(TaskProcessorConfig, restarted._config).max_concurrent_tasks is None
    assert cast(TaskProcessorConfig, restarted._config).task_timeout is None
    # The effective snapshot is still concrete (auto-tuned / defaulted).
    assert restarted._current_reloadable_settings().max_concurrent_tasks == restarted.max_concurrent_tasks
    assert restarted._current_reloadable_settings().task_timeout == 3.0
