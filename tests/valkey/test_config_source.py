"""Tests for ``ValkeyConfigSource`` and the ValkeyWorker startup config read.

The source unit tests drive ``ValkeyConfigSource`` directly against the shared
``DummyClient``; the startup tests exercise ``initialize()`` through the
``client_factory=`` injection seam (AR-074) so no live Valkey server is needed.
"""

import logging
from typing import cast

import pytest

from scietex.service import ValkeyWorker
from scietex.service.config import TaskProcessorConfig
from scietex.service.config_reload import (
    ConfigSections,
    ReloadableSettings,
    encode_config_envelope,
    write_local_config,
)
from scietex.service.valkey._glide import GlideClient
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig
from scietex.service.valkey.config_source import ValkeyConfigSource

from ._helpers import DummyClient, _patch_glide_and_handler

_CONFIG_KEY = "scietex:svc:config"

# A complete, in-bounds snapshot of the eight reloadable core fields.
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


def _source(client: DummyClient, *, key: str = _CONFIG_KEY) -> ValkeyConfigSource:
    return ValkeyConfigSource(
        client_provider=lambda: cast(GlideClient, client),
        key=key,
        logger=logging.getLogger("test_config_source"),
    )


# --- source unit tests ------------------------------------------------------


@pytest.mark.asyncio
async def test_load_returns_stored_bytes():
    """``load`` GETs the resolved key and returns the stored bytes."""
    client = DummyClient(get_value=b"envelope-bytes")
    source = _source(client)

    assert await source.load() == b"envelope-bytes"
    assert client.gets == [_CONFIG_KEY]


@pytest.mark.asyncio
async def test_load_missing_key_returns_none():
    """An absent key yields ``None`` (the reloader falls back to local/default)."""
    client = DummyClient(get_value=None)
    source = _source(client)

    assert await source.load() is None


@pytest.mark.asyncio
async def test_store_writes_key():
    """``store`` SETs the envelope bytes to the resolved key."""
    client = DummyClient()
    source = _source(client)

    await source.store(b"envelope-bytes")

    assert client.sets == [(_CONFIG_KEY, b"envelope-bytes", None)]


@pytest.mark.asyncio
async def test_load_propagates_client_error():
    """A client raising on ``get`` propagates (the reloader maps it to
    ``CONFIG_SOURCE_UNAVAILABLE``; the source itself never swallows it)."""
    client = DummyClient(get_error=RuntimeError("boom"))
    source = _source(client)

    with pytest.raises(RuntimeError):
        await source.load()


@pytest.mark.asyncio
async def test_source_follows_reconnected_client():
    """AR-103: the source reads the *current* client, not the one captured at
    construction, so a reconnect that swaps the client keeps remote config
    working."""
    stale = DummyClient(get_value=b"stale")
    current = DummyClient(get_value=b"current")
    holder: dict[str, DummyClient] = {"client": stale}
    source = ValkeyConfigSource(
        client_provider=lambda: cast(GlideClient, holder["client"]),
        key=_CONFIG_KEY,
        logger=logging.getLogger("test_config_source"),
    )

    assert await source.load() == b"stale"

    holder["client"] = current  # simulate _reconnect() swapping the client

    assert await source.load() == b"current"
    assert stale.gets == [_CONFIG_KEY]
    assert current.gets == [_CONFIG_KEY]


@pytest.mark.asyncio
async def test_load_returns_none_when_disconnected():
    """A disconnected worker (provider yields ``None``) makes ``load`` a no-op
    returning ``None`` rather than raising on a closed client."""
    source = ValkeyConfigSource(
        client_provider=lambda: None,
        key=_CONFIG_KEY,
        logger=logging.getLogger("test_config_source"),
    )

    assert await source.load() is None


@pytest.mark.asyncio
async def test_store_raises_when_disconnected():
    """``store`` cannot silently no-op while disconnected; it raises so the
    reloader maps the failure to ``CONFIG_STORE_FAILED``."""
    source = ValkeyConfigSource(
        client_provider=lambda: None,
        key=_CONFIG_KEY,
        logger=logging.getLogger("test_config_source"),
    )

    with pytest.raises(RuntimeError):
        await source.store(b"envelope-bytes")


# --- worker startup tests ---------------------------------------------------


def _make_worker(tmp_path, client, **config_kwargs) -> ValkeyWorker:
    """Build a ValkeyWorker rooted at ``tmp_path`` with an injected client."""
    cfg_kwargs: dict = {
        "service_name": "svc",
        "remote_config_enabled": True,
        "valkey_config": ValkeyConfig(),
        "conf_dir": tmp_path,
    }
    cfg_kwargs.update(config_kwargs)

    async def factory(cfg):
        return client

    return ValkeyWorker(ValkeyWorkerConfig(**cfg_kwargs), client_factory=factory)


@pytest.mark.asyncio
async def test_initialize_applies_remote_config(monkeypatch, tmp_path):
    """A valid remote envelope at the config key is applied on startup and the
    revision/source reflect it."""
    envelope = encode_config_envelope(
        ConfigSections(core=_settings(task_timeout=7.0)),
        revision=5,
    )
    client = DummyClient(ping_ok=True, get_values={_CONFIG_KEY: envelope})
    _patch_glide_and_handler(monkeypatch)
    worker = _make_worker(tmp_path, client)

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_revision == 5
    assert worker.config_source == "remote"
    assert cast(TaskProcessorConfig, worker._config).task_timeout == 7.0


@pytest.mark.asyncio
async def test_initialize_invalid_remote_does_not_fail(monkeypatch, tmp_path):
    """An invalid remote payload does not fail startup; the default config is
    left in place (availability-first, design §5)."""
    client = DummyClient(ping_ok=True, get_value=b"not-a-valid-envelope")
    _patch_glide_and_handler(monkeypatch)
    worker = _make_worker(tmp_path, client)

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_revision == 0
    assert worker.config_source == "default"
    assert cast(TaskProcessorConfig, worker._config).task_timeout is None


@pytest.mark.asyncio
async def test_initialize_no_remote_no_local_succeeds(monkeypatch, tmp_path):
    """No remote key and no local file succeeds with the default config."""
    client = DummyClient(ping_ok=True, get_value=None)
    _patch_glide_and_handler(monkeypatch)
    worker = _make_worker(tmp_path, client)

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_revision == 0
    assert worker.config_source == "default"


@pytest.mark.asyncio
async def test_initialize_applies_local_config(monkeypatch, tmp_path):
    """A local ``config.yml`` is applied at startup with the ``file`` source."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=_settings(task_timeout=9.0)))
    client = DummyClient(ping_ok=True, get_value=None)  # no remote key
    _patch_glide_and_handler(monkeypatch)
    worker = _make_worker(tmp_path, client)

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_source == "file"
    assert worker.config_revision == 1
    assert cast(TaskProcessorConfig, worker._config).task_timeout == 9.0


@pytest.mark.asyncio
async def test_second_initialize_reapplies_local_config(monkeypatch, tmp_path):
    """A second ``initialize()`` on the same worker re-applies the revision-1
    local snapshot rather than rejecting it as stale (AR-111)."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=_settings(task_timeout=9.0)))
    client = DummyClient(ping_ok=True, get_value=None)  # no remote key
    _patch_glide_and_handler(monkeypatch)
    worker = _make_worker(tmp_path, client)

    assert await worker.initialize() is True
    assert worker.config_source == "file"
    assert worker.config_revision == 1
    assert cast(TaskProcessorConfig, worker._config).task_timeout == 9.0

    assert await worker.initialize() is True
    assert worker.config_source == "file"
    assert worker.config_revision == 1
    assert cast(TaskProcessorConfig, worker._config).task_timeout == 9.0


@pytest.mark.asyncio
async def test_initialize_with_signing_key_applies_local_config(monkeypatch, tmp_path):
    """A local ``config.yml`` applies at startup even with signing enabled: the
    trusted local file skips signature verification (AR-111/D2)."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=_settings(task_timeout=9.0)))
    client = DummyClient(ping_ok=True, get_value=None)  # no remote key
    _patch_glide_and_handler(monkeypatch)
    worker = _make_worker(tmp_path, client, config_signing_key="secret")

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_source == "file"
    assert worker.config_revision == 1
    assert cast(TaskProcessorConfig, worker._config).task_timeout == 9.0


@pytest.mark.asyncio
async def test_initialize_remote_overrides_local(monkeypatch, tmp_path):
    """The remote source stays authoritative over a local snapshot (design §5:
    constructor < config.yml < remote)."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=_settings(task_timeout=9.0)))
    envelope = encode_config_envelope(
        ConfigSections(core=_settings(task_timeout=5.0)),
        revision=3,
    )
    client = DummyClient(ping_ok=True, get_values={_CONFIG_KEY: envelope})
    _patch_glide_and_handler(monkeypatch)
    worker = _make_worker(tmp_path, client)

    ok = await worker.initialize()

    assert ok is True
    assert worker.config_source == "remote"
    assert worker.config_revision == 3
    assert cast(TaskProcessorConfig, worker._config).task_timeout == 5.0


@pytest.mark.asyncio
async def test_initialize_disabled_ignores_local_config_without_error(monkeypatch, tmp_path, caplog):
    """With remote config disabled (the default), a present ``config.yml`` is
    ignored and must not log an ERROR (a disabled feature is not a failure)."""
    write_local_config(tmp_path / "config.yml", ConfigSections(core=_settings(task_timeout=9.0)))
    client = DummyClient(ping_ok=True, get_value=None)
    _patch_glide_and_handler(monkeypatch)
    worker = _make_worker(tmp_path, client, remote_config_enabled=False)

    with caplog.at_level(logging.ERROR):
        ok = await worker.initialize()

    assert ok is True
    assert worker.config_source == "default"
    assert worker.config_revision == 0
    assert cast(TaskProcessorConfig, worker._config).task_timeout is None
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
