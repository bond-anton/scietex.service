"""End-to-end integration tests for the v6 layered config merge pipeline.

Exercises the full layer stack — L0 constructor defaults, L1 bootstrap, L2
declarative ``config.yml``, L3 remote envelope — through a real
:class:`~scietex.service.transport_worker.TransportWorker` that wires
``ConfigManager``, not the reloader in isolation
(`docs/design/layered_config_merge.md` §9).
"""

import os

import msgspec
import pytest

from scietex.service.config import DEFAULT_MAX_TASKS_QUEUE_SIZE, DEFAULT_TASK_TIMEOUT, TaskProcessorConfig
from scietex.service.config_reload import (
    INVALID_CONFIG,
    ConfigApplyOutcome,
    ConfigSections,
    DeclarativeSections,
    encode_config_envelope,
    read_local_config,
    write_local_config,
)
from scietex.service.transport_worker import TransportWorker

DEMO_SECTION = "demo"

#: An explicit, in-bounds base snapshot of the eight reloadable core fields, so
#: ``changed`` names only the fields that actually move.
_BASE_CORE: dict[str, float | int] = {
    "max_concurrent_tasks": 4,
    "task_manager_sleep_time": 0.02,
    "task_queue_manager_sleep_time": 0.02,
    "task_handler_start_timeout": 6.0,
    "task_handler_stop_timeout": 6.0,
    "task_timeout": 4.0,
    "task_queue_fetch_timeout": 2.0,
    "task_cancellation_timeout": 6.0,
}


class DemoSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """A three-field service section: two fields L1 will override, one L0-only."""

    batch_size: int = 100
    greeting: str = "hello"
    retries: int = 3


async def _noop_client_factory(_):
    """A client factory that returns ``None``; these tests never connect."""
    return None


class LayeredWorker(TransportWorker):
    """A transport worker registering one demo section with L0+L1 layers.

    Never connects: the transport hooks are no-ops so the tests drive the
    config pipeline directly, the same way ``tests/transport_worker`` does.
    """

    def __init__(
        self,
        conf_dir,
        *,
        defaults: DemoSettings = DemoSettings(),
        bootstrap=None,
        **config_kwargs: object,
    ) -> None:
        cfg = TaskProcessorConfig(
            conf_dir=conf_dir,
            remote_config_enabled=True,
            **_BASE_CORE,
            **config_kwargs,
        )
        super().__init__(cfg, client_factory=_noop_client_factory)
        self.applied: list[DemoSettings] = []
        self.register_config_settings(
            DEMO_SECTION,
            DemoSettings,
            apply=self.applied.append,
            defaults=defaults,
            bootstrap=bootstrap,
        )
        self.remote_outcome: ConfigApplyOutcome | None = None

    @property
    def client(self) -> object | None:
        return None

    async def _connect_locked(self) -> bool:
        return True

    async def _disconnect_locked(self) -> None:
        return None

    async def _read_remote_outcome(self) -> ConfigApplyOutcome:
        assert self.remote_outcome is not None
        return self.remote_outcome


def _envelope(
    *,
    revision: int,
    core: dict | None = None,
    services: dict[str, dict] | None = None,
) -> bytes:
    """Encode an unsigned remote (L3) envelope carrying ``core``/``services`` patches."""
    return encode_config_envelope(ConfigSections(core=core, services=services or {}), revision=revision)


def _demo(worker: LayeredWorker) -> DemoSettings:
    """The worker's current effective demo section struct."""
    settings = worker.current_config_settings(DEMO_SECTION)
    assert settings is not None
    return settings


def test_seed_bootstrap_resolves_l0_plus_l1(tmp_path):
    """``seed_config_bootstrap`` merges the L1 bootstrap patch onto the L0
    defaults, leaving L0-only fields at their default value."""
    worker = LayeredWorker(tmp_path, bootstrap=lambda: {"batch_size": 777, "greeting": "L1"})
    assert worker.current_config_settings(DEMO_SECTION) is None

    worker.seed_config_bootstrap()

    assert _demo(worker) == DemoSettings(batch_size=777, greeting="L1", retries=3)


@pytest.mark.asyncio
async def test_l2_then_l3_layer_with_l1_field_surviving(tmp_path):
    """A declarative (L2) patch then a remote (L3) patch layer over L1: a field
    set only at L1 survives while L2 and L3 each contribute their own fields."""
    worker = LayeredWorker(tmp_path, bootstrap=lambda: {"batch_size": 777, "greeting": "L1"})
    worker.seed_config_bootstrap()

    # L2 (declarative config.yml): patch only greeting.
    write_local_config(tmp_path / "config.yml", DeclarativeSections(services={DEMO_SECTION: {"greeting": "L2"}}))
    local = await worker._config_manager.apply_local_file()
    assert local is not None and local.applied is True

    # L3 (remote): patch only retries.
    outcome = await worker._config_manager.apply_envelope(
        _envelope(revision=2, services={DEMO_SECTION: {"retries": 9}}),
        source="remote",
    )
    assert outcome.applied is True

    # batch_size (L1) survives; greeting (L2) and retries (L3) are layered on.
    assert _demo(worker) == DemoSettings(batch_size=777, greeting="L2", retries=9)
    assert worker.applied[-1] == DemoSettings(batch_size=777, greeting="L2", retries=9)


@pytest.mark.asyncio
async def test_three_state_rule_end_to_end(tmp_path):
    """L3 ``null`` clears to the L0 default, a value overrides, absence inherits.

    Each state is asserted from a fresh L1 baseline because the L3 layer is
    replaced wholesale on every apply (§3.5): a later patch that omits a key
    falls back to L1, not to the previous L3 value.
    """

    # null clears: batch_size falls back to the L0 default (100).
    worker = LayeredWorker(tmp_path, bootstrap=lambda: {"batch_size": 777})
    worker.seed_config_bootstrap()
    await worker._config_manager.apply_envelope(
        _envelope(revision=1, services={DEMO_SECTION: {"batch_size": None}}),
        source="remote",
    )
    assert _demo(worker).batch_size == 100

    # value sets.
    worker = LayeredWorker(tmp_path, bootstrap=lambda: {"batch_size": 777})
    worker.seed_config_bootstrap()
    await worker._config_manager.apply_envelope(
        _envelope(revision=1, services={DEMO_SECTION: {"batch_size": 5}}),
        source="remote",
    )
    assert _demo(worker).batch_size == 5

    # absent inherits from L1: a greeting-only patch leaves batch_size at L1's value.
    worker = LayeredWorker(tmp_path, bootstrap=lambda: {"batch_size": 777})
    worker.seed_config_bootstrap()
    await worker._config_manager.apply_envelope(
        _envelope(revision=1, services={DEMO_SECTION: {"greeting": "L3"}}),
        source="remote",
    )
    settings = _demo(worker)
    assert settings.batch_size == 777
    assert settings.greeting == "L3"


@pytest.mark.asyncio
async def test_core_partial_patch_changes_only_named_field(tmp_path):
    """A remote core partial patch changes only the named field; the other
    reloadable fields keep their current values and ``changed`` reports one name."""
    worker = LayeredWorker(tmp_path)

    outcome = await worker._config_manager.apply_envelope(
        _envelope(revision=1, core={"task_timeout": 30.0}),
        source="remote",
    )

    assert outcome.applied is True
    assert outcome.changed == ["task_timeout"]
    snapshot = worker._current_reloadable_settings()
    assert snapshot.task_timeout == 30.0
    assert snapshot.max_concurrent_tasks == 4
    assert snapshot.task_queue_fetch_timeout == 2.0
    assert snapshot.task_cancellation_timeout == 6.0


@pytest.mark.asyncio
async def test_core_clear_resolves_to_default(tmp_path):
    """Clearing a core field (``null``) resets it to the ``DEFAULT_*`` constant,
    not the current operator-set value: the key stays present with ``None``, so
    the terminal resolver treats it as "clear", not "inherit"."""
    worker = LayeredWorker(tmp_path)

    # Set task_timeout to a non-default value first.
    await worker._config_manager.apply_envelope(
        _envelope(revision=1, core={"task_timeout": 30.0}),
        source="remote",
    )
    # Clear it: None -> DEFAULT_TASK_TIMEOUT (3.0), not the previous 30.0 nor
    # the constructor's 4.0.
    outcome = await worker._config_manager.apply_envelope(
        _envelope(revision=2, core={"task_timeout": None}),
        source="remote",
    )

    assert outcome.applied is True
    assert outcome.changed == ["task_timeout"]
    assert worker._current_reloadable_settings().task_timeout == DEFAULT_TASK_TIMEOUT


@pytest.mark.asyncio
async def test_core_clear_resolves_to_auto_tune(tmp_path):
    """Clearing ``max_concurrent_tasks`` (``null``) re-resolves it through the
    auto-tune branch when the worker is built with ``auto_tune=True``."""
    worker = LayeredWorker(tmp_path, auto_tune=True)

    # Set an explicit concurrency, then clear it to re-resolve via auto-tune.
    await worker._config_manager.apply_envelope(
        _envelope(revision=1, core={"max_concurrent_tasks": 8}),
        source="remote",
    )
    assert worker._current_reloadable_settings().max_concurrent_tasks == 8

    outcome = await worker._config_manager.apply_envelope(
        _envelope(revision=2, core={"max_concurrent_tasks": None}),
        source="remote",
    )

    assert outcome.applied is True
    assert worker._current_reloadable_settings().max_concurrent_tasks == max(1, os.cpu_count() or 1)


@pytest.mark.asyncio
async def test_core_allowlist_rejects_restart_required_field(tmp_path):
    """A core patch naming a restart-required field is rejected with
    INVALID_CONFIG and leaves the config unchanged."""
    worker = LayeredWorker(tmp_path)

    outcome = await worker._config_manager.apply_envelope(
        _envelope(revision=1, core={"queue_size": 5}),
        source="remote",
    )

    assert outcome.applied is False
    assert outcome.error_code == INVALID_CONFIG
    # queue_size is restart-required, never reloadable, and stays at its default.
    assert worker.queue_size == DEFAULT_MAX_TASKS_QUEUE_SIZE


@pytest.mark.asyncio
async def test_validate_before_swap_leaves_section_and_core_unchanged(tmp_path):
    """A bad service section (unknown field) aborts the whole apply: neither the
    section nor the core moves, even though the core patch was valid."""
    worker = LayeredWorker(tmp_path, bootstrap=lambda: {"batch_size": 777, "greeting": "L1"})
    worker.seed_config_bootstrap()

    good = await worker._config_manager.apply_envelope(
        _envelope(
            revision=1,
            core={"task_timeout": 12.0},
            services={DEMO_SECTION: {"batch_size": 42}},
        ),
        source="remote",
    )
    assert good.applied is True

    before_section = _demo(worker)
    before_core = worker._current_reloadable_settings()

    bad = await worker._config_manager.apply_envelope(
        _envelope(
            revision=2,
            core={"task_timeout": 99.0},
            services={DEMO_SECTION: {"bogus_field": 1}},
        ),
        source="remote",
    )

    assert bad.applied is False
    assert bad.error_code == INVALID_CONFIG
    assert _demo(worker) == before_section
    assert worker._current_reloadable_settings() == before_core


@pytest.mark.asyncio
async def test_validate_before_swap_rejects_out_of_range_core(tmp_path):
    """An out-of-range core value aborts the apply before any section hook runs:
    the section keeps its bootstrap value and the hook is never invoked."""
    worker = LayeredWorker(tmp_path, bootstrap=lambda: {"batch_size": 777})
    worker.seed_config_bootstrap()

    outcome = await worker._config_manager.apply_envelope(
        _envelope(
            revision=1,
            core={"max_concurrent_tasks": 0},
            services={DEMO_SECTION: {"batch_size": 42}},
        ),
        source="remote",
    )

    assert outcome.applied is False
    assert outcome.error_code == INVALID_CONFIG
    # The core candidate failed validation before the hooks loop, so the section
    # hook never ran and the section stayed at its L1 value.
    assert worker.applied == []
    assert _demo(worker) == DemoSettings(batch_size=777, greeting="hello", retries=3)
    assert worker._current_reloadable_settings().max_concurrent_tasks == 4


@pytest.mark.asyncio
async def test_auto_persist_writes_merged_snapshot(tmp_path):
    """A successful remote apply is auto-persisted as the merged patch — the
    union of every layer's explicitly-set keys — not the remote layer alone."""
    worker = LayeredWorker(tmp_path, bootstrap=lambda: {"batch_size": 777, "greeting": "L1"})
    worker.seed_config_bootstrap()

    outcome = await worker._config_manager.apply_envelope(
        _envelope(
            revision=1,
            core={"task_timeout": 30.0},
            services={DEMO_SECTION: {"greeting": "howdy"}},
        ),
        source="remote",
    )
    worker.remote_outcome = outcome

    await worker._reload_remote_config()

    snapshot = read_local_config(tmp_path / "config.yml")
    assert snapshot is not None
    # The service section carries both the L1 field and the L3 override.
    assert snapshot.services[DEMO_SECTION] == {"batch_size": 777, "greeting": "howdy"}
    # The core block is the merged core patch.
    assert snapshot.core == {"task_timeout": 30.0}
