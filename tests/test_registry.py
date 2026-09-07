"""Registry lifecycle contract on the base worker (AR-023)."""

import pytest

from scietex.service import BasicAsyncWorker


@pytest.mark.asyncio
async def test_instance_id_is_auto_generated_unique_string():
    a = BasicAsyncWorker(service_name="svc")
    b = BasicAsyncWorker(service_name="svc")
    assert isinstance(a.instance_id, str)
    assert len(a.instance_id) == 32  # uuid4().hex
    assert a.instance_id != b.instance_id


def test_worker_id_property_removed():
    worker = BasicAsyncWorker(service_name="svc")
    assert not hasattr(worker, "worker_id")


@pytest.mark.asyncio
async def test_base_registry_hooks_are_noop():
    # Base worker must run register/unregister without error and without
    # touching any transport (no client exists on the base worker).
    worker = BasicAsyncWorker(service_name="svc")
    await worker._register_instance()
    await worker._unregister_instance()
