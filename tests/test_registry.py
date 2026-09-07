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


class DummyClient:
    def __init__(self):
        self.sadd_calls: list = []
        self.srem_calls: list = []

    async def sadd(self, *args, **kwargs):
        self.sadd_calls.append(args)

    async def srem(self, *args, **kwargs):
        self.srem_calls.append(args)


@pytest.mark.asyncio
async def test_valkey_register_unregister_issue_sadd_srem(monkeypatch):
    import scietex.service.valkey.valkey_async_worker as mod

    async def create_mock(cfg):
        return DummyClient()

    monkeypatch.setattr(mod, "GlideClient", type("C", (), {"create": staticmethod(create_mock)}))
    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)

    from scietex.service import ValkeyWorker
    from scietex.service.valkey.valkey_config import ValkeyConfig

    worker = ValkeyWorker(service_name="svc", valkey_config=ValkeyConfig())
    client = DummyClient()
    worker._client = client

    await worker._register_instance()
    assert client.sadd_calls == [("scietex:svc:workers", [worker.instance_id])]

    await worker._unregister_instance()
    assert client.srem_calls == [("scietex:svc:workers", [worker.instance_id])]
