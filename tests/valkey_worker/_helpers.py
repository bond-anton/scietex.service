"""Valkey async task processor testing.

Connect-path tests exercise ``connect()``/``disconnect()`` through the
``client_factory=`` injection seam (AR-003), supplying a fake client without a
live Valkey server. Method-unit tests still seed transport/ack state
(``_task_entry_ids``) by assigning ``worker._client`` directly; recovery and
lease-refresh state lives on ``worker._transport`` (Phase 3 transport
extraction, AR-001).
"""

import asyncio
import logging

from scietex.service import ValkeyWorker
from scietex.service.valkey._glide import ConditionalChange
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig


class DummyClient:
    """Mocking Valkey client."""

    def __init__(
        self,
        ping_ok=True,
        xreadgroup_result=None,
        xreadgroup_error=None,
        xautoclaim_result=None,
        xgroup_create_error=None,
        get_value=None,
        set_error=None,
        get_values=None,
        get_error=None,
        delete_error=None,
        sadd_error=None,
        srem_error=None,
    ):
        self._ping_ok = ping_ok
        self.closed = False
        self.xreadgroup_result = xreadgroup_result
        self.xreadgroup_error = xreadgroup_error
        self.xautoclaim_result = xautoclaim_result
        self.xgroup_create_error = xgroup_create_error
        self.get_value = get_value
        self.set_error = set_error
        self.get_values = get_values
        self.get_error = get_error
        self.delete_error = delete_error
        self.sadd_error = sadd_error
        self.srem_error = srem_error
        self.acked: list = []
        self.deleted: list = []
        self.xautoclaim_calls: list = []
        self.xreadgroup_calls: list = []
        self.sets: list = []
        self.set_calls: list = []  # (key, conditional_set) per set() call
        self.gets: list = []
        self.deleted_keys: list = []
        # Keys this client has written, so SET ... NX can detect an existing
        # key. Seeded from get_values via _key_exists (a peer's lease).
        self._keys: set = set()

    def _key_exists(self, key) -> bool:
        """A key "exists" for NX when this client wrote it or get_values seeded it."""
        return key in self._keys or (self.get_values is not None and key in self.get_values)

    async def set(self, key, value=None, expiry=None, *args, **kwargs):
        if self.set_error is not None:
            raise self.set_error
        conditional_set = kwargs.get("conditional_set")
        self.set_calls.append((key, conditional_set))
        # SET NX returns None when the key already exists; otherwise it writes
        # the key and returns a truthy value. Plain sets always succeed.
        if conditional_set is ConditionalChange.ONLY_IF_DOES_NOT_EXIST and self._key_exists(key):
            return None
        self._keys.add(key)
        self.sets.append((key, value, expiry))
        return b"OK"

    async def get(self, key, *args, **kwargs):
        self.gets.append(key)
        if self.get_error is not None:
            raise self.get_error
        if self.get_values is not None:
            return self.get_values.get(key, self.get_value)
        return self.get_value

    async def delete(self, keys, *args, **kwargs):
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted_keys.append(keys)
        for key in keys:
            self._keys.discard(key)
        return len(keys)

    async def sadd(self, *args, **kwargs):
        if self.sadd_error is not None:
            raise self.sadd_error

    async def srem(self, *args, **kwargs):
        if self.srem_error is not None:
            raise self.srem_error

    async def xgroup_create(self, *args, **kwargs):
        if self.xgroup_create_error is not None:
            raise self.xgroup_create_error

    async def xadd(self, *args, **kwargs):
        pass

    async def xack(self, *args, **kwargs):
        self.acked.append(args)

    async def xdel(self, *args, **kwargs):
        self.deleted.append(args)

    async def xreadgroup(self, *args, **kwargs):
        if self.xreadgroup_error is not None:
            raise self.xreadgroup_error
        self.xreadgroup_calls.append(args)
        return self.xreadgroup_result

    async def xautoclaim(self, *args, **kwargs):
        self.xautoclaim_calls.append(args)
        return self.xautoclaim_result

    async def ping(self):
        return self._ping_ok

    async def close(self):
        self.closed = True


class FakeHandler(logging.Handler):
    """Stand-in for ``AsyncValkeyHandler`` in connection tests.

    The real handler's ``start_logging`` spawns a worker that connects to a
    live Valkey server; this fake records its construction arguments and
    start/stop calls without opening a connection, so tests stay deterministic.
    """

    def __init__(self, stream_name, *, valkey_config=None, client=None, **kwargs):
        super().__init__()
        self.stream_name = stream_name
        self.valkey_config = valkey_config
        self.client = client
        self._owns_client = client is None
        self.logging_running_event = asyncio.Event()
        self.start_calls = 0
        self.stop_calls = 0

    async def start_logging(self):
        self.start_calls += 1
        self.logging_running_event.set()

    async def stop_logging(self):
        self.stop_calls += 1
        self.logging_running_event.clear()

    def emit(self, record):
        # Drop records: the fake is only here to record construction/start/stop,
        # not to deliver logs.
        pass


def _patch_glide(monkeypatch):
    """Map the glide connection errors to plain ``Exception`` so a raising
    ``client_factory`` is caught by connect()'s failure path.

    Client creation itself goes through the ``client_factory=`` seam, so the
    ``GlideClient.create`` monkeypatch is no longer needed here.
    """
    import scietex.service.valkey.worker as mod

    monkeypatch.setattr(mod, "GlideConnectionError", Exception)
    monkeypatch.setattr(mod, "GlideTimeoutError", Exception)
    return mod


def _patch_glide_and_handler(monkeypatch):
    """Like :func:`_patch_glide`, but also swap the real logging handler for a
    :class:`FakeHandler` so connect() never opens a second connection."""
    mod = _patch_glide(monkeypatch)
    monkeypatch.setattr(mod, "AsyncValkeyHandler", FakeHandler)
    return mod


def _entry(entry_id: bytes, task_id: str, payload: bytes):
    """Build an xreadgroup/xautoclaim result mapping for one stream entry."""
    return {b"stream": {entry_id: [[task_id.encode("utf-8"), payload]]}}


def _make_tracking_worker(client, *, ttl=3600, service="svc"):
    """Build a ValkeyWorker with an injected client and a known tracking TTL."""
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name=service, task_tracking_ttl=ttl, valkey_config=ValkeyConfig()))
    worker._client = client
    return worker
