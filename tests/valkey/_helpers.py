"""Valkey async task processor testing.

Connect-path tests exercise ``connect()``/``disconnect()`` through the
``client_factory=`` injection seam (AR-074), supplying a fake client without a
live Valkey server. Method-unit tests still seed transport/ack state
(``_task_entry_ids``) by assigning ``worker._client`` directly; recovery and
lease-refresh state lives on ``worker._transport`` (Phase 3 transport
extraction, AR-072).
"""

import asyncio
import logging

from scietex.service import ValkeyWorker
from scietex.service.valkey._glide import ConditionalChange, RequestError
from scietex.service.valkey.config import ValkeyConfig, ValkeyWorkerConfig
from scietex.service.valkey.transport import TASK_FIELD


def _entry_id_gt(a: bytes, b: bytes) -> bool:
    """Compare two ``N-0`` entry ids numerically, ignoring the ``-0`` suffix."""
    return int(a.split(b"-")[0]) > int(b.split(b"-")[0])


class _SharedStreams:
    """In-process Valkey backend shared by several ``DummyClient`` instances.

    Models only the server features the control plane depends on, so two workers
    (each with its own ``DummyClient``) read from the *same* store and the
    stream/key name becomes the only routing decision — exactly as on a real
    broker:

    - Streams: each name maps to an ordered list of ``(entry_id, payload)``
      pairs. ``xadd`` appends with a per-stream monotonically increasing id
      (``b"{n}-0"``); ``xread`` returns the entries after the given cursor. A
      directed command therefore lands in one worker's stream and is invisible
      to the other, while a broadcast lands in a stream both workers read.
    - Keys: a ``str -> bytes`` map backing ``get``/``put``, so ``resolve_owner``
      reads the same tracking record a worker (or the test) wrote.

    ``xinfo_stream`` reports the last entry id, so the transport's first read
    seeds its cursor from the real tail rather than the literal ``$``. Tests
    that publish before the first read therefore seed the cursor first (an
    empty ``fetch``), matching the design's skip-before-startup rule (§4.2).
    """

    def __init__(self) -> None:
        self._streams: dict[str, list[tuple[bytes, bytes]]] = {}
        self._keys: dict[str, bytes] = {}

    def xadd(self, stream_name: str, pairs: list[tuple[bytes, bytes]]) -> bytes:
        """Append one entry to ``stream_name`` and return its fresh entry id."""
        entries = self._streams.setdefault(stream_name, [])
        entry_id = f"{len(entries) + 1}-0".encode()
        for _field, payload in pairs:
            entries.append((entry_id, payload))
        return entry_id

    def xread(self, streams: dict[str, str | bytes]) -> dict | None:
        """Return the entries after each stream's cursor, or ``None`` if none.

        Emits the same ``{stream: {entry_id: [[TASK_FIELD, payload]]}}`` shape
        ``ValkeyTransport._read_control_stream`` consumes. ``0-0`` reads from
        the stream start (the transport's seed for a stream that did not exist
        at startup); a bytes cursor is a previously-returned entry id.
        """
        result: dict[str, dict[bytes, list[list[bytes]]]] = {}
        for stream_name, cursor in streams.items():
            stored = self._streams.get(stream_name, [])
            if cursor == "0-0":
                visible = stored
            elif isinstance(cursor, bytes):
                visible = [e for e in stored if _entry_id_gt(e[0], cursor)]
            else:
                visible = []
            if visible:
                result[stream_name] = {entry_id: [[TASK_FIELD, payload]] for entry_id, payload in visible}
        return result or None

    def get(self, key: str) -> bytes | None:
        """Return the stored value for ``key``, or ``None`` when absent."""
        return self._keys.get(key)

    def put(self, key: str, value: bytes) -> None:
        """Seed a key so ``get`` (and therefore ``resolve_owner``) sees it."""
        self._keys[key] = value

    def xinfo_stream(self, stream_name: str) -> dict[bytes, bytes]:
        """Return the stream's ``last-generated-id``, or raise for a missing stream.

        Mirrors the real server: ``XINFO STREAM`` on an absent key raises
        ``RequestError``, which the transport maps to a ``0-0`` cursor seed.
        """
        stored = self._streams.get(stream_name)
        if not stored:
            raise RequestError("no such key")
        return {b"last-generated-id": stored[-1][0]}


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
        xread_result=None,
        xread_results=None,
        xread_error=None,
        xinfo_stream_result=None,
        expire_error=None,
        streams=None,
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
        self.xread_result = xread_result
        # Optional per-call queue for xread results (the directed and broadcast
        # control reads each issue one xread per fetch). When set it is consumed
        # first; xread_result stays the fallback for single-result tests.
        self.xread_results = list(xread_results) if xread_results is not None else None
        self.xread_error = xread_error
        # Canned XINFO STREAM reply for the cursor-seed read. Tests that do not
        # set it get ``None``, which the transport maps to a ``0-0`` seed.
        self.xinfo_stream_result = xinfo_stream_result
        self.expire_error = expire_error
        # Shared-mode backend (approach (a)): when set, stream reads/appends and
        # key gets are delegated to it so several clients see one shared store.
        # ``None`` keeps the per-call canned-result behaviour for existing tests.
        self._streams = streams
        self.expired: list = []
        self.acked: list = []
        self.deleted: list = []
        self.added: list = []
        self.xautoclaim_calls: list = []
        self.xreadgroup_calls: list = []
        self.xread_calls: list = []
        self.xinfo_stream_calls: list = []
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
        if self._streams is not None:
            return self._streams.get(key)
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
        self.added.append(args)
        if self._streams is not None:
            return self._streams.xadd(args[0], args[1])

    async def xack(self, *args, **kwargs):
        self.acked.append(args)

    async def xdel(self, *args, **kwargs):
        self.deleted.append(args)

    async def xreadgroup(self, *args, **kwargs):
        if self.xreadgroup_error is not None:
            raise self.xreadgroup_error
        self.xreadgroup_calls.append(args)
        return self.xreadgroup_result

    async def xread(self, *args, **kwargs):
        if self.xread_error is not None:
            raise self.xread_error
        self.xread_calls.append(args)
        if self._streams is not None:
            return self._streams.xread(args[0])
        if self.xread_results is not None:
            return self.xread_results.pop(0) if self.xread_results else None
        return self.xread_result

    async def xinfo_stream(self, *args, **kwargs):
        self.xinfo_stream_calls.append(args)
        if self._streams is not None:
            return self._streams.xinfo_stream(args[0])
        return self.xinfo_stream_result

    async def xautoclaim(self, *args, **kwargs):
        self.xautoclaim_calls.append(args)
        return self.xautoclaim_result

    async def expire(self, *args, **kwargs):
        if self.expire_error is not None:
            raise self.expire_error
        self.expired.append(args)

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


def _entry(entry_id: bytes, payload: bytes):
    """Build an xreadgroup/xautoclaim result mapping for one stream entry."""
    return {b"stream": {entry_id: [[TASK_FIELD, payload]]}}


def _make_tracking_worker(client, *, ttl=3600, service="svc", task_lease_ttl=None):
    """Build a ValkeyWorker with an injected client and a known tracking TTL."""
    worker = ValkeyWorker(
        ValkeyWorkerConfig(
            service_name=service,
            task_tracking_ttl=ttl,
            task_lease_ttl=task_lease_ttl,
            valkey_config=ValkeyConfig(),
        )
    )
    worker._client = client
    return worker
