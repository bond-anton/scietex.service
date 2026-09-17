"""Valkey task-stream purge tests: ``purge_task_stream`` and ``_stream_entry_ids`` (AR-094)."""

import logging

import pytest

import scietex.service.valkey.purge as mod
from scietex.service.valkey.purge import purge_task_stream

STREAM = "scietex:test:tasks"
GROUP = "scietex:test:task_group"
CONSUMER = "test-consumer"


class PurgeClient:
    """Fake Valkey client for the purge path.

    ``_helpers.DummyClient`` has no ``xread`` method and returns a single
    static ``xreadgroup`` value (which would make the purge loop spin forever),
    so these tests use their own fake. Each read method pops the next result
    from a caller-supplied sequence and returns ``None`` once exhausted, which
    ``_stream_entry_ids`` maps to ``[]`` to terminate the loop.
    """

    def __init__(self, xreadgroup_results=(), xread_results=(), xack_errors=()):
        self._xreadgroup_results = list(xreadgroup_results)
        self._xread_results = list(xread_results)
        # Per-call xack outcomes: ``None`` succeeds; an exception is raised.
        self._xack_errors = list(xack_errors)
        self.acked = []
        self.deleted = []

    async def xreadgroup(self, streams, group, consumer):
        if self._xreadgroup_results:
            return self._xreadgroup_results.pop(0)
        return None

    async def xread(self, streams):
        if self._xread_results:
            return self._xread_results.pop(0)
        return None

    async def xack(self, stream, group, ids):
        if self._xack_errors:
            error = self._xack_errors.pop(0)
            if error is not None:
                raise error
        self.acked.append((stream, group, ids))

    async def xdel(self, stream, ids):
        self.deleted.append((stream, ids))


def _stream_map(*entry_ids: bytes) -> dict:
    """Build an xreadgroup/xread result keyed by the encoded stream name."""
    return {STREAM.encode("utf-8"): {entry_id: {} for entry_id in entry_ids}}


@pytest.mark.asyncio
async def test_purge_success_returns_count():
    """purge_task_stream deletes entries across both group phases and the
    stream phase, reporting the total count with no errors."""
    client = PurgeClient(
        xreadgroup_results=[
            _stream_map(b"1-0", b"2-0"),  # "0-0" group phase
            None,  # "0-0" phase terminates
            _stream_map(b"3-0"),  # ">" group phase
        ],
        xread_results=[_stream_map(b"4-0", b"5-0")],
    )

    result = await purge_task_stream(client, STREAM, GROUP, CONSUMER)

    assert result.entries_purged == 5
    assert result.errors == ()
    assert [ids for _, ids in client.deleted] == [
        [b"1-0", b"2-0"],
        [b"3-0"],
        [b"4-0", b"5-0"],
    ]


@pytest.mark.asyncio
async def test_purge_empty_stream_returns_zero():
    """An empty stream purges zero entries and reports no errors."""
    client = PurgeClient()

    result = await purge_task_stream(client, STREAM, GROUP, CONSUMER)

    assert result.entries_purged == 0
    assert result.errors == ()


@pytest.mark.asyncio
async def test_purge_reports_partial_failure():
    """A mid-purge failure is reported as an error (never raised) while the
    count reflects entries purged before the failure."""
    client = PurgeClient(
        xreadgroup_results=[
            _stream_map(b"1-0", b"2-0"),
            None,
            _stream_map(b"3-0"),
        ],
        xack_errors=[None, RuntimeError("boom")],
    )

    result = await purge_task_stream(client, STREAM, GROUP, CONSUMER)

    assert result.errors == ("boom",)
    assert result.entries_purged == 2


@pytest.mark.asyncio
async def test_purge_logs_info_on_success(caplog):
    """A successful purge emits an INFO record mentioning the purged count."""
    client = PurgeClient(xreadgroup_results=[_stream_map(b"1-0")])
    caplog.set_level(logging.INFO, logger=mod.__name__)

    await purge_task_stream(client, STREAM, GROUP, CONSUMER)

    assert any(
        record.levelno == logging.INFO and "Purged 1 task entries" in record.getMessage() for record in caplog.records
    )


@pytest.mark.asyncio
async def test_purge_logs_error_on_failure(caplog):
    """A failed purge emits an ERROR record containing the error text."""
    client = PurgeClient(
        xreadgroup_results=[_stream_map(b"1-0")],
        xack_errors=[RuntimeError("boom")],
    )
    caplog.set_level(logging.ERROR, logger=mod.__name__)

    await purge_task_stream(client, STREAM, GROUP, CONSUMER)

    assert any(record.levelno == logging.ERROR and "boom" in record.getMessage() for record in caplog.records)


def test_stream_entry_ids_extracts():
    """_stream_entry_ids returns [] for a falsy result and the entry-id keys
    for a populated mapping keyed by the encoded stream name."""
    assert mod._stream_entry_ids(None, STREAM) == []
    assert mod._stream_entry_ids({}, STREAM) == []
    assert mod._stream_entry_ids(_stream_map(b"1-0", b"2-0"), STREAM) == [b"1-0", b"2-0"]
