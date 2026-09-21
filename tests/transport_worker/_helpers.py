"""Shared fakes and factory helpers for the isolated TransportWorker suite (AR-102a)."""

import asyncio
from pathlib import Path

from scietex.service.config import TaskProcessorConfig
from scietex.service.config_reload import ConfigApplyOutcome
from scietex.service.transport_worker import TransportWorker


class FakeClient:
    """A minimal stand-in for a broker client (no broker-specific surface)."""


class RecordingTransport:
    """Records the transport hooks the TransportWorker lifecycle drives.

    Only ``refresh_leases`` is observed by this suite; the remaining
    ``TaskTransport`` hooks are no-ops so the instance satisfies the transport
    protocol without importing any broker package.
    """

    def __init__(self, order: list[str]):
        self._order = order

    async def refresh_leases(self) -> None:
        self._order.append("refresh_leases")

    async def fetch(self, sink) -> bool:
        return False

    async def requeue(self, task_data) -> None:
        return None

    async def on_started(self, task_data) -> None:
        return None

    async def ack(self, task_data, task_result, *, cancel_reason=None) -> None:
        return None

    async def on_progress(self, task_id, value) -> None:
        return None

    async def recover_pending_tasks(self, sink):
        return True, False

    async def on_drain(self, task_data) -> None:
        return None


async def _default_factory(_):
    return FakeClient()


class RecordingWorker(TransportWorker):
    """Minimal concrete TransportWorker with a recording transport and fake client."""

    def __init__(self, config: TaskProcessorConfig | None = None) -> None:
        super().__init__(config, client_factory=_default_factory)
        # Shared order list the transport and the monkeypatched watchdog hooks
        # append to, so a test can assert the exact call sequence.
        self.order: list[str] = []
        self._transport = RecordingTransport(self.order)
        self._client: FakeClient | None = None
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.created_clients: list[FakeClient] = []
        # Optional per-call barriers so tests can hold connect/disconnect inside
        # their locked section and observe serialization deterministically.
        self.connect_barrier: asyncio.Event | None = None
        self.disconnect_barrier: asyncio.Event | None = None
        # Remote-outcome hook used by the config-pipeline tests.
        self.remote_outcome: ConfigApplyOutcome | None = None
        self.read_calls = 0

    @property
    def client(self) -> FakeClient | None:
        return self._client

    async def _connect_locked(self) -> bool:
        self.connect_calls += 1
        if self.connect_barrier is not None:
            await self.connect_barrier.wait()
        if self._client is None:
            self._client = FakeClient()
            self.created_clients.append(self._client)
        return True

    async def _disconnect_locked(self) -> None:
        self.disconnect_calls += 1
        if self.disconnect_barrier is not None:
            await self.disconnect_barrier.wait()
        self._client = None

    async def _read_remote_outcome(self) -> ConfigApplyOutcome:
        self.read_calls += 1
        if self.remote_outcome is None:
            raise NotImplementedError
        return self.remote_outcome


def build_worker(conf_dir: Path, *, config: TaskProcessorConfig | None = None) -> RecordingWorker:
    """Build a RecordingWorker rooted at ``conf_dir`` (a temp directory)."""
    cfg = config if config is not None else TaskProcessorConfig(conf_dir=conf_dir)
    return RecordingWorker(cfg)
