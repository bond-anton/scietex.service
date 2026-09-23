"""Polls a Valkey server for broker-level metrics.

Independent of the workers: it owns its own client and connects on app start,
so the BROKERS panel reports the server even when no worker is running.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

import msgspec

from examples.textual.broker_parsing import parse_info
from examples.textual.broker_snapshot import ValkeyBrokerSnapshot
from scietex.service.config import prepare_conf_dir
from scietex.service.valkey._glide import GlideClient, GlideClientConfiguration, InfoSection
from scietex.service.valkey.config import ValkeyConfig, generate_glide_config, read_valkey_config

#: INFO sections requested per poll. A targeted list keeps the reply small;
#: ``ALL`` would pull commandstats and latencystats we never read.
INFO_SECTIONS = [
    InfoSection.SERVER,
    InfoSection.CLIENTS,
    InfoSection.MEMORY,
    InfoSection.STATS,
    InfoSection.CPU,
    InfoSection.KEYSPACE,
]

VALKEY_POLL_INTERVAL = 2.0
VALKEY_STOP_TIMEOUT = 5.0

ClientFactory = Callable[[GlideClientConfiguration], Awaitable[GlideClient]]


def _to_int(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def _to_float(value: str | None) -> float | None:
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def map_info(fields: dict[str, str], keyspace: dict[str, dict[str, int]]) -> ValkeyBrokerSnapshot:
    """Map parsed INFO fields onto a snapshot.

    ``valkey_version`` is preferred over ``redis_version`` because Valkey
    reports both and the former is the accurate one.
    """
    keys_total = sum(counters.get("keys", 0) for counters in keyspace.values())
    return ValkeyBrokerSnapshot(
        version=fields.get("valkey_version") or fields.get("redis_version"),
        uptime_s=_to_int(fields.get("uptime_in_seconds")),
        used_memory=_to_int(fields.get("used_memory")),
        used_memory_peak=_to_int(fields.get("used_memory_peak")),
        connected_clients=_to_int(fields.get("connected_clients")),
        ops_per_sec=_to_float(fields.get("instantaneous_ops_per_sec")),
        used_cpu_sys=_to_float(fields.get("used_cpu_sys")),
        used_cpu_user=_to_float(fields.get("used_cpu_user")),
        keys_total=keys_total if keyspace else None,
    )


class ValkeyBrokerMonitor:
    """Polls ``INFO`` and the project's stream lengths into a snapshot."""

    def __init__(
        self,
        config: ValkeyConfig | None = None,
        *,
        conf_dir: Path | None = None,
        service_name: str = "service",
        client_factory: ClientFactory | None = None,
        poll_interval: float = VALKEY_POLL_INTERVAL,
    ) -> None:
        self._config = config
        self._conf_dir = conf_dir
        self._service_name = service_name
        self._client_factory = client_factory
        self._poll_interval = poll_interval
        self._client: GlideClient | None = None
        self._snapshot = ValkeyBrokerSnapshot()
        self._task: asyncio.Task[None] | None = None

    def snapshot(self) -> ValkeyBrokerSnapshot:
        return self._snapshot

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=VALKEY_STOP_TIMEOUT)
        except (asyncio.CancelledError, TimeoutError):
            # Cancellation is the expected path; a timeout means the client is
            # stuck on an unreachable broker and must not block shutdown.
            pass
        await self._drop_client()

    async def refresh(self) -> None:
        """Run one poll cycle. Public so tests can drive it deterministically."""
        try:
            client = await self._ensure_client()
            await self._refresh(client)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._drop_client()
            self._snapshot = ValkeyBrokerSnapshot(connected=False, error=str(exc))

    async def _run(self) -> None:
        while True:
            await self.refresh()
            await asyncio.sleep(self._poll_interval)

    async def _ensure_client(self) -> GlideClient:
        if self._client is not None:
            return self._client
        config = self._config
        if config is None:
            try:
                config = read_valkey_config(prepare_conf_dir(self._conf_dir), create_default=False)
            except RuntimeError:
                # No config file on disk: fall back to the same localhost
                # defaults a worker would use, without writing a file.
                config = ValkeyConfig()
        client_config = generate_glide_config(config, service_name=self._service_name)
        factory = self._client_factory or GlideClient.create
        self._client = await factory(client_config)
        return self._client

    async def _refresh(self, client: GlideClient) -> None:
        raw = await client.info(INFO_SECTIONS)
        fields, keyspace = parse_info(raw)
        snapshot = map_info(fields, keyspace)
        prefix = f"scietex:{self._service_name}"
        self._snapshot = msgspec.structs.replace(
            snapshot,
            connected=True,
            received_at=time.monotonic(),
            task_stream_len=await client.xlen(f"{prefix}:tasks"),
            log_stream_len=await client.xlen(f"{prefix}:log"),
            control_stream_len=await client.xlen(f"{prefix}:control"),
        )

    async def _drop_client(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        try:
            await client.close()
        except Exception:
            # The connection is already unusable; nothing to recover here.
            pass
