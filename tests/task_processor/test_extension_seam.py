"""Pin the transport as the sole delivery extension seam (AR-112)."""

import logging

import pytest

from scietex.service.config import TaskProcessorConfig
from scietex.service.task_processor import TaskProcessor
from scietex.service.transport import InMemoryTransport


class _SpyTransport(InMemoryTransport):
    """Records which transport hooks the processor reaches through."""

    def __init__(self, *, logger: logging.Logger) -> None:
        super().__init__(logger=logger)
        self.fetched = 0
        self.started = 0
        self.acked = 0

    async def fetch(self, sink) -> bool:
        self.fetched += 1
        return await super().fetch(sink)

    async def on_started(self, task_data) -> None:
        self.started += 1
        await super().on_started(task_data)

    async def ack(self, task_data, task_result, *, cancel_reason=None) -> None:
        self.acked += 1
        await super().ack(task_data, task_result, cancel_reason=cancel_reason)


@pytest.mark.asyncio
async def test_transport_is_the_live_seam():
    """The six hooks are thin delegators: delivery reaches the transport."""
    transport = _SpyTransport(logger=logging.getLogger("test_extension_seam"))
    processor = TaskProcessor(TaskProcessorConfig(service_name="svc"), transport=transport)

    await processor.fetch_tasks()
    assert transport.fetched == 1


@pytest.mark.asyncio
async def test_subclass_hook_override_bypasses_transport():
    """A subclass override of a hook takes effect and bypasses the transport.

    This pins the documented precedence: overriding a compat hook still works
    (back-compat) but the injected transport is not reached for that operation.
    """

    class _OverridingProcessor(TaskProcessor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.overridden_fetches = 0

        async def fetch_tasks(self) -> bool:
            self.overridden_fetches += 1
            return False

    transport = _SpyTransport(logger=logging.getLogger("test_extension_seam"))
    processor = _OverridingProcessor(TaskProcessorConfig(service_name="svc"), transport=transport)

    assert await processor.fetch_tasks() is False
    assert processor.overridden_fetches == 1
    assert transport.fetched == 0
