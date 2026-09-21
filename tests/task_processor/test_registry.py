"""TaskProcessor handler registry tests: registration, naming, and kwargs injection."""

from typing import cast
from uuid import uuid4

import pytest

from scietex.service.task_handler.schemas import TaskData

from ._helpers import (
    DemoProcessor,
    DummyHandler,
    NameDerivedHandler,
    SharedStateHandler,
    ThresholdHandler,
)


@pytest.mark.asyncio
async def test_add_task_handler_registers_by_class_name():
    """add_task_handler takes only the handler class; the lifecycle key is the
    class name (AR-022 v4)."""
    proc = DemoProcessor()
    proc.add_task_handler(DummyHandler)
    await proc._start_task_handler("DummyHandler")
    assert "DummyHandler" in proc.task_handlers


@pytest.mark.asyncio
async def test_add_task_handler_duplicate_class_raises():
    """Registering the same handler class twice must raise (AR-022 v4)."""
    proc = DemoProcessor()
    proc.add_task_handler(DummyHandler)
    with pytest.raises(ValueError):
        proc.add_task_handler(DummyHandler)


@pytest.mark.asyncio
async def test_add_task_handler_named_instances_coexist_and_dispatch():
    """add_task_handler's optional name key lets one class register as several
    distinct instances; dispatch routes by the name-derived supported_tasks
    (AR-053)."""
    proc = DemoProcessor()
    proc.add_task_handler(NameDerivedHandler, name="alpha")
    proc.add_task_handler(NameDerivedHandler, name="beta")

    await proc._start_task_handler("alpha")
    await proc._start_task_handler("beta")
    assert "alpha" in proc.task_handlers
    assert "beta" in proc.task_handlers

    alpha_result = await proc.process_task(TaskData(task_id=str(uuid4()), task="alpha_task", payload=b"alpha"))
    assert alpha_result.status == "success"
    beta_result = await proc.process_task(TaskData(task_id=str(uuid4()), task="beta_task", payload=b"beta"))
    assert beta_result.status == "success"
    assert beta_result.payload == b"beta"

    # no handler claims the other instance's task types
    missing = await proc.process_task(TaskData(task_id=str(uuid4()), task="other_task", payload=b"{}"))
    assert missing.status == "error"
    assert "No handler" in missing.error


@pytest.mark.asyncio
async def test_add_task_handler_named_duplicate_resolved_key_raises():
    """Registering the same resolved name twice — explicit or via the class-name
    default — must still raise ValueError (AR-053 backward compat)."""
    proc = DemoProcessor()
    proc.add_task_handler(NameDerivedHandler, name="alpha")
    with pytest.raises(ValueError):
        proc.add_task_handler(NameDerivedHandler, name="alpha")
    # the class-name default is another resolved key, so it does not collide
    proc.add_task_handler(NameDerivedHandler)
    with pytest.raises(ValueError):
        proc.add_task_handler(NameDerivedHandler)


@pytest.mark.asyncio
async def test_add_task_handler_passes_handler_kwargs_to_constructor():
    """handler_kwargs are forwarded to the handler constructor on every
    instantiation (Option A stateful handlers)."""
    proc = DemoProcessor()
    proc.add_task_handler(ThresholdHandler, threshold=42)
    await proc._start_task_handler("ThresholdHandler")

    handler = cast(ThresholdHandler, proc.task_handlers["ThresholdHandler"])
    assert handler.threshold == 42


@pytest.mark.asyncio
async def test_add_task_handler_stateful_handler_shared_object():
    """A shared mutable object injected via handler_kwargs is held by reference:
    the handler instance holds the SAME object passed in, so mutations through
    the handler are visible to the caller (state shared across its lifetime)."""
    shared: dict = {}
    proc = DemoProcessor()
    proc.add_task_handler(SharedStateHandler, shared=shared)
    await proc._start_task_handler("SharedStateHandler")

    handler = cast(SharedStateHandler, proc.task_handlers["SharedStateHandler"])
    assert handler.shared is shared

    await proc.process_task(TaskData(task_id=str(uuid4()), task="shared", payload=b"{}"))
    assert shared["count"] == 1


@pytest.mark.asyncio
async def test_add_task_handler_unknown_kwarg_raises_type_error():
    """A misspelled handler kwarg raises a loud TypeError at construction, so a
    typo fails fast instead of being silently dropped. The constructor call in
    _start_task_handler is not wrapped by its try/except, so the TypeError
    propagates."""
    proc = DemoProcessor()
    proc.add_task_handler(DummyHandler, threshold=42)
    with pytest.raises(TypeError):
        await proc._start_task_handler("DummyHandler")
