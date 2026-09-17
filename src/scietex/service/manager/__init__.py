"""Manager decorator and lifecycle utilities for ``scietex.service``.

Provides the ``@Manager`` class decorator that wraps async methods
into managed loops with automatic restart on error, and
``ManagerStatus`` for tracking manager lifecycle states.
"""

from collections.abc import Callable, Coroutine
from enum import Enum
from types import MethodType
from typing import Any

MANAGER_REGISTRY_ATTR = "__manager_registry__"


class ManagerStatus(Enum):
    """Lifecycle status of a ``Manager``-decorated loop."""

    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"
    STOPPED = "Stopped"
    FAILED = "Failed"


class Manager:
    """Class-based decorator that wraps an async method into a managed loop.

    When applied to an async method, the method is registered as a manager
    body that ``ManagerRuntime`` runs repeatedly until cancelled. The parent
    ``BasicWorker`` iterates over all ``Manager`` instances recorded on the
    class and its MRO (via the ``MANAGER_REGISTRY_ATTR`` registry) and
    executes them as ``asyncio.Task`` objects with automatic restart on error.

    Args:
        name: Human-readable name for the manager. Required; must be a
            non-empty string.
        cleanup: Optional async callable that runs when the manager
            stops. Receives the worker instance as its argument.
    """

    def __init__(
        self,
        name: str,
        cleanup: Callable[[Any], Coroutine[None, None, None]] | None = None,
    ) -> None:
        """Initialize the Manager decorator.

        Args:
            name: Human-readable name for the manager. Required; must be a
                non-empty string.
            cleanup: Optional async callable that runs when the manager
                stops. Receives the worker instance as its argument.

        Raises:
            TypeError: If ``name`` is not a non-empty string. The bare
                ``@Manager`` form (which passes the decorated function as
                ``name``) is caught here.
        """
        if not isinstance(name, str) or not name.strip():
            raise TypeError('Manager requires an explicit non-empty name; use @Manager(name="...").')
        self.name: str = name
        self.cleanup: Callable[[Any], Coroutine[None, None, None]] | None = cleanup
        self.method: Callable[[Any], Coroutine[None, None, None]] | None = None
        self.owner: type | None = None
        self.attribute_name: str | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        """Record the manager on its owning class when the class is created.

        Stores the owning class and attribute name for diagnostics, then
        appends ``self`` to the class's manager registry
        (``MANAGER_REGISTRY_ATTR``). The registry is read and created from
        the class's own ``__dict__`` only, so a subclass never mutates or
        inherits a base class's registry list. Identity deduplication makes
        the same ``Manager`` aliased under two attribute names safe.

        Args:
            owner: The class the manager is being assigned to.
            name: The attribute name the manager is assigned under.
        """
        self.owner = owner
        self.attribute_name = name
        registry = owner.__dict__.get(MANAGER_REGISTRY_ATTR)
        if registry is None:
            registry = []
            setattr(owner, MANAGER_REGISTRY_ATTR, registry)
        if not any(entry is self for entry in registry):
            registry.append(self)

    def __call__(self, method: Callable[[Any], Coroutine[None, None, None]]) -> "Manager":
        """Apply the decorator to an async method.

        Stores the method reference and returns ``self`` so the decorated
        method can be used as a ``Manager`` instance by
        ``ManagerRuntime.iter_manager_definitions()``.

        Args:
            method: The async method to wrap as a manager loop.

        Returns:
            ``self``, which can be inspected by the worker to discover
            and execute the manager.
        """
        self.method = method
        return self

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        """Descriptor protocol: bind the wrapped method to the instance.

        Because ``Manager`` is a descriptor, the decorated method remains
        callable as a normal bound method (``self._heartbeat_manager()``)
        while the class attribute still holds the ``Manager`` instance that
        ``ManagerRuntime.iter_manager_definitions()`` discovers.

        Args:
            instance: The worker instance the manager is accessed through,
                or ``None`` when accessed on the class.
            owner: The owning class.

        Returns:
            The bound method when accessed through an instance, otherwise
            ``self`` (the ``Manager`` instance).
        """
        if instance is None or self.method is None:
            return self
        return MethodType(self.method, instance)


def register_manager(
    owner: type,
    method: Callable[[Any], Coroutine[None, None, None]],
    *,
    name: str,
    cleanup: Callable[[Any], Coroutine[None, None, None]] | None = None,
    attribute_name: str | None = None,
    replace: bool = True,
) -> Manager:
    """Register a manager on a class explicitly, without the ``@Manager`` decorator.

    A lower-level entry point for cases where the decorated-method syntax is
    unavailable (e.g. a manager assembled dynamically or defined outside the
    class body). The manager is recorded in the class's manager registry
    (``MANAGER_REGISTRY_ATTR``) so ``ManagerRuntime.iter_manager_definitions``
    discovers it alongside ``@Manager``-decorated methods.

    Args:
        owner: The class to register the manager on.
        method: The async callable the manager executes, receiving the worker
            instance as its argument.
        name: Human-readable name for the manager. Required.
        cleanup: Optional async callable that runs when the manager stops.
            Receives the worker instance as its argument.
        attribute_name: Optional binding aid. When given, the manager is also
            assigned as ``owner.<attribute_name>`` so it can be reached through
            the descriptor protocol. It is never used as the manager's identity.
        replace: When ``True`` (default), a manager with the same ``name`` on
            the same ``owner`` is replaced in place, preserving order. When
            ``False``, the manager is always appended, allowing duplicate names
            to be reported by the discovery-time collision warning.

    Returns:
        The registered ``Manager`` instance.

    Raises:
        TypeError: If ``owner`` is not a class.
    """
    if not isinstance(owner, type):
        raise TypeError("register_manager() requires a class as 'owner'")

    manager = Manager(name=name, cleanup=cleanup)
    manager.method = method
    manager.owner = owner
    manager.attribute_name = attribute_name
    if attribute_name is not None:
        setattr(owner, attribute_name, manager)

    registry = owner.__dict__.get(MANAGER_REGISTRY_ATTR)
    if registry is None:
        registry = []
        setattr(owner, MANAGER_REGISTRY_ATTR, registry)

    if replace:
        for index, entry in enumerate(registry):
            if entry.name == name:
                registry[index] = manager
                break
        else:
            registry.append(manager)
    else:
        registry.append(manager)

    return manager


__all__ = [
    "Manager",
    "ManagerStatus",
    "register_manager",
]
