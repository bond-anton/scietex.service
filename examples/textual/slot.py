"""Slot model for the slot-keyed dashboard.

A slot is one fixed grid position that is either empty or holds a single
worker of one kind (``"valkey"`` or ``"mqtt"``). The slot's identity is stable
across worker lifecycle transitions: the same :func:`slot_key` names both the
slot's ``RichLog`` widget id and its ``TextualLogHandler`` source, so a slot's
log history survives a worker Exit followed by a re-create.
"""

from __future__ import annotations

from dataclasses import dataclass

from textual.widgets import RichLog

from .scietex_bridge import TextualLogHandler
from .worker_card import WorkerCard
from .worker_process import WorkerIdentity, WorkerProcess


@dataclass
class Slot:
    """One fixed grid position, either empty or holding a worker."""

    index: int
    worker: WorkerProcess | None = None
    identity: WorkerIdentity | None = None
    handler: TextualLogHandler | None = None
    kind: str | None = None
    log: RichLog | None = None
    card: WorkerCard | None = None
    running: bool = False


def slot_key(index: int) -> str:
    """Stable identity for a slot, shared by its log widget and handler source."""
    return f"slot-{index}"
