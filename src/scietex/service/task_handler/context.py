"""Narrow context passed to task handlers instead of the full worker."""

import logging
from dataclasses import dataclass


@dataclass(frozen=True)
class TaskHandlerContext:
    """Minimal, read-only context a handler needs from its processor.

    Replaces passing the full worker so handlers cannot reach processor
    internals (task queue, running tasks, lifecycle state).
    """

    service_name: str
    worker_id: int
    logger: logging.Logger
