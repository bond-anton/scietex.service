"""Back-compat shim for the worker heartbeat schema.

The :class:`Heartbeat` struct now lives in :mod:`scietex.service.heartbeat`;
this module re-exports it so the documented import path
``scietex.service.valkey.schemas.Heartbeat`` keeps resolving.
"""

from ..heartbeat import Heartbeat

__all__ = ["Heartbeat"]
