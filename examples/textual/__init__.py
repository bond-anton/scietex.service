"""Textual TUI example for ``scietex.service``.

A minimal single-worker dashboard: one ``ValkeyWorker`` driven from a central
worker card (instance id + Start/Stop) with a live log panel fed through the
``scietex.logging`` bridge.

``scietex_bridge``, ``worker_card``, and ``app`` import Textual; the package
itself (and ``ui_worker``) stays importable without the optional ``textual``
extra.
"""

__all__ = [
    "app",
    "scietex_bridge",
    "ui_worker",
    "worker_card",
]
