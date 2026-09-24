"""Textual TUI example for ``scietex.service``.

A multi-slot dashboard: four fixed slots laid out as a 2x2 grid of selectable
worker cards. Each slot is either empty or holds a single on-demand worker of
one kind (Valkey or MQTT); an occupied card shows the kind and a Start/Stop
toggle, and each slot feeds its own log stream through ``scietex.textual``.
Selecting a card swaps the log panel to that slot's stream, and each
stream keeps its history while hidden -- and across a worker Exit followed by a
re-create.

``worker_card``, ``slot``, and ``app`` import Textual; the
package itself (and ``ui_worker``) stays importable without the optional
``textual`` extra. The broker modules (``broker_snapshot``, ``broker_parsing``,
``broker_card``, ``valkey_monitor``, ``mqtt_monitor``) import Textual only in
``broker_card``; the monitors and parsers stay importable without it. Likewise
``producer`` and ``worker_process`` stay importable without Textual, while
``producer_card`` needs it.
"""

__all__ = [
    "app",
    "broker_card",
    "broker_parsing",
    "broker_snapshot",
    "mqtt_monitor",
    "producer",
    "producer_card",
    "slot",
    "ui_worker",
    "valkey_monitor",
    "worker_card",
    "worker_process",
]
