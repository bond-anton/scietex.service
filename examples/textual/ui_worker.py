"""UI-facing worker mixin and concrete Valkey/MQTT workers.

``BasicWorker.start`` registers SIGINT/SIGTERM handlers through
``_setup_signal_handlers``; ``loop.add_signal_handler`` is last-wins, so the
worker must not fight the Textual app (which owns SIGINT) over the handlers.
The mixin neutralizes both hooks so the app can run the worker on its loop.

Each concrete class is defined conditionally on its availability flag
(``VALKEY_AVAILABLE`` / ``MQTT_AVAILABLE``) so this module imports even when the
extra is absent; the fallback classes raise a clear error only if constructed.
"""

from scietex.service import MQTT_AVAILABLE, VALKEY_AVAILABLE, BasicWorker

from .task_handlers import FastTaskHandler, SlowTaskHandler


class UiWorkerMixin:
    """Neutralize signal handling and register the demo task handlers.

    Must be the first base in the MRO (``class UiValkeyWorker(UiWorkerMixin,
    ValkeyWorker)``) so these overrides win over ``BasicWorker``'s and
    ``super().__init__`` forwards to the concrete transport worker.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.add_task_handler(FastTaskHandler)
        self.add_task_handler(SlowTaskHandler)

    def _setup_signal_handlers(self) -> None:
        """No-op: the TUI owns signal handling, not the worker."""
        return None

    def _remove_signal_handlers(self) -> None:
        """No-op, mirroring the disabled setup."""
        return None

    def _ensure_logging_handler(self) -> None:
        """Disable the transport logging handler.

        The TUI renders the worker's logs in the parent through
        ``_ProcessLogHandler``, so shipping them to the Valkey/MQTT log stream
        as well is redundant. It is also harmful: the transport handler consumes
        one broker round-trip per record, so at DEBUG volume its bounded backend
        queue overflows and every dropped record writes a full traceback to
        stderr synchronously on the event loop, stalling the worker. Returning
        ``None`` makes both transports' ``_connect_locked`` guards skip
        registration and start, and leaves their ``cleanup`` guards a no-op.
        """
        return None


if VALKEY_AVAILABLE:
    from scietex.service import ValkeyWorker

    class UiValkeyWorker(UiWorkerMixin, ValkeyWorker):
        """A :class:`~scietex.service.ValkeyWorker` safe to run on a shared loop."""

else:

    class UiValkeyWorker(UiWorkerMixin):
        """Placeholder: Valkey is unavailable, so constructing this raises."""

        def __init__(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError(
                "Valkey support is unavailable; install the 'valkey' extra "
                "(scietex.service[valkey]) to run UiValkeyWorker."
            )


if MQTT_AVAILABLE:
    from scietex.service import MqttWorker

    class UiMqttWorker(UiWorkerMixin, MqttWorker):
        """An :class:`~scietex.service.MqttWorker` safe to run on a shared loop."""

else:

    class UiMqttWorker(UiWorkerMixin):
        """Placeholder: MQTT is unavailable, so constructing this raises."""

        def __init__(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError(
                "MQTT support is unavailable; install the 'mqtt' extra (scietex.service[mqtt]) to run UiMqttWorker."
            )


#: Maps a kind string to the UI worker class serving it. The placeholder
#: classes (when an extra is absent) still resolve here; they raise on
#: construction, which keeps the unavailable path explicit.
_WORKER_CLASSES: dict[str, type] = {
    "valkey": UiValkeyWorker,
    "mqtt": UiMqttWorker,
}

_VALID_KINDS = ", ".join(sorted(_WORKER_CLASSES))


def build_ui_worker(kind: str, *, theme: object) -> BasicWorker:
    """Construct the UI worker for ``kind`` with ``theme``."""
    worker_class = _WORKER_CLASSES.get(kind)
    if worker_class is None:
        raise ValueError(f"Unknown worker kind {kind!r}; expected one of: {_VALID_KINDS}.")
    return worker_class(theme=theme)


def worker_unavailable(kind: str) -> bool:
    """Whether the optional extra backing ``kind`` is missing."""
    if kind == "valkey":
        return not VALKEY_AVAILABLE
    if kind == "mqtt":
        return not MQTT_AVAILABLE
    raise ValueError(f"Unknown worker kind {kind!r}; expected one of: {_VALID_KINDS}.")


def worker_class_label(worker_class: type[BasicWorker]) -> str:
    """Return the transport label for a worker class, falling back to its name.

    The fallback exists because ``_transport_name`` is semi-private: a subclass
    may not define it, in which case the class name is the honest label. The
    class-accepting form serves callers that hold no instance.
    """
    name = getattr(worker_class, "_transport_name", None)
    return name if name is not None else worker_class.__name__


def worker_kind_label(worker: BasicWorker) -> str:
    """Return the transport label for a worker instance."""
    return worker_class_label(type(worker))
