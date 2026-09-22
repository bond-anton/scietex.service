"""UI-facing worker mixin and concrete Valkey worker.

``BasicWorker.start`` registers SIGINT/SIGTERM handlers through
``_setup_signal_handlers``; ``loop.add_signal_handler`` is last-wins, so the
worker must not fight the Textual app (which owns SIGINT) over the handlers.
The mixin neutralizes both hooks so the app can run the worker on its loop.

The concrete class is defined conditionally on ``VALKEY_AVAILABLE`` so this
module imports even when the extra is absent; the fallback class raises a clear
error only if constructed.
"""

from scietex.service import VALKEY_AVAILABLE


class UiWorkerMixin:
    """Neutralize signal handling so the TUI owns the event loop's signals.

    Must be the first base in the MRO (``class UiValkeyWorker(UiWorkerMixin,
    ValkeyWorker)``) so these overrides win over ``BasicWorker``'s.
    """

    def _setup_signal_handlers(self) -> None:
        """No-op: the TUI owns signal handling, not the worker."""
        return None

    def _remove_signal_handlers(self) -> None:
        """No-op, mirroring the disabled setup."""
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
