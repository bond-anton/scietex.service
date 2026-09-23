"""``python -m examples.textual`` entry point for the Textual worker TUI."""

import multiprocessing.resource_tracker

from .app import TextualWorkerApp


def main() -> None:
    """Run the TUI; Textual owns SIGINT/SIGTERM handling."""
    # Textual's terminal driver redirects stderr, leaving sys.stderr.fileno()
    # at -1. multiprocessing's resource tracker passes that fd to fork_exec
    # when it first launches, which rejects -1 with "bad value(s) in
    # fds_to_keep". Starting the tracker here, while stderr is still a real
    # descriptor, sidesteps the failure for every later spawn.
    multiprocessing.resource_tracker.ensure_running()
    TextualWorkerApp().run()


if __name__ == "__main__":
    main()
