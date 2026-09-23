"""``python -m examples.textual`` entry point for the Textual worker TUI."""

import argparse
import logging
import multiprocessing.resource_tracker

from .app import TextualWorkerApp


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the example's command-line options."""
    parser = argparse.ArgumentParser(description="Run the Textual worker dashboard example.")
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug-level logging (default: info)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run the TUI; Textual owns SIGINT/SIGTERM handling."""
    args = parse_args(argv)
    # Textual's terminal driver redirects stderr, leaving sys.stderr.fileno()
    # at -1. multiprocessing's resource tracker passes that fd to fork_exec
    # when it first launches, which rejects -1 with "bad value(s) in
    # fds_to_keep". Starting the tracker here, while stderr is still a real
    # descriptor, sidesteps the failure for every later spawn.
    multiprocessing.resource_tracker.ensure_running()
    TextualWorkerApp(log_level=logging.DEBUG if args.debug else logging.INFO).run()


if __name__ == "__main__":
    main()
