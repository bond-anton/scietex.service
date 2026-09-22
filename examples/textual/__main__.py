"""``python -m examples.textual`` entry point for the Textual worker TUI."""

from .app import TextualWorkerApp


def main() -> None:
    """Run the TUI; Textual owns SIGINT/SIGTERM handling."""
    TextualWorkerApp().run()


if __name__ == "__main__":
    main()
