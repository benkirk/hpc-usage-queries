#!/usr/bin/env python3
"""Convenience wrapper: jobhist-history → jobhist history

Exposes the history subcommand group directly, allowing
selective deployment independent of sync and resource commands.
"""


def main():
    """Convenience wrapper that calls history command directly."""
    from job_history.cli import history

    history()


if __name__ == "__main__":
    main()
