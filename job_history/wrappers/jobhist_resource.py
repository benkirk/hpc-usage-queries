#!/usr/bin/env python3
"""Convenience wrapper: jobhist-resource → jobhist resource

Exposes the resource subcommand group directly, allowing
selective deployment independent of sync and history commands.
"""


def main():
    """Convenience wrapper that calls resource command directly."""
    from job_history.cli import resource

    resource()


if __name__ == "__main__":
    main()
