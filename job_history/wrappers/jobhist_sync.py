#!/usr/bin/env python3
"""Convenience wrapper: jobhist-sync → jobhist sync

Allows selective deployment of the sync command.
For example, sync can be restricted to administrators while
jobhist-history and jobhist-resource are available to all users.
"""


def main():
    """Convenience wrapper that calls sync command directly."""
    from job_history.sync_cli.sync import sync

    sync()


if __name__ == "__main__":
    main()
