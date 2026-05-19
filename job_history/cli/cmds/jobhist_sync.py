"""Convenience entry point: ``jobhist-sync`` → ``jobhist sync``.

Enables selective deployment of the sync command (e.g. restricting
ingestion to an admin-only image while history/resource are shipped to
all users).
"""

import sys

from job_history.cli.core import EXIT_KEYBOARD_INTERRUPT
from job_history.cli.sync import sync


def main():
    try:
        sync()
    except KeyboardInterrupt:
        sys.exit(EXIT_KEYBOARD_INTERRUPT)


if __name__ == "__main__":
    main()
