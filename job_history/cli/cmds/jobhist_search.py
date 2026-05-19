"""Convenience entry point: ``jobhist-search`` → ``jobhist search``.

Enables selective deployment of the search subcommand independent of
sync / history / resource (e.g. a read-only image that ships only
``jobhist-search``). Re-enters through the full ``cli`` group so the
top-level Context is built normally and ``--format`` / ``--verbose``
remain available.
"""

import sys

from job_history.cli.core import EXIT_KEYBOARD_INTERRUPT
from job_history.cli.cmds.jobhist import cli


def main():
    sys.argv.insert(1, "search")
    try:
        cli()
    except KeyboardInterrupt:
        sys.exit(EXIT_KEYBOARD_INTERRUPT)


if __name__ == "__main__":
    main()
