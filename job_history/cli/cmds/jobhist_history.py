"""Convenience entry point: ``jobhist-history`` → ``jobhist history``.

Enables selective deployment of the history subcommand group
independent of sync and resource (e.g. an unprivileged read-only image
that ships only ``jobhist-history``).

Implementation note: the history subgroup expects a :class:`Context`
object built by the top-level ``cli`` callback. So this wrapper prepends
``history`` to ``argv`` and re-enters through the full ``cli`` group —
that way the Context is built normally and ``--format`` /
``--verbose`` remain available at the top level.
"""

import sys

from job_history.cli.core import EXIT_KEYBOARD_INTERRUPT
from job_history.cli.cmds.jobhist import cli


def main():
    sys.argv.insert(1, "history")
    try:
        cli()
    except KeyboardInterrupt:
        sys.exit(EXIT_KEYBOARD_INTERRUPT)


if __name__ == "__main__":
    main()
