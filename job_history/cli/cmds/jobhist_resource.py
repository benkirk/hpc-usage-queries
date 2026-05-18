"""Convenience entry point: ``jobhist-resource`` → ``jobhist resource``.

See :mod:`job_history.cli.cmds.jobhist_history` for the rationale on
re-entering through the full ``cli`` group rather than calling the
subgroup function directly.
"""

import sys

from job_history.cli.core import EXIT_KEYBOARD_INTERRUPT
from job_history.cli.cmds.jobhist import cli


def main():
    sys.argv.insert(1, "resource")
    try:
        cli()
    except KeyboardInterrupt:
        sys.exit(EXIT_KEYBOARD_INTERRUPT)


if __name__ == "__main__":
    main()
