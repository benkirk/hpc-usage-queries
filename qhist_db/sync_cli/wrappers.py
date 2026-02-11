"""Backward compatibility wrappers for legacy entry points.

These wrappers allow old entry points (qhist-sync, qhist-parse-logs) to work
seamlessly by delegating to the new Click-based sync commands.
"""

import sys
import click
from .remote_sync import remote as remote_sync
from .local_sync import local as local_sync


def sync_wrapper():
    """Legacy qhist-sync entry point.

    Delegates to: qhist-db sync remote

    Preserves all existing command-line arguments and behavior.
    """
    # Change program name for help text
    sys.argv[0] = "qhist-sync"

    # Invoke remote sync command
    remote_sync(standalone_mode=True)


def parse_logs_wrapper():
    """Legacy qhist-parse-logs entry point.

    Delegates to: qhist-db sync local

    Preserves all existing command-line arguments and behavior.
    """
    # Change program name for help text
    sys.argv[0] = "qhist-parse-logs"

    # Invoke local sync command
    local_sync(standalone_mode=True)
