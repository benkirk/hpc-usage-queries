"""Backward compatibility wrapper for legacy entry point.

This wrapper allows the old qhist-parse-logs entry point to work
seamlessly by delegating to the new Click-based sync local command.
"""

import sys
import click
from .local_sync import local as local_sync


def parse_logs_wrapper():
    """Legacy qhist-parse-logs entry point.

    Delegates to: qhist-db sync local

    Preserves all existing command-line arguments and behavior.
    """
    # Change program name for help text
    sys.argv[0] = "qhist-parse-logs"

    # Invoke local sync command
    local_sync(standalone_mode=True)
