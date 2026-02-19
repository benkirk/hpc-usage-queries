"""Wrapper for invoking the sync local command programmatically."""

import sys
import click
from .local_sync import local as local_sync


def parse_logs_wrapper():
    """Invoke the sync local command.

    Delegates to: jobhist sync local

    Preserves all existing command-line arguments and behavior.
    """
    # Invoke local sync command
    local_sync(standalone_mode=True)
