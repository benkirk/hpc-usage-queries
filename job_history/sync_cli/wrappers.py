"""Wrapper for invoking the sync command programmatically."""

import click
from .sync import sync


def parse_logs_wrapper():
    """Invoke the sync command.

    Delegates to: jobhist sync

    Preserves all existing command-line arguments and behavior.
    """
    sync(standalone_mode=True)
