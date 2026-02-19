"""Sync command group for jobhist CLI."""

import click


@click.group()
def sync():
    """Sync job data from local PBS accounting logs.

    Parses PBS accounting log files from local filesystem.

    \b
    Example:
      jobhist sync local -m derecho -l ./data/pbs_logs -d 2024-01-29
    """
    pass


# Subcommands registered in main CLI module to avoid circular imports
