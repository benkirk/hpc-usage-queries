"""Sync command group for qhist-db CLI."""

import click


@click.group()
def sync():
    """Sync job data from various sources.

    Supported sources:
    - remote: SSH to HPC machines (derecho, casper)
    - local: Parse PBS accounting logs from local filesystem

    \b
    Examples:
      qhist-db sync remote -m all --start 2024-01-01 -v
      qhist-db sync local -m derecho -l ./data/pbs_logs -d 2024-01-29
    """
    pass


# Subcommands registered in main CLI module to avoid circular imports
