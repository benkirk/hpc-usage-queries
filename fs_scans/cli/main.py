"""Unified CLI entry point for fs-scans command."""

import click

from .import_cmd import import_cmd
from .query_cmd import query_cmd
from .analyze_cmd import analyze_cmd


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option()
def fs_scans_cli():
    """Filesystem scan database toolkit.

    Import filesystem scan logs and query directory statistics.

    \b
    Examples:
      fs-scans import scan.log                # Import with auto-detection
      fs-scans import scan.log --format gpfs  # Import with explicit format
      fs-scans query asp --min-size 1G -d 4   # Query a filesystem
      fs-scans query all --single-owner       # Query all filesystems

    \b
    For help on a specific command:
      fs-scans import --help
      fs-scans query --help
    """
    pass


# Register subcommands
fs_scans_cli.add_command(import_cmd, name="import")
fs_scans_cli.add_command(query_cmd, name="query")
fs_scans_cli.add_command(analyze_cmd, name="analyze")


if __name__ == "__main__":
    fs_scans_cli()
