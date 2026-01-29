"""Unified CLI entry point for fs-scans command."""

import click

from .import_cmd import import_cmd
from .query_cmd import query_cmd
from .analyze_cmd import analyze_cmd


@click.group()
@click.version_option()
def fs_scans_cli():
    """Filesystem scan database toolkit.

    Import filesystem scan logs and query directory statistics.

    Examples:
        # Import a scan log (auto-detect format)
        fs-scans import scan.log

        # Import with explicit format
        fs-scans import scan.log --format gpfs

        # Query a filesystem
        fs-scans query asp --min-size 1G -d 4

        # Query all filesystems
        fs-scans query all --single-owner

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
