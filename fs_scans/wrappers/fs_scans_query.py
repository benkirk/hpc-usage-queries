#!/usr/bin/env python3
"""Convenience wrapper: fs-scans-query → fs-scans query

This wrapper allows selective deployment of the query command.
For example, you can expose fs-scans-query to all users while
keeping fs-scans-import restricted to administrators only.
"""

import sys


def main():
    """Inject 'query' subcommand and call main CLI."""
    from fs_scans.cli.main import fs_scans_cli

    # Set program name for help text
    sys.argv[0] = "fs-scans-query"
    # Inject 'query' subcommand
    sys.argv.insert(1, "query")
    # Call main CLI
    fs_scans_cli()


if __name__ == "__main__":
    main()
