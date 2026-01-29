#!/usr/bin/env python3
"""Convenience wrapper: fs-scans-analyze → fs-scans analyze

This wrapper allows selective deployment of the analyze command.
For example, you can expose fs-scans-query to all users while
keeping fs-scans-import restricted to administrators only.
"""

import sys


def main():
    """Inject 'analyze' subcommand and call main CLI."""
    from fs_scans.cli.main import fs_scans_cli

    # Set program name for help text
    sys.argv[0] = "fs-scans-analyze"
    # Inject 'analyze' subcommand
    sys.argv.insert(1, "analyze")
    # Call main CLI
    fs_scans_cli()


if __name__ == "__main__":
    main()
