#!/usr/bin/env python3
"""Convenience wrapper: fs-scans-import → fs-scans import

This wrapper allows selective deployment of the import command.
For example, you can expose fs-scans-query to all users while
keeping fs-scans-import restricted to administrators only.
"""

import sys


def main():
    """Convenience wrapper that calls import command directly."""
    from fs_scans.cli.import_cmd import import_cmd

    # Call import command directly (no need to inject subcommand)
    import_cmd()


if __name__ == "__main__":
    main()
