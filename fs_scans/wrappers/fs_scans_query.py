#!/usr/bin/env python3
"""Convenience wrapper: fs-scans-query → fs-scans query

This wrapper allows selective deployment of the query command.
For example, you can expose fs-scans-query to all users while
keeping fs-scans-import restricted to administrators only.
"""

import sys


def main():
    """Convenience wrapper that calls query command directly."""
    from fs_scans.cli.query_cmd import query_cmd

    # Call query command directly (no need to inject subcommand)
    query_cmd()


if __name__ == "__main__":
    main()
