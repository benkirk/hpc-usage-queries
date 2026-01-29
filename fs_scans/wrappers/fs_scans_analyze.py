#!/usr/bin/env python3
"""Convenience wrapper: fs-scans-analyze → fs-scans analyze

This wrapper allows selective deployment of the analyze command.
For example, you can expose fs-scans-query to all users while
keeping fs-scans-import restricted to administrators only.
"""

import sys


def main():
    """Convenience wrapper that calls analyze command directly."""
    from fs_scans.cli.analyze_cmd import analyze_cmd

    # Call analyze command directly (no need to inject subcommand)
    analyze_cmd()


if __name__ == "__main__":
    main()
