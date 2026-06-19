#!/usr/bin/env python3
"""Convenience wrapper: fs-scans-consolidate → fs-scans consolidate

This wrapper allows selective deployment of the consolidate command,
e.g. restricting it to administrators / the weekly publish job while
exposing fs-scans-query to all users.
"""


def main():
    """Convenience wrapper that calls the consolidate command directly."""
    from fs_scans.cli.consolidate_cmd import consolidate_cmd

    consolidate_cmd()


if __name__ == "__main__":
    main()
