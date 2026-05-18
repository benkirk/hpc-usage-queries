"""Sync subcommand wiring.

The existing ``jobhist sync`` Click command in :mod:`job_history.sync.cli`
is already a well-structured single-function command with mutually-exclusive
mode flags (``--upsert`` / ``--incremental`` / ``--recalculate`` /
``--resummarize``). It does not produce queryable output that benefits
from the Context/Exporter pattern used by history and resource — its
job is data ingestion with per-mode logging.

So Phase 4 keeps the existing command in place and re-exports it under
:mod:`job_history.cli.sync` so the new entry point can register it
alongside the SAM-aligned history/resource groups.
"""

from job_history.sync.cli import sync

__all__ = ["sync"]
