"""Sync subpackage for job_history.

Re-exports public API and provides backward-compat wrapper for sync_pbs_logs_bulk.
"""

from .base import SyncBase
from .importer import SyncPBSLogs, JobImporter
from .slurm import SyncSLURMLogs


def sync_pbs_logs_bulk(session, machine, log_dir, **kwargs) -> dict:
    """Backward-compat wrapper. Prefer SyncPBSLogs(session, machine).sync()."""
    return SyncPBSLogs(session, machine).sync(log_dir, **kwargs)


__all__ = [
    "SyncBase",
    "SyncPBSLogs",
    "SyncSLURMLogs",
    "JobImporter",
    "sync_pbs_logs_bulk",
]
