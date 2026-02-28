"""Placeholder for future SLURM sacct log synchronization."""

from .base import SyncBase


class SyncSLURMLogs(SyncBase):
    """Placeholder for future SLURM sacct log synchronization."""

    @classmethod
    def scheduler_name(cls):
        return "SLURM"

    def fetch_records(self, *args, **kwargs):
        raise NotImplementedError("SLURM sync not yet implemented")

    def sync(self, *args, **kwargs):
        raise NotImplementedError("SLURM sync not yet implemented")
