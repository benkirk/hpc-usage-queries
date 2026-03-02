"""Placeholder for future SLURM sacct log synchronization."""

from pathlib import Path
from typing import Iterator

from .base import SyncBase


class SyncSLURMLogs(SyncBase):
    """Placeholder for future SLURM sacct log synchronization."""

    SCHEDULER_NAME = "SLURM"

    def fetch_records(self, log_dir: str | Path | None, period: str) -> Iterator[dict]:
        raise NotImplementedError("SLURM sync not yet implemented")
