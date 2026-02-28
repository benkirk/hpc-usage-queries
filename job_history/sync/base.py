"""Abstract base class for scheduler log synchronization."""

from abc import ABC, abstractmethod

from sqlalchemy.orm import Session


class SyncBase(ABC):
    """Abstract base for scheduler log synchronization."""

    def __init__(self, session: Session, machine: str):
        self.session = session
        self.machine = machine

    @classmethod
    def scheduler_name(cls) -> str:
        return cls.__name__

    @abstractmethod
    def fetch_records(self, log_dir, period=None, start_date=None, end_date=None):
        """Yield normalized job dicts from scheduler logs."""
        ...

    @abstractmethod
    def sync(self, log_dir, period=None, start_date=None, end_date=None,
             dry_run=False, batch_size=1000, verbose=False,
             force=False, generate_summary=True) -> dict:
        """parse → insert → charge → summarize.

        Returns:
            dict: {fetched, inserted, errors, days_summarized, days_failed, days_skipped}
        """
        ...
