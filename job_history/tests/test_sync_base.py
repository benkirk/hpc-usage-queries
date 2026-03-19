"""Tests for SyncBase._insert_batch and _bulk_insert_jobs conflict handling."""

from datetime import datetime, timezone

import pytest

from job_history.sync.base import SyncBase
from job_history.database import Job


# ---------------------------------------------------------------------------
# Stub syncer (same pattern as test_sync_incremental.py)
# ---------------------------------------------------------------------------

class StubSyncer(SyncBase):
    SCHEDULER_NAME = "stub"

    def __init__(self, session, machine, records):
        super().__init__(session, machine)
        self._records = records

    def fetch_records(self, log_dir, period):
        yield from self._records


TARGET_DT = datetime(2025, 6, 1, 18, 0, 0, tzinfo=timezone.utc)
TARGET_DATE = "2025-06-01"


def _make_record(job_id: str, end_dt: datetime) -> dict:
    submit = end_dt.replace(hour=8)
    return {
        "job_id": job_id,
        "user": "testuser",
        "account": "NCAR0001",
        "queue": "main",
        "submit": submit,
        "eligible": submit,
        "start": submit,
        "end": end_dt,
        "elapsed": 3600,
        "walltime": 3600,
        "numcpus": 128,
        "numgpus": 0,
        "numnodes": 1,
        "memory": 0,
    }


def _resolve_fks(syncer: StubSyncer, record: dict) -> dict:
    """Manually resolve user/account/queue to FKs (mirrors _insert_batch logic)."""
    rec = record.copy()
    rec['user_id'] = syncer.cache.get_or_create_user(rec['user']).id
    rec['account_id'] = syncer.cache.get_or_create_account(rec['account']).id
    rec['queue_id'] = syncer.cache.get_or_create_queue(rec['queue']).id
    return rec


# ---------------------------------------------------------------------------
# Tests for _bulk_insert_jobs
# ---------------------------------------------------------------------------

class TestBulkInsertJobsConflict:
    """_bulk_insert_jobs must silently skip (job_id, submit) duplicates."""

    def test_first_insert_returns_one(self, in_memory_session):
        syncer = StubSyncer(in_memory_session, "derecho", [])
        rec = _resolve_fks(syncer, _make_record("conflict.1", TARGET_DT))

        n = syncer._bulk_insert_jobs([rec])
        assert n == 1
        assert in_memory_session.query(Job).filter_by(job_id="conflict.1").count() == 1

    def test_duplicate_silently_skipped(self, in_memory_session):
        """Second call with same (job_id, submit) must return 0, not raise."""
        syncer = StubSyncer(in_memory_session, "derecho", [])
        rec = _resolve_fks(syncer, _make_record("conflict.2", TARGET_DT))

        n1 = syncer._bulk_insert_jobs([rec])
        assert n1 == 1

        # Bypass Python-side dedup — call directly with a conflicting record
        n2 = syncer._bulk_insert_jobs([rec])
        assert n2 == 0

        # DB must still have exactly one row
        assert in_memory_session.query(Job).filter_by(job_id="conflict.2").count() == 1

    def test_mixed_batch_counts_only_inserted(self, in_memory_session):
        """Batch with one new + one duplicate returns 1, not 2."""
        syncer = StubSyncer(in_memory_session, "derecho", [])
        rec_a = _resolve_fks(syncer, _make_record("conflict.3a", TARGET_DT))
        rec_b = _resolve_fks(syncer, _make_record("conflict.3b", TARGET_DT))

        syncer._bulk_insert_jobs([rec_a])  # pre-insert rec_a

        n = syncer._bulk_insert_jobs([rec_a, rec_b])  # rec_a conflicts, rec_b is new
        assert n == 1
        assert in_memory_session.query(Job).count() == 2


# ---------------------------------------------------------------------------
# Tests for _insert_batch end-to-end with conflict
# ---------------------------------------------------------------------------

class TestInsertBatchConflictEndToEnd:
    """_insert_batch must not raise when the DB already contains the record."""

    def test_insert_batch_idempotent(self, in_memory_session):
        """Calling _insert_batch twice with the same record is idempotent."""
        syncer = StubSyncer(in_memory_session, "derecho", [])

        # Use full sync for the first insert (resolves FKs, calculates charges)
        StubSyncer(in_memory_session, "derecho", [_make_record("idem.1", TARGET_DT)]).sync(
            log_dir=None, period=TARGET_DATE
        )
        assert in_memory_session.query(Job).filter_by(job_id="idem.1").count() == 1

        # Second sync with same record — should not raise UniqueViolation
        stats = StubSyncer(in_memory_session, "derecho", [_make_record("idem.1", TARGET_DT)]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )
        # Python-side dedup catches this; inserted=0 regardless of DB path
        assert stats["inserted"] == 0
        assert in_memory_session.query(Job).filter_by(job_id="idem.1").count() == 1
