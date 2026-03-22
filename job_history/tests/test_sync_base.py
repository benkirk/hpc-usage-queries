"""Tests for SyncBase._insert_batch and _bulk_insert_jobs conflict handling."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from sqlalchemy.exc import IntegrityError

from job_history.sync.base import SyncBase
from job_history.database import Job
from job_history.database.models import LookupCache, User, Account, Queue


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


# ---------------------------------------------------------------------------
# Tests for LookupCache concurrent-insert race condition
# ---------------------------------------------------------------------------

class TestLookupCacheConcurrentInsert:
    """LookupCache must survive concurrent inserts from another process.

    Scenario: a long-running sync and a rapid daily sync share the same
    PostgreSQL DB.  The daily sync inserts a new user between the time the
    long-running sync loads its LookupCache and the time it tries to insert
    that same user.  Without the savepoint fix this raises UniqueViolation
    and kills the entire long-running sync.
    """

    def _simulate_external_insert(self, session, model, field, value):
        """Insert a lookup row directly, simulating another process."""
        obj = model(**{field: value})
        session.add(obj)
        session.flush()
        return obj

    def test_get_or_create_user_survives_concurrent_insert(self, in_memory_session):
        """If another process inserts the user first, get_or_create_user must
        return the existing row rather than raising IntegrityError."""
        cache = LookupCache(in_memory_session)

        # Simulate the external insert AFTER cache was loaded (empty at that point)
        self._simulate_external_insert(in_memory_session, User, "username", "prao")
        in_memory_session.commit()

        # Cache doesn't know about "prao" yet — this is the race condition
        assert "prao" not in cache._users

        # Must not raise; must return the row that the "other process" inserted
        user = cache.get_or_create_user("prao")
        assert user.username == "prao"
        assert user.id is not None

        # Cache is now primed; second call is a no-op hit
        user2 = cache.get_or_create_user("prao")
        assert user2.id == user.id
        assert in_memory_session.query(User).filter_by(username="prao").count() == 1

    def test_get_or_create_account_survives_concurrent_insert(self, in_memory_session):
        """Same race-condition protection for accounts."""
        cache = LookupCache(in_memory_session)
        self._simulate_external_insert(in_memory_session, Account, "account_name", "RACE0001")
        in_memory_session.commit()

        acct = cache.get_or_create_account("RACE0001")
        assert acct.account_name == "RACE0001"
        assert in_memory_session.query(Account).filter_by(account_name="RACE0001").count() == 1

    def test_get_or_create_queue_survives_concurrent_insert(self, in_memory_session):
        """Same race-condition protection for queues."""
        cache = LookupCache(in_memory_session)
        self._simulate_external_insert(in_memory_session, Queue, "queue_name", "raceq")
        in_memory_session.commit()

        queue = cache.get_or_create_queue("raceq")
        assert queue.queue_name == "raceq"
        assert in_memory_session.query(Queue).filter_by(queue_name="raceq").count() == 1

    def test_full_sync_survives_concurrent_user_insert(self, in_memory_session):
        """End-to-end: a sync that encounters a user already inserted by another
        process mid-batch must complete without error."""
        # First, get a clean cache state
        cache = LookupCache(in_memory_session)
        assert "newuser" not in cache._users

        # Simulate another process inserting "newuser" into the DB
        self._simulate_external_insert(in_memory_session, User, "username", "newuser")
        in_memory_session.commit()

        # Now a sync batch arrives with records attributed to "newuser"
        rec = _make_record("race.job.1", TARGET_DT)
        rec["user"] = "newuser"

        syncer = StubSyncer(in_memory_session, "derecho", [rec])
        # This must not raise UniqueViolation
        stats = syncer.sync(log_dir=None, period=TARGET_DATE)
        assert stats["inserted"] == 1
        assert in_memory_session.query(Job).filter_by(job_id="race.job.1").count() == 1
