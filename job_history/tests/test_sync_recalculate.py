"""Tests for --recalculate sync mode and _fill_missing_charges()."""

from datetime import datetime, timezone

import pytest

from job_history.sync.base import SyncBase
from job_history.database import Job, JobCharge, DailySummary


# ---------------------------------------------------------------------------
# Stub syncer — yields a configurable list of records
# ---------------------------------------------------------------------------

class StubSyncer(SyncBase):
    """Minimal SyncBase subclass for unit testing; fetch_records() yields fixed records."""

    SCHEDULER_NAME = "stub"

    def __init__(self, session, machine, records=None):
        super().__init__(session, machine)
        self._records = records or []

    def fetch_records(self, log_dir, period):
        yield from self._records


def _make_record(job_id: str, end_dt: datetime, queue: str = "main") -> dict:
    """Build a minimal valid job record dict for testing."""
    submit = end_dt.replace(hour=8)
    return {
        "job_id": job_id,
        "user": "testuser",
        "account": "NCAR0001",
        "queue": queue,
        "submit": submit,
        "eligible": submit,
        "start": submit,
        "end": end_dt,
        "elapsed": 3600,
        "walltime": 3600,
        "numcpus": 4,
        "numgpus": 0,
        "numnodes": 1,
        "memory": 1073741824,  # 1 GB
    }


# Jobs whose end timestamp falls within the Mountain-Time day 2025-06-01:
# MDT = UTC-6, so 2025-06-01 MT = 2025-06-01 06:00 UTC .. 2025-06-02 06:00 UTC
TARGET_DATE = "2025-06-01"
TARGET_DT = datetime(2025, 6, 1, 18, 0, 0, tzinfo=timezone.utc)  # noon MT
JOB_A = _make_record("job.A.1", TARGET_DT)
JOB_B = _make_record("job.B.1", TARGET_DT)


# ---------------------------------------------------------------------------
# Helper: insert jobs then strip their charges to simulate missing charges
# ---------------------------------------------------------------------------

def _insert_and_strip_charges(session, machine, records):
    """Sync records and then delete all resulting job_charges rows.

    Simulates the historical bulk-load scenario where jobs were inserted
    without corresponding charge records.
    """
    StubSyncer(session, machine, records).sync(log_dir=None, period=TARGET_DATE)
    session.query(JobCharge).delete()
    session.commit()
    assert session.query(JobCharge).count() == 0, "charges must be stripped"


# ---------------------------------------------------------------------------
# Tests: --recalculate mode
# ---------------------------------------------------------------------------

class TestRecalculateMode:
    """--recalculate recomputes charges from DB jobs without parsing logs."""

    def test_recalculate_writes_charges_from_db(self, in_memory_session):
        """Charges are computed and upserted for all jobs in the date range."""
        _insert_and_strip_charges(in_memory_session, "derecho", [JOB_A, JOB_B])
        job_count = in_memory_session.query(Job).count()
        assert job_count == 2

        stats = StubSyncer(in_memory_session, "derecho").sync(
            log_dir=None, period=TARGET_DATE, recalculate=True
        )

        charges = in_memory_session.query(JobCharge).all()
        assert len(charges) == 2, "one charge row per job"
        assert all(c.charge_version == 1 for c in charges), "version=1 (real, not placeholder)"
        assert all(c.cpu_hours > 0 for c in charges), "cpu_hours computed from elapsed * numcpus"
        assert stats["recalculated"] == 2
        assert stats["fetched"] == 2

    def test_recalculate_does_not_insert_or_modify_jobs(self, in_memory_session):
        """No new Job rows appear; existing Job fields are unchanged."""
        _insert_and_strip_charges(in_memory_session, "derecho", [JOB_A])
        original_job = in_memory_session.query(Job).one()
        original_elapsed = original_job.elapsed

        StubSyncer(in_memory_session, "derecho").sync(
            log_dir=None, period=TARGET_DATE, recalculate=True
        )

        assert in_memory_session.query(Job).count() == 1, "no new rows inserted"
        refreshed = in_memory_session.query(Job).one()
        assert refreshed.elapsed == original_elapsed, "job fields untouched"

    def test_recalculate_dry_run_makes_no_db_writes(self, in_memory_session):
        """dry_run=True: charges are computed but not written to the DB."""
        _insert_and_strip_charges(in_memory_session, "derecho", [JOB_A])

        stats = StubSyncer(in_memory_session, "derecho").sync(
            log_dir=None, period=TARGET_DATE, recalculate=True, dry_run=True
        )

        assert in_memory_session.query(JobCharge).count() == 0, "no writes in dry-run"
        assert stats["recalculated"] == 0, "dry-run does not count as recalculated"
        assert stats["fetched"] == 1, "but jobs were still fetched and counted"

    def test_recalculate_generates_daily_summary(self, in_memory_session):
        """generate_summary=True (default): daily_summary rows are created."""
        _insert_and_strip_charges(in_memory_session, "derecho", [JOB_A])

        StubSyncer(in_memory_session, "derecho").sync(
            log_dir=None, period=TARGET_DATE, recalculate=True
        )

        summaries = in_memory_session.query(DailySummary).filter(
            DailySummary.user_id.isnot(None)
        ).all()
        assert len(summaries) >= 1, "daily_summary should be populated"

    def test_recalculate_skips_summary_when_disabled(self, in_memory_session):
        """generate_summary=False: no daily_summary rows created by recalculate."""
        _insert_and_strip_charges(in_memory_session, "derecho", [JOB_A])
        # Clear any summary rows that _insert_and_strip_charges may have created
        in_memory_session.query(DailySummary).delete()
        in_memory_session.commit()

        StubSyncer(in_memory_session, "derecho").sync(
            log_dir=None, period=TARGET_DATE, recalculate=True, generate_summary=False
        )

        assert in_memory_session.query(DailySummary).count() == 0, \
            "no summary rows should be created when generate_summary=False"

    def test_recalculate_mutual_exclusion_with_upsert(self, in_memory_session):
        """recalculate=True with upsert=True raises ValueError."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
                log_dir=None, period=TARGET_DATE, recalculate=True, upsert=True
            )

    def test_recalculate_mutual_exclusion_with_incremental(self, in_memory_session):
        """recalculate=True with incremental=True raises ValueError."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
                log_dir=None, period=TARGET_DATE, recalculate=True, incremental=True
            )

    def test_recalculate_overwrites_placeholder_charges(self, in_memory_session):
        """charge_version=0 placeholder rows are overwritten with version=1."""
        # Sync to create jobs
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )
        # Manually set all charges back to placeholder version
        in_memory_session.query(JobCharge).update(
            {"charge_version": 0, "cpu_hours": 0.0, "gpu_hours": 0.0}
        )
        in_memory_session.commit()

        StubSyncer(in_memory_session, "derecho").sync(
            log_dir=None, period=TARGET_DATE, recalculate=True
        )

        charges = in_memory_session.query(JobCharge).all()
        assert all(c.charge_version == 1 for c in charges), "placeholders overwritten"
        assert all(c.cpu_hours > 0 for c in charges), "real values written"


# ---------------------------------------------------------------------------
# Tests: _fill_missing_charges() in plain / incremental mode
# ---------------------------------------------------------------------------

class TestFillMissingCharges:
    """Plain and incremental sync fill charges for uncharged existing jobs."""

    def test_plain_sync_fills_missing_charges_for_existing_job(self, in_memory_session):
        """Re-running plain sync on a day fills charges for any uncharged jobs."""
        # First sync: insert job, get charges written
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )
        # Strip charges to simulate missing charges
        in_memory_session.query(JobCharge).delete()
        in_memory_session.commit()
        assert in_memory_session.query(JobCharge).count() == 0

        # Second sync (plain mode, same records): should fill missing charges
        # The day is already summarized, so we need to bypass that skip
        # by using incremental (which bypasses the summarized-day skip)
        stats = StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )

        assert stats["inserted"] == 0, "no new records (job already exists)"
        charges = in_memory_session.query(JobCharge).all()
        assert len(charges) > 0, "missing charges should be filled"
        assert all(c.charge_version == 1 for c in charges)

    def test_fill_missing_does_not_modify_job_fields(self, in_memory_session):
        """_fill_missing_charges does not change any Job table columns."""
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )
        original_elapsed = in_memory_session.query(Job).one().elapsed

        in_memory_session.query(JobCharge).delete()
        in_memory_session.commit()

        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )

        assert in_memory_session.query(Job).one().elapsed == original_elapsed

    def test_fill_missing_is_noop_when_charges_present(self, in_memory_session):
        """If all existing jobs already have charges, no DB writes occur."""
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )
        before = in_memory_session.query(JobCharge).one().cpu_hours

        # Second incremental sync: job exists, charges exist — should be a no-op
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )

        after = in_memory_session.query(JobCharge).one().cpu_hours
        assert before == after, "existing correct charges should not be overwritten"
