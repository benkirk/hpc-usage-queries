"""Tests for --incremental sync mode."""

from datetime import datetime, timezone

import pytest
import click

from job_history.sync.base import SyncBase
from job_history.database import Job, DailySummary


# ---------------------------------------------------------------------------
# Stub syncer — yields a configurable list of records
# ---------------------------------------------------------------------------

class StubSyncer(SyncBase):
    """Minimal SyncBase subclass for unit testing; fetch_records() yields fixed records."""

    SCHEDULER_NAME = "stub"

    def __init__(self, session, machine, records):
        super().__init__(session, machine)
        self._records = records  # list of dicts to yield

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
        "numcpus": 128,
        "numgpus": 0,
        "numnodes": 1,
        "memory": 0,
    }


TARGET_DATE = "2025-06-01"
TARGET_DT = datetime(2025, 6, 1, 18, 0, 0, tzinfo=timezone.utc)
JOB_A = _make_record("job.A.1", TARGET_DT)
JOB_B = _make_record("job.B.1", TARGET_DT)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIncrementalBypassesSkip:
    """Incremental mode must process already-summarized days."""

    def test_incremental_processes_summarized_day(self, in_memory_session):
        """A day with a daily_summary is not skipped in incremental mode."""
        # Initial sync: insert job A and summarize
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )
        assert in_memory_session.query(DailySummary).count() >= 1, "day should be summarized"

        # Incremental with job A + new job B — should NOT skip
        stats = StubSyncer(in_memory_session, "derecho", [JOB_A, JOB_B]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )
        assert stats["days_skipped"] == 0, "incremental must not skip summarized days"
        assert stats["fetched"] == 2
        assert stats["inserted"] == 1   # only job B is new

    def test_normal_sync_skips_summarized_day(self, in_memory_session):
        """Confirm baseline: normal sync skips the day so no new records inserted."""
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )
        stats = StubSyncer(in_memory_session, "derecho", [JOB_A, JOB_B]).sync(
            log_dir=None, period=TARGET_DATE
        )
        assert stats["days_skipped"] == 1
        assert stats["inserted"] == 0


class TestIncrementalSummaryTrigger:
    """Summary should only regenerate when new records were actually inserted."""

    def test_no_resummarize_when_nothing_new(self, in_memory_session):
        """Incremental with no new records must not re-summarize."""
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )
        initial_summary_count = in_memory_session.query(DailySummary).count()

        stats = StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )
        assert stats["inserted"] == 0
        assert stats["days_summarized"] == 0
        assert in_memory_session.query(DailySummary).count() == initial_summary_count

    def test_resummarize_when_new_record_inserted(self, in_memory_session):
        """Incremental with at least one new record must re-summarize."""
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )

        stats = StubSyncer(in_memory_session, "derecho", [JOB_A, JOB_B]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )
        assert stats["inserted"] == 1
        assert stats["days_summarized"] == 1


class TestIncrementalNoUpdate:
    """Incremental must not modify existing job records."""

    def test_existing_record_not_updated(self, in_memory_session):
        """An existing job's fields must be unchanged after an incremental sync."""
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )

        # Fetch original queue value
        original_job = in_memory_session.query(Job).filter_by(job_id="job.A.1").first()
        original_queue = original_job.queue

        # Present the same job with a different queue
        modified = {**JOB_A, "queue": "premium"}
        StubSyncer(in_memory_session, "derecho", [modified]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )

        in_memory_session.expire(original_job)
        assert original_job.queue == original_queue, "incremental must not update existing records"

    def test_incremental_stats_updated_is_zero(self, in_memory_session):
        """Incremental mode must report 0 updated records."""
        StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
            log_dir=None, period=TARGET_DATE
        )
        stats = StubSyncer(in_memory_session, "derecho", [JOB_A, JOB_B]).sync(
            log_dir=None, period=TARGET_DATE, incremental=True
        )
        assert stats["updated"] == 0


class TestIncrementalUpsertConflict:
    """--incremental and --upsert are mutually exclusive."""

    def test_raises_when_both_set(self, in_memory_session):
        with pytest.raises(ValueError, match="mutually exclusive"):
            StubSyncer(in_memory_session, "derecho", [JOB_A]).sync(
                log_dir=None, period=TARGET_DATE, incremental=True, upsert=True
            )


class TestIncrementalCLIConflict:
    """CLI --incremental + --upsert raises click.Abort."""

    def test_cli_raises_abort(self):
        from click.testing import CliRunner
        from job_history.sync.cli import sync

        runner = CliRunner()
        result = runner.invoke(sync, ["-m", "derecho", "--incremental", "--upsert"])
        assert result.exit_code != 0
