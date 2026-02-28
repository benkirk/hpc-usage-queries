"""Integration tests for PBS log parsing with small sample data."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from job_history.database import Base
from job_history.database import Job, Account, User, Queue, JobCharge, JobRecord
from job_history.sync.pbs import fetch_jobs_from_pbs_logs
from job_history.sync import SyncPBSLogs, JobImporter


@pytest.fixture
def test_db():
    """Create a temporary test database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    session = Session(engine)
    yield session

    session.close()
    engine.dispose()
    Path(db_path).unlink()


class TestDerechodPBSParsing:
    """Tests for Derecho PBS log parsing."""

    def test_fetch_derecho_sample(self):
        """Fetch and parse small Derecho sample log."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/derecho"

        jobs = list(fetch_jobs_from_pbs_logs(
            log_dir=fixture_dir,
            machine="derecho",
            date="2026-01-29"
        ))

        assert len(jobs) > 0, "Should parse at least some jobs"

        for job in jobs:
            assert job["job_id"], "All jobs should have job_id"
            assert job["user"], "All jobs should have user"
            assert job["queue"], "All jobs should have queue"

        jobs_with_cputype = [j for j in jobs if j.get("cputype") == "milan"]
        assert len(jobs_with_cputype) > 0, "Should infer milan CPU type for derecho"

    def test_sync_derecho_to_database(self, test_db):
        """Sync Derecho sample to database."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/derecho"

        stats = SyncPBSLogs(test_db, "derecho").sync(
            log_dir=str(fixture_dir),
            period="2026-01-29",
            verbose=False
        )

        assert stats["fetched"] > 0, "Should fetch jobs"
        assert stats["inserted"] > 0, "Should insert new jobs"
        assert stats["inserted"] == stats["fetched"] - stats["errors"], "Inserted = fetched - errors"

        job_count = test_db.query(Job).count()
        assert job_count == stats["inserted"], "DB count should match inserted count"

        user_count = test_db.query(User).count()
        account_count = test_db.query(Account).count()
        queue_count = test_db.query(Queue).count()

        assert user_count > 0, "Should create users"
        assert account_count > 0, "Should create accounts"
        assert queue_count > 0, "Should create queues"

        charge_count = test_db.query(JobCharge).count()
        assert charge_count == stats["inserted"], "Should calculate charges for all jobs"


class TestCasperPBSParsing:
    """Tests for Casper PBS log parsing."""

    def test_fetch_casper_sample(self):
        """Fetch and parse small Casper sample log."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        jobs = list(fetch_jobs_from_pbs_logs(
            log_dir=fixture_dir,
            machine="casper",
            date="2026-01-30"
        ))

        assert len(jobs) == 7, "Should parse all 7 jobs from casper sample"

        gpu_jobs = [j for j in jobs if j.get("gputype")]
        assert len(gpu_jobs) == 4, "Should have 4 GPU jobs"

        gpu_types = {j["gputype"] for j in gpu_jobs}
        assert "a100" in gpu_types, "Should extract a100 GPU type"
        assert "v100" in gpu_types, "Should infer v100 from nvgpu queue"
        assert "h100" in gpu_types, "Should extract h100 GPU type"
        assert "l40" in gpu_types, "Should extract l40 GPU type"

        cpu_jobs = [j for j in jobs if not j.get("gputype")]
        assert len(cpu_jobs) == 3, "Should have 3 CPU-only jobs"

    def test_casper_gpu_type_extraction(self):
        """Verify GPU type extraction from select strings and queues."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        jobs = list(fetch_jobs_from_pbs_logs(
            log_dir=fixture_dir,
            machine="casper",
            date="2026-01-30"
        ))

        a100_job = next(j for j in jobs if j["queue"] == "a100")
        assert a100_job["gputype"] == "a100", "a100 queue should extract a100 GPU type"
        assert a100_job["numgpus"] == 1, "Should have 1 GPU"

        h100_job = next(j for j in jobs if j["queue"] == "h100")
        assert h100_job["gputype"] == "h100", "h100 queue should extract h100 GPU type"
        assert h100_job["numgpus"] == 4, "Should have 4 GPUs"

        nvgpu_job = next(j for j in jobs if j["queue"] == "nvgpu")
        assert nvgpu_job["gputype"] == "v100", "nvgpu queue should infer v100"

        l40_job = next(j for j in jobs if j["queue"] == "l40")
        assert l40_job["gputype"] == "l40", "l40 queue should extract l40 GPU type"

    def test_sync_casper_to_database(self, test_db):
        """Sync Casper sample to database."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        stats = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir),
            period="2026-01-30",
            verbose=False
        )

        assert stats["fetched"] == 7, "Should fetch all 7 jobs"
        assert stats["inserted"] == 7, "Should insert all 7 jobs"
        assert stats["errors"] == 0, "Should have no errors"

        gpu_jobs = test_db.query(Job).filter(Job.gputype.isnot(None)).all()
        assert len(gpu_jobs) == 4, "Should have 4 GPU jobs in DB"

        gpu_types_in_db = {j.gputype for j in gpu_jobs}
        assert gpu_types_in_db == {"a100", "v100", "h100", "l40"}, "All GPU types should be preserved"


class TestJobImporter:
    """Tests for JobImporter with PBS data."""

    def test_importer_creates_normalized_records(self, test_db):
        """Verify JobImporter creates users, accounts, queues."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        jobs = list(fetch_jobs_from_pbs_logs(
            log_dir=fixture_dir,
            machine="casper",
            date="2026-01-30"
        ))

        importer = JobImporter(test_db, "casper")
        prepared = [importer.prepare_record(j) for j in jobs]

        for record in prepared:
            assert "user_id" in record, "Should have user_id"
            assert "account_id" in record, "Should have account_id"
            assert "queue_id" in record, "Should have queue_id"
            assert record["user_id"] is not None, "user_id should be set"
            assert record["account_id"] is not None, "account_id should be set"
            assert record["queue_id"] is not None, "queue_id should be set"

    def test_importer_deduplicates_lookups(self, test_db):
        """Verify JobImporter caches users/accounts/queues."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        jobs = list(fetch_jobs_from_pbs_logs(
            log_dir=fixture_dir,
            machine="casper",
            date="2026-01-30"
        ))

        importer = JobImporter(test_db, "casper")
        for job in jobs:
            importer.prepare_record(job)

        user_count = test_db.query(User).count()
        account_count = test_db.query(Account).count()
        queue_count = test_db.query(Queue).count()

        assert user_count <= len(jobs), "Should deduplicate users"
        assert account_count <= len(jobs), "Should deduplicate accounts"
        assert queue_count <= len(jobs), "Should deduplicate queues"

        # Specifically for this sample:
        # 6 unique users (testuser1 appears twice)
        # 6 unique accounts
        # 5 unique queues (cpu, a100, nvgpu, h100, l40)
        assert user_count == 6, "Sample has 6 unique users"
        assert account_count == 6, "Sample has 6 unique accounts"
        assert queue_count == 5, "Sample has 5 unique queues"


class TestDuplicateHandling:
    """Tests for duplicate job detection."""

    def test_duplicate_prevention(self, test_db):
        """Verify re-syncing same data doesn't create duplicates."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        # First sync without generating a summary so the day is not marked
        # as summarized; the second sync then naturally re-runs the day.
        stats1 = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir),
            period="2026-01-30",
            generate_summary=False,
            verbose=False
        )

        stats2 = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir),
            period="2026-01-30",
            generate_summary=False,
            verbose=False
        )

        assert stats1["inserted"] == 7, "First sync should insert 7 jobs"
        assert stats2["fetched"] == 7, "Second sync should still fetch 7 jobs"
        assert stats2["inserted"] == 0, "Second sync should insert 0 (duplicates)"
        assert stats2["updated"] == 0, "Second sync should update 0 (no --upsert)"

        job_count = test_db.query(Job).count()
        assert job_count == 7, "Should still have only 7 jobs (no duplicates)"


class TestErrorHandling:
    """Tests for error handling."""

    def test_missing_log_file(self, test_db):
        """Verify graceful handling of missing log files."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        stats = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir),
            period="2099-01-01",  # This file doesn't exist
            verbose=False
        )

        assert stats["days_failed"] == 1, "Should mark day as failed"
        assert stats["fetched"] == 0, "Should not fetch any jobs"
        assert stats["inserted"] == 0, "Should not insert any jobs"

    def test_invalid_log_directory(self, test_db):
        """Verify error for non-existent log directory."""
        with pytest.raises(RuntimeError, match="Log directory not found"):
            SyncPBSLogs(test_db, "casper").sync(
                log_dir="/nonexistent/path",
                period="2026-01-30",
                verbose=False
            )


class TestUpsert:
    """Tests for upsert (update existing records) behavior."""

    def test_no_upsert_skips_existing(self, test_db):
        """Without --upsert, re-sync returns updated=0 and leaves records unchanged."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            generate_summary=False, verbose=False
        )

        # Corrupt a field to confirm it is NOT restored without upsert
        job = test_db.query(Job).first()
        original_elapsed = job.elapsed
        job.elapsed = -999
        test_db.commit()

        stats = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            generate_summary=False, verbose=False
        )

        assert stats["updated"] == 0, "No upsert → updated should be 0"
        test_db.refresh(job)
        assert job.elapsed == -999, "Field should remain unchanged without --upsert"

    def test_upsert_updates_job_fields(self, test_db):
        """After --upsert, a manually-dirtied job field is restored to parsed value."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            generate_summary=False, verbose=False
        )

        job = test_db.query(Job).first()
        original_elapsed = job.elapsed
        original_name = job.name
        job.elapsed = -999
        job.name = "CORRUPTED"
        test_db.commit()

        stats = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            generate_summary=False, upsert=True, verbose=False
        )

        assert stats["updated"] == 7, "All 7 existing jobs should be updated"
        assert stats["inserted"] == 0, "No new inserts expected"

        test_db.refresh(job)
        assert job.elapsed == original_elapsed, "elapsed should be restored to parsed value"
        assert job.name == original_name, "name should be restored to parsed value"

    def test_upsert_recalculates_charges(self, test_db):
        """After --upsert, a manually-zeroed charge is restored to calculated value."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            generate_summary=False, verbose=False
        )

        # Verify charges exist, then zero them out
        charges_before = test_db.query(JobCharge).all()
        assert len(charges_before) == 7
        original_cpu = charges_before[0].cpu_hours
        for c in charges_before:
            c.cpu_hours = 0.0
        test_db.commit()

        SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            generate_summary=False, upsert=True, verbose=False
        )

        charges_after = test_db.query(JobCharge).all()
        assert len(charges_after) == 7, "Should still have 7 charge rows"
        assert any(c.cpu_hours > 0 for c in charges_after), "Charges should be recalculated"

    def test_upsert_replaces_job_records(self, test_db):
        """After --upsert, JobRecord rows are replaced with fresh-parsed raw records."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            generate_summary=False, verbose=False
        )

        record_count_before = test_db.query(JobRecord).count()
        assert record_count_before == 7, "Should have 7 raw records after initial sync"

        SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            generate_summary=False, upsert=True, verbose=False
        )

        record_count_after = test_db.query(JobRecord).count()
        assert record_count_after == 7, "Should still have 7 raw records after upsert"

    def test_upsert_bypasses_summarized_skip(self, test_db):
        """--upsert re-parses days even when already summarized."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        # First sync WITH summary generation
        stats1 = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30", verbose=False
        )
        assert stats1["days_summarized"] == 1

        # Without upsert: day is skipped
        stats_no_upsert = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30", verbose=False
        )
        assert stats_no_upsert["days_skipped"] == 1, "Day should be skipped without upsert"

        # With upsert: day is re-parsed
        stats_upsert = SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30",
            upsert=True, verbose=False
        )
        assert stats_upsert["days_skipped"] == 0, "Upsert should bypass summarized skip"
        assert stats_upsert["fetched"] == 7, "Should re-fetch all 7 jobs"
        assert stats_upsert["updated"] == 7, "Should update all 7 existing jobs"


class TestResummarize:
    """Tests for --resummarize (summary-only recompute)."""

    def test_resummarize_regenerates_summary(self, test_db):
        """--resummarize recomputes daily_summary from current Job/JobCharge data."""
        from job_history.database import DailySummary
        from datetime import date as dt_date

        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        # Initial sync to populate jobs and a summary
        SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30", verbose=False
        )

        summary_count = test_db.query(DailySummary).filter(
            DailySummary.date == dt_date(2026, 1, 30)
        ).count()
        assert summary_count > 0, "Initial sync should create summary rows"

        # Delete the summary to simulate corruption
        test_db.query(DailySummary).filter(
            DailySummary.date == dt_date(2026, 1, 30)
        ).delete()
        test_db.commit()

        # Resummarize without any log path
        stats = SyncPBSLogs(test_db, "casper").sync(
            log_dir=None,
            period="2026-01-30",
            resummarize_only=True,
            verbose=False
        )

        assert stats["days_summarized"] == 1, "Should regenerate summary for 1 day"
        assert stats["fetched"] == 0, "Should not fetch any records"
        assert stats["inserted"] == 0, "Should not insert any records"

        restored_count = test_db.query(DailySummary).filter(
            DailySummary.date == dt_date(2026, 1, 30)
        ).count()
        assert restored_count > 0, "Summary should be restored by --resummarize"

    def test_resummarize_does_not_need_log_path(self, test_db):
        """--resummarize works without a log directory."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"
        SyncPBSLogs(test_db, "casper").sync(
            log_dir=str(fixture_dir), period="2026-01-30", verbose=False
        )

        # Pass log_dir=None — should not raise
        stats = SyncPBSLogs(test_db, "casper").sync(
            log_dir=None,
            period="2026-01-30",
            resummarize_only=True,
            verbose=False
        )
        assert stats["days_summarized"] == 1
