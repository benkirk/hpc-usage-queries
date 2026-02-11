"""Integration tests for PBS log parsing with small sample data."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from qhist_db.database import Base
from qhist_db.models import Job, Account, User, Queue, JobCharge
from qhist_db.pbs_local import fetch_jobs_from_pbs_logs
from qhist_db.sync import sync_pbs_logs_bulk, JobImporter


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

        # Should have parsed some jobs from 100-line sample
        assert len(jobs) > 0, "Should parse at least some jobs"

        # All jobs should have required fields
        for job in jobs:
            assert job["job_id"], "All jobs should have job_id"
            assert job["user"], "All jobs should have user"
            assert job["queue"], "All jobs should have queue"

        # At least some should have milan CPU type (derecho default)
        jobs_with_cputype = [j for j in jobs if j.get("cputype") == "milan"]
        assert len(jobs_with_cputype) > 0, "Should infer milan CPU type for derecho"

    def test_sync_derecho_to_database(self, test_db):
        """Sync Derecho sample to database."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/derecho"

        stats = sync_pbs_logs_bulk(
            session=test_db,
            machine="derecho",
            log_dir=str(fixture_dir),
            period="2026-01-29",
            verbose=False
        )

        # Should have fetched and inserted jobs
        assert stats["fetched"] > 0, "Should fetch jobs"
        assert stats["inserted"] > 0, "Should insert new jobs"
        assert stats["inserted"] == stats["fetched"] - stats["errors"], "Inserted = fetched - errors"

        # Verify jobs in database
        job_count = test_db.query(Job).count()
        assert job_count == stats["inserted"], "DB count should match inserted count"

        # Verify foreign keys were created
        user_count = test_db.query(User).count()
        account_count = test_db.query(Account).count()
        queue_count = test_db.query(Queue).count()

        assert user_count > 0, "Should create users"
        assert account_count > 0, "Should create accounts"
        assert queue_count > 0, "Should create queues"

        # Verify charges were calculated
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

        # Should have exactly 7 jobs from the sample
        assert len(jobs) == 7, "Should parse all 7 jobs from casper sample"

        # Check GPU type extraction
        gpu_jobs = [j for j in jobs if j.get("gputype")]
        assert len(gpu_jobs) == 4, "Should have 4 GPU jobs"

        # Verify specific GPU types
        gpu_types = {j["gputype"] for j in gpu_jobs}
        assert "a100" in gpu_types, "Should extract a100 GPU type"
        assert "v100" in gpu_types, "Should infer v100 from nvgpu queue"
        assert "h100" in gpu_types, "Should extract h100 GPU type"
        assert "l40" in gpu_types, "Should extract l40 GPU type"

        # Verify CPU jobs don't have GPU type
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

        # Find specific jobs by queue
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

        stats = sync_pbs_logs_bulk(
            session=test_db,
            machine="casper",
            log_dir=str(fixture_dir),
            period="2026-01-30",
            verbose=False
        )

        # Should have fetched and inserted all 7 jobs
        assert stats["fetched"] == 7, "Should fetch all 7 jobs"
        assert stats["inserted"] == 7, "Should insert all 7 jobs"
        assert stats["errors"] == 0, "Should have no errors"

        # Verify GPU types in database
        gpu_jobs = test_db.query(Job).filter(Job.gputype.isnot(None)).all()
        assert len(gpu_jobs) == 4, "Should have 4 GPU jobs in DB"

        gpu_types_in_db = {j.gputype for j in gpu_jobs}
        assert gpu_types_in_db == {"a100", "v100", "h100", "l40"}, "All GPU types should be preserved"


class TestJobImporter:
    """Tests for JobImporter with PBS data."""

    def test_importer_creates_normalized_records(self, test_db):
        """Verify JobImporter creates users, accounts, queues."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        # Fetch jobs
        jobs = list(fetch_jobs_from_pbs_logs(
            log_dir=fixture_dir,
            machine="casper",
            date="2026-01-30"
        ))

        # Use JobImporter
        importer = JobImporter(test_db, "casper")

        # Prepare records
        prepared = [importer.prepare_record(j) for j in jobs]

        # All should have foreign keys
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

        # Process all jobs
        for job in jobs:
            importer.prepare_record(job)

        # Count unique users/accounts/queues in database
        user_count = test_db.query(User).count()
        account_count = test_db.query(Account).count()
        queue_count = test_db.query(Queue).count()

        # Should be less than number of jobs (deduplication)
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

        # First sync
        stats1 = sync_pbs_logs_bulk(
            session=test_db,
            machine="casper",
            log_dir=str(fixture_dir),
            period="2026-01-30",
            verbose=False
        )

        # Second sync (should be idempotent)
        stats2 = sync_pbs_logs_bulk(
            session=test_db,
            machine="casper",
            log_dir=str(fixture_dir),
            period="2026-01-30",
            verbose=False,
            force=True  # Force re-sync
        )

        # First sync should insert jobs
        assert stats1["inserted"] == 7, "First sync should insert 7 jobs"

        # Second sync should insert 0 (duplicates detected)
        assert stats2["fetched"] == 7, "Second sync should still fetch 7 jobs"
        assert stats2["inserted"] == 0, "Second sync should insert 0 (duplicates)"

        # Database should still have only 7 jobs
        job_count = test_db.query(Job).count()
        assert job_count == 7, "Should still have only 7 jobs (no duplicates)"


class TestErrorHandling:
    """Tests for error handling."""

    def test_missing_log_file(self, test_db):
        """Verify graceful handling of missing log files."""
        fixture_dir = Path(__file__).parent / "fixtures/pbs_logs/casper"

        # Try to sync a date that doesn't exist
        # Should handle gracefully by marking day as failed
        stats = sync_pbs_logs_bulk(
            session=test_db,
            machine="casper",
            log_dir=str(fixture_dir),
            period="2026-01-01",  # This file doesn't exist
            verbose=False
        )

        # Should mark the day as failed
        assert stats["days_failed"] == 1, "Should mark day as failed"
        assert stats["fetched"] == 0, "Should not fetch any jobs"
        assert stats["inserted"] == 0, "Should not insert any jobs"

    def test_invalid_log_directory(self, test_db):
        """Verify error for non-existent log directory."""
        with pytest.raises(RuntimeError, match="PBS log directory not found"):
            sync_pbs_logs_bulk(
                session=test_db,
                machine="casper",
                log_dir="/nonexistent/path",
                period="2026-01-30",
                verbose=False
            )
