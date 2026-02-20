"""Tests for ORM models."""

from datetime import datetime, timezone

import pytest
from sqlalchemy.exc import IntegrityError

from job_history.models import Job, DailySummary, JobRecord


# Mock PbsRecord class for testing (must be module-level to be pickleable)
class MockPbsRecord:
    """Mock PbsRecord object for testing."""

    def __init__(self, job_id="123456.desched1", user="testuser", queue="cpu"):
        self.id = job_id
        self.user = user
        self.queue = queue


class TestJobModel:
    """Tests for Job model."""

    def test_create_job(self, in_memory_session):
        """Should create a job record with all fields."""
        job = Job(
            job_id="123456.desched1",
            short_id=123456,
            name="test_job",
            user="testuser",
            account="NCAR0001",
            queue="main",
            status="0",
            submit=datetime(2025, 1, 15, 17, 0, 0, tzinfo=timezone.utc),
            elapsed=3600,
            numcpus=256,
            numnodes=2,
        )
        in_memory_session.add(job)
        in_memory_session.commit()

        # Verify retrieval
        retrieved = in_memory_session.query(Job).filter_by(job_id="123456.desched1").first()
        assert retrieved is not None
        assert retrieved.user == "testuser"
        assert retrieved.numcpus == 256

    def test_unique_constraint(self, in_memory_session):
        """Same job_id + submit should raise IntegrityError."""
        submit_time = datetime(2025, 1, 15, 17, 0, 0, tzinfo=timezone.utc)

        job1 = Job(job_id="123456.desched1", submit=submit_time, user="user1")
        in_memory_session.add(job1)
        in_memory_session.commit()

        # Attempt to add duplicate
        job2 = Job(job_id="123456.desched1", submit=submit_time, user="user2")
        in_memory_session.add(job2)

        with pytest.raises(IntegrityError):
            in_memory_session.commit()

    def test_same_job_id_different_submit(self, in_memory_session):
        """Same job_id with different submit times should be allowed (job ID reuse)."""
        job1 = Job(
            job_id="123456.desched1",
            submit=datetime(2024, 1, 15, 17, 0, 0, tzinfo=timezone.utc),
            user="user1",
        )
        job2 = Job(
            job_id="123456.desched1",
            submit=datetime(2025, 1, 15, 17, 0, 0, tzinfo=timezone.utc),
            user="user2",
        )

        in_memory_session.add_all([job1, job2])
        in_memory_session.commit()

        count = in_memory_session.query(Job).filter_by(job_id="123456.desched1").count()
        assert count == 2

    def test_to_dict(self, in_memory_session):
        """to_dict should return all column values."""
        job = Job(
            job_id="123456.desched1",
            short_id=123456,
            user="testuser",
            account="NCAR0001",
        )
        in_memory_session.add(job)
        in_memory_session.commit()

        job_dict = job.to_dict()
        assert job_dict["job_id"] == "123456.desched1"
        assert job_dict["user"] == "testuser"
        assert "id" in job_dict  # auto-generated PK

    def test_nullable_fields(self, in_memory_session):
        """Most fields should be nullable."""
        job = Job(job_id="minimal.job")
        in_memory_session.add(job)
        in_memory_session.commit()

        retrieved = in_memory_session.query(Job).first()
        assert retrieved.user is None
        assert retrieved.elapsed is None
        assert retrieved.memory is None


class TestDailySummaryModel:
    """Tests for DailySummary model."""

    def test_create_summary(self, in_memory_session):
        """Should create a daily summary record."""
        from datetime import date

        summary = DailySummary(
            date=date(2025, 1, 15),
            user="testuser",
            account="NCAR0001",
            queue="main",
            job_count=10,
            cpu_hours=100.0,
            gpu_hours=0.0,
            memory_hours=500.0,
        )
        in_memory_session.add(summary)
        in_memory_session.commit()

        retrieved = in_memory_session.query(DailySummary).first()
        assert retrieved.job_count == 10
        assert retrieved.cpu_hours == 100.0

    def test_unique_constraint(self, in_memory_session):
        """Same (date, user, account, queue) should raise IntegrityError."""
        from datetime import date

        summary1 = DailySummary(
            date=date(2025, 1, 15),
            user="testuser",
            account="NCAR0001",
            queue="main",
            job_count=10,
        )
        in_memory_session.add(summary1)
        in_memory_session.commit()

        summary2 = DailySummary(
            date=date(2025, 1, 15),
            user="testuser",
            account="NCAR0001",
            queue="main",
            job_count=20,
        )
        in_memory_session.add(summary2)

        with pytest.raises(IntegrityError):
            in_memory_session.commit()

    def test_different_queues_allowed(self, in_memory_session):
        """Same user/account/date with different queues should be allowed."""
        from datetime import date

        summary1 = DailySummary(
            date=date(2025, 1, 15),
            user="testuser",
            account="NCAR0001",
            queue="main",
            job_count=10,
        )
        summary2 = DailySummary(
            date=date(2025, 1, 15),
            user="testuser",
            account="NCAR0001",
            queue="develop",
            job_count=5,
        )

        in_memory_session.add_all([summary1, summary2])
        in_memory_session.commit()

        count = in_memory_session.query(DailySummary).count()
        assert count == 2


class TestJobRecordModel:
    """Tests for JobRecord model."""

    def test_job_record_round_trip(self, in_memory_session):
        """Test pickle → compress → store → retrieve → decompress → unpickle."""
        import pickle
        import gzip

        pbs_record = MockPbsRecord()

        # Create Job
        job = Job(job_id="123456.desched1", submit=datetime.now(timezone.utc))
        job.user = "testuser"
        in_memory_session.add(job)
        in_memory_session.flush()

        # Create JobRecord
        compressed = gzip.compress(pickle.dumps(pbs_record))
        job_record = JobRecord(job_id=job.id, compressed_data=compressed)
        in_memory_session.add(job_record)
        in_memory_session.commit()

        # Retrieve via property
        retrieved_record = job.pbs_record
        assert retrieved_record is not None
        assert retrieved_record.id == "123456.desched1"
        assert retrieved_record.user == "testuser"

    def test_job_without_record(self, in_memory_session):
        """Jobs without JobRecord should return None."""
        job = Job(job_id="ssh.job", submit=datetime.now(timezone.utc))
        job.user = "sshuser"
        in_memory_session.add(job)
        in_memory_session.commit()

        assert job.pbs_record is None

    def test_pbs_record_caching(self, in_memory_session):
        """Verify instance-level caching works."""
        import pickle
        import gzip

        pbs_record = MockPbsRecord(job_id="cached.123")
        job = Job(job_id="cached.123", submit=datetime.now(timezone.utc))
        job.user = "cacheuser"
        in_memory_session.add(job)
        in_memory_session.flush()

        compressed = gzip.compress(pickle.dumps(pbs_record))
        job_record = JobRecord(job_id=job.id, compressed_data=compressed)
        in_memory_session.add(job_record)
        in_memory_session.commit()

        # First access - will decompress
        record1 = job.pbs_record
        # Second access - should use cache
        record2 = job.pbs_record

        # Verify same object returned (cache hit)
        assert record1 is record2

    def test_from_pbs_record_class_method(self, in_memory_session):
        """Test JobRecord.from_pbs_record() class method."""
        pbs_record = MockPbsRecord(job_id="method.test", user="methoduser", queue="cpu")

        # Create Job
        job = Job(job_id="method.test", submit=datetime.now(timezone.utc))
        job.user = "methoduser"
        in_memory_session.add(job)
        in_memory_session.flush()

        # Use class method to create JobRecord
        job_record = JobRecord.from_pbs_record(job.id, pbs_record)
        in_memory_session.add(job_record)
        in_memory_session.commit()

        # Verify it was created and can be retrieved
        assert job_record.job_id == job.id
        assert job_record.compressed_data is not None
        assert len(job_record.compressed_data) > 0

    def test_to_pbs_record_method(self, in_memory_session):
        """Test JobRecord.to_pbs_record() method."""
        pbs_record = MockPbsRecord(job_id="to.test", user="touser", queue="gpu")

        # Create Job and JobRecord using class method
        job = Job(job_id="to.test", submit=datetime.now(timezone.utc))
        job.user = "touser"
        in_memory_session.add(job)
        in_memory_session.flush()

        job_record = JobRecord.from_pbs_record(job.id, pbs_record)
        in_memory_session.add(job_record)
        in_memory_session.commit()

        # Use instance method to retrieve
        retrieved = job_record.to_pbs_record()
        assert retrieved is not None
        assert retrieved.id == "to.test"
        assert retrieved.user == "touser"
        assert retrieved.queue == "gpu"
