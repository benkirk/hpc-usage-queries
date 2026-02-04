"""Tests for summary generation."""

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from qhist_db.models import Job, DailySummary
from qhist_db.summary import get_summarized_dates, generate_daily_summary


class TestGetSummarizedDates:
    """Tests for get_summarized_dates function."""

    def test_empty_database(self, in_memory_session):
        """Empty database should return empty set."""
        result = get_summarized_dates(in_memory_session)
        assert result == set()

    def test_with_summaries(self, in_memory_session):
        """Should return dates that have summaries."""
        summary1 = DailySummary(
            date=date(2025, 1, 15),
            user="user1",
            account="NCAR0001",
            queue="main",
            job_count=5,
        )
        summary2 = DailySummary(
            date=date(2025, 1, 16),
            user="user1",
            account="NCAR0001",
            queue="main",
            job_count=3,
        )
        in_memory_session.add_all([summary1, summary2])
        in_memory_session.commit()

        result = get_summarized_dates(in_memory_session)
        assert result == {date(2025, 1, 15), date(2025, 1, 16)}

    def test_distinct_dates(self, in_memory_session):
        """Should return unique dates even with multiple summaries per date."""
        summary1 = DailySummary(
            date=date(2025, 1, 15),
            user="user1",
            account="NCAR0001",
            queue="main",
            job_count=5,
        )
        summary2 = DailySummary(
            date=date(2025, 1, 15),
            user="user2",
            account="NCAR0002",
            queue="main",
            job_count=3,
        )
        in_memory_session.add_all([summary1, summary2])
        in_memory_session.commit()

        result = get_summarized_dates(in_memory_session)
        assert result == {date(2025, 1, 15)}


class TestGenerateDailySummary:
    """Tests for generate_daily_summary function."""

    @pytest.fixture
    def db_with_jobs_and_view(self, in_memory_engine, in_memory_session):
        """Create jobs and related records for testing."""
        from qhist_db.models import User, Account, Queue, JobCharge

        # Create lookup records
        user1 = User(username="user1")
        user2 = User(username="user2")
        acct1 = Account(account_name="NCAR0001")
        acct2 = Account(account_name="NCAR0002")
        queue1 = Queue(queue_name="main")
        queue2 = Queue(queue_name="develop")
        
        in_memory_session.add_all([user1, user2, acct1, acct2, queue1, queue2])
        in_memory_session.flush()  # To get IDs

        # Add test jobs
        job1 = Job(
            job_id="1.desched1",
            user="user1",
            account="NCAR0001",
            queue="main",
            user_id=user1.id,
            account_id=acct1.id,
            queue_id=queue1.id,
            end=datetime(2025, 1, 15, 18, 0, 0, tzinfo=timezone.utc),
            elapsed=3600,
            numnodes=2,
            numcpus=256,
            numgpus=0,
            memory=107374182400,
        )
        job2 = Job(
            job_id="2.desched1",
            user="user1",
            account="NCAR0001",
            queue="main",
            user_id=user1.id,
            account_id=acct1.id,
            queue_id=queue1.id,
            end=datetime(2025, 1, 15, 19, 0, 0, tzinfo=timezone.utc),
            elapsed=7200,
            numnodes=4,
            numcpus=512,
            numgpus=0,
            memory=214748364800,
        )
        job3 = Job(
            job_id="3.desched1",
            user="user2",
            account="NCAR0002",
            queue="develop",
            user_id=user2.id,
            account_id=acct2.id,
            queue_id=queue2.id,
            end=datetime(2025, 1, 15, 20, 0, 0, tzinfo=timezone.utc),
            elapsed=1800,
            numnodes=1,
            numcpus=32,
            numgpus=0,
            memory=32212254720,
        )
        in_memory_session.add_all([job1, job2, job3])
        in_memory_session.flush() # Get Job IDs

        # Add job charges
        charge1 = JobCharge(job_id=job1.id, cpu_hours=10.0, gpu_hours=0.0, memory_hours=10.0)
        charge2 = JobCharge(job_id=job2.id, cpu_hours=20.0, gpu_hours=0.0, memory_hours=20.0)
        charge3 = JobCharge(job_id=job3.id, cpu_hours=5.0, gpu_hours=0.0, memory_hours=5.0)
        
        in_memory_session.add_all([charge1, charge2, charge3])
        in_memory_session.commit()

        return in_memory_session

    def test_generate_summary(self, db_with_jobs_and_view):
        """Should generate summary from jobs ending on target date."""
        session = db_with_jobs_and_view
        result = generate_daily_summary(session, "derecho", date(2025, 1, 15))

        assert result["rows_inserted"] == 2  # user1/NCAR0001/main and user2/NCAR0002/develop

        # Verify summaries
        summaries = session.query(DailySummary).all()
        assert len(summaries) == 2

        # Check user1's summary (2 jobs)
        user1_summary = session.query(DailySummary).filter_by(
            user="user1", account="NCAR0001"
        ).first()
        assert user1_summary.job_count == 2

    def test_replace_existing(self, db_with_jobs_and_view):
        """replace=True should delete existing summaries first."""
        session = db_with_jobs_and_view

        # Generate initial summary
        generate_daily_summary(session, "derecho", date(2025, 1, 15))
        assert session.query(DailySummary).count() == 2

        # Generate again with replace
        result = generate_daily_summary(session, "derecho", date(2025, 1, 15), replace=True)
        assert result["rows_deleted"] == 2
        assert result["rows_inserted"] == 2
        assert session.query(DailySummary).count() == 2

    def test_skip_without_replace(self, db_with_jobs_and_view):
        """Without replace, should skip if summary exists."""
        session = db_with_jobs_and_view

        # Generate initial summary
        generate_daily_summary(session, "derecho", date(2025, 1, 15))

        # Try to generate again without replace
        result = generate_daily_summary(session, "derecho", date(2025, 1, 15), replace=False)
        assert result["rows_inserted"] == 0

    def test_no_jobs_on_date(self, db_with_jobs_and_view):
        """Should handle dates with no jobs."""
        session = db_with_jobs_and_view

        result = generate_daily_summary(session, "derecho", date(2025, 1, 14))
        assert result["rows_inserted"] == 0
