"""Tests for the JobQueries interface."""

import pytest
from datetime import date, datetime, timedelta, timezone

from qhist_db.models import Job, DailySummary
from qhist_db.queries import JobQueries


@pytest.fixture
def sample_jobs(in_memory_session):
    """Create sample jobs for testing queries."""
    base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    jobs = [
        # User alice, account NCAR0001
        Job(
            job_id="100.desched1",
            short_id=100,
            name="job1",
            user="alice",
            account="NCAR0001",
            queue="main",
            status="F",
            submit=base_time,
            end=base_time + timedelta(hours=1),
            elapsed=3600,
            numcpus=128,
            numgpus=0,
            numnodes=1,
        ),
        Job(
            job_id="101.desched1",
            short_id=101,
            name="job2",
            user="alice",
            account="NCAR0001",
            queue="main",
            status="F",
            submit=base_time + timedelta(hours=2),
            end=base_time + timedelta(hours=4),
            elapsed=7200,
            numcpus=256,
            numgpus=0,
            numnodes=2,
        ),
        # User bob, account NCAR0002
        Job(
            job_id="102.desched1",
            short_id=102,
            name="job3",
            user="bob",
            account="NCAR0002",
            queue="gpudev",
            status="F",
            submit=base_time + timedelta(days=1),
            end=base_time + timedelta(days=1, hours=1),
            elapsed=3600,
            numcpus=64,
            numgpus=4,
            numnodes=1,
        ),
        # User charlie, account NCAR0001, different queue
        Job(
            job_id="103.desched1",
            short_id=103,
            name="job4",
            user="charlie",
            account="NCAR0001",
            queue="develop",
            status="R",  # Running
            submit=base_time + timedelta(days=2),
            end=base_time + timedelta(days=2, hours=0.5),
            elapsed=1800,
            numcpus=32,
            numgpus=0,
            numnodes=1,
        ),
        # Older job for alice (outside typical range)
        Job(
            job_id="104.desched1",
            short_id=104,
            name="job5",
            user="alice",
            account="NCAR0001",
            queue="main",
            status="F",
            submit=base_time - timedelta(days=30),
            end=base_time - timedelta(days=30) + timedelta(hours=1),
            elapsed=3600,
            numcpus=128,
            numgpus=0,
            numnodes=1,
        ),
    ]

    for job in jobs:
        in_memory_session.add(job)

    in_memory_session.commit()
    return jobs


@pytest.fixture
def sample_daily_summaries(in_memory_session):
    """Create sample daily summaries for testing."""
    base_date = date(2025, 1, 15)

    summaries = [
        DailySummary(
            date=base_date,
            user="alice",
            account="NCAR0001",
            queue="main",
            job_count=5,
            charge_hours=100.0,
            cpu_hours=80.0,
            gpu_hours=0.0,
            memory_hours=50.0,
        ),
        DailySummary(
            date=base_date + timedelta(days=1),
            user="alice",
            account="NCAR0001",
            queue="main",
            job_count=3,
            charge_hours=60.0,
            cpu_hours=50.0,
            gpu_hours=0.0,
            memory_hours=30.0,
        ),
        DailySummary(
            date=base_date,
            user="bob",
            account="NCAR0002",
            queue="gpudev",
            job_count=2,
            charge_hours=40.0,
            cpu_hours=20.0,
            gpu_hours=10.0,
            memory_hours=15.0,
        ),
    ]

    for summary in summaries:
        in_memory_session.add(summary)

    in_memory_session.commit()
    return summaries


class TestJobQueries:
    """Test suite for JobQueries interface."""

    def test_init(self, in_memory_session):
        """Test JobQueries initialization."""
        queries = JobQueries(in_memory_session)
        assert queries.session == in_memory_session

    def test_jobs_by_user_basic(self, in_memory_session, sample_jobs):
        """Test basic user job query."""
        queries = JobQueries(in_memory_session)
        jobs = queries.jobs_by_user("alice")

        assert len(jobs) == 3
        assert all(j.user == "alice" for j in jobs)

    def test_jobs_by_user_with_date_range(self, in_memory_session, sample_jobs):
        """Test user job query with date filtering."""
        queries = JobQueries(in_memory_session)

        # Query for recent jobs only (exclude the 30-day-old job)
        start = date(2025, 1, 15)
        end = date(2025, 1, 20)

        jobs = queries.jobs_by_user("alice", start=start, end=end)

        assert len(jobs) == 2
        assert all(j.user == "alice" for j in jobs)
        assert all(j.end >= datetime.combine(start, datetime.min.time()) for j in jobs)
        assert all(j.end <= datetime.combine(end, datetime.max.time()) for j in jobs)

    def test_jobs_by_user_with_status(self, in_memory_session, sample_jobs):
        """Test user job query with status filter."""
        queries = JobQueries(in_memory_session)

        jobs = queries.jobs_by_user("alice", status="F")

        assert len(jobs) == 3
        assert all(j.status == "F" for j in jobs)

    def test_jobs_by_user_with_queue(self, in_memory_session, sample_jobs):
        """Test user job query with queue filter."""
        queries = JobQueries(in_memory_session)

        jobs = queries.jobs_by_user("alice", queue="main")

        assert len(jobs) == 3
        assert all(j.queue == "main" for j in jobs)

    def test_jobs_by_user_no_results(self, in_memory_session, sample_jobs):
        """Test user query with no matching results."""
        queries = JobQueries(in_memory_session)

        jobs = queries.jobs_by_user("nonexistent")

        assert len(jobs) == 0

    def test_jobs_by_account(self, in_memory_session, sample_jobs):
        """Test account job query."""
        queries = JobQueries(in_memory_session)

        jobs = queries.jobs_by_account("NCAR0001")

        assert len(jobs) == 4
        assert all(j.account == "NCAR0001" for j in jobs)

    def test_jobs_by_account_with_date_range(self, in_memory_session, sample_jobs):
        """Test account job query with date filtering."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 15)
        end = date(2025, 1, 20)

        jobs = queries.jobs_by_account("NCAR0001", start=start, end=end)

        assert len(jobs) == 3

    def test_jobs_by_queue(self, in_memory_session, sample_jobs):
        """Test queue job query."""
        queries = JobQueries(in_memory_session)

        jobs = queries.jobs_by_queue("main")

        assert len(jobs) == 3
        assert all(j.queue == "main" for j in jobs)

    def test_usage_summary_basic(self, in_memory_session, sample_jobs):
        """Test basic usage summary."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 15)
        end = date(2025, 1, 20)

        summary = queries.usage_summary("NCAR0001", start, end)

        assert summary["job_count"] == 3
        assert summary["total_elapsed_seconds"] == 3600 + 7200 + 1800
        assert summary["total_cpu_seconds"] == (128 * 3600) + (256 * 7200) + (32 * 1800)
        assert summary["total_gpu_seconds"] == 0
        assert "alice" in summary["users"]
        assert "charlie" in summary["users"]
        assert "main" in summary["queues"]
        assert "develop" in summary["queues"]

    def test_usage_summary_no_jobs(self, in_memory_session, sample_jobs):
        """Test usage summary with no matching jobs."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 1)
        end = date(2025, 1, 10)

        summary = queries.usage_summary("NCAR0001", start, end)

        assert summary["job_count"] == 0
        assert summary["total_elapsed_seconds"] == 0
        assert summary["total_cpu_seconds"] == 0
        assert summary["total_gpu_seconds"] == 0
        assert summary["users"] == []
        assert summary["queues"] == []

    def test_usage_summary_with_gpus(self, in_memory_session, sample_jobs):
        """Test usage summary with GPU jobs."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 16)
        end = date(2025, 1, 17)

        summary = queries.usage_summary("NCAR0002", start, end)

        assert summary["job_count"] == 1
        assert summary["total_gpu_seconds"] == 4 * 3600  # 4 GPUs * 1 hour

    def test_user_summary(self, in_memory_session, sample_jobs):
        """Test user summary."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 15)
        end = date(2025, 1, 20)

        summary = queries.user_summary("alice", start, end)

        assert summary["job_count"] == 2
        assert summary["total_elapsed_seconds"] == 3600 + 7200
        assert "NCAR0001" in summary["accounts"]
        assert "main" in summary["queues"]

    def test_user_summary_no_jobs(self, in_memory_session, sample_jobs):
        """Test user summary with no matching jobs."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 1)
        end = date(2025, 1, 10)

        summary = queries.user_summary("alice", start, end)

        assert summary["job_count"] == 0
        assert summary["accounts"] == []
        assert summary["queues"] == []

    def test_daily_summary_by_account(self, in_memory_session, sample_daily_summaries):
        """Test daily summary query by account."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 15)
        end = date(2025, 1, 16)

        summaries = queries.daily_summary_by_account("NCAR0001", start, end)

        assert len(summaries) == 2
        assert all(s.account == "NCAR0001" for s in summaries)
        assert summaries[0].date == date(2025, 1, 15)
        assert summaries[1].date == date(2025, 1, 16)

    def test_daily_summary_by_user(self, in_memory_session, sample_daily_summaries):
        """Test daily summary query by user."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 15)
        end = date(2025, 1, 16)

        summaries = queries.daily_summary_by_user("alice", start, end)

        assert len(summaries) == 2
        assert all(s.user == "alice" for s in summaries)

    def test_top_users_by_jobs(self, in_memory_session, sample_jobs):
        """Test top users query."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 15)
        end = date(2025, 1, 20)

        top_users = queries.top_users_by_jobs(start, end, limit=3)

        assert len(top_users) == 3
        assert top_users[0]["user"] == "alice"
        assert top_users[0]["job_count"] == 2
        assert top_users[1]["user"] in ["bob", "charlie"]
        assert all(u["job_count"] >= 1 for u in top_users)

    def test_top_users_with_limit(self, in_memory_session, sample_jobs):
        """Test top users query with limit."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 15)
        end = date(2025, 1, 20)

        top_users = queries.top_users_by_jobs(start, end, limit=1)

        assert len(top_users) == 1
        assert top_users[0]["user"] == "alice"

    def test_queue_statistics(self, in_memory_session, sample_jobs):
        """Test queue statistics."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 15)
        end = date(2025, 1, 20)

        stats = queries.queue_statistics(start, end)

        # Should have stats for main, gpudev, and develop
        assert len(stats) == 3

        # Find main queue stats
        main_stats = next(s for s in stats if s["queue"] == "main")
        assert main_stats["job_count"] == 2
        assert main_stats["total_elapsed_seconds"] == 3600 + 7200
        assert main_stats["avg_elapsed_seconds"] > 0

    def test_queue_statistics_no_jobs(self, in_memory_session, sample_jobs):
        """Test queue statistics with no jobs in range."""
        queries = JobQueries(in_memory_session)

        start = date(2025, 1, 1)
        end = date(2025, 1, 10)

        stats = queries.queue_statistics(start, end)

        assert len(stats) == 0

    def test_jobs_ordered_by_end_time(self, in_memory_session, sample_jobs):
        """Test that jobs are ordered by end time descending."""
        queries = JobQueries(in_memory_session)

        jobs = queries.jobs_by_user("alice")

        # Should be in descending order by end time
        for i in range(len(jobs) - 1):
            assert jobs[i].end >= jobs[i + 1].end
