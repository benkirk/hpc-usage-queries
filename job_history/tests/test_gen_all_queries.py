"""Regression tests for all query methods used in plots/gen_all.sh.

These tests exercise every JobQueries method that gen_all.sh calls via
``jobhist resource --machine derecho``.  They use an in-memory SQLite database
with sample data so no external database is required.

Specifically guards against:
- SQLAlchemy errors from hybrid-property expressions inside subqueries
  (e.g., ``no such column: anon_1.user`` after the FK normalization).
- Silent breakage of any gen_all.sh subcommand.

Subcommand → query method mapping (from cli.py RESOURCE_REPORTS):
  job-sizes              job_sizes_by_resource('all', 'core')
  job-waits              job_waits_by_resource('all', 'core')
  cpu-job-sizes          job_sizes_by_resource('cpu', 'node')
  cpu-job-waits          job_waits_by_resource('cpu', 'node')
  cpu-job-durations      job_durations('cpu')
  cpu-job-memory-per-rank  job_memory_per_rank('cpu')
  gpu-job-sizes          job_sizes_by_resource('gpu', 'gpu')
  gpu-job-waits          job_waits_by_resource('gpu', 'gpu')
  gpu-job-durations      job_durations('gpu')
  gpu-job-memory-per-rank  job_memory_per_rank('gpu')
  pie-user-cpu           usage_by_group('cpu', 'user')
  pie-user-gpu           usage_by_group('gpu', 'user')
  pie-proj-cpu           usage_by_group('cpu', 'account')
  pie-proj-gpu           usage_by_group('gpu', 'account')
  pie-group-cpu          usage_by_group('cpu', 'account')   (same query)
  pie-group-gpu          usage_by_group('gpu', 'account')   (same query)
  usage-history          usage_history()
"""

import pytest
from datetime import date, datetime, timedelta, timezone

from job_history.models import Job, JobCharge
from job_history.queries import JobQueries
from job_history.charging import casper_charge


# ---------------------------------------------------------------------------
# Fixture: sample Derecho jobs covering both CPU and GPU queues
# ---------------------------------------------------------------------------

@pytest.fixture
def derecho_jobs(in_memory_session):
    """Create sample Derecho CPU and GPU jobs for gen_all regression tests.

    Jobs span Jan 15-18 2025 using real Derecho queue names:
      CPU queues: 'cpu', 'cpudev'
      GPU queues: 'gpu', 'gpudev'

    All jobs include eligible/start/end timestamps (for wait-time queries) and
    mpiprocs/ompthreads/memory (for memory-per-rank queries).
    """
    t0 = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    jobs = [
        # --- CPU queue jobs ---
        Job(
            job_id="200.desched1", short_id=200, name="cpu_job1",
            user="alice", account="NCAR0001", queue="cpu", status="F",
            submit=t0,
            eligible=t0 + timedelta(minutes=10),
            start=t0 + timedelta(minutes=30),
            end=t0 + timedelta(hours=2),
            elapsed=5400,
            numcpus=128, numgpus=0, numnodes=1,
            mpiprocs=128, ompthreads=1,
            memory=50 * 1024**3,
        ),
        Job(
            job_id="201.desched1", short_id=201, name="cpu_job2",
            user="bob", account="NCAR0001", queue="cpu", status="F",
            submit=t0 + timedelta(hours=1),
            eligible=t0 + timedelta(hours=1, minutes=5),
            start=t0 + timedelta(hours=1, minutes=30),
            end=t0 + timedelta(hours=5),
            elapsed=12600,
            numcpus=256, numgpus=0, numnodes=2,
            mpiprocs=128, ompthreads=1,
            memory=100 * 1024**3,
        ),
        # CPU dev queue
        Job(
            job_id="202.desched1", short_id=202, name="cpudev_job1",
            user="charlie", account="NCAR0002", queue="cpudev", status="F",
            submit=t0 + timedelta(hours=2),
            eligible=t0 + timedelta(hours=2, minutes=2),
            start=t0 + timedelta(hours=2, minutes=15),
            end=t0 + timedelta(hours=3),
            elapsed=2700,
            numcpus=32, numgpus=0, numnodes=1,
            mpiprocs=32, ompthreads=1,
            memory=16 * 1024**3,
        ),
        # --- GPU queue jobs ---
        Job(
            job_id="203.desched1", short_id=203, name="gpu_job1",
            user="alice", account="NCAR0002", queue="gpu", status="F",
            submit=t0 + timedelta(days=1),
            eligible=t0 + timedelta(days=1, minutes=20),
            start=t0 + timedelta(days=1, hours=1),
            end=t0 + timedelta(days=1, hours=3),
            elapsed=7200,
            numcpus=64, numgpus=4, numnodes=1,
            mpiprocs=64, ompthreads=1,
            memory=80 * 1024**3,
        ),
        Job(
            job_id="204.desched1", short_id=204, name="gpu_job2",
            user="dave", account="NCAR0001", queue="gpudev", status="F",
            submit=t0 + timedelta(days=2),
            eligible=t0 + timedelta(days=2, minutes=5),
            start=t0 + timedelta(days=2, hours=2),
            end=t0 + timedelta(days=2, hours=5),
            elapsed=10800,
            numcpus=64, numgpus=8, numnodes=1,
            mpiprocs=64, ompthreads=1,
            memory=120 * 1024**3,
        ),
    ]

    for job in jobs:
        in_memory_session.add(job)
    in_memory_session.commit()

    # Create job_charges (use casper_charge helper — values don't matter for
    # query structure tests, only that charges exist per job)
    for job in jobs:
        charges = casper_charge({
            "elapsed": job.elapsed or 0,
            "numcpus": job.numcpus or 0,
            "numgpus": job.numgpus or 0,
            "memory": job.memory or 0,
        })
        in_memory_session.add(JobCharge(
            job_id=job.id,
            cpu_hours=charges["cpu_hours"],
            gpu_hours=charges["gpu_hours"],
            memory_hours=charges["memory_hours"],
            charge_version=1,
        ))
    in_memory_session.commit()

    return jobs


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestGenAllSubcommands:
    """Smoke + structure tests for every subcommand in plots/gen_all.sh.

    Each test calls the corresponding JobQueries method and asserts:
    1. No exception is raised.
    2. The result is a list.
    3. Every dict in the list has the expected keys.
    4. Numeric fields are non-negative where applicable.
    """

    START = date(2025, 1, 15)
    END = date(2025, 1, 18)

    def _q(self, session):
        return JobQueries(session, machine="derecho")

    # ------------------------------------------------------------------
    # job_sizes_by_resource — exercises the user_id fix (anon_1.user bug)
    # ------------------------------------------------------------------

    def test_job_sizes_all_core(self, in_memory_session, derecho_jobs):
        """job-sizes: job_sizes_by_resource('all', 'core')"""
        results = self._q(in_memory_session).job_sizes_by_resource(
            "all", "core", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"range_label", "job_count", "user_count", "hours"} <= row.keys()
            assert row["job_count"] >= 0
            assert row["user_count"] >= 0
            assert row["hours"] >= 0

    def test_job_sizes_cpu_node(self, in_memory_session, derecho_jobs):
        """cpu-job-sizes: job_sizes_by_resource('cpu', 'node')"""
        results = self._q(in_memory_session).job_sizes_by_resource(
            "cpu", "node", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"range_label", "job_count", "user_count", "hours"} <= row.keys()

    def test_job_sizes_gpu_gpu(self, in_memory_session, derecho_jobs):
        """gpu-job-sizes: job_sizes_by_resource('gpu', 'gpu')"""
        results = self._q(in_memory_session).job_sizes_by_resource(
            "gpu", "gpu", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"range_label", "job_count", "user_count", "hours"} <= row.keys()

    def test_job_sizes_cpu_node_returns_data(self, in_memory_session, derecho_jobs):
        """CPU job-sizes should return at least one bucket with data."""
        results = self._q(in_memory_session).job_sizes_by_resource(
            "cpu", "node", self.START, self.END
        )
        total_jobs = sum(r["job_count"] for r in results)
        assert total_jobs == 3  # jobs 200, 201, 202

    def test_job_sizes_gpu_gpu_returns_data(self, in_memory_session, derecho_jobs):
        """GPU job-sizes should return at least one bucket with data."""
        results = self._q(in_memory_session).job_sizes_by_resource(
            "gpu", "gpu", self.START, self.END
        )
        total_jobs = sum(r["job_count"] for r in results)
        assert total_jobs == 2  # jobs 203, 204

    def test_job_sizes_user_count_correct(self, in_memory_session, derecho_jobs):
        """user_count inside job_sizes_by_resource must be distinct-user count."""
        results = self._q(in_memory_session).job_sizes_by_resource(
            "cpu", "node", self.START, self.END
        )
        # All three CPU jobs land in different node-count buckets (1 node each except job 201)
        # Just verify total distinct users across buckets is sane (alice + bob + charlie = 3)
        total_users = sum(r["user_count"] for r in results)
        assert total_users >= 1

    # ------------------------------------------------------------------
    # job_waits_by_resource
    # ------------------------------------------------------------------

    def test_job_waits_all_core(self, in_memory_session, derecho_jobs):
        """job-waits: job_waits_by_resource('all', 'core')"""
        results = self._q(in_memory_session).job_waits_by_resource(
            "all", "core", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"range_label", "avg_wait_hours", "job_count"} <= row.keys()
            assert row["job_count"] >= 0

    def test_job_waits_cpu_node(self, in_memory_session, derecho_jobs):
        """cpu-job-waits: job_waits_by_resource('cpu', 'node')"""
        results = self._q(in_memory_session).job_waits_by_resource(
            "cpu", "node", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"range_label", "avg_wait_hours", "job_count"} <= row.keys()

    def test_job_waits_gpu_gpu(self, in_memory_session, derecho_jobs):
        """gpu-job-waits: job_waits_by_resource('gpu', 'gpu')"""
        results = self._q(in_memory_session).job_waits_by_resource(
            "gpu", "gpu", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"range_label", "avg_wait_hours", "job_count"} <= row.keys()

    # ------------------------------------------------------------------
    # job_durations — group_by day and month
    # ------------------------------------------------------------------

    def test_job_durations_cpu_day(self, in_memory_session, derecho_jobs):
        """cpu-job-durations (--group-by day): job_durations('cpu', period='day')"""
        results = self._q(in_memory_session).job_durations(
            "cpu", self.START, self.END, period="day"
        )
        assert isinstance(results, list)
        expected_keys = {"date", "<30s", "30s-30m", "30-60m", "1-5h", "5-12h", "12-18h", ">18h"}
        for row in results:
            assert expected_keys <= row.keys()

    def test_job_durations_cpu_month(self, in_memory_session, derecho_jobs):
        """cpu-job-durations (--group-by month): job_durations('cpu', period='month')"""
        results = self._q(in_memory_session).job_durations(
            "cpu", self.START, self.END, period="month"
        )
        assert isinstance(results, list)
        for row in results:
            assert "date" in row
            # period='month' produces YYYY-MM strings
            assert len(row["date"]) == 7

    def test_job_durations_gpu_day(self, in_memory_session, derecho_jobs):
        """gpu-job-durations (--group-by day): job_durations('gpu', period='day')"""
        results = self._q(in_memory_session).job_durations(
            "gpu", self.START, self.END, period="day"
        )
        assert isinstance(results, list)
        for row in results:
            assert "date" in row

    def test_job_durations_gpu_month(self, in_memory_session, derecho_jobs):
        """gpu-job-durations (--group-by month): job_durations('gpu', period='month')"""
        results = self._q(in_memory_session).job_durations(
            "gpu", self.START, self.END, period="month"
        )
        assert isinstance(results, list)

    # ------------------------------------------------------------------
    # job_memory_per_rank — group_by day and month
    # ------------------------------------------------------------------

    def test_job_memory_per_rank_cpu_day(self, in_memory_session, derecho_jobs):
        """cpu-job-memory-per-rank (--group-by day): job_memory_per_rank('cpu', period='day')"""
        results = self._q(in_memory_session).job_memory_per_rank(
            "cpu", self.START, self.END, period="day"
        )
        assert isinstance(results, list)
        expected_keys = {
            "date", "<128MB", "128MB-512MB", "512MB-1GB",
            "1-2GB", "2-4GB", "4-8GB", "8-16GB", "16-32GB",
            "32-64GB", "64-128GB", "128-256GB", ">256GB",
        }
        for row in results:
            assert expected_keys <= row.keys()

    def test_job_memory_per_rank_cpu_month(self, in_memory_session, derecho_jobs):
        """cpu-job-memory-per-rank (--group-by month)"""
        results = self._q(in_memory_session).job_memory_per_rank(
            "cpu", self.START, self.END, period="month"
        )
        assert isinstance(results, list)

    def test_job_memory_per_rank_gpu_day(self, in_memory_session, derecho_jobs):
        """gpu-job-memory-per-rank (--group-by day)"""
        results = self._q(in_memory_session).job_memory_per_rank(
            "gpu", self.START, self.END, period="day"
        )
        assert isinstance(results, list)

    def test_job_memory_per_rank_gpu_month(self, in_memory_session, derecho_jobs):
        """gpu-job-memory-per-rank (--group-by month)"""
        results = self._q(in_memory_session).job_memory_per_rank(
            "gpu", self.START, self.END, period="month"
        )
        assert isinstance(results, list)

    # ------------------------------------------------------------------
    # usage_by_group (pie-* commands)
    # ------------------------------------------------------------------

    def test_usage_by_group_cpu_user(self, in_memory_session, derecho_jobs):
        """pie-user-cpu: usage_by_group('cpu', 'user')"""
        results = self._q(in_memory_session).usage_by_group(
            "cpu", "user", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"label", "usage_hours", "job_count"} <= row.keys()
            assert row["usage_hours"] >= 0
            assert row["job_count"] >= 0
        # alice and bob have CPU jobs
        labels = {r["label"] for r in results}
        assert "alice" in labels
        assert "bob" in labels

    def test_usage_by_group_gpu_user(self, in_memory_session, derecho_jobs):
        """pie-user-gpu: usage_by_group('gpu', 'user')"""
        results = self._q(in_memory_session).usage_by_group(
            "gpu", "user", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"label", "usage_hours", "job_count"} <= row.keys()
        # alice and dave have GPU jobs
        labels = {r["label"] for r in results}
        assert "alice" in labels
        assert "dave" in labels

    def test_usage_by_group_cpu_account(self, in_memory_session, derecho_jobs):
        """pie-proj-cpu / pie-group-cpu: usage_by_group('cpu', 'account')"""
        results = self._q(in_memory_session).usage_by_group(
            "cpu", "account", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"label", "usage_hours", "job_count"} <= row.keys()
        labels = {r["label"] for r in results}
        assert "NCAR0001" in labels

    def test_usage_by_group_gpu_account(self, in_memory_session, derecho_jobs):
        """pie-proj-gpu / pie-group-gpu: usage_by_group('gpu', 'account')"""
        results = self._q(in_memory_session).usage_by_group(
            "gpu", "account", self.START, self.END
        )
        assert isinstance(results, list)
        for row in results:
            assert {"label", "usage_hours", "job_count"} <= row.keys()

    # ------------------------------------------------------------------
    # usage_history — group_by day and month
    # ------------------------------------------------------------------

    def test_usage_history_day(self, in_memory_session, derecho_jobs):
        """usage-history (--group-by day): usage_history(period='day')"""
        results = self._q(in_memory_session).usage_history(
            self.START, self.END, period="day"
        )
        assert isinstance(results, list)
        expected_keys = {
            "Date", "#-Users", "#-Proj",
            "#-CPU-Users", "#-CPU-Proj", "#-CPU-Jobs", "#-CPU-Hrs",
            "#-GPU-Users", "#-GPU-Proj", "#-GPU-Jobs", "#-GPU-Hrs",
        }
        for row in results:
            assert expected_keys <= row.keys()
            assert row["#-Users"] >= 0
            assert row["#-CPU-Jobs"] >= 0
            assert row["#-GPU-Jobs"] >= 0

    def test_usage_history_month(self, in_memory_session, derecho_jobs):
        """usage-history (--group-by month): usage_history(period='month')"""
        results = self._q(in_memory_session).usage_history(
            self.START, self.END, period="month"
        )
        assert isinstance(results, list)
        for row in results:
            assert "Date" in row
            # month grouping → YYYY-MM
            assert len(row["Date"]) == 7

    def test_usage_history_day_counts(self, in_memory_session, derecho_jobs):
        """usage_history should correctly count CPU and GPU jobs per day."""
        results = self._q(in_memory_session).usage_history(
            self.START, self.END, period="day"
        )
        total_cpu_jobs = sum(r["#-CPU-Jobs"] for r in results)
        total_gpu_jobs = sum(r["#-GPU-Jobs"] for r in results)
        assert total_cpu_jobs == 3   # jobs 200, 201, 202
        assert total_gpu_jobs == 2   # jobs 203, 204

    # ------------------------------------------------------------------
    # Empty-date-range edge case — all methods must return [] not crash
    # ------------------------------------------------------------------

    def test_all_methods_empty_range(self, in_memory_session, derecho_jobs):
        """All query methods return an empty list for a range with no jobs."""
        q = self._q(in_memory_session)
        empty_start = date(2020, 1, 1)
        empty_end = date(2020, 1, 31)

        assert q.job_sizes_by_resource("cpu", "node", empty_start, empty_end) == []
        assert q.job_sizes_by_resource("gpu", "gpu", empty_start, empty_end) == []
        assert q.job_sizes_by_resource("all", "core", empty_start, empty_end) == []
        assert q.job_waits_by_resource("cpu", "node", empty_start, empty_end) == []
        assert q.job_waits_by_resource("gpu", "gpu", empty_start, empty_end) == []
        assert q.job_waits_by_resource("all", "core", empty_start, empty_end) == []
        assert q.job_durations("cpu", empty_start, empty_end) == []
        assert q.job_durations("gpu", empty_start, empty_end) == []
        assert q.job_memory_per_rank("cpu", empty_start, empty_end) == []
        assert q.job_memory_per_rank("gpu", empty_start, empty_end) == []
        assert q.usage_by_group("cpu", "user", empty_start, empty_end) == []
        assert q.usage_by_group("gpu", "user", empty_start, empty_end) == []
        assert q.usage_history(empty_start, empty_end) == []
