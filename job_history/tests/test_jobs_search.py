"""Tests for the programmatic ``JobQueries.jobs_search`` API.

This is the contract SAM (project_samuel) and other consumers will rely on,
mirroring the dict-row shape of ``daily_summary_report``.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from job_history.database import Job, JobCharge
from job_history.queries import JobQueries
from job_history.cli.search.columns import COLUMNS, DEFAULT_COLUMNS


@pytest.fixture
def search_jobs(in_memory_session):
    """Three jobs across two users, two accounts, two queues + matching charges.

    Spread across three distinct end times so date filtering is testable
    without aliasing.
    """
    base = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    jobs = [
        Job(
            job_id="100.desched1", short_id=100, name="alice-1",
            user="alice", account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=1),
            elapsed=3600, numcpus=128, numgpus=0, numnodes=1, walltime=7200,
        ),
        Job(
            job_id="101.desched1", short_id=101, name="alice-2",
            user="alice", account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(days=1, hours=1),
            elapsed=7200, numcpus=256, numgpus=0, numnodes=2, walltime=14400,
        ),
        Job(
            job_id="102.desched1", short_id=102, name="bob-1",
            user="bob", account="NCAR0002", queue="gpudev", status="F",
            submit=base, start=base, end=base + timedelta(days=2, hours=1),
            elapsed=3600, numcpus=64, numgpus=4, numnodes=1, walltime=7200,
        ),
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.flush()  # populate primary keys for JobCharge FK

    # JobCharge stores raw hours + qos_factor; *_charges are computed in
    # project_row as hours × qos_factor. alice-2 carries qos_factor=0.5 so
    # the test_computed_charges_apply_qos check has something to verify.
    charges = [
        JobCharge(job_id=jobs[0].id, cpu_hours=128.0, gpu_hours=0.0,
                  memory_hours=10.0, qos_factor=1.0, charge_version=1),
        JobCharge(job_id=jobs[1].id, cpu_hours=512.0, gpu_hours=0.0,
                  memory_hours=40.0, qos_factor=0.5, charge_version=1),
        JobCharge(job_id=jobs[2].id, cpu_hours=64.0, gpu_hours=16.0,
                  memory_hours=20.0, qos_factor=1.0, charge_version=1),
    ]
    for c in charges:
        in_memory_session.add(c)
    in_memory_session.commit()
    return jobs


class TestJobsSearchBasic:
    def test_empty_db_returns_empty_list(self, in_memory_session):
        rows = JobQueries(in_memory_session).jobs_search(
            start=date(2025, 1, 1), end=date(2025, 1, 31),
        )
        assert rows == []

    def test_default_columns_returned(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search()
        assert len(rows) == 3
        # Default schema: every row has exactly the DEFAULT_COLUMNS keys.
        for row in rows:
            assert set(row.keys()) == set(DEFAULT_COLUMNS)

    def test_ordering_is_end_desc(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search()
        # bob's job has the latest end → first
        assert rows[0]["job_id"] == "102.desched1"
        assert rows[-1]["job_id"] == "100.desched1"


class TestJobsSearchFilters:
    def test_user_filter(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(user="alice")
        assert {r["job_id"] for r in rows} == {"100.desched1", "101.desched1"}

    def test_project_filter(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(account="NCAR0002")
        assert [r["user"] for r in rows] == ["bob"]

    def test_queue_filter(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(queue="gpudev")
        assert [r["job_id"] for r in rows] == ["102.desched1"]

    def test_combined_filters(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            user="alice", account="NCAR0001", queue="main",
        )
        assert len(rows) == 2

    def test_combined_filters_no_match(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            user="alice", queue="gpudev",  # alice has no gpudev jobs
        )
        assert rows == []

    def test_status_filter(self, in_memory_session, search_jobs):
        # All sample jobs are 'F'; assert filter is applied, then negative.
        assert len(JobQueries(in_memory_session).jobs_search(status="F")) == 3
        assert JobQueries(in_memory_session).jobs_search(status="Q") == []

    def test_date_range_filter(self, in_memory_session, search_jobs):
        # Job ends span 2025-01-15 13:00 .. 2025-01-17 13:00 (naive UTC).
        # Window [2025-01-16, 2025-01-16] catches only the middle job.
        rows = JobQueries(in_memory_session).jobs_search(
            start=date(2025, 1, 16), end=date(2025, 1, 16),
        )
        assert [r["job_id"] for r in rows] == ["101.desched1"]


class TestJobsSearchColumns:
    def test_charge_fields_populated_via_outer_join(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(user="alice")
        # alice's first job has cpu_hours=128, second has 512
        cpu_h_by_job = {r["job_id"]: r["cpu_hours"] for r in rows}
        assert cpu_h_by_job["100.desched1"] == 128.0
        assert cpu_h_by_job["101.desched1"] == 512.0

    def test_outer_join_handles_missing_charge(self, in_memory_session):
        # Add a job with no matching JobCharge — outer join keeps the row.
        base = datetime(2025, 2, 1, 12, 0, 0)
        in_memory_session.add(Job(
            job_id="999.desched1", short_id=999, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=1),
            elapsed=3600, numcpus=1, numgpus=0, numnodes=1,
        ))
        in_memory_session.commit()
        rows = JobQueries(in_memory_session).jobs_search(user="alice")
        target = next(r for r in rows if r["job_id"] == "999.desched1")
        assert target["cpu_hours"] is None
        assert target["gpu_hours"] is None
        # Job columns still populated.
        assert target["numcpus"] == 1

    def test_custom_columns_projection(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            user="alice",
            columns=("job_id", "numnodes", "cpu_hours"),
        )
        for r in rows:
            assert set(r.keys()) == {"job_id", "numnodes", "cpu_hours"}

    def test_unknown_column_raises(self, in_memory_session, search_jobs):
        with pytest.raises(ValueError, match="Unknown column"):
            JobQueries(in_memory_session).jobs_search(columns=("job_id", "foo"))

    def test_datetime_serialized_to_iso(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            user="alice", columns=("job_id", "end"),
        )
        for r in rows:
            assert isinstance(r["end"], str)
            # ISO-ish: "YYYY-MM-DD HH:MM:SS"
            assert r["end"][:4].isdigit()
            assert r["end"][4] == "-"

    def test_limit_truncates_results(self, in_memory_session, search_jobs):
        # 3 jobs in the fixture; limit=1 should return just the most-recent.
        rows = JobQueries(in_memory_session).jobs_search(limit=1)
        assert len(rows) == 1
        # Job.end DESC → bob's job (102) is first
        assert rows[0]["job_id"] == "102.desched1"

    def test_limit_larger_than_result_is_safe(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(limit=999)
        assert len(rows) == 3

    def test_limit_none_returns_all(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(limit=None)
        assert len(rows) == 3

    def test_limit_invalid_raises(self, in_memory_session, search_jobs):
        with pytest.raises(ValueError, match="limit must be a positive integer"):
            JobQueries(in_memory_session).jobs_search(limit=0)
        with pytest.raises(ValueError, match="limit must be a positive integer"):
            JobQueries(in_memory_session).jobs_search(limit=-5)
        with pytest.raises(ValueError, match="limit must be a positive integer"):
            JobQueries(in_memory_session).jobs_search(limit="10")

    def test_limit_emits_sql_limit_clause(self, in_memory_session, search_jobs):
        # Server-side truncation: the compiled SQL must contain LIMIT, not a
        # Python slice after the fact. Catch a future refactor that drops to
        # all()[:n] instead of .limit(n).
        from sqlalchemy import event
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_search(limit=2)
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        # At least one SELECT against jobs carries a LIMIT clause.
        assert any("LIMIT" in s.upper() and "jobs" in s.lower() for s in statements), \
            f"Expected LIMIT in compiled SQL; got:\n{statements}"

    def test_computed_charges_apply_qos(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            user="alice",
            columns=("job_id", "cpu_hours", "qos_factor", "cpu_charges"),
        )
        by_id = {r["job_id"]: r for r in rows}
        # alice-1: 128 cpu_h × 1.0 qos = 128 charges
        assert by_id["100.desched1"]["cpu_charges"] == pytest.approx(128.0)
        # alice-2: 512 cpu_h × 0.5 qos = 256 charges
        assert by_id["101.desched1"]["cpu_charges"] == pytest.approx(256.0)
