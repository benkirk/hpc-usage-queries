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

    def test_project_filter_accepts_sequence(self, in_memory_session, search_jobs):
        # Multi-projcode form lets webapp callers pass an entire project
        # tree (parent + descendants) in one query — see SAM jobs route.
        rows = JobQueries(in_memory_session).jobs_search(
            account=["NCAR0001", "NCAR0002"]
        )
        # Union of single-account results: 2 alice jobs + 1 bob job.
        assert {r["job_id"] for r in rows} == {
            "100.desched1", "101.desched1", "102.desched1",
        }

    def test_project_filter_single_item_sequence(self, in_memory_session, search_jobs):
        # A 1-element sequence should behave identically to passing the
        # bare projcode string.
        rows = JobQueries(in_memory_session).jobs_search(account=["NCAR0002"])
        assert [r["user"] for r in rows] == ["bob"]

    def test_project_filter_empty_sequence(self, in_memory_session, search_jobs):
        # Empty sequence → `IN ()` → no rows. Sanity check that we don't
        # silently fall through to "no filter".
        rows = JobQueries(in_memory_session).jobs_search(account=[])
        assert rows == []

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


class TestJobsSearchPagination:
    """offset + sort_by/sort_dir + jobs_count, added for paginated webapp UIs."""

    def test_offset_shifts_window(self, in_memory_session, search_jobs):
        # 3 jobs total, default order is Job.end DESC → 102, 101, 100.
        page1 = JobQueries(in_memory_session).jobs_search(limit=2, offset=0)
        assert [r["job_id"] for r in page1] == ["102.desched1", "101.desched1"]
        page2 = JobQueries(in_memory_session).jobs_search(limit=2, offset=2)
        assert [r["job_id"] for r in page2] == ["100.desched1"]

    def test_offset_zero_is_no_op(self, in_memory_session, search_jobs):
        a = JobQueries(in_memory_session).jobs_search(limit=3, offset=0)
        b = JobQueries(in_memory_session).jobs_search(limit=3)
        assert [r["job_id"] for r in a] == [r["job_id"] for r in b]

    def test_offset_invalid_raises(self, in_memory_session, search_jobs):
        with pytest.raises(ValueError, match="offset must be a non-negative integer"):
            JobQueries(in_memory_session).jobs_search(offset=-1)
        with pytest.raises(ValueError, match="offset must be a non-negative integer"):
            JobQueries(in_memory_session).jobs_search(offset="5")

    def test_sort_by_elapsed_asc(self, in_memory_session, search_jobs):
        # elapsed values: alice-1=3600, alice-2=7200, bob-1=3600
        rows = JobQueries(in_memory_session).jobs_search(
            sort_by="elapsed", sort_dir="asc",
        )
        elapsed = [r["elapsed"] for r in rows]
        assert elapsed == sorted(elapsed)

    def test_sort_by_elapsed_desc(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            sort_by="elapsed", sort_dir="desc",
        )
        # 7200 should come first
        assert rows[0]["elapsed"] == 7200

    def test_sort_by_computed_cpu_charges(self, in_memory_session, search_jobs):
        # cpu_charges = cpu_hours × qos_factor:
        # alice-1: 128×1.0=128; alice-2: 512×0.5=256; bob-1: 64×1.0=64
        rows = JobQueries(in_memory_session).jobs_search(
            sort_by="cpu_charges", sort_dir="desc",
        )
        assert [r["job_id"] for r in rows] == [
            "101.desched1",  # 256
            "100.desched1",  # 128
            "102.desched1",  # 64
        ]

    def test_sort_by_unknown_raises(self, in_memory_session, search_jobs):
        with pytest.raises(ValueError, match="Unknown sort_by"):
            JobQueries(in_memory_session).jobs_search(sort_by="not_a_column")

    def test_sort_dir_invalid_raises(self, in_memory_session, search_jobs):
        with pytest.raises(ValueError, match="sort_dir must be"):
            JobQueries(in_memory_session).jobs_search(
                sort_by="elapsed", sort_dir="sideways",
            )

    def test_sort_dir_ignored_when_sort_by_is_none(self, in_memory_session, search_jobs):
        # sort_dir='sideways' is normally invalid; with sort_by=None the
        # default Job.end DESC order applies and sort_dir is not validated.
        rows = JobQueries(in_memory_session).jobs_search(sort_dir="sideways")
        assert rows[0]["job_id"] == "102.desched1"


class TestJobsSearchHasGpus:
    def test_has_gpus_true_returns_only_gpu_jobs(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(has_gpus=True)
        # Only bob-1 has numgpus=4; alice's jobs have numgpus=0.
        assert [r["job_id"] for r in rows] == ["102.desched1"]

    def test_has_gpus_false_returns_cpu_only(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(has_gpus=False)
        # alice-1 and alice-2 have numgpus=0.
        assert {r["job_id"] for r in rows} == {"100.desched1", "101.desched1"}

    def test_has_gpus_none_ignored(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(has_gpus=None)
        assert len(rows) == 3

    def test_has_gpus_false_includes_null_numgpus(self, in_memory_session, search_jobs):
        # A job with numgpus=NULL should still be classified as CPU-only.
        base = datetime(2025, 2, 1, 12, 0, 0)
        in_memory_session.add(Job(
            job_id="888.desched1", short_id=888, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=1),
            elapsed=3600, numcpus=1, numnodes=1, numgpus=None,
        ))
        in_memory_session.commit()
        rows = JobQueries(in_memory_session).jobs_search(has_gpus=False)
        assert "888.desched1" in {r["job_id"] for r in rows}


class TestJobsCount:
    def test_count_matches_search_length(self, in_memory_session, search_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count() == len(q.jobs_search())

    def test_count_respects_filters(self, in_memory_session, search_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(user="alice") == 2
        assert q.jobs_count(account="NCAR0002") == 1
        assert q.jobs_count(has_gpus=True) == 1
        assert q.jobs_count(has_gpus=False) == 2

    def test_count_accepts_account_sequence(self, in_memory_session, search_jobs):
        q = JobQueries(in_memory_session)
        # Union of NCAR0001 (2) + NCAR0002 (1) = 3 — matches jobs_search.
        assert q.jobs_count(account=["NCAR0001", "NCAR0002"]) == 3
        # Empty sequence is "no rows", not "no filter".
        assert q.jobs_count(account=[]) == 0

    def test_count_empty_when_no_match(self, in_memory_session, search_jobs):
        assert JobQueries(in_memory_session).jobs_count(user="nobody") == 0

    def test_count_unaffected_by_limit_or_offset_args(self, in_memory_session, search_jobs):
        # jobs_count deliberately does not accept limit/offset/columns/sort —
        # callers should never have to plumb pagination kwargs through.
        with pytest.raises(TypeError):
            JobQueries(in_memory_session).jobs_count(limit=1)
        with pytest.raises(TypeError):
            JobQueries(in_memory_session).jobs_count(offset=5)
