"""Tests for the programmatic ``JobQueries.jobs_search`` API.

This is the contract SAM (project_samuel) and other consumers will rely on,
mirroring the dict-row shape of ``daily_summary_report``.
"""

from datetime import date, datetime, timedelta, timezone

import logging

import pytest
from sqlalchemy import func

from job_history.database import DailySummary, Job, JobCharge
from job_history.queries import JobQueries
from job_history.queries.jobs import (
    QueryConfig, _HISTOGRAM_SPECS, _LOOKUP_DIMS, _MAX_SUMMARY_BANDS,
    _MAX_TIMESERIES_BANDS, _SUMMARY_SERVICEABLE_FILTERS, _USAGE_SORT_KEYS,
)
from job_history.sync.summary import generate_daily_summary
from job_history.columns import COLUMNS, DEFAULT_COLUMNS


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
            user="alice", account="NCAR0001", queue="main", qos="premium", status="F",
            submit=base, start=base, end=base + timedelta(hours=1),
            elapsed=3600, numcpus=128, numgpus=0, numnodes=1, walltime=7200,
        ),
        Job(
            job_id="101.desched1", short_id=101, name="alice-2",
            user="alice", account="NCAR0001", queue="main", qos="economy", status="F",
            submit=base, start=base, end=base + timedelta(days=1, hours=1),
            elapsed=7200, numcpus=256, numgpus=0, numnodes=2, walltime=14400,
        ),
        Job(
            job_id="102.desched1", short_id=102, name="bob-1",
            user="bob", account="NCAR0002", queue="gpudev", qos="regular", status="F",
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
        # Empty sequence → no rows. Sanity check that we don't silently fall
        # through to "no filter" — including on the FK path, where there are
        # no ids to look up and the clause must still be false.
        rows = JobQueries(in_memory_session).jobs_search(account=[])
        assert rows == []

    def test_queue_filter(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(queue="gpudev")
        assert [r["job_id"] for r in rows] == ["102.desched1"]

    def test_qos_filter(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(qos="premium")
        assert [r["job_id"] for r in rows] == ["100.desched1"]

    def test_qos_filter_no_match(self, in_memory_session, search_jobs):
        # 'uncharged' is a real seed value but no fixture job uses it.
        rows = JobQueries(in_memory_session).jobs_search(qos="uncharged")
        assert rows == []

    def test_qos_filter_combined_with_user(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(user="alice", qos="economy")
        assert [r["job_id"] for r in rows] == ["101.desched1"]

    @pytest.mark.parametrize("dim", sorted(_LOOKUP_DIMS))
    def test_lookup_filter_unresolvable_name_matches_nothing(
            self, in_memory_session, search_jobs, dim):
        # The FK path resolves the name to an id first. A name with no
        # lookup row must mean "no rows" — exactly what `Job.user == name`
        # meant — and never "drop the filter".
        q = JobQueries(in_memory_session)
        assert q.jobs_search(**{dim: "no-such-value"}) == []
        assert q.jobs_count(**{dim: "no-such-value"}) == 0

    def test_account_sequence_ignores_unresolvable_members(
            self, in_memory_session, search_jobs):
        # A project tree may name a projcode with no jobs on this machine;
        # the resolvable members must still match, as under the hybrid's
        # `IN (…)`.
        q = JobQueries(in_memory_session)
        assert q.jobs_count(account=["NCAR0002", "NO-SUCH-PROJ"]) == \
            q.jobs_count(account=["NCAR0002"]) == 1

    @pytest.mark.parametrize("dim", sorted(_LOOKUP_DIMS))
    def test_lookup_filter_uses_the_fk_not_a_correlated_subquery(
            self, in_memory_session, search_jobs, dim):
        """The WHERE-clause counterpart of the ORDER BY guard above.

        `Job.user`-style hybrids compile to a correlated scalar subquery
        that PostgreSQL cannot turn into an index lookup — it plans a Seq
        Scan with a per-row SubPlan (measured 406 ms vs 16 ms for the FK
        form on a one-month derecho window). Pin the emitted shape so a
        "simplification" back to the hybrid fails loudly.
        """
        from sqlalchemy import event
        model, fk_col, name_col = _LOOKUP_DIMS[dim]
        value = {"user": "alice", "account": "NCAR0001",
                 "queue": "main", "qos": "premium"}[dim]
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            assert JobQueries(in_memory_session).jobs_count(**{dim: value}) > 0
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        table = model.__tablename__
        scan = next(s for s in statements if " jobs" in s.lower())
        assert f"jobs.{fk_col.key}" in scan
        assert f"(SELECT {table}.{name_col.key}" not in scan

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

    def test_exit_status_filter(self, in_memory_session, search_jobs):
        # All sample jobs are 'F'; assert filter is applied, then negative.
        q = JobQueries(in_memory_session)
        assert len(q.jobs_search(exit_status="F")) == 3
        assert q.jobs_search(exit_status="Q") == []

    def test_date_range_filter(self, in_memory_session, search_jobs):
        # Job ends span 2025-01-15 13:00 .. 2025-01-17 13:00 (naive UTC).
        # In America/Denver (MST, UTC-7 in January) those are 06:00 on the
        # 15th/16th/17th respectively, so each lands on the same site-local
        # day as its UTC date. Window [2025-01-16, 2025-01-16] catches only
        # the middle job.
        rows = JobQueries(in_memory_session).jobs_search(
            start=date(2025, 1, 16), end=date(2025, 1, 16),
        )
        assert [r["job_id"] for r in rows] == ["101.desched1"]


class TestJobsSearchSiteTimezone:
    """``start`` / ``end`` filters are site-local days, not raw UTC dates.

    The plugin's ``DailySummary`` rollup bins jobs by site-local day
    (configured via ``JOB_HISTORY_SITE_TIMEZONE``); ``jobs_search`` must
    use the same convention so the per-job drill-down agrees with the
    daily totals.  Regression for the silent under-count where evening
    Mountain Time jobs that ended after 00:00 UTC were dropped from
    the previous day's drill-down.
    """

    @pytest.fixture
    def evening_mt_jobs(self, in_memory_session):
        """Two boundary jobs that end shortly after midnight UTC.

        ``early_utc`` ends ``2026-05-18 02:00 UTC`` = ``2026-05-17 20:00 MDT``
        → site-local day **2026-05-17**.

        ``late_utc`` ends ``2026-05-18 23:00 UTC`` = ``2026-05-18 17:00 MDT``
        → site-local day **2026-05-18**.

        A naive-UTC filter (the pre-fix behaviour) would lump both into
        2026-05-18; the site-TZ filter splits them correctly.
        """
        jobs = [
            Job(
                job_id="evening.1", short_id=1, name="evening-mt",
                user="benkirk", account="SCSG0001", queue="htc", status="F",
                submit=datetime(2026, 5, 17, 18, 0),
                start=datetime(2026, 5, 17, 18, 0),
                end=datetime(2026, 5, 18, 2, 0),       # 20:00 MDT on 5/17
                elapsed=28800, numcpus=1, numgpus=0, numnodes=1, walltime=28800,
            ),
            Job(
                job_id="afternoon.2", short_id=2, name="next-day-pm",
                user="benkirk", account="SCSG0001", queue="htc", status="F",
                submit=datetime(2026, 5, 18, 15, 0),
                start=datetime(2026, 5, 18, 15, 0),
                end=datetime(2026, 5, 18, 23, 0),      # 17:00 MDT on 5/18
                elapsed=28800, numcpus=1, numgpus=0, numnodes=1, walltime=28800,
            ),
        ]
        for j in jobs:
            in_memory_session.add(j)
        in_memory_session.commit()
        return jobs

    def test_evening_mt_job_belongs_to_prior_day_in_denver(
        self, in_memory_session, evening_mt_jobs,
    ):
        # America/Denver is the default SITE_TIMEZONE — no patch needed.
        rows = JobQueries(in_memory_session).jobs_search(
            start=date(2026, 5, 17), end=date(2026, 5, 17),
        )
        assert [r["job_id"] for r in rows] == ["evening.1"]

    def test_afternoon_mt_job_belongs_to_its_utc_day_in_denver(
        self, in_memory_session, evening_mt_jobs,
    ):
        rows = JobQueries(in_memory_session).jobs_search(
            start=date(2026, 5, 18), end=date(2026, 5, 18),
        )
        assert [r["job_id"] for r in rows] == ["afternoon.2"]

    def test_utc_timezone_keeps_old_naive_behavior(
        self, in_memory_session, evening_mt_jobs, monkeypatch,
    ):
        # With SITE_TIMEZONE=UTC, the new filter is mathematically
        # equivalent to the old naive-UTC behaviour: both evening MT jobs
        # land on their raw UTC date (2026-05-18) regardless of MDT offset.
        from job_history.database.config import JobHistoryConfig
        monkeypatch.setattr(JobHistoryConfig, "SITE_TIMEZONE", "UTC")
        rows = JobQueries(in_memory_session).jobs_search(
            start=date(2026, 5, 18), end=date(2026, 5, 18),
        )
        assert {r["job_id"] for r in rows} == {"evening.1", "afternoon.2"}

    def test_end_boundary_is_half_open(
        self, in_memory_session, evening_mt_jobs,
    ):
        # A job ending exactly at midnight site-local on day D+1 belongs
        # to D+1, not D — the half-open interval is the only way to make
        # consecutive-day queries non-overlapping.  Insert a job that ends
        # exactly at 2026-05-18 00:00 MDT (= 2026-05-18 06:00 UTC) and
        # confirm it lands on 5/18 from the day-D query and on 5/18 from
        # the day-D+1 query.
        in_memory_session.add(Job(
            job_id="midnight.3", short_id=3, name="midnight-mt",
            user="benkirk", account="SCSG0001", queue="htc", status="F",
            submit=datetime(2026, 5, 17, 23, 0),
            start=datetime(2026, 5, 17, 23, 0),
            end=datetime(2026, 5, 18, 6, 0),       # exactly 00:00 MDT on 5/18
            elapsed=25200, numcpus=1, numgpus=0, numnodes=1, walltime=25200,
        ))
        in_memory_session.commit()
        rows_d   = JobQueries(in_memory_session).jobs_search(
            start=date(2026, 5, 17), end=date(2026, 5, 17),
        )
        rows_d1  = JobQueries(in_memory_session).jobs_search(
            start=date(2026, 5, 18), end=date(2026, 5, 18),
        )
        assert "midnight.3" not in {r["job_id"] for r in rows_d}
        assert "midnight.3" in     {r["job_id"] for r in rows_d1}


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

    @pytest.mark.parametrize("dim", sorted(_LOOKUP_DIMS))
    def test_sort_by_lookup_column_asc(self, in_memory_session, search_jobs, dim):
        rows = JobQueries(in_memory_session).jobs_search(
            sort_by=dim, sort_dir="asc", columns=("job_id", dim),
        )
        values = [r[dim] for r in rows]
        assert values == sorted(values)
        assert len(set(values)) > 1, "fixture must span >1 value to order by"

    @pytest.mark.parametrize("dim", sorted(_LOOKUP_DIMS))
    def test_sort_by_lookup_column_desc(self, in_memory_session, search_jobs, dim):
        q = JobQueries(in_memory_session)
        values = [r[dim] for r in q.jobs_search(
            sort_by=dim, sort_dir="desc", columns=("job_id", dim))]
        assert values == sorted(values, reverse=True)

    @pytest.mark.parametrize("dim", sorted(_LOOKUP_DIMS))
    def test_sort_by_lookup_joins_instead_of_correlated_subquery(
            self, in_memory_session, search_jobs, dim):
        # The Job.user/account/queue/qos hybrids' SQL side is a correlated
        # scalar subquery, re-evaluated per row (measured 10x slower).
        # Sorting must join the lookup table instead — pin the emitted shape.
        from sqlalchemy import event
        model, _fk_col, name_col = _LOOKUP_DIMS[dim]
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_search(sort_by=dim)
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        table = model.__tablename__
        stmt = next(s for s in statements if "ORDER BY" in s.upper())
        assert f"JOIN {table}" in stmt
        assert f"(SELECT {table}.{name_col.key}" not in stmt

    @pytest.mark.parametrize("dim", sorted(_LOOKUP_DIMS))
    def test_sort_by_lookup_keeps_count_agreement(
            self, in_memory_session, search_jobs, dim):
        # OUTER join: a lookup sort must never drop rows vs jobs_count.
        q = JobQueries(in_memory_session)
        assert len(q.jobs_search(sort_by=dim)) == q.jobs_count()

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


class TestJobsSearchResourceRanges:
    """search_jobs: (nodes, cpus, gpus) = (1,128,0), (2,256,0), (1,64,4)."""

    def test_min_gpus_selects_gpu_jobs(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(min_gpus=1)
        # Only bob-1 has numgpus=4; alice's jobs have numgpus=0.
        assert [r["job_id"] for r in rows] == ["102.desched1"]

    def test_max_gpus_zero_selects_cpu_only(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(max_gpus=0)
        # alice-1 and alice-2 have numgpus=0.
        assert {r["job_id"] for r in rows} == {"100.desched1", "101.desched1"}

    def test_unset_bounds_ignored(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(min_gpus=None, max_gpus=None)
        assert len(rows) == 3

    def test_range_filters_are_null_strict(self, in_memory_session, search_jobs):
        # A NULL numgpus fails both comparisons, so it drops out of either
        # bound. No row in production has a NULL here (verified across 34M
        # rows on both machines), but the semantics are pinned deliberately:
        # NULL means "unknown", not "zero".
        base = datetime(2025, 2, 1, 12, 0, 0)
        in_memory_session.add(Job(
            job_id="888.desched1", short_id=888, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=1),
            elapsed=3600, numcpus=1, numnodes=1, numgpus=None,
        ))
        in_memory_session.commit()
        q = JobQueries(in_memory_session)
        assert "888.desched1" not in {r["job_id"] for r in q.jobs_search(max_gpus=0)}
        assert "888.desched1" not in {r["job_id"] for r in q.jobs_search(min_gpus=0)}

    def test_bounds_are_inclusive(self, in_memory_session, search_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(min_cpus=128, max_cpus=128) == 1

    def test_min_and_max_nodes(self, in_memory_session, search_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(min_nodes=2) == 1
        assert q.jobs_count(max_nodes=1) == 2
        assert q.jobs_count(min_nodes=1, max_nodes=2) == 3

    def test_inverted_range_returns_empty_not_error(self, in_memory_session, search_jobs):
        assert JobQueries(in_memory_session).jobs_count(min_nodes=5, max_nodes=1) == 0

    def test_all_six_compose(self, in_memory_session, search_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            min_nodes=1, max_nodes=1, min_cpus=64, max_cpus=64,
            min_gpus=4, max_gpus=4, columns=("job_id",),
        )
        assert [r["job_id"] for r in rows] == ["102.desched1"]

    def test_count_agrees_with_search(self, in_memory_session, search_jobs):
        q = JobQueries(in_memory_session)
        kw = {"min_nodes": 1, "max_cpus": 256}
        assert q.jobs_count(**kw) == len(q.jobs_search(**kw))


@pytest.fixture
def job_id_jobs(in_memory_session):
    """Job rows that mirror the real-DB ``job_id`` shape variety.

    Confirmed against the live ``derecho_jobs`` / ``casper_jobs`` Postgres
    DBs: scalar jobs carry ``short_id`` populated by pbsparse; **array
    parents and elements have ``short_id = NULL``** (1.74M of 12.27M rows).
    The filter must therefore key off ``job_id`` text alone.

    The decoy ``60491170.desched1`` exists to prove the boundary-anchored
    LIKE does not bleed a 7-digit prefix into an 8-digit id.
    """
    base = datetime(2026, 5, 1, 12, 0, 0)
    jobs = [
        # Scalar — pbsparse populates short_id
        Job(job_id="6049117.desched1",       short_id=6049117, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=1),
            elapsed=3600, numcpus=1, numgpus=0, numnodes=1),
        # Array parent + elements — short_id is NULL in real data
        Job(job_id="6049117[].desched1",     short_id=None, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=2),
            elapsed=3600, numcpus=1, numgpus=0, numnodes=1),
        Job(job_id="6049117[0].desched1",    short_id=None, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=3),
            elapsed=3600, numcpus=1, numgpus=0, numnodes=1),
        Job(job_id="6049117[28].desched1",   short_id=None, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=4),
            elapsed=3600, numcpus=1, numgpus=0, numnodes=1),
        # Cross-host array element — same [N], different scheduler suffix.
        # (Job.job_id+submit is the unique constraint; we offset the submit.)
        Job(job_id="6049117[28].casper-pbs", short_id=None, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base + timedelta(seconds=1), start=base,
            end=base + timedelta(hours=5),
            elapsed=3600, numcpus=1, numgpus=0, numnodes=1),
        # Decoy: same 7-digit prefix; must NOT match `--job-id 6049117`.
        Job(job_id="60491170.desched1",      short_id=60491170, user="bob",
            account="NCAR0002", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=6),
            elapsed=3600, numcpus=1, numgpus=0, numnodes=1),
        # Decoy: unrelated id.
        Job(job_id="9999999.desched1",       short_id=9999999, user="bob",
            account="NCAR0002", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=7),
            elapsed=3600, numcpus=1, numgpus=0, numnodes=1),
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.commit()
    return jobs


class TestJobsSearchJobIdFilter:
    """``job_id`` filter dispatches by input shape (see jobs_search docstring).

    Input *with* a ``.`` → exact match on ``Job.job_id``.
    Input *without* a ``.`` → boundary-anchored prefix LIKE on ``Job.job_id``
    (``input.%`` OR ``input[%``), so digits alone match every variant
    (scalar + parent + elements) of one job but never bleed into a longer
    numeric prefix.
    """

    def test_digits_match_scalar_parent_and_array_elements(
        self, in_memory_session, job_id_jobs,
    ):
        rows = JobQueries(in_memory_session).jobs_search(job_id="6049117")
        assert {r["job_id"] for r in rows} == {
            "6049117.desched1",         # scalar form
            "6049117[].desched1",       # array parent
            "6049117[0].desched1",      # element
            "6049117[28].desched1",     # element
            "6049117[28].casper-pbs",   # cross-host element
        }

    def test_digits_no_substring_bleed(
        self, in_memory_session, job_id_jobs,
    ):
        # Regression guard: 7-digit '6049117' must not match the 8-digit
        # neighbour '60491170.desched1' (boundary anchor on `.` or `[`).
        rows = JobQueries(in_memory_session).jobs_search(job_id="6049117")
        assert "60491170.desched1" not in {r["job_id"] for r in rows}

    def test_array_parent_only(self, in_memory_session, job_id_jobs):
        # Empty-brackets form selects just the parent marker row.
        rows = JobQueries(in_memory_session).jobs_search(job_id="6049117[]")
        assert [r["job_id"] for r in rows] == ["6049117[].desched1"]

    def test_array_element_cross_host(self, in_memory_session, job_id_jobs):
        # No host suffix → matches the same [N] across every scheduler host.
        rows = JobQueries(in_memory_session).jobs_search(job_id="6049117[28]")
        assert {r["job_id"] for r in rows} == {
            "6049117[28].desched1",
            "6049117[28].casper-pbs",
        }

    def test_exact_full_id_with_host(self, in_memory_session, job_id_jobs):
        # Presence of `.` short-circuits to an exact match.
        rows = JobQueries(in_memory_session).jobs_search(
            job_id="6049117[28].desched1",
        )
        assert [r["job_id"] for r in rows] == ["6049117[28].desched1"]

    def test_exact_full_id_scalar(self, in_memory_session, job_id_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            job_id="6049117.desched1",
        )
        assert [r["job_id"] for r in rows] == ["6049117.desched1"]

    def test_no_match(self, in_memory_session, job_id_jobs):
        rows = JobQueries(in_memory_session).jobs_search(job_id="42")
        assert rows == []

    def test_whitespace_is_stripped(self, in_memory_session, job_id_jobs):
        # Stray whitespace from shell quoting must not turn an exact match
        # into "no rows".
        rows = JobQueries(in_memory_session).jobs_search(job_id="  6049117  ")
        assert {r["job_id"] for r in rows} == {
            "6049117.desched1",
            "6049117[].desched1",
            "6049117[0].desched1",
            "6049117[28].desched1",
            "6049117[28].casper-pbs",
        }

    def test_empty_string_is_no_filter(self, in_memory_session, job_id_jobs):
        # Mirrors how user/queue/qos/status handle "" — falsy → not applied.
        rows = JobQueries(in_memory_session).jobs_search(job_id="")
        assert len(rows) == len(job_id_jobs)

    def test_none_is_no_filter(self, in_memory_session, job_id_jobs):
        rows = JobQueries(in_memory_session).jobs_search(job_id=None)
        assert len(rows) == len(job_id_jobs)

    def test_works_with_null_short_id(self, in_memory_session, job_id_jobs):
        # Regression guard: array rows have short_id=NULL in real data, so
        # the filter MUST work off Job.job_id text. If anyone reintroduces
        # a short_id-based path, the four NULL-short_id rows would silently
        # drop out of the result here.
        rows = JobQueries(in_memory_session).jobs_search(job_id="6049117")
        null_short_id_returned = [r for r in rows if "[" in r["job_id"]]
        assert len(null_short_id_returned) == 4

    def test_count_matches_search(self, in_memory_session, job_id_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(job_id="6049117") == len(q.jobs_search(job_id="6049117"))
        assert q.jobs_count(job_id="6049117[28]") == 2
        assert q.jobs_count(job_id="6049117[28].desched1") == 1
        assert q.jobs_count(job_id="42") == 0

    def test_combines_with_other_filters(self, in_memory_session, job_id_jobs):
        # All five 6049117* rows belong to alice — restricting by user is a
        # no-op for the matching set, but excludes the bob-owned decoys.
        rows = JobQueries(in_memory_session).jobs_search(
            user="alice", job_id="6049117",
        )
        assert len(rows) == 5
        # Bob's decoy '60491170.desched1' is excluded both by job_id boundary
        # AND by user — confirm the AND composition.
        rows_bob = JobQueries(in_memory_session).jobs_search(
            user="bob", job_id="6049117",
        )
        assert rows_bob == []


class TestJobsCount:
    def test_count_matches_search_length(self, in_memory_session, search_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count() == len(q.jobs_search())

    def test_count_respects_filters(self, in_memory_session, search_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(user="alice") == 2
        assert q.jobs_count(account="NCAR0002") == 1
        assert q.jobs_count(min_gpus=1) == 1
        assert q.jobs_count(max_gpus=0) == 2
        assert q.jobs_count(qos="premium") == 1
        assert q.jobs_count(qos="economy") == 1
        assert q.jobs_count(qos="regular") == 1
        assert q.jobs_count(qos="uncharged") == 0

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


@pytest.fixture
def name_jobs(in_memory_session):
    """Job names exercising case, underscores, and NULL.

    `cesm_b1850` vs `cesmXb1850` is the regression pair for the LIKE `_` leak:
    without escaping, an ignore-case search for 'cesm_b*' matches both.
    """
    base = datetime(2025, 3, 1, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    specs = [
        ("300.desched1", "wrf_cycle_01"),
        ("301.desched1", "wrf_cycle_02"),
        ("302.desched1", "WRF_CYCLE_03"),   # case variant
        ("303.desched1", "cesm_b1850"),
        ("304.desched1", "cesmXb1850"),     # `_` leak decoy
        ("305.desched1", "postproc.restart"),
        ("306.desched1", None),             # NULL name
    ]
    jobs = [
        Job(job_id=jid, short_id=300 + i, name=nm, user="alice",
            account="NCAR0001", queue="main", status="0",
            submit=base, start=base, end=base + timedelta(hours=i + 1),
            elapsed=3600, numcpus=128, numgpus=0, numnodes=1)
        for i, (jid, nm) in enumerate(specs)
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.commit()
    return jobs


class TestJobsSearchNameFilter:
    """Shell-glob filter on Job.name.

    SQLite GLOB path only; the PostgreSQL regex path is covered by the
    compiled-SQL assertions in test_query_builders.py.
    """

    def _names(self, session, **kw):
        return {r["name"] for r in JobQueries(session).jobs_search(
            columns=("job_id", "name"), **kw)}

    def test_star_wildcard(self, in_memory_session, name_jobs):
        assert self._names(in_memory_session, name="wrf_*") == {
            "wrf_cycle_01", "wrf_cycle_02"}

    def test_question_mark_matches_exactly_one(self, in_memory_session, name_jobs):
        assert self._names(in_memory_session, name="wrf_cycle_0?") == {
            "wrf_cycle_01", "wrf_cycle_02"}
        assert self._names(in_memory_session, name="wrf_cycle_?") == set()

    def test_case_sensitive_by_default(self, in_memory_session, name_jobs):
        assert "WRF_CYCLE_03" not in self._names(in_memory_session, name="wrf_*")

    def test_ignore_case_matches_both(self, in_memory_session, name_jobs):
        assert self._names(in_memory_session, name="wrf_*", ignore_case=True) == {
            "wrf_cycle_01", "wrf_cycle_02", "WRF_CYCLE_03"}

    def test_ignore_case_underscore_is_literal(self, in_memory_session, name_jobs):
        # THE regression guard for the fs_scans `_` leak: with LIKE and no
        # escaping, 'cesm_b*' -> 'cesm_b%' would also match 'cesmXb1850'.
        assert self._names(
            in_memory_session, name="cesm_b*", ignore_case=True,
        ) == {"cesm_b1850"}

    def test_case_sensitive_underscore_is_literal(self, in_memory_session, name_jobs):
        # GLOB has no `_` wildcard at all, so this holds on the other path too.
        assert self._names(in_memory_session, name="cesm_b*") == {"cesm_b1850"}

    def test_dot_is_literal(self, in_memory_session, name_jobs):
        assert self._names(in_memory_session, name="*.restart") == {"postproc.restart"}

    def test_multiple_patterns_are_ored(self, in_memory_session, name_jobs):
        assert self._names(
            in_memory_session, name=["wrf_cycle_01", "*.restart"],
        ) == {"wrf_cycle_01", "postproc.restart"}

    def test_null_name_never_matches(self, in_memory_session, name_jobs):
        # NULL GLOB 'x' is NULL -> excluded, even for the match-everything glob.
        matched = self._names(in_memory_session, name="*")
        assert None not in matched
        assert len(matched) == 6

    def test_none_is_no_filter(self, in_memory_session, name_jobs):
        assert len(JobQueries(in_memory_session).jobs_search(name=None)) == len(name_jobs)

    def test_empty_string_is_no_filter(self, in_memory_session, name_jobs):
        assert len(JobQueries(in_memory_session).jobs_search(name="")) == len(name_jobs)

    def test_empty_sequence_is_no_filter_unlike_account(self, in_memory_session, name_jobs):
        # Deliberate divergence from `account=[]`, which means "no rows".
        # Click's multiple=True hands us () for an unsupplied -N, so the empty
        # case MUST be the identity or the CLI default returns nothing.
        q = JobQueries(in_memory_session)
        assert len(q.jobs_search(name=[])) == len(name_jobs)
        assert len(q.jobs_search(name=[""])) == len(name_jobs)
        # Contrast, to pin the asymmetry:
        assert q.jobs_search(account=[]) == []

    def test_count_matches_search(self, in_memory_session, name_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(name="wrf_*") == 2
        assert q.jobs_count(name="wrf_*", ignore_case=True) == 3
        assert q.jobs_count(name="*") == 6      # NULL row excluded
        assert q.jobs_count(name=[]) == len(name_jobs)

    def test_combines_with_other_filters(self, in_memory_session, name_jobs):
        assert JobQueries(in_memory_session).jobs_search(user="bob", name="wrf_*") == []


@pytest.fixture
def wait_jobs(in_memory_session):
    """Jobs with distinct eligible_secs, including the pre-2025 NULL case."""
    base = datetime(2025, 4, 1, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    specs = [("400.desched1", 0), ("401.desched1", 1800),
             ("402.desched1", 7200), ("403.desched1", None)]
    jobs = [
        Job(job_id=jid, short_id=400 + i, name=f"w{i}", user="alice",
            account="NCAR0001", queue="main", status="0",
            submit=base, start=base, end=base + timedelta(hours=i + 1),
            elapsed=3600, eligible_secs=secs,
            numcpus=128, numgpus=0, numnodes=1)
        for i, (jid, secs) in enumerate(specs)
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.commit()
    return jobs


class TestJobsSearchWaitFilters:
    def test_min_wait_excludes_shorter(self, in_memory_session, wait_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            min_eligible_secs=1800, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {"401.desched1", "402.desched1"}

    def test_max_wait_excludes_longer(self, in_memory_session, wait_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            max_eligible_secs=1800, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {"400.desched1", "401.desched1"}

    def test_null_eligible_secs_excluded_by_min(self, in_memory_session, wait_jobs):
        # min=0 is not "everything": the NULL row (derecho before
        # eligible_time_enable) has no wait measurement and drops out.
        rows = JobQueries(in_memory_session).jobs_search(
            min_eligible_secs=0, columns=("job_id",))
        assert "403.desched1" not in {r["job_id"] for r in rows}
        assert len(rows) == 3

    def test_null_eligible_secs_excluded_by_max(self, in_memory_session, wait_jobs):
        # The dangerous direction: "jobs that waited at most 1h" must NOT
        # silently include jobs whose wait is unknown.
        rows = JobQueries(in_memory_session).jobs_search(
            max_eligible_secs=3600, columns=("job_id",))
        assert "403.desched1" not in {r["job_id"] for r in rows}

    def test_zero_bound_is_not_none(self, in_memory_session, wait_jobs):
        # 0 is falsy — guards against an `if min_eligible_secs:` regression.
        assert JobQueries(in_memory_session).jobs_count(max_eligible_secs=0) == 1

    def test_count_agrees_with_search(self, in_memory_session, wait_jobs):
        q = JobQueries(in_memory_session)
        for kw in ({"min_eligible_secs": 1800}, {"max_eligible_secs": 3600},
                   {"min_eligible_secs": 0, "max_eligible_secs": 7200}):
            assert q.jobs_count(**kw) == len(q.jobs_search(**kw))


_GIB = 1024 ** 3  # matches sync/charging.BYTES_PER_GB


@pytest.fixture
def elapsed_reqmem_jobs(in_memory_session):
    """Jobs with distinct (elapsed, reqmem), including NULLs and a zero.

    The zero-elapsed row exists for the falsy-zero guard (``max_elapsed=0``
    must not be treated as unset); the NULL rows pin the NULL-strict
    semantics for both new bound pairs.
    """
    base = datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    specs = [
        ("500.desched1", 0,     4 * _GIB),    # zero elapsed
        ("501.desched1", 3600,  64 * _GIB),
        ("502.desched1", 86400, 512 * _GIB),
        ("503.desched1", None,  16 * _GIB),   # NULL elapsed
        ("504.desched1", 7200,  None),        # NULL reqmem
    ]
    jobs = [
        Job(job_id=jid, short_id=500 + i, name=f"er{i}", user="alice",
            account="NCAR0001", queue="main", status="0",
            submit=base, start=base, end=base + timedelta(hours=i + 1),
            elapsed=elapsed, reqmem=reqmem,
            numcpus=128, numgpus=0, numnodes=1)
        for i, (jid, elapsed, reqmem) in enumerate(specs)
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.commit()
    return jobs


class TestJobsSearchElapsedReqmemFilters:
    """(elapsed s, reqmem GiB) = (0,4), (3600,64), (86400,512), (None,16), (7200,None)."""

    def test_min_elapsed_excludes_shorter(self, in_memory_session, elapsed_reqmem_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            min_elapsed=3600, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {
            "501.desched1", "502.desched1", "504.desched1"}

    def test_max_elapsed_excludes_longer(self, in_memory_session, elapsed_reqmem_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            max_elapsed=3600, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {"500.desched1", "501.desched1"}

    def test_elapsed_bounds_inclusive(self, in_memory_session, elapsed_reqmem_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(min_elapsed=3600, max_elapsed=3600) == 1

    def test_max_elapsed_zero_is_not_treated_as_unset(
            self, in_memory_session, elapsed_reqmem_jobs):
        # 0 is falsy — guards against an `if max_elapsed:` regression.
        assert JobQueries(in_memory_session).jobs_count(max_elapsed=0) == 1

    def test_null_elapsed_excluded_by_both_bounds(
            self, in_memory_session, elapsed_reqmem_jobs):
        q = JobQueries(in_memory_session)
        assert "503.desched1" not in {
            r["job_id"] for r in q.jobs_search(min_elapsed=0)}
        assert "503.desched1" not in {
            r["job_id"] for r in q.jobs_search(max_elapsed=10 ** 6)}

    def test_min_reqmem_excludes_smaller(self, in_memory_session, elapsed_reqmem_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            min_reqmem=64 * _GIB, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {"501.desched1", "502.desched1"}

    def test_max_reqmem_excludes_larger(self, in_memory_session, elapsed_reqmem_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            max_reqmem=16 * _GIB, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {"500.desched1", "503.desched1"}

    def test_reqmem_bounds_inclusive(self, in_memory_session, elapsed_reqmem_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(min_reqmem=64 * _GIB, max_reqmem=64 * _GIB) == 1

    def test_null_reqmem_excluded_by_both_bounds(
            self, in_memory_session, elapsed_reqmem_jobs):
        q = JobQueries(in_memory_session)
        assert "504.desched1" not in {
            r["job_id"] for r in q.jobs_search(min_reqmem=0)}
        assert "504.desched1" not in {
            r["job_id"] for r in q.jobs_search(max_reqmem=1024 * _GIB)}

    def test_count_agrees_with_search(self, in_memory_session, elapsed_reqmem_jobs):
        q = JobQueries(in_memory_session)
        for kw in ({"min_elapsed": 3600}, {"max_reqmem": 64 * _GIB},
                   {"min_elapsed": 0, "max_elapsed": 86400},
                   {"min_reqmem": 4 * _GIB, "max_reqmem": 512 * _GIB}):
            assert q.jobs_count(**kw) == len(q.jobs_search(**kw))


@pytest.fixture
def memory_jobs(in_memory_session):
    """Jobs with distinct (reqmem, memory) pairs pinning the wasted delta.

    Includes an over-request row (used > requested → negative wasted), an
    exact-fit row (wasted == 0, guarding the falsy-zero bound), and a NULL
    in each column separately — either NULL must make the delta NULL.
    """
    base = datetime(2025, 5, 2, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    specs = [
        # (jid, reqmem, memory)              wasted = reqmem − memory
        ("600.desched1", 4 * _GIB,  2 * _GIB),    # 2 GiB
        ("601.desched1", 64 * _GIB, 4 * _GIB),    # 60 GiB
        ("602.desched1", 8 * _GIB,  12 * _GIB),   # −4 GiB (over request)
        ("603.desched1", None,      16 * _GIB),   # NULL reqmem → NULL delta
        ("604.desched1", 32 * _GIB, None),        # NULL memory → NULL delta
        ("605.desched1", 2 * _GIB,  2 * _GIB),    # 0 (exact fit)
    ]
    jobs = [
        Job(job_id=jid, short_id=600 + i, name=f"mem{i}", user="alice",
            account="NCAR0001", queue="main", status="0",
            submit=base, start=base, end=base + timedelta(hours=i + 1),
            elapsed=3600, reqmem=reqmem, memory=memory,
            numcpus=128, numgpus=0, numnodes=1)
        for i, (jid, reqmem, memory) in enumerate(specs)
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.commit()
    return jobs


class TestJobsSearchMemoryFilters:
    """(reqmem, memory) GiB = (4,2), (64,4), (8,12), (None,16), (32,None), (2,2)."""

    def test_min_memory_used_excludes_smaller(self, in_memory_session, memory_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            min_memory_used=4 * _GIB, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {
            "601.desched1", "602.desched1", "603.desched1"}

    def test_max_memory_used_excludes_larger(self, in_memory_session, memory_jobs):
        rows = JobQueries(in_memory_session).jobs_search(
            max_memory_used=2 * _GIB, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {"600.desched1", "605.desched1"}

    def test_memory_used_bounds_inclusive(self, in_memory_session, memory_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(min_memory_used=4 * _GIB,
                            max_memory_used=4 * _GIB) == 1

    def test_null_memory_excluded_by_both_bounds(
            self, in_memory_session, memory_jobs):
        q = JobQueries(in_memory_session)
        assert "604.desched1" not in {
            r["job_id"] for r in q.jobs_search(min_memory_used=0)}
        assert "604.desched1" not in {
            r["job_id"] for r in q.jobs_search(max_memory_used=1024 * _GIB)}

    def test_min_wasted_zero_excludes_over_request(
            self, in_memory_session, memory_jobs):
        # min=0 keeps exact-fit and under-use, drops the negative delta and
        # both NULL-delta rows. 0 is falsy — also guards the `is not None`
        # convention for the new pair.
        rows = JobQueries(in_memory_session).jobs_search(
            min_memory_wasted=0, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {
            "600.desched1", "601.desched1", "605.desched1"}

    def test_negative_max_selects_over_request_jobs(
            self, in_memory_session, memory_jobs):
        # The 'over request' histogram band replays as max_memory_wasted=-1.
        rows = JobQueries(in_memory_session).jobs_search(
            max_memory_wasted=-1, columns=("job_id",))
        assert {r["job_id"] for r in rows} == {"602.desched1"}

    def test_negative_min_is_not_clamped(self, in_memory_session, memory_jobs):
        # A negative floor must include the over-request row — clamping
        # negatives to zero would silently hide those jobs.
        rows = JobQueries(in_memory_session).jobs_search(
            min_memory_wasted=-(4 * _GIB), columns=("job_id",))
        assert {r["job_id"] for r in rows} == {
            "600.desched1", "601.desched1", "602.desched1", "605.desched1"}

    def test_wasted_bounds_inclusive(self, in_memory_session, memory_jobs):
        q = JobQueries(in_memory_session)
        assert q.jobs_count(min_memory_wasted=-(4 * _GIB),
                            max_memory_wasted=-(4 * _GIB)) == 1

    def test_either_null_column_nulls_the_delta(
            self, in_memory_session, memory_jobs):
        # SQL NULL propagation: reqmem − NULL and NULL − memory are both
        # NULL, so 603 AND 604 drop out under any wasted bound.
        rows = JobQueries(in_memory_session).jobs_search(
            min_memory_wasted=-(10 ** 15), columns=("job_id",))
        assert {r["job_id"] for r in rows} == {
            "600.desched1", "601.desched1", "602.desched1", "605.desched1"}

    def test_count_agrees_with_search(self, in_memory_session, memory_jobs):
        q = JobQueries(in_memory_session)
        for kw in ({"min_memory_used": 4 * _GIB}, {"max_memory_used": 2 * _GIB},
                   {"max_memory_wasted": -1},
                   {"min_memory_wasted": -(4 * _GIB), "max_memory_wasted": 60 * _GIB}):
            assert q.jobs_count(**kw) == len(q.jobs_search(**kw))


class TestFilterSignatureParity:
    """jobs_search / jobs_count / jobs_facets / the helper must stay in sync.

    Without this, adding a filter to jobs_search and forgetting jobs_count
    yields a paginated UI whose 'N total' disagrees with its rows — the
    quietest possible bug. The private helper takes no defaults, so the
    call-site half of the mistake raises TypeError at runtime; this catches
    the signature half at collection time.
    """

    SEARCH_ONLY = {"columns", "limit", "offset", "sort_by", "sort_dir"}
    FACET_ONLY = {"facets", "self_exclude", "limit"}
    HIST_ONLY = {"dimension", "owners_limit", "owners_sort_by", "owners_by"}
    USAGE_ONLY = {"dimension", "limit", "sort_by"}
    TIMESERIES_ONLY = {"period", "owners_limit", "owners_sort_by", "owners_by"}

    @staticmethod
    def _params(fn):
        import inspect
        return set(inspect.signature(fn).parameters) - {"self"}

    def test_jobs_count_accepts_every_jobs_search_filter(self):
        search = self._params(JobQueries.jobs_search) - self.SEARCH_ONLY
        assert search == self._params(JobQueries.jobs_count)

    def test_jobs_facets_accepts_every_jobs_search_filter(self):
        search = self._params(JobQueries.jobs_search) - self.SEARCH_ONLY
        assert search == self._params(JobQueries.jobs_facets) - self.FACET_ONLY

    def test_jobs_histogram_accepts_every_jobs_search_filter(self):
        search = self._params(JobQueries.jobs_search) - self.SEARCH_ONLY
        assert search == self._params(JobQueries.jobs_histogram) - self.HIST_ONLY

    def test_jobs_usage_by_accepts_every_jobs_search_filter(self):
        search = self._params(JobQueries.jobs_search) - self.SEARCH_ONLY
        assert search == self._params(JobQueries.jobs_usage_by) - self.USAGE_ONLY

    def test_jobs_timeseries_accepts_every_jobs_search_filter(self):
        search = self._params(JobQueries.jobs_search) - self.SEARCH_ONLY
        assert search == (
            self._params(JobQueries.jobs_timeseries) - self.TIMESERIES_ONLY)

    def test_helper_covers_exactly_the_filter_set(self):
        search = self._params(JobQueries.jobs_search) - self.SEARCH_ONLY
        helper = self._params(JobQueries._apply_jobs_search_filters) - {"query"}
        assert search == helper

    def test_helper_params_have_no_defaults(self):
        """Defaults would let a forgotten jobs_count arg fail *silently*."""
        import inspect
        sig = inspect.signature(JobQueries._apply_jobs_search_filters)
        for name, param in sig.parameters.items():
            if name in ("self", "query"):
                continue
            assert param.default is inspect.Parameter.empty, name


@pytest.fixture
def facet_jobs(in_memory_session):
    """Jobs spanning two queues, two QoS, two users, and exit codes.

    Includes one job with no queue so the NULL-FK facet bucket is exercised.
    """
    base = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    specs = [
        # (job_id, user, account, queue, qos, exit code)
        ("500.desched1", "alice", "NCAR0001", "main",   "premium", "0"),
        ("501.desched1", "alice", "NCAR0001", "main",   "regular", "0"),
        ("502.desched1", "alice", "NCAR0001", "main",   "regular", "1"),
        ("503.desched1", "bob",   "NCAR0002", "gpudev", "regular", "0"),
        ("504.desched1", "bob",   "NCAR0002", "gpudev", "premium", "271"),
        ("505.desched1", "carol", "NCAR0002", None,     "regular", "0"),
    ]
    jobs = [
        Job(job_id=jid, short_id=500 + i, name=f"f{i}", user=u, account=a,
            queue=q, qos=qs, status=st,
            submit=base, start=base, end=base + timedelta(hours=i + 1),
            elapsed=3600, numcpus=128, numgpus=0, numnodes=1)
        for i, (jid, u, a, q, qs, st) in enumerate(specs)
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.commit()
    return jobs


class TestJobsFacets:
    @staticmethod
    def _as_map(rows):
        return {r["value"]: r["count"] for r in rows}

    def test_row_contract(self, in_memory_session, facet_jobs):
        facets = JobQueries(in_memory_session).jobs_facets()
        assert set(facets) == {"queue", "qos", "exit_status"}
        for rows in facets.values():
            for row in rows:
                assert set(row) == {"value", "count"}

    def test_counts_are_correct(self, in_memory_session, facet_jobs):
        facets = JobQueries(in_memory_session).jobs_facets()
        assert self._as_map(facets["qos"]) == {"regular": 4, "premium": 2}
        assert self._as_map(facets["exit_status"]) == {"0": 4, "1": 1, "271": 1}

    def test_null_fk_surfaces_as_none(self, in_memory_session, facet_jobs):
        # Dropping the NULL bucket would make the facet rows silently
        # under-sum against jobs_count.
        queue_counts = self._as_map(
            JobQueries(in_memory_session).jobs_facets()["queue"])
        assert queue_counts == {"main": 3, "gpudev": 2, None: 1}

    def test_rows_sum_to_jobs_count_without_self_exclusion(
            self, in_memory_session, facet_jobs):
        q = JobQueries(in_memory_session)
        total = q.jobs_count()
        facets = q.jobs_facets(self_exclude=False)
        for dim, rows in facets.items():
            assert sum(r["count"] for r in rows) == total, dim

    def test_ordering_is_count_desc_then_value_asc(self, in_memory_session, facet_jobs):
        facets = JobQueries(in_memory_session).jobs_facets()
        counts = [r["count"] for r in facets["exit_status"]]
        assert counts == sorted(counts, reverse=True)
        # Ties break on value ascending, with None last.
        queue_rows = facets["queue"]
        assert queue_rows[-1]["value"] is None

    def test_self_exclusion_keeps_own_dimension_open(self, in_memory_session, facet_jobs):
        # With queue='main' selected, the queue facet must still list every
        # queue (so the user can switch), while the other facets narrow.
        facets = JobQueries(in_memory_session).jobs_facets(queue="main")
        assert self._as_map(facets["queue"]) == {"main": 3, "gpudev": 2, None: 1}
        assert self._as_map(facets["qos"]) == {"regular": 2, "premium": 1}

    def test_self_exclude_false_collapses_own_dimension(
            self, in_memory_session, facet_jobs):
        facets = JobQueries(in_memory_session).jobs_facets(
            queue="main", self_exclude=False)
        assert self._as_map(facets["queue"]) == {"main": 3}

    def test_other_excluded_dimensions_still_apply(self, in_memory_session, facet_jobs):
        # Two faceted filters at once: the qos facet respects queue='main',
        # and the queue facet respects qos='regular' — each drops only its own.
        facets = JobQueries(in_memory_session).jobs_facets(
            queue="main", qos="regular")
        assert self._as_map(facets["qos"]) == {"regular": 2, "premium": 1}
        assert self._as_map(facets["queue"]) == {"main": 2, "gpudev": 1, None: 1}

    def test_account_is_never_self_excluded(self, in_memory_session, facet_jobs):
        # Security invariant: account is the authorization scope in every
        # real caller, so its counts must stay inside the requested scope.
        facets = JobQueries(in_memory_session).jobs_facets(
            account="NCAR0001", facets=("account", "queue"))
        assert self._as_map(facets["account"]) == {"NCAR0001": 3}
        assert self._as_map(facets["queue"]) == {"main": 3}

    def test_opt_in_dimensions(self, in_memory_session, facet_jobs):
        facets = JobQueries(in_memory_session).jobs_facets(facets=("user",))
        assert self._as_map(facets["user"]) == {"alice": 3, "bob": 2, "carol": 1}

    def test_limit_truncates_without_other_bucket(self, in_memory_session, facet_jobs):
        rows = JobQueries(in_memory_session).jobs_facets(
            facets=("user",), limit=2)["user"]
        assert [r["value"] for r in rows] == ["alice", "bob"]
        # The tail is dropped, not folded into a synthetic "other" row.
        assert sum(r["count"] for r in rows) == 5

    def test_respects_the_full_filter_set(self, in_memory_session, facet_jobs):
        # Facets must reflect the same filters jobs_search would apply,
        # including the new name glob.
        facets = JobQueries(in_memory_session).jobs_facets(name="f0")
        assert self._as_map(facets["qos"]) == {"premium": 1}

    def test_unknown_facet_raises(self, in_memory_session, facet_jobs):
        with pytest.raises(ValueError, match="Unknown facet"):
            JobQueries(in_memory_session).jobs_facets(facets=("nope",))

    def test_bad_limit_raises(self, in_memory_session, facet_jobs):
        with pytest.raises(ValueError, match="positive integer"):
            JobQueries(in_memory_session).jobs_facets(limit=0)

    def test_empty_facets_returns_empty_dict(self, in_memory_session, facet_jobs):
        assert JobQueries(in_memory_session).jobs_facets(facets=()) == {}

    def test_all_facets_come_from_one_aggregate_scan(self, in_memory_session, facet_jobs):
        """N facets must not mean N scans of the jobs table.

        Also pins the grouping to the integer FKs: ``GROUP BY Job.queue``
        would emit the LookupMixin hybrid's correlated scalar subquery, which
        measured 10x slower on PostgreSQL.
        """
        from sqlalchemy import event
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_facets(
                facets=("queue", "qos", "exit_status", "user"))
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        aggregates = [s for s in statements
                      if "GROUP BY" in s.upper() and " jobs" in s.lower()]
        assert len(aggregates) == 1, \
            f"expected 1 aggregate scan, got {len(aggregates)}:\n{aggregates}"
        for fk in ("jobs.queue_id", "jobs.qos_id", "jobs.user_id"):
            assert fk in aggregates[0], f"{fk} missing — grouping on the hybrid?"
        assert "SELECT queues.queue_name" not in aggregates[0]
        assert "SELECT users.username" not in aggregates[0]


@pytest.fixture
def histogram_jobs(in_memory_session):
    """Six jobs spanning several bands on all six histogram dimensions.

    Deliberate edge rows: a NULL ``eligible_secs`` (pre-2025 derecho), a
    NULL ``reqmem``, a job with **no JobCharge row** (must count but add
    0.0 hours), a ``numgpus=0`` row (pins the GPU '0' band), and a
    small-reqmem/large-``memory`` row (pins requested-vs-used).
    """
    base = datetime(2025, 7, 1, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    specs = [
        # (jid, user, acct, elig, nodes, cpus, gpus, elapsed, reqmem, memory)
        ("700.desched1", "alice", "NCAR0001", 30,     1,  128,  0, 20,
         4 * _GIB,   2 * _GIB),
        ("701.desched1", "alice", "NCAR0001", 120,    2,  256,  0, 3600,
         64 * _GIB,  4 * _GIB),
        ("702.desched1", "alice", "NCAR0001", 7200,   16, 2048, 4, 90000,
         512 * _GIB, 100 * _GIB),
        # NULL wait; reqmem < 1 GiB while USED memory is 600 GiB.
        ("703.desched1", "bob",   "NCAR0002", None,   1,  1,    0, 30,
         _GIB // 2,  600 * _GIB),
        # NULL reqmem; wait beyond 2 days.
        ("704.desched1", "bob",   "NCAR0002", 200000, 1,  64,   2, 1800,
         None,       None),
        # Zero wait and zero elapsed; carries NO JobCharge row.
        ("705.desched1", "carol", "NCAR0002", 0,      1,  1,    0, 0,
         2 * _GIB,   _GIB),
    ]
    jobs = [
        Job(job_id=jid, short_id=700 + i, name=f"h{i}", user=u, account=a,
            queue="main", status="0",
            submit=base, start=base, end=base + timedelta(hours=i + 1),
            eligible_secs=elig, numnodes=nodes, numcpus=cpus, numgpus=gpus,
            elapsed=elapsed, reqmem=reqmem, memory=memory)
        for i, (jid, u, a, elig, nodes, cpus, gpus, elapsed, reqmem, memory)
        in enumerate(specs)
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.flush()

    hours = {  # job_id -> (cpu_hours, gpu_hours); 705 stays charge-less
        "700.desched1": (10.0, 0.0),
        "701.desched1": (100.0, 0.0),
        "702.desched1": (5000.0, 400.0),
        "703.desched1": (1.0, 0.0),
        "704.desched1": (30.0, 60.0),
    }
    for j in jobs:
        if j.job_id in hours:
            cpu, gpu = hours[j.job_id]
            in_memory_session.add(JobCharge(
                job_id=j.id, cpu_hours=cpu, gpu_hours=gpu,
                memory_hours=0.0, qos_factor=1.0, charge_version=1))
    in_memory_session.commit()
    return jobs


@pytest.fixture
def floor_band_jobs(in_memory_session):
    """Two jobs pinning the *domain floor* of every histogram dimension.

    ``_bucket_case`` tests only ``hi``, so a value below the first band's
    ``lo`` is still claimed by that band's arm — labelled with a band whose
    advertised bounds exclude it. Production carries exactly that shape:
    404 derecho / 21 casper rows have ``numcpus=0`` while ``CPU_HIST_BUCKETS``
    used to start at 1, so the "1" bar over-counted and clicking it returned
    fewer rows than it claimed.

    ``zero`` sits at the floor of all seven columns, ``one`` one step above
    it. They must land in *different* bands on every dimension.
    """
    base = datetime(2025, 8, 1, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    specs = [
        # (jid, elig, nodes, cpus, gpus, elapsed, reqmem, memory)
        ("800.desched1", 0,  0, 0, 0, 0,  0,    0),
        ("801.desched1", 60, 1, 1, 1, 30, _GIB, _GIB),
    ]
    for i, (jid, elig, nodes, cpus, gpus, elapsed, reqmem, memory) in enumerate(specs):
        in_memory_session.add(Job(
            job_id=jid, short_id=800 + i, name=f"f{i}", user="dave",
            account="NCAR0003", queue="main", status="0",
            submit=base, start=base, end=base + timedelta(hours=i + 1),
            eligible_secs=elig, numnodes=nodes, numcpus=cpus, numgpus=gpus,
            elapsed=elapsed, reqmem=reqmem, memory=memory))
    in_memory_session.commit()


class TestJobsHistogram:
    """(wait, nodes, cpus, gpus, elapsed, reqmem) per the histogram_jobs docstring."""

    @staticmethod
    def _by_label(out):
        return {b["label"]: b for b in out["buckets"]}

    @staticmethod
    def _assert_bands_round_trip(q):
        """Every non-empty band replays as jobs_search bounds and reproduces
        its own count, on every dimension."""
        for dim in _HISTOGRAM_SPECS:
            out = q.jobs_histogram(dim)
            for band in out["buckets"]:
                if band["job_count"] == 0:
                    continue
                kw = {out["min_param"]: band["lo"]}
                if band["hi"] is not None:
                    kw[out["max_param"]] = band["hi"]
                assert q.jobs_count(**kw) == band["job_count"], \
                    (dim, band["label"])

    def test_bucket_vector_complete_ordered_with_zeros(
            self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram("wait")
        assert [b["label"] for b in out["buckets"]] == \
            [label for label, _lo, _hi in QueryConfig.WAIT_BUCKETS]
        # Unpopulated bands are present with explicit zeros.
        empty = self._by_label(out)["12-24h"]
        assert (empty["job_count"], empty["cpu_hours"], empty["gpu_hours"]) == \
            (0, 0.0, 0.0)

    def test_wait_counts_per_bucket(self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram("wait")
        by = self._by_label(out)
        assert by["<1m"]["job_count"] == 2      # 30 s and 0 s
        assert by["1-5m"]["job_count"] == 1     # 120 s
        assert by["2-4h"]["job_count"] == 1     # 7200 s
        assert by[">2d"]["job_count"] == 1      # 200000 s
        assert out["null_count"] == 1
        assert out["total_count"] == 6

    def test_each_dimension_buckets_its_own_column(
            self, in_memory_session, histogram_jobs):
        q = JobQueries(in_memory_session)
        checks = {           # dimension -> (band, expected job_count)
            "nodes":    ("1", 4),
            "cpus":     ("65-128", 1),
            "gpus":     ("0", 4),
            "duration": ("<30s", 2),           # 20 s and 0 s
            "memory":   ("1-10GB", 2),         # 4 GiB and 2 GiB requested
        }
        for dim, (label, expected) in checks.items():
            out = q.jobs_histogram(dim)
            assert self._by_label(out)[label]["job_count"] == expected, dim

    def test_hours_summed_and_chargeless_job_adds_zero(
            self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram("wait")
        band = self._by_label(out)["<1m"]
        # 700 contributes 10.0 cpu-h; charge-less 705 contributes the count
        # and 0.0 hours (outer join, SUM ignores its NULL charge row).
        assert band["job_count"] == 2
        assert band["cpu_hours"] == pytest.approx(10.0)
        assert band["gpu_hours"] == pytest.approx(0.0)

    def test_null_row_in_no_bucket_and_total_identity(
            self, in_memory_session, histogram_jobs):
        q = JobQueries(in_memory_session)
        out = q.jobs_histogram("wait")
        assert sum(b["job_count"] for b in out["buckets"]) == 5
        assert out["null_count"] == 1
        assert out["total_count"] == q.jobs_count()

    def test_bounds_round_trip_into_filters(self, in_memory_session, histogram_jobs):
        # THE contract test: every non-empty band replays as jobs_search
        # bounds and reproduces its own count, across all six dimensions.
        # This is what forces min/max_elapsed + min/max_reqmem to exist.
        self._assert_bands_round_trip(JobQueries(in_memory_session))

    def test_bounds_round_trip_at_the_domain_floor(
            self, in_memory_session, floor_band_jobs):
        # The same contract where histogram_jobs cannot reach it: a value at
        # the floor of each column. Regression for numcpus=0 being counted
        # in the "1" bar while min_cpus=1/max_cpus=1 excluded it.
        self._assert_bands_round_trip(JobQueries(in_memory_session))

    def test_domain_floor_gets_its_own_band(
            self, in_memory_session, floor_band_jobs):
        # Every count-valued table must reach 0, or _bucket_case folds the
        # zero row into the band above it (see the leading-band note there).
        q = JobQueries(in_memory_session)
        for dim in ("cpus", "nodes", "gpus"):
            by = self._by_label(q.jobs_histogram(dim))
            assert by["0"]["job_count"] == 1, dim
            assert by["1"]["job_count"] == 1, dim

    def test_bucket_tables_are_closed_and_contiguous(self):
        # Structural guard on the tables themselves, so a new dimension
        # cannot reintroduce the numcpus=0 bug. _bucket_case's ladder tests
        # only `hi`, so a table that fails to reach its column's floor
        # mislabels sub-floor rows, and one that fails to end open-ended
        # mislabels over-range rows into else_ — both breaking the bar↔
        # jobs_search round-trip while leaving total_count intact.
        for dim, (_col, buckets, _unit, _mn, _mx) in _HISTOGRAM_SPECS.items():
            first, last = buckets[0], buckets[-1]
            # lo=None is the signed-dimension floor (memory_wasted); every
            # other column is non-negative, so its floor is 0.
            assert first[1] in (0, None), (dim, "first band misses the floor")
            assert last[2] is None, (dim, "last band is not open-ended")
            for (label, _lo, hi), (nxt, lo_next, _hi) in zip(buckets, buckets[1:]):
                assert hi is not None, (dim, label, "gap: closed mid-table band")
                assert lo_next == hi + 1, (dim, label, nxt, "bands not contiguous")

    def test_memory_buckets_reqmem_not_memory(self, in_memory_session, histogram_jobs):
        # 703 REQUESTED < 1 GiB but USED 600 GiB — it must land in "<1GB".
        # 702's 512 GiB request (100 GiB used) pins the other direction.
        by = self._by_label(JobQueries(in_memory_session).jobs_histogram("memory"))
        assert by["<1GB"]["job_count"] == 1
        assert by["500-1000GB"]["job_count"] == 1
        assert by[">1000GB"]["job_count"] == 0

    def test_memory_used_buckets_memory_not_reqmem(
            self, in_memory_session, histogram_jobs):
        # The mirror of test_memory_buckets_reqmem_not_memory: 703's 600 GiB
        # USED lands in '500-1000GB' even though it requested < 1 GiB, and
        # 702's 100 GiB used (512 GiB requested) sits at the inclusive top
        # of '50-100GB'. 704 has NULL memory → null_count, never a band.
        out = JobQueries(in_memory_session).jobs_histogram("memory_used")
        by = self._by_label(out)
        assert by["500-1000GB"]["job_count"] == 1
        assert by["50-100GB"]["job_count"] == 1
        assert by["1-10GB"]["job_count"] == 3    # 2, 4 and 1 GiB used
        assert by["<1GB"]["job_count"] == 0
        assert out["null_count"] == 1
        assert out["total_count"] == 6

    def test_memory_wasted_over_request_band_leads(
            self, in_memory_session, histogram_jobs):
        # 703 used 600 GiB against a <1 GiB request → negative delta, caught
        # by the leading 'over request' band, which replays as
        # max_memory_wasted=-1 with NO min bound (lo is None).
        out = JobQueries(in_memory_session).jobs_histogram("memory_wasted")
        first = out["buckets"][0]
        assert first["label"] == "over request"
        assert (first["lo"], first["hi"]) == (None, -1)
        assert first["job_count"] == 1

    def test_memory_wasted_counts_and_null_propagation(
            self, in_memory_session, histogram_jobs):
        # Deltas: 700→2 GiB, 701→60 GiB, 702→412 GiB, 705→1 GiB,
        # 703→negative, 704→NULL (both columns NULL → NULL delta).
        out = JobQueries(in_memory_session).jobs_histogram("memory_wasted")
        by = self._by_label(out)
        assert by["1-10GB"]["job_count"] == 2
        assert by["50-100GB"]["job_count"] == 1
        assert by["100-500GB"]["job_count"] == 1
        assert out["null_count"] == 1
        assert out["total_count"] == 6

    def test_memory_wasted_envelope_and_one_aggregate_scan(
            self, in_memory_session, histogram_jobs):
        # The computed-expression dimension must still be one CASE-grouped
        # statement (no per-row subqueries) and self-describe its column.
        from sqlalchemy import event
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            out = JobQueries(in_memory_session).jobs_histogram("memory_wasted")
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        assert (out["column"], out["unit"]) == ("memory_wasted", "bytes")
        assert (out["min_param"], out["max_param"]) == \
            ("min_memory_wasted", "max_memory_wasted")

        aggregates = [s for s in statements
                      if "GROUP BY" in s.upper() and " jobs" in s.lower()]
        assert len(aggregates) == 1
        assert "reqmem" in aggregates[0] and "memory" in aggregates[0]

    def test_memory_used_envelope(self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram("memory_used")
        assert (out["column"], out["unit"]) == ("memory", "bytes")
        assert (out["min_param"], out["max_param"]) == \
            ("min_memory_used", "max_memory_used")

    def test_gpus_zero_band_not_overflow(self, in_memory_session, histogram_jobs):
        by = self._by_label(JobQueries(in_memory_session).jobs_histogram("gpus"))
        assert by["0"]["job_count"] == 4
        assert by[">256"]["job_count"] == 0

    def test_filters_apply(self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram("wait", user="alice")
        assert out["total_count"] == 3
        assert out["null_count"] == 0

    def test_account_scope_always_applies(self, in_memory_session, histogram_jobs):
        # No self-exclusion machinery exists here: account (the SAM
        # authorization boundary) constrains the histogram unconditionally.
        out = JobQueries(in_memory_session).jobs_histogram(
            "duration", account=["NCAR0002"])
        assert out["total_count"] == 3

    def test_envelope_is_self_describing(self, in_memory_session, histogram_jobs):
        q = JobQueries(in_memory_session)
        wait = q.jobs_histogram("wait")
        assert (wait["column"], wait["unit"]) == ("eligible_secs", "seconds")
        assert (wait["min_param"], wait["max_param"]) == \
            ("min_eligible_secs", "max_eligible_secs")
        mem = q.jobs_histogram("memory")
        assert (mem["column"], mem["unit"]) == ("reqmem", "bytes")

    def test_unknown_dimension_raises(self, in_memory_session, histogram_jobs):
        with pytest.raises(ValueError, match="Unknown dimension"):
            JobQueries(in_memory_session).jobs_histogram("walltime")

    def test_one_aggregate_scan(self, in_memory_session, histogram_jobs):
        """The histogram must be one CASE-grouped statement over jobs +
        job_charges, with no hybrid scalar subqueries."""
        from sqlalchemy import event
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_histogram(
                "wait", account="NCAR0001")
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        aggregates = [s for s in statements
                      if "GROUP BY" in s.upper() and " jobs" in s.lower()]
        assert len(aggregates) == 1, \
            f"expected 1 aggregate scan, got {len(aggregates)}:\n{aggregates}"
        assert "CASE" in aggregates[0].upper()
        assert "job_charges" in aggregates[0]
        assert "SELECT users.username" not in aggregates[0]
        assert "SELECT queues.queue_name" not in aggregates[0]


class TestJobsHistogramOwners:
    """owners_limit on jobs_histogram (histogram_jobs fixture).

    Wait-dimension shape: '<1m' band holds 700 (alice, 10 cpu-h) and 705
    (carol, charge-less); 703 (bob) has a NULL wait → null_count.
    """

    @staticmethod
    def _band(out, label):
        return next(b for b in out["buckets"] if b["label"] == label)

    def test_owners_absent_by_default(self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram("wait")
        assert all("owners" not in b for b in out["buckets"])

    def test_envelope_identical_modulo_owners(
            self, in_memory_session, histogram_jobs):
        """The owners variant is the plain envelope plus one appended key per
        bucket — nothing else may move.

        Counts are compared exactly; hours only to a tolerance. The owners
        path adds N per-user partial SUMs in Python instead of taking one
        SQL SUM, and float addition isn't associative, so the two agree to
        rounding and no further — on real derecho data they differ in the
        last ULP on every dimension. This fixture is too small to expose
        that, which is exactly why the tolerance belongs here rather than
        being discovered downstream.
        """
        q = JobQueries(in_memory_session)
        plain = q.jobs_histogram("wait")
        rich = q.jobs_histogram("wait", owners_limit=10)

        assert all(list(b)[-1] == "owners" for b in rich["buckets"])
        assert {k: v for k, v in rich.items() if k != "buckets"} == \
            {k: v for k, v in plain.items() if k != "buckets"}
        for got, want in zip(rich["buckets"], plain["buckets"]):
            assert set(got) - {"owners"} == set(want)
            assert (got["label"], got["lo"], got["hi"], got["job_count"]) == \
                (want["label"], want["lo"], want["hi"], want["job_count"])
            assert got["cpu_hours"] == pytest.approx(want["cpu_hours"])
            assert got["gpu_hours"] == pytest.approx(want["gpu_hours"])

    def test_owners_shape_and_rank(self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram(
            "wait", owners_limit=10)
        band = self._band(out, "<1m")
        assert list(band["owners"]) == ["alice", "carol"]  # 10 cpu-h > 0
        assert band["owners"]["alice"] == {
            "job_count": 1, "cpu_hours": 10.0, "gpu_hours": 0.0,
            "cpu_charges": 10.0, "gpu_charges": 0.0}
        assert band["owners"]["carol"] == {
            "job_count": 1, "cpu_hours": 0.0, "gpu_hours": 0.0,
            "cpu_charges": 0.0, "gpu_charges": 0.0}

    def test_owners_limit_truncates_but_totals_authoritative(
            self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram(
            "wait", owners_limit=1)
        band = self._band(out, "<1m")
        assert list(band["owners"]) == ["alice"]
        # Band totals still cover the truncated tail — the consumer's
        # remainder is totals − Σ owners, an invariant not a guess.
        assert band["job_count"] == 2
        assert band["job_count"] - sum(
            o["job_count"] for o in band["owners"].values()) == 1

    def test_null_band_folds_into_null_count(
            self, in_memory_session, histogram_jobs):
        q = JobQueries(in_memory_session)
        rich = q.jobs_histogram("wait", owners_limit=10)
        assert rich["null_count"] == q.jobs_histogram("wait")["null_count"] == 1
        assert rich["total_count"] == 6

    def test_zero_count_bucket_has_empty_owners(
            self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram(
            "wait", owners_limit=10)
        empty = [b for b in out["buckets"] if b["job_count"] == 0]
        assert empty and all(b["owners"] == {} for b in empty)

    def test_bad_owners_limit_raises(self, in_memory_session, histogram_jobs):
        with pytest.raises(ValueError, match="owners_limit"):
            JobQueries(in_memory_session).jobs_histogram(
                "wait", owners_limit=0)

    def test_owners_by_defaults_to_user(self, in_memory_session, histogram_jobs):
        """Explicit owners_by='user' is byte-identical to the default —
        same code path both times, so exact equality is safe here."""
        q = JobQueries(in_memory_session)
        assert q.jobs_histogram("wait", owners_limit=10) == \
            q.jobs_histogram("wait", owners_limit=10, owners_by="user")

    def test_owners_by_account_shape_and_rank(
            self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_histogram(
            "wait", owners_limit=10, owners_by="account")
        band = self._band(out, "<1m")
        assert list(band["owners"]) == ["NCAR0001", "NCAR0002"]  # 10 cpu-h > 0
        assert band["owners"]["NCAR0001"] == {
            "job_count": 1, "cpu_hours": 10.0, "gpu_hours": 0.0,
            "cpu_charges": 10.0, "gpu_charges": 0.0}
        assert band["owners"]["NCAR0002"] == {
            "job_count": 1, "cpu_hours": 0.0, "gpu_hours": 0.0,
            "cpu_charges": 0.0, "gpu_charges": 0.0}

    def test_owners_by_account_merges_users_within_account(
            self, in_memory_session, histogram_jobs):
        """The nodes '1' band holds alice(NCAR0001) + bob×2 + carol(NCAR0002):
        the two NCAR0002 users must fold into ONE account owner, ranked
        above NCAR0001 by combined hours (91 > 10)."""
        out = JobQueries(in_memory_session).jobs_histogram(
            "nodes", owners_limit=10, owners_by="account")
        band = self._band(out, "1")
        assert list(band["owners"]) == ["NCAR0002", "NCAR0001"]
        assert band["owners"]["NCAR0002"] == {
            "job_count": 3, "cpu_hours": 31.0, "gpu_hours": 60.0,
            "cpu_charges": 31.0, "gpu_charges": 60.0}
        assert band["owners"]["NCAR0001"] == {
            "job_count": 1, "cpu_hours": 10.0, "gpu_hours": 0.0,
            "cpu_charges": 10.0, "gpu_charges": 0.0}

    def test_owners_by_account_totals_unchanged(
            self, in_memory_session, histogram_jobs):
        """Bucket totals / null_count / total_count never depend on the
        owner dimension — only the appended owners key differs."""
        q = JobQueries(in_memory_session)
        by_user = q.jobs_histogram("wait", owners_limit=10)
        by_acct = q.jobs_histogram("wait", owners_limit=10, owners_by="account")
        assert by_acct["null_count"] == by_user["null_count"]
        assert by_acct["total_count"] == by_user["total_count"]
        for got, want in zip(by_acct["buckets"], by_user["buckets"]):
            assert got["job_count"] == want["job_count"]
            assert got["cpu_hours"] == pytest.approx(want["cpu_hours"])
            assert got["gpu_hours"] == pytest.approx(want["gpu_hours"])

    def test_bad_owners_by_raises(self, in_memory_session, histogram_jobs):
        with pytest.raises(ValueError, match="owners_by"):
            JobQueries(in_memory_session).jobs_histogram(
                "wait", owners_limit=10, owners_by="queue")

    def test_account_owners_round_trip_into_filters(
            self, in_memory_session, histogram_jobs, floor_band_jobs):
        """The account-mode drill: jobs_count(account=name, **band bounds)
        equals each listed owner's job_count — the analogue of
        test_owners_round_trip_into_filters."""
        q = JobQueries(in_memory_session)
        checked = 0
        for dim in _HISTOGRAM_SPECS:
            out = q.jobs_histogram(dim, owners_limit=10, owners_by="account")
            for band in out["buckets"]:
                kw = {out["min_param"]: band["lo"]}
                if band["hi"] is not None:
                    kw[out["max_param"]] = band["hi"]
                for name, agg in band["owners"].items():
                    assert q.jobs_count(account=name, **kw) == \
                        agg["job_count"], f"{dim}/{band['label']}/{name}"
                    checked += 1
        assert checked > 0

    def test_one_aggregate_scan_account_owners(
            self, in_memory_session, histogram_jobs):
        from sqlalchemy import event
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_histogram(
                "wait", owners_limit=10, owners_by="account")
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        aggregates = [s for s in statements
                      if "GROUP BY" in s.upper() and " jobs" in s.lower()]
        assert len(aggregates) == 1, \
            f"expected 1 aggregate scan, got {len(aggregates)}:\n{aggregates}"
        assert "jobs.account_id" in aggregates[0]
        assert "account_name" not in aggregates[0]

    def test_one_aggregate_scan_with_owners(
            self, in_memory_session, histogram_jobs):
        from sqlalchemy import event
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_histogram(
                "wait", owners_limit=10)
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        aggregates = [s for s in statements
                      if "GROUP BY" in s.upper() and " jobs" in s.lower()]
        assert len(aggregates) == 1, \
            f"expected 1 aggregate scan, got {len(aggregates)}:\n{aggregates}"
        assert "jobs.user_id" in aggregates[0]
        assert "SELECT users.username" not in aggregates[0]

    def test_owners_round_trip_into_filters(
            self, in_memory_session, histogram_jobs, floor_band_jobs):
        """Level three of the drill-down: every listed owner replays.

        A bar's owner slice must return exactly the jobs it claims when
        clicked — ``jobs_count(user=name, **band bounds)``. This is the
        owners-level analogue of ``_assert_bands_round_trip``, and the same
        class of defect that made the ``cpus`` "1" bar over-count before the
        bucket tables were closed at the domain floor.
        """
        q = JobQueries(in_memory_session)
        checked = 0
        for dim in _HISTOGRAM_SPECS:
            out = q.jobs_histogram(dim, owners_limit=10)
            for band in out["buckets"]:
                kw = {out["min_param"]: band["lo"]}
                if band["hi"] is not None:
                    kw[out["max_param"]] = band["hi"]
                for name, agg in band["owners"].items():
                    assert q.jobs_count(user=name, **kw) == agg["job_count"], \
                        (dim, band["label"], name)
                    checked += 1
                # …and the listed owners never over-claim the band.
                assert sum(o["job_count"] for o in band["owners"].values()) \
                    <= band["job_count"], (dim, band["label"])
        assert checked, "fixtures produced no owners to round-trip"

    def test_owners_sort_by_decides_which_top_n_survives(
            self, in_memory_session, usage_sort_jobs):
        """The bug jobs_usage_by(sort_by=) fixes, one level down.

        All three usage_sort_jobs rows share a band on every dimension, so
        the band's owner list is a pure ranking question: cpuking leads on
        combined hours, gpuqueen on gpu_hours and on job_count. Ranked by
        combined hours a GPU-stacked bar shows the wrong user — measured on
        derecho, the top-5 combined-hours owners of a wait or duration band
        cover ~0% of that band's GPU hours.
        """
        q = JobQueries(in_memory_session)

        def top(**kw):
            out = q.jobs_histogram("wait", owners_limit=1, **kw)
            band = next(b for b in out["buckets"] if b["owners"])
            return list(band["owners"])

        assert top() == ["cpuking"]                        # default: hours
        assert top(owners_sort_by="hours") == ["cpuking"]
        assert top(owners_sort_by="cpu_hours") == ["cpuking"]
        assert top(owners_sort_by="gpu_hours") == ["gpuqueen"]
        assert top(owners_sort_by="job_count") == ["gpuqueen"]

    @pytest.mark.parametrize("key", _USAGE_SORT_KEYS)
    def test_owners_sort_by_never_changes_band_totals(
            self, in_memory_session, usage_sort_jobs, key):
        # Ranking picks *which* owners are listed, never what the band says.
        q = JobQueries(in_memory_session)
        plain = q.jobs_histogram("wait")
        rich = q.jobs_histogram("wait", owners_limit=1, owners_sort_by=key)
        assert [b["job_count"] for b in rich["buckets"]] == \
            [b["job_count"] for b in plain["buckets"]]
        assert rich["total_count"] == plain["total_count"]

    def test_bad_owners_sort_by_raises(self, in_memory_session, histogram_jobs):
        with pytest.raises(ValueError, match="Unknown owners_sort_by"):
            JobQueries(in_memory_session).jobs_histogram(
                "wait", owners_limit=5, owners_sort_by="memory_hours")


@pytest.fixture
def usage_sort_jobs(in_memory_session):
    """The sort_by regression shape: cpuking dominates combined hours with
    one job; gpuqueen dominates gpu_hours AND job_count with two."""
    base = datetime(2025, 9, 1, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    specs = [
        # (jid, user, cpu_hours, gpu_hours)
        ("900.desched1", "cpuking",  1000.0, 0.0),
        ("901.desched1", "gpuqueen", 5.0,    250.0),
        ("902.desched1", "gpuqueen", 5.0,    250.0),
    ]
    jobs = [
        Job(job_id=jid, short_id=900 + i, name=f"u{i}", user=u,
            account="NCAR0009", queue="main", status="0",
            submit=base, start=base, end=base + timedelta(hours=i + 1),
            eligible_secs=60, numnodes=1, numcpus=8, numgpus=0,
            elapsed=600, reqmem=_GIB, memory=_GIB // 2)
        for i, (jid, u, _cpu, _gpu) in enumerate(specs)
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.flush()
    for j, (_jid, _u, cpu, gpu) in zip(jobs, specs):
        in_memory_session.add(JobCharge(
            job_id=j.id, cpu_hours=cpu, gpu_hours=gpu,
            memory_hours=0.0, qos_factor=1.0, charge_version=1))
    in_memory_session.commit()
    return jobs


class TestJobsUsageBy:
    """Uses histogram_jobs: alice 3 jobs (5110 cpu-h, 400 gpu-h), bob 2
    (31 cpu-h, 60 gpu-h), carol 1 (charge-less)."""

    def test_row_contract_and_hours_desc_ordering(
            self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by("user")
        assert out["dimension"] == "user"
        for row in out["rows"]:
            assert set(row) == {"value", "job_count", "cpu_hours", "gpu_hours",
                                "cpu_charges", "gpu_charges"}
        assert [r["value"] for r in out["rows"]] == ["alice", "bob", "carol"]

    def test_totals_match_jobs_count_and_rows_sum(
            self, in_memory_session, histogram_jobs):
        q = JobQueries(in_memory_session)
        out = q.jobs_usage_by("user")
        assert out["totals"]["job_count"] == q.jobs_count() == 6
        assert out["totals"]["cpu_hours"] == pytest.approx(5141.0)
        assert out["totals"]["gpu_hours"] == pytest.approx(460.0)
        assert sum(r["job_count"] for r in out["rows"]) == \
            out["totals"]["job_count"]
        assert sum(r["cpu_hours"] for r in out["rows"]) == \
            pytest.approx(out["totals"]["cpu_hours"])

    def test_chargeless_job_counts_with_zero_hours(
            self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by("user")
        carol = next(r for r in out["rows"] if r["value"] == "carol")
        assert carol["job_count"] == 1
        assert carol["cpu_hours"] == 0.0
        assert carol["gpu_hours"] == 0.0

    def test_limit_truncates_rows_but_not_totals(
            self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by("user", limit=1)
        assert [r["value"] for r in out["rows"]] == ["alice"]
        # totals still describe the WHOLE filtered slice — the consumer's
        # "Other" slice is totals − Σ rows, an invariant not a guess.
        assert out["totals"]["job_count"] == 6
        assert sum(r["job_count"] for r in out["rows"]) <= \
            out["totals"]["job_count"]

    def test_account_scoping_always_applied(self, in_memory_session, histogram_jobs):
        # Security mirror of test_account_is_never_self_excluded: with an
        # account scope pinned, no foreign user or project may appear —
        # even when the dimension IS account.
        q = JobQueries(in_memory_session)
        by_user = q.jobs_usage_by("user", account="NCAR0001")
        assert [r["value"] for r in by_user["rows"]] == ["alice"]
        assert by_user["totals"]["job_count"] == 3
        by_account = q.jobs_usage_by("account", account="NCAR0001")
        assert [r["value"] for r in by_account["rows"]] == ["NCAR0001"]

    def test_filters_apply(self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by("user", min_gpus=1)
        assert {r["value"] for r in out["rows"]} == {"alice", "bob"}
        assert out["totals"]["job_count"] == 2   # 702 and 704

    def test_exit_status_groups_text_directly(self, in_memory_session, histogram_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by("exit_status")
        assert [r["value"] for r in out["rows"]] == ["0"]
        assert out["rows"][0]["job_count"] == 6

    def test_null_fk_surfaces_as_none_last(self, in_memory_session, facet_jobs):
        # facet_jobs: carol's job has queue=None; no charges exist, so all
        # hours are 0.0 and ordering falls back to value asc, None last.
        out = JobQueries(in_memory_session).jobs_usage_by("queue")
        counts = {r["value"]: r["job_count"] for r in out["rows"]}
        assert counts == {"main": 3, "gpudev": 2, None: 1}
        assert out["rows"][-1]["value"] is None
        assert out["totals"]["job_count"] == 6

    def test_unknown_dimension_raises(self, in_memory_session, histogram_jobs):
        with pytest.raises(ValueError, match="Unknown dimension"):
            JobQueries(in_memory_session).jobs_usage_by("facility")

    def test_bad_limit_raises(self, in_memory_session, histogram_jobs):
        with pytest.raises(ValueError, match="positive integer"):
            JobQueries(in_memory_session).jobs_usage_by("user", limit=0)

    def test_sort_by_default_is_combined_hours(
            self, in_memory_session, usage_sort_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by("user")
        assert [r["value"] for r in out["rows"]] == ["cpuking", "gpuqueen"]

    def test_sort_by_gpu_hours_reorders(self, in_memory_session, usage_sort_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by(
            "user", sort_by="gpu_hours")
        assert [r["value"] for r in out["rows"]] == ["gpuqueen", "cpuking"]

    def test_sort_by_job_count_reorders(self, in_memory_session, usage_sort_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by(
            "user", sort_by="job_count")
        assert [r["value"] for r in out["rows"]] == ["gpuqueen", "cpuking"]

    def test_sort_by_decides_which_top_n_survives_limit(
            self, in_memory_session, usage_sort_jobs):
        # The dashboard bug this parameter exists for: ranked by combined
        # hours, a pure-GPU user falls below the cut and a GPU-hours view
        # shows almost nobody. Ranking by the viewed metric keeps them.
        q = JobQueries(in_memory_session)
        by_hours = q.jobs_usage_by("user", limit=1)
        assert [r["value"] for r in by_hours["rows"]] == ["cpuking"]
        by_gpu = q.jobs_usage_by("user", limit=1, sort_by="gpu_hours")
        assert [r["value"] for r in by_gpu["rows"]] == ["gpuqueen"]
        # Totals stay pre-truncation in both rankings.
        assert by_gpu["totals"]["gpu_hours"] == pytest.approx(500.0)
        assert by_gpu["totals"]["job_count"] == 3

    def test_bad_sort_by_raises(self, in_memory_session, usage_sort_jobs):
        with pytest.raises(ValueError, match="Unknown sort_by"):
            JobQueries(in_memory_session).jobs_usage_by(
                "user", sort_by="memory_hours")

    def test_one_aggregate_scan_groups_the_fk(self, in_memory_session, histogram_jobs):
        from sqlalchemy import event
        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_usage_by(
                "user", account="NCAR0001")
        finally:
            event.remove(in_memory_session.bind, "before_cursor_execute", _capture)

        aggregates = [s for s in statements
                      if "GROUP BY" in s.upper() and " jobs" in s.lower()]
        assert len(aggregates) == 1, \
            f"expected 1 aggregate scan, got {len(aggregates)}:\n{aggregates}"
        assert "jobs.user_id" in aggregates[0]
        assert "job_charges" in aggregates[0]
        assert "SELECT users.username" not in aggregates[0]


# ---------------------------------------------------------------------------
# jobs_timeseries — per-period activity series
# ---------------------------------------------------------------------------

@pytest.fixture
def timeseries_jobs(in_memory_session):
    """Five jobs over a 5-day site-local window, with the edges that matter.

    Deliberate rows:

    - **801 ends 02:00 UTC on 07-02**, which is 20:00 MDT on 07-01. It must
      bin into the **07-01** band. Bucketing the raw UTC column (what
      ``PeriodGrouper`` would do) puts it in 07-02 — this row is the guard
      against that regression.
    - **07-03 is idle**, pinning an interior zero band.
    - **803 carries ``qos_factor=0.0``** (the ``uncharged`` QoS): hours > 0
      with charges == 0.
    - **802 carries ``qos_factor=0.5``**, so charges != hours.
    - **804 has NO JobCharge row** — counts, contributes 0.0 to every sum.
    """
    def _utc(y, m, d, hh):
        return datetime(y, m, d, hh, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)

    specs = [
        # (jid, user, acct, end_utc, cpu_h, gpu_h, qos_factor)
        ("800.desched1", "alice", "NCAR0001", _utc(2025, 7, 1, 18), 10.0, 0.0, 1.0),
        # 20:00 MDT on 07-01 — the site-local binning guard.
        ("801.desched1", "alice", "NCAR0001", _utc(2025, 7, 2, 2), 20.0, 0.0, 1.0),
        ("802.desched1", "bob",   "NCAR0002", _utc(2025, 7, 2, 18), 40.0, 8.0, 0.5),
        # 07-03 idle on purpose.
        ("803.desched1", "alice", "NCAR0001", _utc(2025, 7, 4, 18),  5.0, 0.0, 0.0),
        ("804.desched1", "carol", "NCAR0002", _utc(2025, 7, 5, 18), None, None, None),
    ]
    jobs = []
    for i, (jid, user, acct, end, cpu, gpu, factor) in enumerate(specs):
        job = Job(job_id=jid, short_id=800 + i, name=f"t{i}", user=user,
                  account=acct, queue="main", status="0",
                  submit=end - timedelta(hours=1), start=end - timedelta(hours=1),
                  end=end, eligible_secs=60, numnodes=1, numcpus=8, numgpus=0,
                  elapsed=3600, reqmem=_GIB, memory=_GIB // 2)
        in_memory_session.add(job)
        jobs.append((job, cpu, gpu, factor))
    in_memory_session.flush()
    for job, cpu, gpu, factor in jobs:
        if factor is None:
            continue  # 804 stays charge-less
        in_memory_session.add(JobCharge(
            job_id=job.id, cpu_hours=cpu, gpu_hours=gpu, memory_hours=0.0,
            qos_factor=factor, charge_version=1))
    in_memory_session.commit()
    return [j for j, *_ in jobs]


_TS_START = date(2025, 7, 1)
_TS_END = date(2025, 7, 5)


class TestJobsTimeseries:
    """Contract for the per-period series behind the stacked timeline."""

    @staticmethod
    def _band(out, label):
        return next(b for b in out["bands"] if b["label"] == label)

    def test_band_vector_is_zero_filled_and_chronological(
            self, in_memory_session, timeseries_jobs):
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=_TS_START, end=_TS_END)
        assert [b["label"] for b in out["bands"]] == [
            "2025-07-01", "2025-07-02", "2025-07-03", "2025-07-04", "2025-07-05"]
        # The idle day is KEPT as an interior zero, never dropped.
        assert self._band(out, "2025-07-03")["job_count"] == 0
        assert out["period"] == "day"
        assert (out["start"], out["end"]) == ("2025-07-01", "2025-07-05")

    def test_site_local_day_binning_not_utc(
            self, in_memory_session, timeseries_jobs):
        """801 ends 02:00 UTC on 07-02 = 20:00 MDT on 07-01.

        The whole reason this method does not reuse ``PeriodGrouper``: that
        helper formats the raw naive-UTC column, which would file this job
        under 07-02 while ``jobs_search(start=end=2025-07-01)`` returns it.
        """
        q = JobQueries(in_memory_session)
        out = q.jobs_timeseries("day", start=_TS_START, end=_TS_END)
        assert self._band(out, "2025-07-01")["job_count"] == 2
        assert self._band(out, "2025-07-02")["job_count"] == 1
        # And the band agrees with the table it will sit next to.
        assert q.jobs_count(start=date(2025, 7, 1), end=date(2025, 7, 1)) == 2

    def test_total_count_matches_jobs_count(
            self, in_memory_session, timeseries_jobs):
        q = JobQueries(in_memory_session)
        out = q.jobs_timeseries("day", start=_TS_START, end=_TS_END)
        assert out["total_count"] == q.jobs_count(
            start=_TS_START, end=_TS_END) == 5
        assert sum(b["job_count"] for b in out["bands"]) + out["null_count"] \
            == out["total_count"]

    def test_every_band_replays_into_jobs_count(
            self, in_memory_session, timeseries_jobs):
        """The band -> jobs_search round-trip, the calendar analogue of
        ``_assert_bands_round_trip``. This is what a bar click promises."""
        q = JobQueries(in_memory_session)
        for period in ("day", "week", "month"):
            out = q.jobs_timeseries(period, start=_TS_START, end=_TS_END)
            for band in out["bands"]:
                replay = q.jobs_count(
                    start=date.fromisoformat(band["start"]),
                    end=date.fromisoformat(band["end"]))
                assert replay == band["job_count"], (period, band["label"])

    def test_charges_are_qos_weighted(
            self, in_memory_session, timeseries_jobs):
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=_TS_START, end=_TS_END)
        # 07-02: one job, 40 cpu-h at factor 0.5 -> 20 charges, 8 gpu-h -> 4.
        band = self._band(out, "2025-07-02")
        assert band["cpu_hours"] == pytest.approx(40.0)
        assert band["cpu_charges"] == pytest.approx(20.0)
        assert band["gpu_hours"] == pytest.approx(8.0)
        assert band["gpu_charges"] == pytest.approx(4.0)

    def test_uncharged_qos_yields_hours_with_zero_charges(
            self, in_memory_session, timeseries_jobs):
        """qos_factor 0.0 is a real value, not missing data: a charges view
        legitimately shows an empty bar where an hours view shows work."""
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=_TS_START, end=_TS_END)
        band = self._band(out, "2025-07-04")
        assert band["job_count"] == 1
        assert band["cpu_hours"] == pytest.approx(5.0)
        assert band["cpu_charges"] == 0.0

    def test_chargeless_job_counts_but_adds_no_hours(
            self, in_memory_session, timeseries_jobs):
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=_TS_START, end=_TS_END)
        band = self._band(out, "2025-07-05")
        assert band["job_count"] == 1
        assert band["cpu_hours"] == 0.0 and band["cpu_charges"] == 0.0

    def test_totals_are_granularity_invariant(
            self, in_memory_session, timeseries_jobs):
        """day/week/month partition the same window, so every metric total
        must agree — the strongest single check that the ladder tiles."""
        q = JobQueries(in_memory_session)
        outs = [q.jobs_timeseries(p, start=_TS_START, end=_TS_END)
                for p in ("day", "week", "month")]
        for key in ("job_count", "cpu_hours", "gpu_hours",
                    "cpu_charges", "gpu_charges"):
            values = [o["totals"][key] for o in outs]
            assert values[0] == pytest.approx(values[1]) == pytest.approx(
                values[2]), key

    def test_derived_window_when_dates_omitted(
            self, in_memory_session, timeseries_jobs):
        """No dates is an opt-in to full history; the domain comes from the
        observed MIN/MAX of Job.end, converted back to site-local days."""
        out = JobQueries(in_memory_session).jobs_timeseries("day")
        assert (out["start"], out["end"]) == ("2025-07-01", "2025-07-05")
        assert out["total_count"] == 5

    def test_empty_slice_yields_no_bands(
            self, in_memory_session, timeseries_jobs):
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", user="nobody")
        assert out["bands"] == [] and out["total_count"] == 0

    @pytest.mark.parametrize("period", ["day", "week", "month"])
    def test_inverted_window_is_empty_not_an_error(
            self, in_memory_session, timeseries_jobs, period):
        """start > end answers 0 rows, matching jobs_count/jobs_search rather
        than raising. Reachable from the explorer's free-text date fields, and
        an empty ladder would otherwise IndexError inside _period_case."""
        q = JobQueries(in_memory_session)
        out = q.jobs_timeseries(
            period, start=date(2025, 7, 5), end=date(2025, 7, 1))
        assert out["bands"] == []
        assert out["total_count"] == 0 == q.jobs_count(
            start=date(2025, 7, 5), end=date(2025, 7, 1))

    def test_null_end_jobs_land_in_null_count_not_a_band(
            self, in_memory_session, timeseries_jobs):
        """Job.end is nullable. Such rows match an unbounded query but belong
        to no calendar band, so they keep total_count == jobs_count honest.

        They ride out on the MIN/MAX probe rather than the series statement,
        which is what lets the series be re-bound to the resolved window
        without losing them — a bounded date filter drops NULL ``end``.
        """
        in_memory_session.add(Job(
            job_id="899.desched1", short_id=899, name="no-end", user="alice",
            account="NCAR0001", queue="main", status="0",
            submit=datetime(2025, 7, 1, 12), start=None, end=None,
            eligible_secs=60, numnodes=1, numcpus=8, numgpus=0))
        in_memory_session.commit()

        q = JobQueries(in_memory_session)
        out = q.jobs_timeseries("day")          # unbounded: no date filter
        assert out["null_count"] == 1
        assert sum(b["job_count"] for b in out["bands"]) == 5
        assert out["total_count"] == 6 == q.jobs_count()
        # A bounded window excludes it via the date filter instead.
        bounded = q.jobs_timeseries("day", start=_TS_START, end=_TS_END)
        assert bounded["null_count"] == 0 and bounded["total_count"] == 5

    def test_all_null_end_slice_still_counts(self, in_memory_session):
        """A slice whose rows ALL have NULL ``end`` has no domain to band
        over — but the rows match the filters, so dropping them would break
        ``total_count == jobs_count()``, the invariant this method advertises.
        """
        for i in range(3):
            in_memory_session.add(Job(
                job_id=f"9{i}0.desched1", short_id=900 + i, name="pending",
                user="alice", account="NCAR0001", queue="main", status=None,
                submit=datetime(2025, 7, 1, 12), start=None, end=None,
                eligible_secs=60, numnodes=1, numcpus=8, numgpus=0))
        in_memory_session.commit()

        q = JobQueries(in_memory_session)
        out = q.jobs_timeseries("day")
        assert out["bands"] == []
        assert out["null_count"] == 3
        assert out["total_count"] == 3 == q.jobs_count()

    def test_empty_partial_window_echoes_the_supplied_bound(
            self, in_memory_session, timeseries_jobs):
        """One bound supplied and nothing matched: echo it rather than
        discarding it. Only a MISSING bound is ever derived."""
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=date(2030, 1, 1), user="nobody")
        assert out["bands"] == []
        assert out["start"] == "2030-01-01"

    def test_series_statement_is_bounded_to_the_resolved_window(
            self, in_memory_session, timeseries_jobs):
        """The ladder's totality must rest on the WHERE clause, not on the
        probe's snapshot: otherwise a row synced between the two lands in the
        ``else_`` arm and silently breaks the last band's replay."""
        from sqlalchemy import event

        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_timeseries("day")  # unbounded
        finally:
            event.remove(
                in_memory_session.bind, "before_cursor_execute", _capture)

        series = [s for s in statements
                  if "GROUP BY" in s.upper() and " jobs" in s.lower()]
        assert series, statements
        for stmt in series:
            assert "jobs.\"end\" >=" in stmt or "jobs.end >=" in stmt, stmt
            assert "jobs.\"end\" <" in stmt or "jobs.end <" in stmt, stmt

    def test_filters_narrow_the_series(
            self, in_memory_session, timeseries_jobs):
        """The point of the method: it honours the whole jobs_search filter
        set, which no other per-period query here does."""
        q = JobQueries(in_memory_session)
        out = q.jobs_timeseries("day", start=_TS_START, end=_TS_END, user="alice")
        assert out["total_count"] == 3 == q.jobs_count(
            start=_TS_START, end=_TS_END, user="alice")
        assert self._band(out, "2025-07-02")["job_count"] == 0  # bob's day

    # -- owners ------------------------------------------------------------

    def test_owners_legend_is_identical_in_every_band(
            self, in_memory_session, timeseries_jobs):
        """A stacked chart maps key -> colour once; a series that changed
        position or vanished mid-axis would recolour the plot."""
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=_TS_START, end=_TS_END, owners_limit=2)
        keysets = {tuple(b["owners"]) for b in out["bands"]}
        assert len(keysets) == 1
        # Global rank by combined hours: bob 48 > alice 35 > carol 0.
        assert keysets.pop() == ("bob", "alice")

    def test_owners_are_zero_filled_in_idle_bands(
            self, in_memory_session, timeseries_jobs):
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=_TS_START, end=_TS_END, owners_limit=2)
        idle = self._band(out, "2025-07-03")
        assert idle["owners"]["bob"] == {
            "job_count": 0, "cpu_hours": 0.0, "gpu_hours": 0.0,
            "cpu_charges": 0.0, "gpu_charges": 0.0}

    def test_owners_remainder_is_derivable(
            self, in_memory_session, timeseries_jobs):
        """"Others" is band totals - Sigma owners, never synthesized. carol
        falls outside the top-2, so 07-05 is pure remainder."""
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=_TS_START, end=_TS_END, owners_limit=2)
        for band in out["bands"]:
            others = band["job_count"] - sum(
                o["job_count"] for o in band["owners"].values())
            assert others >= 0
        band = self._band(out, "2025-07-05")
        assert band["job_count"] == 1
        assert sum(o["job_count"] for o in band["owners"].values()) == 0

    def test_owners_by_account_switches_the_axis(
            self, in_memory_session, timeseries_jobs):
        out = JobQueries(in_memory_session).jobs_timeseries(
            "day", start=_TS_START, end=_TS_END, owners_limit=2,
            owners_by="account")
        assert tuple(out["bands"][0]["owners"]) == ("NCAR0002", "NCAR0001")
        assert out["owners_by"] == "account"

    def test_owners_sort_by_decides_which_top_n_survives(
            self, in_memory_session, timeseries_jobs):
        """bob leads on hours; alice leads on job_count (3 vs 1)."""
        q = JobQueries(in_memory_session)
        by_hours = q.jobs_timeseries(
            "day", start=_TS_START, end=_TS_END, owners_limit=1)
        by_jobs = q.jobs_timeseries(
            "day", start=_TS_START, end=_TS_END, owners_limit=1,
            owners_sort_by="job_count")
        assert tuple(by_hours["bands"][0]["owners"]) == ("bob",)
        assert tuple(by_jobs["bands"][0]["owners"]) == ("alice",)

    def test_owners_never_change_band_totals(
            self, in_memory_session, timeseries_jobs):
        q = JobQueries(in_memory_session)
        plain = q.jobs_timeseries("day", start=_TS_START, end=_TS_END)
        rich = q.jobs_timeseries(
            "day", start=_TS_START, end=_TS_END, owners_limit=2)
        for a, b in zip(plain["bands"], rich["bands"]):
            assert a["job_count"] == b["job_count"]
            assert a["cpu_hours"] == pytest.approx(b["cpu_hours"])
            assert list(b)[-1] == "owners", "owners must be appended last"

    def test_owners_sort_by_charges_ranks_by_charges(
            self, in_memory_session, timeseries_jobs):
        """alice: 35 hours -> 30 charges (one job uncharged).
        bob: 48 hours -> 24 charges (factor 0.5). Ranking flips."""
        q = JobQueries(in_memory_session)
        out = q.jobs_timeseries(
            "day", start=_TS_START, end=_TS_END, owners_limit=2,
            owners_sort_by="charges")
        assert tuple(out["bands"][0]["owners"]) == ("alice", "bob")

    # -- cost guard --------------------------------------------------------

    def test_two_aggregate_scans_with_owners_one_without(
            self, in_memory_session, timeseries_jobs):
        from sqlalchemy import event

        def _count_aggregates(**kwargs):
            statements = []

            @event.listens_for(in_memory_session.bind, "before_cursor_execute")
            def _capture(conn, cursor, statement, params, context, executemany):
                statements.append(statement)

            try:
                JobQueries(in_memory_session).jobs_timeseries(
                    "day", start=_TS_START, end=_TS_END, **kwargs)
            finally:
                event.remove(
                    in_memory_session.bind, "before_cursor_execute", _capture)
            return [s for s in statements
                    if "GROUP BY" in s.upper() and " jobs" in s.lower()]

        assert len(_count_aggregates()) == 1
        with_owners = _count_aggregates(owners_limit=2)
        assert len(with_owners) == 2, with_owners
        # Grouped on the integer FK, names resolved separately.
        assert all("jobs.user_id" in s for s in with_owners)
        assert not any("SELECT users.username" in s for s in with_owners)

    # -- validation --------------------------------------------------------

    def test_bad_period_raises(self, in_memory_session, timeseries_jobs):
        with pytest.raises(ValueError, match="Unknown period"):
            JobQueries(in_memory_session).jobs_timeseries("fortnight")

    def test_quarter_is_not_a_timeseries_period(
            self, in_memory_session, timeseries_jobs):
        """PeriodGrouper offers quarter/year; this method deliberately does
        not — a chart that wide wants month bands, and the vocabularies are
        independent on purpose."""
        with pytest.raises(ValueError, match="Unknown period"):
            JobQueries(in_memory_session).jobs_timeseries("quarter")

    def test_bad_owners_limit_raises(self, in_memory_session, timeseries_jobs):
        with pytest.raises(ValueError, match="owners_limit"):
            JobQueries(in_memory_session).jobs_timeseries(
                "day", start=_TS_START, end=_TS_END, owners_limit=0)

    def test_bad_owners_by_raises(self, in_memory_session, timeseries_jobs):
        with pytest.raises(ValueError, match="owners_by"):
            JobQueries(in_memory_session).jobs_timeseries(
                "day", start=_TS_START, end=_TS_END, owners_by="queue")

    def test_bad_owners_sort_by_raises(self, in_memory_session, timeseries_jobs):
        with pytest.raises(ValueError, match="owners_sort_by"):
            JobQueries(in_memory_session).jobs_timeseries(
                "day", start=_TS_START, end=_TS_END, owners_sort_by="bogus")

    def test_band_cap_raises_rather_than_emitting_a_huge_ladder(
            self, in_memory_session, timeseries_jobs):
        """The CASE ladder costs ~O(bands/2) comparisons per scanned row, so
        an unbounded day-granular window is refused, not silently served."""
        with pytest.raises(ValueError, match="cap"):
            JobQueries(in_memory_session).jobs_timeseries(
                "day", start=date(2020, 1, 1), end=date(2025, 12, 31))


class TestPeriodBands:
    """Structural guards on the band table — no database needed.

    The calendar analogue of ``test_bucket_tables_are_closed_and_contiguous``:
    the ladder must tile the window exactly, or bands double-count or drop
    jobs and the band -> jobs_search round-trip breaks.
    """

    @staticmethod
    def _bands(start, end, period):
        from job_history.queries.jobs import _period_bands
        return _period_bands(start, end, period)

    @pytest.mark.parametrize("period", ["day", "week", "month"])
    @pytest.mark.parametrize("span", [1, 2, 7, 31, 90, 365])
    def test_bands_tile_the_window_exactly(self, period, span):
        start = date(2026, 1, 15)
        end = start + timedelta(days=span - 1)
        bands = self._bands(start, end, period)
        assert bands, (period, span)
        # Clipped to the window at both edges.
        assert bands[0]["start"] == start
        assert bands[-1]["end"] == end
        # Contiguous with no gaps and no overlaps.
        for prev, nxt in zip(bands, bands[1:]):
            assert prev["end"] + timedelta(days=1) == nxt["start"]
        # Every day of the window belongs to exactly one band.
        covered = sum((b["end"] - b["start"]).days + 1 for b in bands)
        assert covered == span

    @pytest.mark.parametrize("period", ["day", "week", "month"])
    def test_hi_utc_is_strictly_increasing(self, period):
        bands = self._bands(date(2026, 1, 1), date(2026, 12, 31), period)
        his = [b["hi_utc"] for b in bands]
        assert his == sorted(his) and len(set(his)) == len(his)

    def test_labels_are_unique_and_sort_chronologically(self):
        for period in ("day", "week", "month"):
            bands = self._bands(date(2025, 11, 1), date(2026, 3, 1), period)
            labels = [b["label"] for b in bands]
            assert len(set(labels)) == len(labels)
            # Zero-padded, so lexicographic == chronological.
            assert labels == sorted(labels)

    def test_dst_days_are_23_and_25_hours_wide(self):
        """The reason boundaries are computed with zoneinfo rather than a
        fixed offset: a fixed shift silently misfiles an hour of jobs twice
        a year, on exactly the days an operator is most likely to look."""
        from job_history.queries.jobs import _site_midnight_utc
        widths = {}
        for day in (date(2026, 3, 7), date(2026, 3, 8),
                    date(2026, 10, 31), date(2026, 11, 1)):
            lo = _site_midnight_utc(day)
            hi = _site_midnight_utc(day + timedelta(days=1))
            widths[day] = (hi - lo).total_seconds() / 3600
        assert widths[date(2026, 3, 7)] == 24     # ordinary day
        assert widths[date(2026, 3, 8)] == 23     # spring forward
        assert widths[date(2026, 10, 31)] == 24   # ordinary day
        assert widths[date(2026, 11, 1)] == 25    # fall back

    def test_week_bands_snap_to_monday_but_clip_to_the_window(self):
        """Whole ISO weeks keep two overlapping windows comparable; the
        clip keeps each band replaying to exactly its own job_count."""
        bands = self._bands(date(2026, 3, 4), date(2026, 3, 20), "week")
        assert [b["label"] for b in bands] == [
            "2026-W10", "2026-W11", "2026-W12"]
        # Wednesday start is reported as-is, not backdated to the Monday.
        assert bands[0]["start"] == date(2026, 3, 4)
        assert bands[1]["start"] == date(2026, 3, 9)   # a real Monday
        assert bands[-1]["end"] == date(2026, 3, 20)


class TestChargesAcrossAggregations:
    """``charges = hours x qos_factor`` must mean the same thing in every
    aggregate, or a metric pill shows different numbers per tab.

    Uses ``timeseries_jobs`` because its factors are 1.0 / 0.5 / 0.0 —
    ``histogram_jobs`` is uniformly 1.0, where charges and hours coincide
    and a swapped formula would pass unnoticed.
    """

    #: alice 3 jobs: 10@1.0 + 20@1.0 + 5@0.0 = 35 cpu-h -> 30 charges.
    #: bob   1 job:  40@0.5                             -> 20 charges,
    #:                8 gpu-h @0.5                      ->  4 charges.
    #: carol 1 job:  charge-less                        ->  0 / 0.
    EXPECTED = {
        "alice": (35.0, 30.0, 0.0, 0.0),
        "bob":   (40.0, 20.0, 8.0, 4.0),
        "carol": (0.0, 0.0, 0.0, 0.0),
    }

    def test_usage_by_rows_carry_qos_weighted_charges(
            self, in_memory_session, timeseries_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by("user")
        rows = {r["value"]: r for r in out["rows"]}
        for user, (cpu_h, cpu_c, gpu_h, gpu_c) in self.EXPECTED.items():
            assert rows[user]["cpu_hours"] == pytest.approx(cpu_h), user
            assert rows[user]["cpu_charges"] == pytest.approx(cpu_c), user
            assert rows[user]["gpu_hours"] == pytest.approx(gpu_h), user
            assert rows[user]["gpu_charges"] == pytest.approx(gpu_c), user

    def test_usage_by_totals_include_charges(
            self, in_memory_session, timeseries_jobs):
        out = JobQueries(in_memory_session).jobs_usage_by("user")
        assert out["totals"]["cpu_hours"] == pytest.approx(75.0)
        assert out["totals"]["cpu_charges"] == pytest.approx(50.0)
        assert out["totals"]["gpu_charges"] == pytest.approx(4.0)

    def test_histogram_bands_and_owners_carry_charges(
            self, in_memory_session, timeseries_jobs):
        """All five jobs share numcpus=8, so one band holds the lot — the
        band and its owners must both report the weighted sums."""
        out = JobQueries(in_memory_session).jobs_histogram(
            "cpus", owners_limit=3)
        band = next(b for b in out["buckets"] if b["job_count"] == 5)
        assert band["cpu_hours"] == pytest.approx(75.0)
        assert band["cpu_charges"] == pytest.approx(50.0)
        assert band["owners"]["alice"]["cpu_charges"] == pytest.approx(30.0)
        assert band["owners"]["bob"]["cpu_charges"] == pytest.approx(20.0)

    def test_every_aggregate_agrees_on_total_charges(
            self, in_memory_session, timeseries_jobs):
        """The cross-method identity a metric pill depends on."""
        q = JobQueries(in_memory_session)
        usage = q.jobs_usage_by("user")["totals"]
        hist = q.jobs_histogram("cpus")
        series = q.jobs_timeseries("day")["totals"]
        hist_totals = {
            key: sum(b[key] for b in hist["buckets"])
            for key in ("cpu_charges", "gpu_charges")
        }
        for key in ("cpu_charges", "gpu_charges"):
            assert usage[key] == pytest.approx(hist_totals[key]), key
            assert usage[key] == pytest.approx(series[key]), key

    def test_sort_by_charges_is_accepted_everywhere(
            self, in_memory_session, timeseries_jobs):
        """``charges`` joins the shared _USAGE_SORT_KEYS vocabulary, so the
        ranking metric can follow a charges metric pill on every surface."""
        assert "charges" in _USAGE_SORT_KEYS
        q = JobQueries(in_memory_session)
        # bob leads on hours (48 > 35); alice leads on charges (30 > 24).
        assert q.jobs_usage_by(
            "user", sort_by="hours")["rows"][0]["value"] == "bob"
        assert q.jobs_usage_by(
            "user", sort_by="charges")["rows"][0]["value"] == "alice"


# ---------------------------------------------------------------------------
# jobs_timeseries — the daily_summary fast path
# ---------------------------------------------------------------------------

@pytest.fixture
def summarized_timeseries(in_memory_session, timeseries_jobs):
    """``timeseries_jobs`` plus the real ``daily_summary`` rollup over it.

    Deliberately runs :func:`generate_daily_summary` rather than hand-building
    rows: the fast path's whole claim is that it agrees with the scan path,
    and hand-built summaries would let a rollup bug hide behind a matching
    fixture. Returns the watermark (last summarized day).
    """
    day = _TS_START
    while day <= _TS_END:
        generate_daily_summary(in_memory_session, "casper", day, replace=True)
        day += timedelta(days=1)
    in_memory_session.commit()
    return _TS_END


def _force_scan(monkeypatch):
    """Disable the fast path for the duration of a test."""
    monkeypatch.setattr(
        JobQueries, "_timeseries_uses_summary", lambda *a, **k: False)


class TestTimeseriesSummaryRouting:
    """Which path answers, and why."""

    def test_no_summary_rows_means_no_fast_path(
            self, in_memory_session, timeseries_jobs):
        """An unsummarized database must not silently return empty bands."""
        q = JobQueries(in_memory_session)
        assert q._timeseries_uses_summary(
            {"start": _TS_START, "end": _TS_END}, _TS_START, _TS_END,
            "user") is False

    def test_serviceable_filters_route_to_the_summary(
            self, in_memory_session, summarized_timeseries):
        q = JobQueries(in_memory_session)
        for extra in ({}, {"user": "alice"}, {"account": "NCAR0001"},
                      {"queue": "main"}, {"ignore_case": True}):
            filters = {"start": _TS_START, "end": _TS_END, **extra}
            assert q._timeseries_uses_summary(
                filters, _TS_START, _TS_END, "user") is True, extra

    @pytest.mark.parametrize("unserviceable", [
        {"qos": "regular"},          # not in daily_summary's key
        {"exit_status": "0"},        # per-job attribute
        {"job_id": "800"},
        {"name": "t0"},
        {"min_nodes": 0},            # 0 is a real bound, not "unset"
        {"max_cpus": 8},
        {"min_memory_wasted": 1},
    ])
    def test_unserviceable_filters_fall_back(
            self, in_memory_session, summarized_timeseries, unserviceable):
        """Every filter the rollup aggregated away must force the jobs scan —
        serving it off the summary would silently ignore it."""
        q = JobQueries(in_memory_session)
        filters = {"start": _TS_START, "end": _TS_END, **unserviceable}
        assert q._timeseries_uses_summary(
            filters, _TS_START, _TS_END, "user") is False

    def test_every_filter_is_classified(self):
        """No filter may be accidentally serviceable: the whitelist plus the
        aggregated-away set must together cover the shared filter signature,
        so a NEW filter defaults to the safe path rather than being ignored."""
        import inspect
        params = {
            name for name, p in
            inspect.signature(JobQueries.jobs_timeseries).parameters.items()
            if p.kind == p.KEYWORD_ONLY
        } - {"owners_limit", "owners_sort_by", "owners_by"}
        assert _SUMMARY_SERVICEABLE_FILTERS <= params
        # Anything outside the whitelist must be rejected by the predicate.
        q = JobQueries.__new__(JobQueries)
        for extra in params - _SUMMARY_SERVICEABLE_FILTERS:
            filters = {"start": _TS_START, "end": _TS_END, extra: 1}
            assert q._timeseries_uses_summary(
                filters, _TS_START, _TS_END, "user") is False, extra

    def test_window_past_the_watermark_falls_back(
            self, in_memory_session, summarized_timeseries):
        """The summary lags jobs. A window touching an unsummarized day would
        come back short, so the whole window falls back rather than hybridising.
        """
        q = JobQueries(in_memory_session)
        assert q._timeseries_uses_summary(
            {"start": _TS_START, "end": _TS_END + timedelta(days=1)},
            _TS_START, _TS_END + timedelta(days=1), "user") is False
        # Exactly at the watermark is still fine.
        assert q._timeseries_uses_summary(
            {"start": _TS_START, "end": _TS_END},
            _TS_START, _TS_END, "user") is True

    def test_window_reaching_back_before_coverage_falls_back(
            self, in_memory_session, summarized_timeseries):
        """A trailing watermark is not enough. The rollup need not reach back
        to the start of history — a partial ``--resummarize`` leaves an
        earlier gap — and those days would come back as zero bands while the
        scan path finds jobs."""
        q = JobQueries(in_memory_session)
        earlier = _TS_START - timedelta(days=30)
        assert q._timeseries_uses_summary(
            {"start": earlier, "end": _TS_END}, earlier, _TS_END,
            "user") is False

    def test_a_single_missing_interior_day_falls_back(
            self, in_memory_session, summarized_timeseries):
        """The failure mode a watermark check misses entirely: one skipped
        day mid-window silently becomes a zero band."""
        q = JobQueries(in_memory_session)
        assert q._timeseries_uses_summary(
            {"start": _TS_START, "end": _TS_END}, _TS_START, _TS_END,
            "user") is True
        in_memory_session.query(DailySummary).filter(
            DailySummary.date == date(2025, 7, 3)).delete()
        in_memory_session.commit()
        assert q._timeseries_uses_summary(
            {"start": _TS_START, "end": _TS_END}, _TS_START, _TS_END,
            "user") is False

    def test_no_jobs_marker_days_still_count_as_covered(
            self, in_memory_session, timeseries_jobs):
        """An idle day has only a NO_JOBS marker row. It IS summarized, so it
        must not force a fallback — otherwise a quiet weekend would drop the
        whole window onto the slow path."""
        # 07-03 is idle in the fixture, so the rollup writes it as a marker.
        day = _TS_START
        while day <= _TS_END:
            generate_daily_summary(in_memory_session, "casper", day,
                                   replace=True)
            day += timedelta(days=1)
        in_memory_session.commit()
        marker = in_memory_session.query(DailySummary).filter(
            DailySummary.date == date(2025, 7, 3)).one()
        assert marker.user_id is None and marker.job_count == 0
        assert JobQueries(in_memory_session)._timeseries_uses_summary(
            {"start": _TS_START, "end": _TS_END}, _TS_START, _TS_END,
            "user") is True

    def test_routing_decision_is_logged(
            self, in_memory_session, summarized_timeseries, caplog):
        """The two paths are deliberately indistinguishable from the envelope,
        so the log is the ONLY way to answer "is the fast path firing?" in
        production — a 7.4s timeline must not look like a 15ms one."""
        q = JobQueries(in_memory_session)
        with caplog.at_level(logging.DEBUG, logger="job_history.queries.jobs"):
            q.jobs_timeseries("day", start=_TS_START, end=_TS_END)
        assert "daily_summary path (5/5 days covered)" in caplog.text

        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="job_history.queries.jobs"):
            q.jobs_timeseries("day", start=_TS_START, end=_TS_END, qos="regular")
        assert "jobs-scan path" in caplog.text and "qos" in caplog.text

        # A coverage shortfall reports the numbers, so a genuine sync gap is
        # distinguishable from the ordinary "today isn't summarized yet" case.
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="job_history.queries.jobs"):
            q.jobs_timeseries("day", start=_TS_START,
                              end=_TS_END + timedelta(days=2))
        assert "jobs-scan path (coverage 5/7 days)" in caplog.text

    def test_fast_path_never_touches_the_jobs_table(
            self, in_memory_session, summarized_timeseries):
        """The point of the fast path: no scan of ``jobs`` at all."""
        from sqlalchemy import event

        statements = []

        @event.listens_for(in_memory_session.bind, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)

        try:
            JobQueries(in_memory_session).jobs_timeseries(
                "day", start=_TS_START, end=_TS_END, owners_limit=2)
        finally:
            event.remove(
                in_memory_session.bind, "before_cursor_execute", _capture)

        aggregates = [s for s in statements if "GROUP BY" in s.upper()]
        assert aggregates
        assert all("daily_summary" in s for s in aggregates), aggregates
        assert not any("FROM jobs" in s for s in aggregates), aggregates


class TestTimeseriesPathEquivalence:
    """The decisive contract: a consumer cannot tell which path ran.

    Counts, labels, window echo and owner keys must match **exactly**; the
    float metrics only approximately, because the rollup pre-sums each
    ``(date, user, account, queue)`` group and this re-folds those subtotals
    while the scan sums every job in one pass — and float addition is not
    associative.
    """

    _FLOAT_KEYS = ("cpu_hours", "gpu_hours", "cpu_charges", "gpu_charges")

    @classmethod
    def _assert_same(cls, fast, scan, ctx=""):
        assert fast.keys() == scan.keys(), ctx
        for key in ("period", "owners_by", "start", "end",
                    "null_count", "total_count"):
            assert fast[key] == scan[key], f"{ctx}: {key}"
        assert [b["label"] for b in fast["bands"]] == \
               [b["label"] for b in scan["bands"]], ctx
        for fb, sb in zip(fast["bands"], scan["bands"]):
            where = f"{ctx}: band {fb['label']}"
            assert fb.keys() == sb.keys(), where
            assert fb["job_count"] == sb["job_count"], where
            assert (fb["start"], fb["end"]) == (sb["start"], sb["end"]), where
            for key in cls._FLOAT_KEYS:
                assert fb[key] == pytest.approx(sb[key]), f"{where}: {key}"
            if "owners" in fb:
                # Same legend, same rank ORDER — a stacked chart keys colour
                # off position, so a reordered legend is a visible defect.
                assert list(fb["owners"]) == list(sb["owners"]), where
                for name, fm in fb["owners"].items():
                    assert fm["job_count"] == sb["owners"][name]["job_count"], \
                        f"{where}: {name}"
                    for key in cls._FLOAT_KEYS:
                        assert fm[key] == pytest.approx(
                            sb["owners"][name][key]), f"{where}: {name}.{key}"
        for key in ("job_count",):
            assert fast["totals"][key] == scan["totals"][key], ctx
        for key in cls._FLOAT_KEYS:
            assert fast["totals"][key] == pytest.approx(
                scan["totals"][key]), f"{ctx}: totals.{key}"

    CASES = [
        ("plain", {}),
        ("owners", {"owners_limit": 2}),
        ("owners by account", {"owners_limit": 2, "owners_by": "account"}),
        ("owners by charges", {"owners_limit": 2, "owners_sort_by": "charges"}),
        ("owners by job_count", {"owners_limit": 1,
                                 "owners_sort_by": "job_count"}),
        ("user filter", {"user": "alice"}),
        ("account filter", {"account": "NCAR0002"}),
        ("queue filter", {"queue": "main"}),
        ("account seq", {"account": ["NCAR0001", "NCAR0002"]}),
        ("scoped + owners", {"account": "NCAR0001", "owners_limit": 2}),
        ("unresolvable user", {"user": "nobody"}),
    ]

    @pytest.mark.parametrize("period", ["day", "week", "month"])
    @pytest.mark.parametrize("label,kwargs",
                             CASES, ids=[c[0] for c in CASES])
    def test_paths_agree(self, in_memory_session, summarized_timeseries,
                         monkeypatch, period, label, kwargs):
        q = JobQueries(in_memory_session)
        fast = q.jobs_timeseries(
            period, start=_TS_START, end=_TS_END, **kwargs)
        _force_scan(monkeypatch)
        scan = q.jobs_timeseries(
            period, start=_TS_START, end=_TS_END, **kwargs)
        self._assert_same(fast, scan, f"{period}/{label}")

    def test_paths_agree_on_a_derived_window(
            self, in_memory_session, summarized_timeseries, monkeypatch):
        """The derived-domain path also has to route and agree."""
        q = JobQueries(in_memory_session)
        fast = q.jobs_timeseries("day", end=_TS_END)
        _force_scan(monkeypatch)
        scan = q.jobs_timeseries("day", end=_TS_END)
        self._assert_same(fast, scan, "derived window")

    def test_fast_path_totals_match_jobs_count(
            self, in_memory_session, summarized_timeseries):
        """The invariant survives the routing — a fast path that quietly
        under-counted would still look self-consistent."""
        q = JobQueries(in_memory_session)
        out = q.jobs_timeseries("day", start=_TS_START, end=_TS_END)
        assert out["total_count"] == q.jobs_count(
            start=_TS_START, end=_TS_END) == 5

    def test_fast_path_bands_replay_into_jobs_count(
            self, in_memory_session, summarized_timeseries):
        """A bar click still lands on exactly that bar's jobs."""
        q = JobQueries(in_memory_session)
        for period in ("day", "week", "month"):
            out = q.jobs_timeseries(period, start=_TS_START, end=_TS_END)
            for band in out["bands"]:
                assert q.jobs_count(
                    start=date.fromisoformat(band["start"]),
                    end=date.fromisoformat(band["end"])
                ) == band["job_count"], (period, band["label"])


class TestTimeseriesBandCaps:
    """The two caps differ because the two paths cost ~700x differently."""

    def test_summary_cap_is_looser_than_the_scan_cap(self):
        assert _MAX_SUMMARY_BANDS > _MAX_TIMESERIES_BANDS

    def test_scan_path_refuses_a_ladder_past_its_cap(
            self, in_memory_session, timeseries_jobs):
        with pytest.raises(ValueError, match="jobs scan"):
            JobQueries(in_memory_session).jobs_timeseries(
                "day", start=date(2020, 1, 1), end=date(2025, 12, 31))

    @staticmethod
    def _mark_covered(session, start, end):
        """Seed NO_JOBS markers, exactly as the rollup does for an empty day.

        Cheaper than running the real rollup over 1200 days, and it is the
        coverage predicate's actual input: one row per processed date.
        """
        day = start
        while day <= end:
            session.add(DailySummary(
                date=day, user_id=None, account_id=None, queue_id=None,
                job_count=0, cpu_hours=0.0, gpu_hours=0.0, memory_hours=0.0,
                cpu_charges=0.0, gpu_charges=0.0, memory_charges=0.0))
            day += timedelta(days=1)
        session.commit()

    def test_summary_path_allows_a_window_the_scan_path_refuses(
            self, in_memory_session, summarized_timeseries):
        """Two years of daily bands is a 15s query against ``jobs`` and a
        21ms one against the rollup, so only one of them gets refused."""
        q = JobQueries(in_memory_session)
        wide_start = _TS_END - timedelta(days=_MAX_TIMESERIES_BANDS + 10)
        self._mark_covered(in_memory_session, wide_start,
                           _TS_START - timedelta(days=1))
        out = q.jobs_timeseries("day", start=wide_start, end=_TS_END)
        assert len(out["bands"]) == _MAX_TIMESERIES_BANDS + 11
        assert out["total_count"] == 5      # zero-filled outside the data

    def test_summary_path_still_has_a_payload_backstop(
            self, in_memory_session, summarized_timeseries):
        q = JobQueries(in_memory_session)
        too_wide = _TS_END - timedelta(days=_MAX_SUMMARY_BANDS + 5)
        self._mark_covered(in_memory_session, too_wide,
                           _TS_START - timedelta(days=1))
        with pytest.raises(ValueError, match="daily_summary"):
            q.jobs_timeseries("day", start=too_wide, end=_TS_END)


class TestTimeseriesSummaryCoverageLimit:
    """The one place the two paths can legitimately disagree — pinned, not
    hidden, so it fails loudly if the precondition ever stops holding.

    :func:`generate_daily_summary` cannot store a job whose
    ``user_id`` / ``account_id`` / ``queue_id`` is NULL, because a NULL FK
    triple is already the NO_JOBS marker. The ``jobs`` scan path counts such
    a job; the rollup has nowhere to put it. Measured 0 of 21.0M rows on
    casper_jobs, so this is theory — but it is theory with a silent
    under-count as its failure mode, which is why it gets a test.
    """

    def test_unattributable_job_is_absent_from_the_rollup(
            self, in_memory_session, timeseries_jobs):
        in_memory_session.add(Job(
            job_id="880.desched1", short_id=880, name="orphan",
            user=None, account=None, queue=None, status="0",
            submit=datetime(2025, 7, 2, 12), start=datetime(2025, 7, 2, 12),
            end=datetime(2025, 7, 2, 18), eligible_secs=60,
            numnodes=1, numcpus=8, numgpus=0))
        in_memory_session.commit()
        day = _TS_START
        while day <= _TS_END:
            generate_daily_summary(in_memory_session, "casper", day,
                                   replace=True)
            day += timedelta(days=1)
        in_memory_session.commit()

        q = JobQueries(in_memory_session)
        # The scan path counts it...
        assert q.jobs_count(start=_TS_START, end=_TS_END) == 6
        # ...and the rollup does not, by construction.
        rolled = in_memory_session.query(
            func.sum(DailySummary.job_count)).filter(
            DailySummary.date >= _TS_START,
            DailySummary.date <= _TS_END).scalar()
        assert rolled == 5, (
            "daily_summary gained a NULL-FK row — if sync.summary changed, "
            "the fast path's coverage caveat needs revisiting")

    def test_chargeless_job_is_now_in_the_rollup(
            self, in_memory_session, summarized_timeseries):
        """Regression guard on the inner->LEFT join fix in sync.summary: job
        804 has no JobCharge row and must still be counted, with zero hours,
        exactly as every outer-joining query reports it."""
        row = in_memory_session.query(
            func.sum(DailySummary.job_count),
            func.sum(DailySummary.cpu_hours),
        ).filter(DailySummary.date == date(2025, 7, 5)).one()
        assert row[0] == 1
        assert (row[1] or 0.0) == pytest.approx(0.0)
