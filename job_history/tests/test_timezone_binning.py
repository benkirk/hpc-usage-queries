"""Tests for timezone-aware day boundary binning.

The 'day' used for daily_summary and --recalculate is defined by the site
timezone (JH_SITE_TIMEZONE, default America/Denver), not by UTC.  A job
whose UTC end-time falls before midnight UTC but after midnight in the site
timezone must be assigned to the *previous* local day, not the UTC day.

All tests monkeypatch JobHistoryConfig.SITE_TIMEZONE so no env-var setup
is required.  The in-memory SQLite fixture is used throughout.

                           ┌─────────────────────────────────────────────┐
                           │ JOB ENDS at 2025-06-01 02:00 UTC (naive)    │
 Timezone        UTC-offset  Local time          Local day               │
 ──────────────  ─────────  ──────────────────  ──────────────────────── │
 UTC             +0         2025-06-01 02:00    2025-06-01  ← in June 1 │
 America/Denver  −6 (MDT)   2025-05-31 20:00    2025-05-31  ← in May 31 │
 America/NewYork −4 (EDT)   2025-05-31 22:00    2025-05-31  ← in May 31 │
                           └─────────────────────────────────────────────┘

Winter (MST = UTC-7) case:
 job ends 2025-01-15 02:00 UTC  →  2025-01-14 19:00 MST  →  Jan 14 for Denver
"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from job_history.database import DailySummary, Job, JobCharge
from job_history.database.config import JobHistoryConfig
from job_history.database.models import LookupCache
from job_history.sync.base import SyncBase
from job_history.sync.summary import generate_daily_summary


# ---------------------------------------------------------------------------
# Shared boundary timestamps (all naive UTC)
# ---------------------------------------------------------------------------

# Summer boundary: 02:00 UTC = 20:00 MDT (May 31) = 22:00 EDT (May 31)
SUMMER_END_UTC = datetime(2025, 6, 1, 2, 0, 0)      # in May 31 for Denver/NY; June 1 for UTC
SUMMER_MID_UTC = datetime(2025, 6, 1, 12, 0, 0)     # in June 1 for all timezones

# Winter boundary: 02:00 UTC = 19:00 MST (Jan 14) — UTC-7 in winter
WINTER_END_UTC = datetime(2025, 1, 15, 2, 0, 0)     # in Jan 14 for Denver; Jan 15 for UTC
WINTER_MID_UTC = datetime(2025, 1, 15, 12, 0, 0)    # in Jan 15 for all timezones

JUNE_1  = date(2025, 6, 1)
MAY_31  = date(2025, 5, 31)
JAN_15  = date(2025, 1, 15)
JAN_14  = date(2025, 1, 14)


# ---------------------------------------------------------------------------
# Helper: insert a job with charges directly into the DB
# ---------------------------------------------------------------------------

def _insert_job(session, end_utc: datetime, job_id: str = "tz.test.1") -> Job:
    """Insert a minimal charged job with a known naive-UTC end time."""
    cache = LookupCache(session)
    user = cache.get_or_create_user("tzuser")
    account = cache.get_or_create_account("TZACCT0001")
    queue = cache.get_or_create_queue("regular")
    session.flush()

    job = Job(
        job_id=job_id,
        submit=end_utc,
        eligible=end_utc,
        start=end_utc,
        end=end_utc,
        elapsed=3600,
        numcpus=4,
        numgpus=0,
        numnodes=1,
        memory=0,
        status="0",
        user_id=user.id,
        account_id=account.id,
        queue_id=queue.id,
    )
    session.add(job)
    session.flush()

    charge = JobCharge(
        job_id=job.id,
        cpu_hours=4.0,
        gpu_hours=0.0,
        memory_hours=0.0,
        qos_factor=1.0,
        charge_version=1,
    )
    session.add(charge)
    session.commit()
    return job


def _summary_jobs_for_date(session, target_date: date) -> list:
    """Return real (non-marker) DailySummary rows for a date."""
    return (
        session.query(DailySummary)
        .filter(
            DailySummary.date == target_date,
            DailySummary.user_id.isnot(None),
        )
        .all()
    )


# ---------------------------------------------------------------------------
# Stub syncer (needed by _recalculate_charges)
# ---------------------------------------------------------------------------

class _StubSyncer(SyncBase):
    SCHEDULER_NAME = "stub"

    def fetch_records(self, log_dir, period):
        return iter([])


# ---------------------------------------------------------------------------
# Tests: generate_daily_summary timezone binning
# ---------------------------------------------------------------------------

class TestGenerateDailySummaryTimezone:
    """Daily summaries must use the site timezone for day boundaries."""

    # ── Summer (MDT = UTC-6) ─────────────────────────────────────────────

    def test_summer_boundary_job_in_may31_for_denver(self, in_memory_session):
        """02:00 UTC job is 20:00 MDT on May 31 → belongs to May 31 for Denver."""
        _insert_job(in_memory_session, SUMMER_END_UTC)

        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "America/Denver"):
            generate_daily_summary(in_memory_session, "casper", MAY_31)
            generate_daily_summary(in_memory_session, "casper", JUNE_1)

        assert len(_summary_jobs_for_date(in_memory_session, MAY_31)) == 1, \
            "job should appear in May 31 summary"
        assert len(_summary_jobs_for_date(in_memory_session, JUNE_1)) == 0, \
            "job should NOT appear in June 1 summary"

    def test_summer_boundary_job_in_june1_for_utc(self, in_memory_session):
        """02:00 UTC is 02:00 UTC → belongs to June 1 for UTC timezone."""
        _insert_job(in_memory_session, SUMMER_END_UTC)

        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "UTC"):
            generate_daily_summary(in_memory_session, "casper", MAY_31)
            generate_daily_summary(in_memory_session, "casper", JUNE_1)

        assert len(_summary_jobs_for_date(in_memory_session, MAY_31)) == 0, \
            "job should NOT appear in May 31 summary"
        assert len(_summary_jobs_for_date(in_memory_session, JUNE_1)) == 1, \
            "job should appear in June 1 summary"

    def test_summer_boundary_job_in_may31_for_new_york(self, in_memory_session):
        """02:00 UTC is 22:00 EDT on May 31 → belongs to May 31 for America/New_York."""
        _insert_job(in_memory_session, SUMMER_END_UTC)

        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "America/New_York"):
            generate_daily_summary(in_memory_session, "casper", MAY_31)
            generate_daily_summary(in_memory_session, "casper", JUNE_1)

        assert len(_summary_jobs_for_date(in_memory_session, MAY_31)) == 1, \
            "job should appear in May 31 summary (22:00 EDT is still May 31)"
        assert len(_summary_jobs_for_date(in_memory_session, JUNE_1)) == 0

    def test_midday_job_in_june1_for_all_timezones(self, in_memory_session):
        """12:00 UTC is clearly inside June 1 for all timezones (Denver, NY, UTC)."""
        _insert_job(in_memory_session, SUMMER_MID_UTC)

        for tz in ("America/Denver", "America/New_York", "UTC"):
            in_memory_session.query(DailySummary).delete()
            in_memory_session.commit()

            with patch.object(JobHistoryConfig, "SITE_TIMEZONE", tz):
                generate_daily_summary(in_memory_session, "casper", JUNE_1)

            rows = _summary_jobs_for_date(in_memory_session, JUNE_1)
            assert len(rows) == 1, \
                f"12:00 UTC job should be in June 1 for timezone {tz!r}"

    # ── Winter (MST = UTC-7): Denver shifts one hour further ───────────────

    def test_winter_boundary_job_in_jan14_for_denver(self, in_memory_session):
        """02:00 UTC in winter (MST=UTC-7) = 19:00 MST Jan 14 → belongs to Jan 14 for Denver."""
        _insert_job(in_memory_session, WINTER_END_UTC)

        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "America/Denver"):
            generate_daily_summary(in_memory_session, "casper", JAN_14)
            generate_daily_summary(in_memory_session, "casper", JAN_15)

        assert len(_summary_jobs_for_date(in_memory_session, JAN_14)) == 1, \
            "winter job should appear in Jan 14 summary (MST is UTC-7)"
        assert len(_summary_jobs_for_date(in_memory_session, JAN_15)) == 0

    def test_winter_boundary_job_in_jan15_for_utc(self, in_memory_session):
        """02:00 UTC in winter → belongs to Jan 15 for UTC timezone."""
        _insert_job(in_memory_session, WINTER_END_UTC)

        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "UTC"):
            generate_daily_summary(in_memory_session, "casper", JAN_14)
            generate_daily_summary(in_memory_session, "casper", JAN_15)

        assert len(_summary_jobs_for_date(in_memory_session, JAN_14)) == 0
        assert len(_summary_jobs_for_date(in_memory_session, JAN_15)) == 1

    # ── Two jobs; different bins under Denver, same bin under UTC ──────────

    def test_two_jobs_split_across_days_by_denver_tz(self, in_memory_session):
        """Boundary job (02:00 UTC) and mid-day job (12:00 UTC) fall in different
        local days under Denver timezone (May 31 vs June 1), but same UTC day."""
        _insert_job(in_memory_session, SUMMER_END_UTC, "tz.boundary.1")  # May 31 in Denver
        _insert_job(in_memory_session, SUMMER_MID_UTC, "tz.midday.1")    # June 1 in Denver

        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "America/Denver"):
            generate_daily_summary(in_memory_session, "casper", MAY_31)
            generate_daily_summary(in_memory_session, "casper", JUNE_1)

        may31 = _summary_jobs_for_date(in_memory_session, MAY_31)
        june1 = _summary_jobs_for_date(in_memory_session, JUNE_1)
        assert len(may31) == 1, "boundary job should be in May 31"
        assert len(june1) == 1, "mid-day job should be in June 1"

    def test_two_jobs_same_day_under_utc(self, in_memory_session):
        """Both 02:00 UTC and 12:00 UTC fall in June 1 under UTC timezone."""
        _insert_job(in_memory_session, SUMMER_END_UTC, "tz.boundary.1")
        _insert_job(in_memory_session, SUMMER_MID_UTC, "tz.midday.1")

        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "UTC"):
            generate_daily_summary(in_memory_session, "casper", JUNE_1)

        june1 = _summary_jobs_for_date(in_memory_session, JUNE_1)
        # Both jobs in one row (same user/account/queue aggregated together)
        assert len(june1) >= 1, "both jobs should appear in June 1 under UTC"
        total_cpu = sum(r.cpu_hours for r in june1)
        assert total_cpu == pytest.approx(8.0), \
            "total CPU hours should reflect both jobs (4+4)"


# ---------------------------------------------------------------------------
# Tests: _recalculate_charges timezone binning
# ---------------------------------------------------------------------------

class TestRecalculateChargesTimezone:
    """--recalculate must use JH_SITE_TIMEZONE when selecting jobs by date."""

    def _strip_charges(self, session):
        session.query(JobCharge).delete()
        session.commit()
        assert session.query(JobCharge).count() == 0

    def test_denver_june1_excludes_boundary_job(self, in_memory_session):
        """--recalculate June 1 under Denver tz must NOT charge the 02:00 UTC
        job (it belongs to May 31 local time)."""
        _insert_job(in_memory_session, SUMMER_END_UTC, "tz.boundary.1")  # ends 02:00 UTC
        _insert_job(in_memory_session, SUMMER_MID_UTC, "tz.midday.1")    # ends 12:00 UTC
        self._strip_charges(in_memory_session)

        syncer = _StubSyncer(in_memory_session, "casper")
        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "America/Denver"):
            stats = syncer.sync(
                log_dir=None, period="2025-06-01",
                recalculate=True, generate_summary=False,
            )

        assert stats["recalculated"] == 1, \
            "only mid-day job (12:00 UTC) should be in June 1 local date"

        charged_jobs = in_memory_session.query(Job).join(
            JobCharge, Job.id == JobCharge.job_id
        ).filter(JobCharge.charge_version == 1).all()
        assert len(charged_jobs) == 1
        assert charged_jobs[0].job_id == "tz.midday.1"

    def test_denver_may31_includes_boundary_job(self, in_memory_session):
        """--recalculate May 31 under Denver tz MUST charge the 02:00 UTC
        job (it is local time May 31 20:00 MDT)."""
        _insert_job(in_memory_session, SUMMER_END_UTC, "tz.boundary.1")
        _insert_job(in_memory_session, SUMMER_MID_UTC, "tz.midday.1")
        self._strip_charges(in_memory_session)

        syncer = _StubSyncer(in_memory_session, "casper")
        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "America/Denver"):
            stats = syncer.sync(
                log_dir=None, period="2025-05-31",
                recalculate=True, generate_summary=False,
            )

        assert stats["recalculated"] == 1, \
            "only boundary job (02:00 UTC = May 31 MDT) should be in May 31 local date"

        charged_jobs = in_memory_session.query(Job).join(
            JobCharge, Job.id == JobCharge.job_id
        ).filter(JobCharge.charge_version == 1).all()
        assert len(charged_jobs) == 1
        assert charged_jobs[0].job_id == "tz.boundary.1"

    def test_utc_june1_includes_boundary_job(self, in_memory_session):
        """--recalculate June 1 under UTC tz MUST charge both jobs (both are
        June 1 in UTC)."""
        _insert_job(in_memory_session, SUMMER_END_UTC, "tz.boundary.1")
        _insert_job(in_memory_session, SUMMER_MID_UTC, "tz.midday.1")
        self._strip_charges(in_memory_session)

        syncer = _StubSyncer(in_memory_session, "casper")
        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "UTC"):
            stats = syncer.sync(
                log_dir=None, period="2025-06-01",
                recalculate=True, generate_summary=False,
            )

        assert stats["recalculated"] == 2, \
            "both jobs (02:00 and 12:00 UTC) should be in June 1 under UTC"

    def test_winter_denver_recalculate_uses_mst_offset(self, in_memory_session):
        """In winter, Denver is MST (UTC-7).  A job at 02:00 UTC belongs to
        the previous local day (19:00 MST), so --recalculate Jan 15 Denver
        must NOT pick it up."""
        _insert_job(in_memory_session, WINTER_END_UTC, "tz.winter.1")
        _insert_job(in_memory_session, WINTER_MID_UTC, "tz.winter.mid")
        self._strip_charges(in_memory_session)

        syncer = _StubSyncer(in_memory_session, "casper")
        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "America/Denver"):
            stats = syncer.sync(
                log_dir=None, period="2025-01-15",
                recalculate=True, generate_summary=False,
            )

        # Only the 12:00 UTC job should be picked up for Jan 15 (MST start = 07:00 UTC)
        assert stats["recalculated"] == 1

        charged = in_memory_session.query(Job).join(
            JobCharge, Job.id == JobCharge.job_id
        ).filter(JobCharge.charge_version == 1).all()
        assert charged[0].job_id == "tz.winter.mid", \
            "mid-day job (12:00 UTC) should be charged for Jan 15 Denver"

    def test_recalculate_generates_summary_in_correct_bin(self, in_memory_session):
        """--recalculate regenerates daily_summary using the same timezone,
        so the summary date matches the local date, not the UTC date."""
        _insert_job(in_memory_session, SUMMER_END_UTC)  # 02:00 UTC = May 31 local
        self._strip_charges(in_memory_session)

        syncer = _StubSyncer(in_memory_session, "casper")
        with patch.object(JobHistoryConfig, "SITE_TIMEZONE", "America/Denver"):
            # Recalculating May 31 should find and charge the job, then summarize it
            syncer.sync(
                log_dir=None, period="2025-05-31",
                recalculate=True, generate_summary=True,
            )

        # daily_summary for May 31 should have the job
        may31_rows = _summary_jobs_for_date(in_memory_session, MAY_31)
        june1_rows = _summary_jobs_for_date(in_memory_session, JUNE_1)
        assert len(may31_rows) >= 1, "job should appear in May 31 summary"
        assert len(june1_rows) == 0, "job should NOT appear in June 1 summary"
