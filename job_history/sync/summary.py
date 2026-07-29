"""Daily summary generation for charging data."""

from datetime import date, datetime, time, timedelta, timezone
from typing import Set
from zoneinfo import ZoneInfo

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from ..database.config import JobHistoryConfig
from ..database.models import DailySummary, Job


def get_summarized_dates(session: Session) -> Set[date]:
    """Get the set of dates that have already been summarized.

    Args:
        session: SQLAlchemy session

    Returns:
        Set of date objects that have entries in daily_summary
    """
    result = session.query(DailySummary.date).distinct().all()
    return {row[0] for row in result}


def generate_daily_summary(
    session: Session,
    machine: str,
    target_date: date,
    replace: bool = False,
) -> dict:
    """Generate daily summary for a specific date.

    Aggregates job data from the job_charges table into the daily_summary table.
    Uses UTC timestamp ranges that match the Mountain Time day to ensure
    consistent attribution.

    The join to ``job_charges`` is a LEFT join on purpose. It used to be an
    inner join, which silently dropped any job without a charge row from the
    rollup — and therefore from :meth:`~job_history.queries.JobQueries.daily_summary_report`,
    a charging surface. ``trg_ensure_job_charge`` makes that impossible on the
    production databases, but a rollup should not depend on a trigger for its
    arithmetic: such a job now counts with zero hours, exactly as it does in
    ``jobs_search`` / ``jobs_usage_by`` / ``jobs_timeseries``, which all outer
    join. ``JobQueries._timeseries_uses_summary`` relies on that agreement.

    Rows whose ``user_id`` / ``account_id`` / ``queue_id`` is NULL are still
    excluded, and that one is NOT incidental: a NULL FK triple is already the
    NO_JOBS marker (``DailySummary._null_sentinel``), so an unattributable job
    cannot be stored without becoming indistinguishable from one.

    Args:
        session: SQLAlchemy session
        machine: Machine name (kept for API compatibility)
        target_date: Date to summarize
        replace: If True, delete existing summary for this date first

    Returns:
        Dict with statistics about the summary generation
    """
    _ = machine  # All machines now use same summary structure
    stats = {"rows_deleted": 0, "rows_inserted": 0}

    # Site timezone (configured via JOB_HISTORY_SITE_TIMEZONE, default "America/Denver")
    site_timezone = ZoneInfo(JobHistoryConfig.SITE_TIMEZONE)

    # Calculate UTC range for the site-local day (JOB_HISTORY_SITE_TIMEZONE).
    # Jobs are stored with naive UTC timestamps (epoch → UTC, tzinfo stripped).
    # We must compare against naive UTC boundaries so that psycopg2 does not
    # perform a local-timezone conversion when binding the parameters to a
    # TIMESTAMP WITHOUT TIME ZONE column.
    start_dt = datetime.combine(target_date, time.min).replace(tzinfo=site_timezone)
    end_dt = datetime.combine(target_date + timedelta(days=1), time.min).replace(tzinfo=site_timezone)

    # Naive UTC: strip tzinfo so psycopg2 stores/compares as-is (no conversion)
    start_utc = start_dt.astimezone(timezone.utc).replace(tzinfo=None)
    end_utc = end_dt.astimezone(timezone.utc).replace(tzinfo=None)

    # Delete existing summaries for this date if replacing
    if replace:
        deleted = session.query(DailySummary).filter(
            DailySummary.date == target_date
        ).delete()
        stats["rows_deleted"] = deleted
        session.commit()

    # Check if summary already exists
    existing = session.query(DailySummary).filter(
        DailySummary.date == target_date
    ).first()

    if existing and not replace:
        return stats

    # Aggregate from job_charges table with foreign keys
    sql = text(
        """
        INSERT INTO daily_summary (date, user_id, account_id, queue_id,
                                 job_count, cpu_hours, gpu_hours, memory_hours,
                                 cpu_charges, gpu_charges, memory_charges)
        SELECT
            :target_date as date,
            j.user_id,
            j.account_id,
            j.queue_id,
            COUNT(*) as job_count,
            COALESCE(SUM(jc.cpu_hours   ), 0.0) as cpu_hours,
            COALESCE(SUM(jc.gpu_hours   ), 0.0) as gpu_hours,
            COALESCE(SUM(jc.memory_hours), 0.0) as memory_hours,
            COALESCE(SUM(jc.cpu_hours    * jc.qos_factor), 0.0) as cpu_charges,
            COALESCE(SUM(jc.gpu_hours    * jc.qos_factor), 0.0) as gpu_charges,
            COALESCE(SUM(jc.memory_hours * jc.qos_factor), 0.0) as memory_charges
        FROM jobs j
        LEFT JOIN job_charges jc ON j.id = jc.job_id
        WHERE j.end >= :start_utc AND j.end < :end_utc
          AND j.user_id IS NOT NULL
          AND j.account_id IS NOT NULL
          AND j.queue_id IS NOT NULL
        GROUP BY j.user_id, j.account_id, j.queue_id
    """
    )

    result = session.execute(sql, {
        "target_date": target_date.isoformat(),
        "start_utc": start_utc,
        "end_utc": end_utc
    })
    session.commit()

    stats["rows_inserted"] = result.rowcount

    # If no rows were inserted, create a marker to indicate date was processed
    # This prevents infinite re-fetching for days with no job completions
    if result.rowcount == 0:
        marker_sql = text(
            """
            INSERT INTO daily_summary (date, user_id, account_id, queue_id,
                                      job_count, cpu_hours, gpu_hours, memory_hours,
                                      cpu_charges, gpu_charges, memory_charges)
            VALUES (:target_date, NULL, NULL, NULL, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            """
        )
        session.execute(marker_sql, {"target_date": target_date.isoformat()})
        session.commit()
        stats["rows_inserted"] = 1  # Mark as processed

    return stats


def generate_summaries_for_range(
    session: Session,
    machine: str,
    start_date: date,
    end_date: date,
    replace: bool = False,
    verbose: bool = False,
) -> dict:
    """Generate daily summaries for a date range.

    Args:
        session: SQLAlchemy session
        machine: Machine name
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        replace: If True, replace existing summaries
        verbose: If True, print progress

    Returns:
        Dict with total statistics
    """
    from datetime import timedelta

    stats = {"total_rows": 0, "days_processed": 0, "days_skipped": 0}

    current = start_date
    while current <= end_date:
        if verbose:
            print(f"  Summarizing {current}...", end=" ", flush=True)

        day_stats = generate_daily_summary(session, machine, current, replace)

        if day_stats["rows_inserted"] > 0:
            stats["total_rows"] += day_stats["rows_inserted"]
            stats["days_processed"] += 1
            if verbose:
                print(f"{day_stats['rows_inserted']} rows")
        else:
            stats["days_skipped"] += 1
            if verbose:
                print("skipped (already exists or no data)")

        current += timedelta(days=1)

    return stats
