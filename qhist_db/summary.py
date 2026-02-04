"""Daily summary generation for charging data."""

from datetime import date, datetime, time, timedelta, timezone
from typing import Set
from zoneinfo import ZoneInfo

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from .models import DailySummary, Job


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

    # Mountain Time zone (handles MST/MDT automatically)
    mountain = ZoneInfo("America/Denver")
    
    # Calculate UTC range for the local day
    # target_date 00:00:00 MT
    start_dt = datetime.combine(target_date, time.min).replace(tzinfo=mountain)
    # target_date + 1 00:00:00 MT
    end_dt = datetime.combine(target_date + timedelta(days=1), time.min).replace(tzinfo=mountain)
    
    start_utc = start_dt.astimezone(timezone.utc)
    end_utc = end_dt.astimezone(timezone.utc)

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
        INSERT INTO daily_summary (date, user, account, queue, user_id, account_id, queue_id,
                                 job_count, cpu_hours, gpu_hours, memory_hours)
        SELECT
            :target_date as date,
            u.username as user,
            a.account_name as account,
            q.queue_name as queue,
            j.user_id,
            j.account_id,
            j.queue_id,
            COUNT(*) as job_count,
            SUM(jc.cpu_hours) as cpu_hours,
            SUM(jc.gpu_hours) as gpu_hours,
            SUM(jc.memory_hours) as memory_hours
        FROM jobs j
        JOIN job_charges jc ON j.id = jc.job_id
        LEFT JOIN users u ON j.user_id = u.id
        LEFT JOIN accounts a ON j.account_id = a.id
        LEFT JOIN queues q ON j.queue_id = q.id
        WHERE j.end >= :start_utc AND j.end < :end_utc
          AND j.user_id IS NOT NULL
          AND j.account_id IS NOT NULL
          AND j.queue_id IS NOT NULL
        GROUP BY j.user_id, j.account_id, j.queue_id, u.username, a.account_name, q.queue_name
    """
    )

    result = session.execute(sql, {
        "target_date": target_date.isoformat(),
        "start_utc": start_utc,
        "end_utc": end_utc
    })
    session.commit()

    stats["rows_inserted"] = result.rowcount
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
