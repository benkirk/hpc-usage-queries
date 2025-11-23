"""Daily summary generation for charging data."""

from datetime import date
from typing import Set

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

    Aggregates job data from the v_jobs_charged view into the daily_summary table.

    Args:
        session: SQLAlchemy session
        machine: Machine name ('casper' or 'derecho')
        target_date: Date to summarize
        replace: If True, delete existing summary for this date first

    Returns:
        Dict with statistics about the summary generation
    """
    stats = {"rows_deleted": 0, "rows_inserted": 0}

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

    # Use raw SQL to aggregate from the charging view
    # The view columns differ by machine
    if machine == "casper":
        sql = text("""
            INSERT INTO daily_summary (date, user, account, queue, job_count, cpu_hours, gpu_hours, memory_hours)
            SELECT
                date(end) as date,
                user,
                account,
                queue,
                COUNT(*) as job_count,
                SUM(cpu_hours) as cpu_hours,
                SUM(gpu_hours) as gpu_hours,
                SUM(memory_hours) as memory_hours
            FROM v_jobs_charged
            WHERE date(end) = :target_date
              AND user IS NOT NULL
              AND account IS NOT NULL
              AND queue IS NOT NULL
            GROUP BY date(end), user, account, queue
        """)
    else:  # derecho
        sql = text("""
            INSERT INTO daily_summary (date, user, account, queue, job_count, charge_hours)
            SELECT
                date(end) as date,
                user,
                account,
                queue,
                COUNT(*) as job_count,
                SUM(charge_hours) as charge_hours
            FROM v_jobs_charged
            WHERE date(end) = :target_date
              AND user IS NOT NULL
              AND account IS NOT NULL
              AND queue IS NOT NULL
            GROUP BY date(end), user, account, queue
        """)

    result = session.execute(sql, {"target_date": target_date.isoformat()})
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
