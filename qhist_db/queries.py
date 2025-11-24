"""Query interface for common HPC job history queries.

This module provides a Python API for common queries against the job history
database. It wraps SQLAlchemy queries with a convenient interface for:
- Finding jobs by user, account, or queue
- Generating usage summaries and statistics
- Filtering by date ranges and status
"""

from datetime import date, datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session

from .models import Job, DailySummary, JobCharged


class JobQueries:
    """High-level query interface for job history data.

    This class provides convenient methods for common queries without
    requiring direct knowledge of the underlying SQLAlchemy models.

    Example:
        >>> from qhist_db import get_session, JobQueries
        >>> session = get_session("derecho")
        >>> queries = JobQueries(session)
        >>> jobs = queries.jobs_by_user("jdoe", start=date(2024, 1, 1))
    """

    def __init__(self, session: Session):
        """Initialize query interface.

        Args:
            session: SQLAlchemy session for database access
        """
        self.session = session

    def jobs_by_user(
        self,
        user: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        status: Optional[str] = None,
        queue: Optional[str] = None,
    ) -> List[Job]:
        """Get all jobs for a user, optionally filtered by date range and other criteria.

        Args:
            user: Username to query
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            status: Optional job status filter (e.g., 'F' for finished)
            queue: Optional queue name filter

        Returns:
            List of Job objects matching the criteria
        """
        query = self.session.query(Job).filter(Job.user == user)

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        if status:
            query = query.filter(Job.status == status)
        if queue:
            query = query.filter(Job.queue == queue)

        return query.order_by(Job.end.desc()).all()

    def jobs_by_account(
        self,
        account: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        status: Optional[str] = None,
    ) -> List[Job]:
        """Get all jobs for an account, optionally filtered by date range.

        Args:
            account: Account name to query
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            status: Optional job status filter (e.g., 'F' for finished)

        Returns:
            List of Job objects matching the criteria
        """
        query = self.session.query(Job).filter(Job.account == account)

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        if status:
            query = query.filter(Job.status == status)

        return query.order_by(Job.end.desc()).all()

    def jobs_by_queue(
        self,
        queue: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Job]:
        """Get all jobs for a queue, optionally filtered by date range.

        Args:
            queue: Queue name to query
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of Job objects matching the criteria
        """
        query = self.session.query(Job).filter(Job.queue == queue)

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        return query.order_by(Job.end.desc()).all()

    def usage_summary(
        self,
        account: str,
        start: date,
        end: date,
    ) -> Dict[str, Any]:
        """Get usage summary for an account over a date range.

        Aggregates job counts and resource usage using charging hours from
        the v_jobs_charged view, which applies machine-specific charging rules.

        Args:
            account: Account name to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            Dict with aggregated metrics:
                - job_count: Total number of jobs
                - total_elapsed_seconds: Sum of all job elapsed times
                - total_cpu_hours: Sum of computed CPU hours (from charging view)
                - total_gpu_hours: Sum of computed GPU hours (from charging view)
                - total_memory_hours: Sum of computed memory hours (from charging view)
                - users: List of unique users
                - queues: List of unique queues
        """
        query = self.session.query(JobCharged).filter(
            and_(
                JobCharged.account == account,
                JobCharged.end >= datetime.combine(start, datetime.min.time()),
                JobCharged.end <= datetime.combine(end, datetime.max.time()),
            )
        )

        jobs = query.all()

        if not jobs:
            return {
                "job_count": 0,
                "total_elapsed_seconds": 0,
                "total_cpu_hours": 0.0,
                "total_gpu_hours": 0.0,
                "total_memory_hours": 0.0,
                "users": [],
                "queues": [],
            }

        total_elapsed = sum((j.elapsed or 0) for j in jobs)
        total_cpu_hours = sum((j.cpu_hours or 0.0) for j in jobs)
        total_gpu_hours = sum((j.gpu_hours or 0.0) for j in jobs)
        total_memory_hours = sum((j.memory_hours or 0.0) for j in jobs)

        unique_users = sorted(set(j.user for j in jobs if j.user))
        unique_queues = sorted(set(j.queue for j in jobs if j.queue))

        return {
            "job_count": len(jobs),
            "total_elapsed_seconds": total_elapsed,
            "total_cpu_hours": total_cpu_hours,
            "total_gpu_hours": total_gpu_hours,
            "total_memory_hours": total_memory_hours,
            "users": unique_users,
            "queues": unique_queues,
        }

    def user_summary(
        self,
        user: str,
        start: date,
        end: date,
    ) -> Dict[str, Any]:
        """Get usage summary for a user over a date range.

        Aggregates job counts and resource usage using charging hours from
        the v_jobs_charged view, which applies machine-specific charging rules.

        Args:
            user: Username to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            Dict with aggregated metrics similar to usage_summary
        """
        query = self.session.query(JobCharged).filter(
            and_(
                JobCharged.user == user,
                JobCharged.end >= datetime.combine(start, datetime.min.time()),
                JobCharged.end <= datetime.combine(end, datetime.max.time()),
            )
        )

        jobs = query.all()

        if not jobs:
            return {
                "job_count": 0,
                "total_elapsed_seconds": 0,
                "total_cpu_hours": 0.0,
                "total_gpu_hours": 0.0,
                "total_memory_hours": 0.0,
                "accounts": [],
                "queues": [],
            }

        total_elapsed = sum((j.elapsed or 0) for j in jobs)
        total_cpu_hours = sum((j.cpu_hours or 0.0) for j in jobs)
        total_gpu_hours = sum((j.gpu_hours or 0.0) for j in jobs)
        total_memory_hours = sum((j.memory_hours or 0.0) for j in jobs)

        unique_accounts = sorted(set(j.account for j in jobs if j.account))
        unique_queues = sorted(set(j.queue for j in jobs if j.queue))

        return {
            "job_count": len(jobs),
            "total_elapsed_seconds": total_elapsed,
            "total_cpu_hours": total_cpu_hours,
            "total_gpu_hours": total_gpu_hours,
            "total_memory_hours": total_memory_hours,
            "accounts": unique_accounts,
            "queues": unique_queues,
        }

    def daily_summary_by_account(
        self,
        account: str,
        start: date,
        end: date,
    ) -> List[DailySummary]:
        """Get daily summaries for an account over a date range.

        Uses pre-aggregated DailySummary table for efficient retrieval.

        Args:
            account: Account name to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            List of DailySummary objects
        """
        query = self.session.query(DailySummary).filter(
            and_(
                DailySummary.account == account,
                DailySummary.date >= start,
                DailySummary.date <= end,
            )
        )

        return query.order_by(DailySummary.date).all()

    def daily_summary_by_user(
        self,
        user: str,
        start: date,
        end: date,
    ) -> List[DailySummary]:
        """Get daily summaries for a user over a date range.

        Uses pre-aggregated DailySummary table for efficient retrieval.

        Args:
            user: Username to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            List of DailySummary objects
        """
        query = self.session.query(DailySummary).filter(
            and_(
                DailySummary.user == user,
                DailySummary.date >= start,
                DailySummary.date <= end,
            )
        )

        return query.order_by(DailySummary.date).all()

    def top_users_by_jobs(
        self,
        start: date,
        end: date,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get top users by job count in a date range.

        Args:
            start: Start date (inclusive)
            end: End date (inclusive)
            limit: Maximum number of users to return

        Returns:
            List of dicts with 'user' and 'job_count' keys
        """
        result = (
            self.session.query(
                Job.user,
                func.count(Job.id).label("job_count")
            )
            .filter(
                and_(
                    Job.end >= datetime.combine(start, datetime.min.time()),
                    Job.end <= datetime.combine(end, datetime.max.time()),
                )
            )
            .group_by(Job.user)
            .order_by(func.count(Job.id).desc())
            .limit(limit)
            .all()
        )

        return [{"user": user, "job_count": count} for user, count in result]

    def queue_statistics(
        self,
        start: date,
        end: date,
    ) -> List[Dict[str, Any]]:
        """Get statistics by queue for a date range.

        Args:
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            List of dicts with queue statistics
        """
        result = (
            self.session.query(
                Job.queue,
                func.count(Job.id).label("job_count"),
                func.sum(Job.elapsed).label("total_elapsed"),
                func.avg(Job.elapsed).label("avg_elapsed"),
            )
            .filter(
                and_(
                    Job.end >= datetime.combine(start, datetime.min.time()),
                    Job.end <= datetime.combine(end, datetime.max.time()),
                )
            )
            .group_by(Job.queue)
            .order_by(func.count(Job.id).desc())
            .all()
        )

        return [
            {
                "queue": queue,
                "job_count": count,
                "total_elapsed_seconds": elapsed or 0,
                "avg_elapsed_seconds": avg or 0,
            }
            for queue, count, elapsed, avg in result
        ]


if __name__ == "__main__":
    """Example usage of the JobQueries interface."""
    import sys
    from datetime import timedelta
    from .database import get_session

    # Check if database exists
    try:
        # Connect to Derecho by default (change to "casper" if needed)
        machine = "derecho"
        session = get_session(machine)
        queries = JobQueries(session)

        print(f"=== JobQueries Examples ({machine}) ===\n")

        # Example 1: Get recent jobs for a specific user
        print("Example 1: Recent jobs by user")
        print("-" * 50)

        # Get the date range for last 7 days
        end_date = date.today()
        start_date = end_date - timedelta(days=7)

        # Find a user with jobs in the database
        result = session.query(Job.user).filter(Job.user.isnot(None)).limit(1).first()

        if result:
            example_user = result[0]
            jobs = queries.jobs_by_user(example_user, start=start_date, end=end_date)
            print(f"User: {example_user}")
            print(f"Date range: {start_date} to {end_date}")
            print(f"Found {len(jobs)} jobs")

            if jobs:
                print(f"\nFirst 3 jobs:")
                for job in jobs[:3]:
                    print(f"  - Job {job.job_id}: {job.queue}, elapsed={job.elapsed}s")
        else:
            print("No users found in database")

        print()

        # Example 2: Usage summary for an account
        print("Example 2: Account usage summary")
        print("-" * 50)

        # Find an account with jobs
        result = session.query(Job.account).filter(Job.account.isnot(None)).limit(1).first()

        if result:
            example_account = result[0]
            summary = queries.usage_summary(example_account, start=start_date, end=end_date)
            print(f"Account: {example_account}")
            print(f"Date range: {start_date} to {end_date}")
            print(f"Job count: {summary['job_count']}")
            print(f"Total elapsed: {summary['total_elapsed_seconds']:,} seconds")
            print(f"Total CPU-hours: {summary['total_cpu_hours']:,.2f}")
            print(f"Total GPU-hours: {summary['total_gpu_hours']:,.2f}")
            print(f"Total Memory-hours: {summary['total_memory_hours']:,.2f}")
            print(f"Users: {', '.join(summary['users'][:5])}")
            print(f"Queues: {', '.join(summary['queues'])}")
        else:
            print("No accounts found in database")

        print()

        # Example 3: Top users by job count
        print("Example 3: Top 5 users by job count")
        print("-" * 50)

        top_users = queries.top_users_by_jobs(start=start_date, end=end_date, limit=5)

        if top_users:
            for i, user_stat in enumerate(top_users, 1):
                print(f"{i}. {user_stat['user']}: {user_stat['job_count']} jobs")
        else:
            print("No jobs found in date range")

        print()

        # Example 4: Queue statistics
        print("Example 4: Queue statistics")
        print("-" * 50)

        queue_stats = queries.queue_statistics(start=start_date, end=end_date)

        if queue_stats:
            for stat in queue_stats[:5]:  # Show top 5 queues
                avg_hours = stat['avg_elapsed_seconds'] / 3600
                print(f"{stat['queue']}:")
                print(f"  Jobs: {stat['job_count']}")
                print(f"  Avg elapsed: {avg_hours:.2f} hours")
        else:
            print("No queue statistics available")

        print()

        # Example 5: Daily summaries (if available)
        print("Example 5: Daily summaries for user")
        print("-" * 50)

        if result:
            result = session.query(DailySummary.user).limit(1).first()
            if result:
                example_user = result[0]
                daily = queries.daily_summary_by_user(example_user, start=start_date, end=end_date)
                print(f"User: {example_user}")
                print(f"Found {len(daily)} daily summary records")

                if len(daily) > 3:
                    print("\nFirst 3 days:")
                    for summary in daily[:3]:
                        print(f"  {summary.date}: {summary.job_count} jobs, "
                              f"{summary.cpu_hours:.2f} CPU-hours")
            else:
                print("No daily summaries found in database")

        session.close()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        print("\nMake sure you have:")
        print("1. Run 'make sync' to populate the database")
        print("2. Or specify a different machine with the QHIST_DERECHO_DB env var")
        sys.exit(1)
