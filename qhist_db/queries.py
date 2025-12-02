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


from sqlalchemy import case

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

    def gpu_job_waits_by_gpu_ranges(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get GPU job wait statistics grouped by GPU count ranges.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'gpu_range', 'avg_wait_hours', and 'job_count' keys.
        """
        gpu_ranges = [
            (4, 4), (8, 8), (9, 16), (17, 32), (33, 64), (65, 96), 
            (97, 128), (129, 256), (257, 320)
        ]
        gpu_queues = ['gpu', 'gpudev', 'pgpu']

        gpu_range_case = case(
            *[
                (and_(Job.numgpus >= low, Job.numgpus <= high), f"{low}-{high}" if low != high else str(low))
                for low, high in gpu_ranges
            ],
            else_=">320"
        ).label("gpu_range_label")

        wait_time_seconds = func.julianday(Job.start) - func.julianday(Job.eligible)
        wait_time_hours = wait_time_seconds * 24

        subquery = self.session.query(
            Job.id,
            gpu_range_case,
            wait_time_hours.label("wait_hours")
        ).filter(Job.queue.in_(gpu_queues))

        if start:
            subquery = subquery.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            subquery = subquery.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        
        subquery = subquery.subquery()

        query = self.session.query(
            subquery.c.gpu_range_label,
            func.avg(subquery.c.wait_hours).label("avg_wait_hours"),
            func.count(subquery.c.id).label("job_count")
        ).group_by(subquery.c.gpu_range_label)

        order_cases = {f"{low}-{high}" if low != high else str(low): i for i, (low, high) in enumerate(gpu_ranges)}
        order_cases['>320'] = len(gpu_ranges)
        order_expression = case(
            order_cases,
            value=subquery.c.gpu_range_label
        )

        results = query.order_by(order_expression).all()

        return [
            {
                "gpu_range": row[0],
                "avg_wait_hours": row[1] or 0.0,
                "job_count": row[2],
            }
            for row in results
        ]

    def pie_user_gpu(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get GPU usage statistics grouped by user for a pie chart.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'User-ids', 'Usage' (GPU hours), and 'Counts' (job count) keys.
        """
        gpu_queues = ['gpu', 'gpudev', 'pgpu']

        query = self.session.query(
            Job.user.label("User-ids"),
            func.sum(JobCharged.gpu_hours).label("Usage"),
            func.count(Job.id).label("Counts")
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(gpu_queues))

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by(Job.user).order_by(func.sum(JobCharged.gpu_hours).desc()).all()

        return [
            {
                "User-ids": row[0],
                "Usage": row[1] or 0.0,
                "Counts": row[2] or 0,
            }
            for row in results
        ]

    def pie_user_cpu(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get CPU usage statistics grouped by user for a pie chart.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'User-ids', 'Usage' (CPU hours), and 'Counts' (job count) keys.
        """
        cpu_queues = ['cpu', 'cpudev']

        query = self.session.query(
            Job.user.label("User-ids"),
            func.sum(JobCharged.cpu_hours).label("Usage"),
            func.count(Job.id).label("Counts")
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(cpu_queues))

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by(Job.user).order_by(func.sum(JobCharged.cpu_hours).desc()).all()

        return [
            {
                "User-ids": row[0],
                "Usage": row[1] or 0.0,
                "Counts": row[2] or 0,
            }
            for row in results
        ]

    def pie_group_gpu(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get GPU usage statistics grouped by account for a pie chart.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'Accounts', 'Usage' (GPU hours), and 'Counts' (job count) keys.
        """
        gpu_queues = ['gpu', 'gpudev', 'pgpu']

        query = self.session.query(
            Job.account.label("Accounts"),
            func.sum(JobCharged.gpu_hours).label("Usage"),
            func.count(Job.id).label("Counts")
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(gpu_queues))

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by(Job.account).order_by(func.sum(JobCharged.gpu_hours).desc()).all()

        return [
            {
                "Accounts": row[0],
                "Usage": row[1] or 0.0,
                "Counts": row[2] or 0,
            }
            for row in results
        ]

    def pie_group_cpu(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get CPU usage statistics grouped by account for a pie chart.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'Accounts', 'Usage' (CPU hours), and 'Counts' (job count) keys.
        """
        cpu_queues = ['cpu', 'cpudev']

        query = self.session.query(
            Job.account.label("Accounts"),
            func.sum(JobCharged.cpu_hours).label("Usage"),
            func.count(Job.id).label("Counts")
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(cpu_queues))

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by(Job.account).order_by(func.sum(JobCharged.cpu_hours).desc()).all()

        return [
            {
                "Accounts": row[0],
                "Usage": row[1] or 0.0,
                "Counts": row[2] or 0,
            }
            for row in results
        ]

    def gpu_job_durations_by_day(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get GPU job duration statistics by day.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'date' and duration bucket keys.
        """
        duration_buckets = {
            "<30s": Job.elapsed < 30,
            "30s-30m": and_(Job.elapsed >= 30, Job.elapsed < 1800),
            "30-60m": and_(Job.elapsed >= 1800, Job.elapsed < 3600),
            "1-5h": and_(Job.elapsed >= 3600, Job.elapsed < 18000),
            "5-12h": and_(Job.elapsed >= 18000, Job.elapsed < 43200),
            "12-18h": and_(Job.elapsed >= 43200, Job.elapsed < 64800),
            ">18h": Job.elapsed >= 64800,
        }
        gpu_queues = ['gpu', 'gpudev', 'pgpu']

        query = self.session.query(
            func.date(Job.end).label("job_date"),
            *[func.sum(case((bucket, JobCharged.gpu_hours), else_=0)).label(label) for label, bucket in duration_buckets.items()]
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(gpu_queues))

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by("job_date").order_by("job_date").all()

        return [
            {
                "date": row[0],
                **{label: row[i+1] or 0.0 for i, label in enumerate(duration_buckets.keys())}
            }
            for row in results
        ]
        
    def gpu_job_sizes_by_gpu_ranges(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get GPU job size statistics grouped by GPU count ranges.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'gpu_range', 'job_count', 'user_count', and 'gpu_hours' keys.
        """
        gpu_ranges = [
            (4, 4), (8, 8), (9, 16), (17, 32), (33, 64), (65, 96), 
            (97, 128), (129, 256), (257, 320)
        ]
        gpu_queues = ['gpu', 'gpudev', 'pgpu']

        gpu_range_case = case(
            *[
                (and_(Job.numgpus >= low, Job.numgpus <= high), f"{low}-{high}" if low != high else str(low))
                for low, high in gpu_ranges
            ],
            else_=">320"
        ).label("gpu_range_label")

        subquery = self.session.query(
            Job.id,
            Job.user,
            JobCharged.gpu_hours,
            gpu_range_case
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(gpu_queues))

        if start:
            subquery = subquery.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            subquery = subquery.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        
        subquery = subquery.subquery()

        query = self.session.query(
            subquery.c.gpu_range_label,
            func.count(subquery.c.id).label("job_count"),
            func.count(func.distinct(subquery.c.user)).label("user_count"),
            func.sum(subquery.c.gpu_hours).label("gpu_hours")
        ).group_by(subquery.c.gpu_range_label)
        
        order_cases = {f"{low}-{high}" if low != high else str(low): i for i, (low, high) in enumerate(gpu_ranges)}
        order_cases['>320'] = len(gpu_ranges)
        order_expression = case(
            order_cases,
            value=subquery.c.gpu_range_label
        )

        results = query.order_by(order_expression).all()

        return [
            {
                "gpu_range": row[0],
                "job_count": row[1],
                "user_count": row[2],
                "gpu_hours": row[3] or 0.0,
            }
            for row in results
        ]

    def usage_history_by_day(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get usage history by day.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with usage history statistics for each day.
        """
        cpu_queues = ['cpu', 'cpudev']
        gpu_queues = ['gpu', 'gpudev', 'pgpu']

        # Common subquery for date filtering
        date_filter = []
        if start:
            date_filter.append(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            date_filter.append(Job.end <= datetime.combine(end, datetime.max.time()))

        # 1. Total unique users per day
        total_users_subquery = self.session.query(
            func.strftime("%Y-%m-%d", Job.end).label("day"),
            func.count(func.distinct(Job.user)).label("total_users")
        ).filter(*date_filter).group_by("day").subquery()

        # 2. Total unique projects per day
        total_projects_subquery = self.session.query(
            func.strftime("%Y-%m-%d", Job.end).label("day"),
            func.count(func.distinct(Job.account)).label("total_projects")
        ).filter(*date_filter).group_by("day").subquery()
        
        # 3. CPU stats per day
        cpu_stats_subquery = self.session.query(
            func.strftime("%Y-%m-%d", Job.end).label("day"),
            func.count(func.distinct(Job.user)).label("cpu_users"),
            func.count(func.distinct(Job.account)).label("cpu_projects"),
            func.count(Job.id).label("cpu_jobs"),
            func.sum(JobCharged.cpu_hours).label("cpu_hours")
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(cpu_queues), *date_filter).group_by("day").subquery()

        # 4. GPU stats per day
        gpu_stats_subquery = self.session.query(
            func.strftime("%Y-%m-%d", Job.end).label("day"),
            func.count(func.distinct(Job.user)).label("gpu_users"),
            func.count(func.distinct(Job.account)).label("gpu_projects"),
            func.count(Job.id).label("gpu_jobs"),
            func.sum(JobCharged.gpu_hours).label("gpu_hours")
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(gpu_queues), *date_filter).group_by("day").subquery()

        # Join all the subqueries
        query = self.session.query(
            total_users_subquery.c.day,
            total_users_subquery.c.total_users,
            total_projects_subquery.c.total_projects,
            cpu_stats_subquery.c.cpu_users,
            cpu_stats_subquery.c.cpu_projects,
            cpu_stats_subquery.c.cpu_jobs,
            cpu_stats_subquery.c.cpu_hours,
            gpu_stats_subquery.c.gpu_users,
            gpu_stats_subquery.c.gpu_projects,
            gpu_stats_subquery.c.gpu_jobs,
            gpu_stats_subquery.c.gpu_hours
        ).join(
            total_projects_subquery, total_users_subquery.c.day == total_projects_subquery.c.day
        ).outerjoin(
            cpu_stats_subquery, total_users_subquery.c.day == cpu_stats_subquery.c.day
        ).outerjoin(
            gpu_stats_subquery, total_users_subquery.c.day == gpu_stats_subquery.c.day
        ).order_by(total_users_subquery.c.day)

        results = query.all()

        return [
            {
                "Date": row[0],
                "#-Users": row[1] or 0,
                "#-Proj": row[2] or 0,
                "#-CPU-Users": row[3] or 0,
                "#-CPU-Proj": row[4] or 0,
                "#-CPU-Jobs": row[5] or 0,
                "#-CPU-Hrs": row[6] or 0.0,
                "#-GPU-Users": row[7] or 0,
                "#-GPU-Proj": row[8] or 0,
                "#-GPU-Jobs": row[9] or 0,
                "#-GPU-Hrs": row[10] or 0.0,
            }
            for row in results
        ]

    def cpu_job_waits_by_node_ranges(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get CPU job wait statistics grouped by node count ranges.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'node_range', 'avg_wait_hours', and 'job_count' keys.
        """
        node_ranges = [
            (1, 1), (2, 2), (3, 4), (5, 8), (9, 16), (17, 32),
            (33, 64), (65, 128), (129, 256), (257, 512), (513, 1024), (1025, 2048)
        ]
        cpu_queues = ['cpu', 'cpudev']

        node_range_case = case(
            *[
                (and_(Job.numnodes >= low, Job.numnodes <= high), f"{low}-{high}" if low != high else str(low))
                for low, high in node_ranges
            ],
            else_=">2048"
        ).label("node_range_label")

        wait_time_seconds = func.julianday(Job.start) - func.julianday(Job.eligible)
        wait_time_hours = wait_time_seconds * 24

        subquery = self.session.query(
            Job.id,
            node_range_case,
            wait_time_hours.label("wait_hours")
        ).filter(Job.queue.in_(cpu_queues))

        if start:
            subquery = subquery.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            subquery = subquery.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        
        subquery = subquery.subquery()

        query = self.session.query(
            subquery.c.node_range_label,
            func.avg(subquery.c.wait_hours).label("avg_wait_hours"),
            func.count(subquery.c.id).label("job_count")
        ).group_by(subquery.c.node_range_label)

        order_cases = {f"{low}-{high}" if low != high else str(low): i for i, (low, high) in enumerate(node_ranges)}
        order_cases['>2048'] = len(node_ranges)
        order_expression = case(
            order_cases,
            value=subquery.c.node_range_label
        )

        results = query.order_by(order_expression).all()

        return [
            {
                "node_range": row[0],
                "avg_wait_hours": row[1] or 0.0,
                "job_count": row[2],
            }
            for row in results
        ]

    def cpu_job_sizes_by_node_ranges(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get CPU job size statistics grouped by node count ranges.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'node_range', 'job_count', 'user_count', and 'core_hours' keys.
        """
        node_ranges = [
            (1, 1), (2, 2), (3, 4), (5, 8), (9, 16), (17, 32),
            (33, 64), (65, 128), (129, 256), (257, 512), (513, 1024), (1025, 2048)
        ]
        cpu_queues = ['cpu', 'cpudev']

        node_range_case = case(
            *[
                (and_(Job.numnodes >= low, Job.numnodes <= high), f"{low}-{high}" if low != high else str(low))
                for low, high in node_ranges
            ],
            else_=">2048"
        ).label("node_range_label")

        subquery = self.session.query(
            Job.id,
            Job.user,
            JobCharged.cpu_hours,
            node_range_case
        ).join(JobCharged, Job.id == JobCharged.id).filter(Job.queue.in_(cpu_queues))

        if start:
            subquery = subquery.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            subquery = subquery.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        
        subquery = subquery.subquery()

        query = self.session.query(
            subquery.c.node_range_label,
            func.count(subquery.c.id).label("job_count"),
            func.count(func.distinct(subquery.c.user)).label("user_count"),
            func.sum(subquery.c.cpu_hours).label("core_hours")
        ).group_by(subquery.c.node_range_label)
        
        order_cases = {f"{low}-{high}" if low != high else str(low): i for i, (low, high) in enumerate(node_ranges)}
        order_cases['>2048'] = len(node_ranges)
        order_expression = case(
            order_cases,
            value=subquery.c.node_range_label
        )

        results = query.order_by(order_expression).all()

        return [
            {
                "node_range": row[0],
                "job_count": row[1],
                "user_count": row[2],
                "core_hours": row[3] or 0.0,
            }
            for row in results
        ]

    def cpu_job_durations_by_day(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get CPU job duration statistics by day.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'date' and duration bucket keys.
        """
        duration_buckets = {
            "<30s": Job.elapsed < 30,
            "30s-30m": and_(Job.elapsed >= 30, Job.elapsed < 1800),
            "30-60m": and_(Job.elapsed >= 1800, Job.elapsed < 3600),
            "1-5h": and_(Job.elapsed >= 3600, Job.elapsed < 18000),
            "5-12h": and_(Job.elapsed >= 18000, Job.elapsed < 43200),
            "12-18h": and_(Job.elapsed >= 43200, Job.elapsed < 64800),
            ">18h": Job.elapsed >= 64800,
        }

        query = self.session.query(
            func.date(Job.end).label("job_date"),
            *[func.sum(case((bucket, JobCharged.cpu_hours), else_=0)).label(label) for label, bucket in duration_buckets.items()]
        ).join(JobCharged, Job.id == JobCharged.id)

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by("job_date").order_by("job_date").all()

        return [
            {
                "date": row[0],
                **{label: row[i+1] or 0.0 for i, label in enumerate(duration_buckets.keys())}
            }
            for row in results
        ]
        
    def job_waits_by_core_ranges(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job wait statistics grouped by core count ranges.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'core_range', 'avg_wait_hours', and 'job_count' keys.
        """
        core_ranges = [
            (1, 1), (2, 2), (3, 4), (5, 8), (9, 16), (17, 32),
            (33, 48), (49, 64), (65, 96), (97, 128)
        ]

        core_range_case = case(
            *[
                (and_(Job.numcpus >= low, Job.numcpus <= high), f"{low}-{high}" if low != high else str(low))
                for low, high in core_ranges
            ],
            else_=">128"
        ).label("core_range_label")
        
        wait_time_seconds = func.julianday(Job.start) - func.julianday(Job.eligible)
        wait_time_hours = wait_time_seconds * 24

        subquery = self.session.query(
            Job.id,
            core_range_case,
            wait_time_hours.label("wait_hours")
        )

        if start:
            subquery = subquery.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            subquery = subquery.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        
        subquery = subquery.subquery()

        query = self.session.query(
            subquery.c.core_range_label,
            func.avg(subquery.c.wait_hours).label("avg_wait_hours"),
            func.count(subquery.c.id).label("job_count")
        ).group_by(subquery.c.core_range_label)

        order_cases = {f"{low}-{high}" if low != high else str(low): i for i, (low, high) in enumerate(core_ranges)}
        order_cases['>128'] = len(core_ranges)
        order_expression = case(
            order_cases,
            value=subquery.c.core_range_label
        )

        results = query.order_by(order_expression).all()

        return [
            {
                "core_range": row[0],
                "avg_wait_hours": row[1] or 0.0,
                "job_count": row[2],
            }
            for row in results
        ]

    def job_sizes_by_core_ranges(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job size statistics grouped by core count ranges.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            A list of dicts with 'core_range', 'job_count', 'user_count', and 'core_hours' keys.
        """
        core_ranges = [
            (1, 1), (2, 2), (3, 4), (5, 8), (9, 16), (17, 32),
            (33, 48), (49, 64), (65, 96), (97, 128)
        ]

        # Create the CASE statement for core ranges
        core_range_case = case(
            *[
                (and_(Job.numcpus >= low, Job.numcpus <= high), f"{low}-{high}" if low != high else str(low))
                for low, high in core_ranges
            ],
            else_=">128"
        ).label("core_range_label")

        # Subquery to get the core range for each job
        subquery = self.session.query(
            Job.id,
            Job.user,
            JobCharged.cpu_hours,
            core_range_case
        ).join(JobCharged, Job.id == JobCharged.id)

        if start:
            subquery = subquery.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            subquery = subquery.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        
        subquery = subquery.subquery()

        # Main query to aggregate the results
        query = self.session.query(
            subquery.c.core_range_label,
            func.count(subquery.c.id).label("job_count"),
            func.count(func.distinct(subquery.c.user)).label("user_count"),
            func.sum(subquery.c.cpu_hours).label("core_hours")
        ).group_by(subquery.c.core_range_label)

        # Order the results based on the core_ranges list
        order_cases = {f"{low}-{high}" if low != high else str(low): i for i, (low, high) in enumerate(core_ranges)}
        order_cases['>128'] = len(core_ranges)
        order_expression = case(
            order_cases,
            value=subquery.c.core_range_label
        )

        results = query.order_by(order_expression).all()

        return [
            {
                "core_range": row[0],
                "job_count": row[1],
                "user_count": row[2],
                "core_hours": row[3] or 0.0,
            }
            for row in results
        ]

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

    def jobs_per_user_account_by_period(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of jobs per user per account by period in a date range.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter')

        Returns:
            A list of dicts with 'period', 'user', 'account', and 'job_count' keys.
        """
        if period == "day":
            period_func = func.strftime("%Y-%m-%d", Job.end)
        elif period == "month":
            period_func = func.strftime("%Y-%m", Job.end)
        elif period == "quarter":
            # This is tricky with pure SQL in SQLite. We will get monthly data and aggregate.
            # However, for job counts, we can just sum them up.
            period_func = func.strftime("%Y-%m", Job.end)
        else:
            raise ValueError("Invalid period specified. Must be 'day', 'month', or 'quarter'.")

        query = self.session.query(
            period_func.label("period"),
            Job.user,
            Job.account,
            func.count(Job.id).label("job_count")
        )

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by("period", Job.user, Job.account).order_by("period", Job.user, Job.account).all()

        if period == "quarter":
            quarterly_counts = {} # key: (YYYY-Q, user, account)
            for row in results:
                year, month = row.period.split('-')
                quarter = (int(month) - 1) // 3 + 1
                q_key = f"{year}-Q{quarter}"
                
                agg_key = (q_key, row.user, row.account)
                quarterly_counts[agg_key] = quarterly_counts.get(agg_key, 0) + row.job_count

            return [{"period": key[0], "user": key[1], "account": key[2], "job_count": value} for key, value in quarterly_counts.items()]


        return [{"period": row[0], "user": row[1], "account": row[2], "job_count": row[3]} for row in results]

    def unique_projects_by_period(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of unique projects by period in a date range.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter')

        Returns:
            A list of dicts with 'period' and 'project_count' keys.
        """
        if period == "quarter":
            # For quarters, get monthly data and aggregate
            monthly_query = self.session.query(
                func.strftime("%Y-%m", Job.end).label("month"),
                Job.account
            ).distinct()

            if start:
                monthly_query = monthly_query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
            if end:
                monthly_query = monthly_query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

            monthly_results = monthly_query.all()
            
            quarterly_projects = {} # key: "YYYY-Q", value: set of projects
            for month_str, project in monthly_results:
                if not project or not month_str:
                    continue
                year, month = map(int, month_str.split('-'))
                quarter = (month - 1) // 3 + 1
                q_key = f"{year}-Q{quarter}"
                if q_key not in quarterly_projects:
                    quarterly_projects[q_key] = set()
                quarterly_projects[q_key].add(project)
            
            results = [{"period": key, "project_count": len(projects)} for key, projects in quarterly_projects.items()]
            return sorted(results, key=lambda x: x['period'])

        if period == "day":
            period_func = func.strftime("%Y-%m-%d", Job.end)
        elif period == "month":
            period_func = func.strftime("%Y-%m", Job.end)
        else:
            raise ValueError("Invalid period specified. Must be 'day', 'month', or 'quarter'.")

        query = self.session.query(
            period_func.label("period"),
            func.count(func.distinct(Job.account)).label("project_count")
        )

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by("period").order_by("period").all()

        return [{"period": row[0], "project_count": row[1]} for row in results]

    def unique_users_by_period(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of unique users by period in a date range.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter')

        Returns:
            A list of dicts with 'period' and 'user_count' keys.
        """
        if period == "quarter":
            # For quarters, get monthly data and aggregate
            monthly_query = self.session.query(
                func.strftime("%Y-%m", Job.end).label("month"),
                Job.user
            ).distinct()

            if start:
                monthly_query = monthly_query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
            if end:
                monthly_query = monthly_query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

            monthly_results = monthly_query.all()
            
            quarterly_users = {} # key: "YYYY-Q", value: set of users
            for month_str, user in monthly_results:
                if not user or not month_str:
                    continue
                year, month = map(int, month_str.split('-'))
                quarter = (month - 1) // 3 + 1
                q_key = f"{year}-Q{quarter}"
                if q_key not in quarterly_users:
                    quarterly_users[q_key] = set()
                quarterly_users[q_key].add(user)
            
            results = [{"period": key, "user_count": len(users)} for key, users in quarterly_users.items()]
            return sorted(results, key=lambda x: x['period'])


        if period == "day":
            period_func = func.strftime("%Y-%m-%d", Job.end)
        elif period == "month":
            period_func = func.strftime("%Y-%m", Job.end)
        else:
            raise ValueError("Invalid period specified. Must be 'day', 'month', or 'quarter'.")

        query = self.session.query(
            period_func.label("period"),
            func.count(func.distinct(Job.user)).label("user_count")
        )

        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))

        results = query.group_by("period").order_by("period").all()

        return [{"period": row[0], "user_count": row[1]} for row in results]

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
