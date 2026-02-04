"""Query interface for common HPC job history queries.

This module provides a Python API for common queries against the job history
database. It wraps SQLAlchemy queries with a convenient interface for:
- Finding jobs by user, account, or queue
- Generating usage summaries and statistics
- Filtering by date ranges and status
"""

from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session

from .models import Job, DailySummary, JobCharge


from sqlalchemy import case


class QueryConfig:
    """Centralized configuration for query patterns and constants."""

    # Machine-specific queue definitions
    MACHINE_QUEUES = {
        'derecho': {
            'cpu': ['cpu', 'cpudev'],
            'gpu': ['gpu', 'gpudev', 'pgpu']
        },
        'casper': {
            'cpu': ['htc', 'gdex', 'largemem', 'vis', 'rda'],
            'gpu': ['nvgpu', 'gpgpu', 'a100', 'h100', 'l40', 'amdgpu']
        }
    }

    # Legacy attributes for backward compatibility (default to Derecho)
    CPU_QUEUES = MACHINE_QUEUES['derecho']['cpu']
    GPU_QUEUES = MACHINE_QUEUES['derecho']['gpu']

    @staticmethod
    def get_cpu_queues(machine: str) -> list:
        """Get CPU queue names for a specific machine.

        Args:
            machine: Machine name ('casper' or 'derecho')

        Returns:
            List of CPU queue names for the machine
        """
        return QueryConfig.MACHINE_QUEUES.get(machine.lower(), {}).get('cpu', QueryConfig.CPU_QUEUES)

    @staticmethod
    def get_gpu_queues(machine: str) -> list:
        """Get GPU queue names for a specific machine.

        Args:
            machine: Machine name ('casper' or 'derecho')

        Returns:
            List of GPU queue names for the machine
        """
        return QueryConfig.MACHINE_QUEUES.get(machine.lower(), {}).get('gpu', QueryConfig.GPU_QUEUES)

    @staticmethod
    def _make_ranges(boundaries: List[int]) -> List[Tuple[int, int]]:
        """Generate range tuples from boundary list.

        The first two boundaries create single-value ranges (e.g., (4,4), (8,8)),
        then subsequent boundaries fill gaps (e.g., (9,16), (17,32)).

        Args:
            boundaries: List of boundary values (e.g., [4, 8, 16, 32, 64])

        Returns:
            List of (low, high) tuples

        Examples:
            >>> QueryConfig._make_ranges([4, 8, 16, 32])
            [(4, 4), (8, 8), (9, 16), (17, 32)]
            >>> QueryConfig._make_ranges([1, 2, 4, 8])
            [(1, 1), (2, 2), (3, 4), (5, 8)]
        """
        if not boundaries:
            return []

        ranges = []

        # First two boundaries are singleton ranges
        for i in range(min(2, len(boundaries))):
            ranges.append((boundaries[i], boundaries[i]))

        # Remaining boundaries fill gaps
        if len(boundaries) > 2:
            prev = boundaries[1] + 1  # Start after second boundary
            for bound in boundaries[2:]:
                ranges.append((prev, bound))
                prev = bound + 1

        return ranges

    # GPU resource ranges: 4, 8, 9-16, 17-32, 33-64, 65-96, 97-128, 129-256, 257-320
    GPU_RANGES = _make_ranges([4, 8, 16, 32, 64, 96, 128, 256, 320])
    GPU_OVERFLOW = ">320"

    # Node resource ranges: 1, 2, 3-4, 5-8, ..., 1025-2048
    NODE_RANGES = _make_ranges([1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048])
    NODE_OVERFLOW = ">2048"

    # Core resource ranges: 1, 2, 3-4, 5-8, ..., 97-128
    CORE_RANGES = _make_ranges([1, 2, 4, 8, 16, 32, 48, 64, 96, 128])
    CORE_OVERFLOW = ">128"

    # Memory resource ranges (GB)
    MEMORY_RANGES = [
        (1, 10), (11, 50), (51, 100), (101, 500), (501, 1000)
    ]
    MEMORY_OVERFLOW = ">1000"

    # Duration buckets (in seconds)
    @staticmethod
    def get_duration_buckets():
        """Get duration bucket definitions.

        Returns as a static method to avoid issues with Job reference at import time.
        """
        return {
            "<30s": Job.elapsed < 30,
            "30s-30m": and_(Job.elapsed >= 30, Job.elapsed < 1800),
            "30-60m": and_(Job.elapsed >= 1800, Job.elapsed < 3600),
            "1-5h": and_(Job.elapsed >= 3600, Job.elapsed < 18000),
            "5-12h": and_(Job.elapsed >= 18000, Job.elapsed < 43200),
            "12-18h": and_(Job.elapsed >= 43200, Job.elapsed < 64800),
            ">18h": Job.elapsed >= 64800,
        }

    @staticmethod
    def get_memory_per_rank_buckets():
        """Get memory-per-rank bucket definitions (mixed MB/GB units).

        Returns buckets for memory per rank histogram using Job.memory
        (actual memory used) divided by (mpiprocs * ompthreads * numnodes).

        Returns as a static method to avoid issues with Job reference at import time.
        """
        from .charging import BYTES_PER_GB
        BYTES_PER_MB = 1024 * 1024

        return {
            "<128MB": Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (128 * BYTES_PER_MB),
            "128MB-512MB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (128 * BYTES_PER_MB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (512 * BYTES_PER_MB)
            ),
            "512MB-1GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (512 * BYTES_PER_MB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < BYTES_PER_GB
            ),
            "1-2GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= BYTES_PER_GB,
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (2 * BYTES_PER_GB)
            ),
            "2-4GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (2 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (4 * BYTES_PER_GB)
            ),
            "4-8GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (4 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (8 * BYTES_PER_GB)
            ),
            "8-16GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (8 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (16 * BYTES_PER_GB)
            ),
            "16-32GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (16 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (32 * BYTES_PER_GB)
            ),
            "32-64GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (32 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (64 * BYTES_PER_GB)
            ),
            "64-128GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (64 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (128 * BYTES_PER_GB)
            ),
            "128-256GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (128 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (256 * BYTES_PER_GB)
            ),
            ">256GB": Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (256 * BYTES_PER_GB),
        }


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

    def __init__(self, session: Session, machine: str = 'derecho'):
        """Initialize query interface.

        Args:
            session: SQLAlchemy session for database access
            machine: Machine name ('casper' or 'derecho') for queue filtering
        """
        self.session = session
        self.machine = machine.lower()

    def _apply_date_filter(self, query, start: Optional[date], end: Optional[date]):
        """Apply consistent date filtering to a query.

        Filters on Job.end time using datetime.combine for proper time boundaries.

        Args:
            query: SQLAlchemy query object
            start: Optional start date (inclusive)
            end: Optional end date (inclusive)

        Returns:
            Filtered query
        """
        if start:
            query = query.filter(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            query = query.filter(Job.end <= datetime.combine(end, datetime.max.time()))
        return query

    def _build_range_case(self, ranges: List[tuple], overflow_label: str, field):
        """Build a CASE statement for resource range bucketing.

        Args:
            ranges: List of (low, high) tuples defining ranges
            overflow_label: Label for values exceeding max range
            field: SQLAlchemy column to apply ranges to (e.g., Job.numgpus)

        Returns:
            SQLAlchemy case statement with label "range_label"
        """
        return case(
            *[
                (and_(field >= low, field <= high),
                 f"{low}-{high}" if low != high else str(low))
                for low, high in ranges
            ],
            else_=overflow_label
        ).label("range_label")

    def _build_range_ordering(self, ranges: List[tuple], overflow_label: str, range_column):
        """Build ordering expression for range-based results.

        Ensures ranges appear in natural order (1, 2, 3-4, 5-8, ..., >max).

        Args:
            ranges: List of (low, high) tuples
            overflow_label: Label for overflow range
            range_column: Column containing range labels

        Returns:
            SQLAlchemy case ordering expression
        """
        order_cases = {
            f"{low}-{high}" if low != high else str(low): i
            for i, (low, high) in enumerate(ranges)
        }
        order_cases[overflow_label] = len(ranges)
        return case(order_cases, value=range_column)

    def usage_by_group(
        self,
        resource_type: str,
        group_by: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get resource usage statistics grouped by user or account.

        Generic factory method replacing pie chart queries.

        Args:
            resource_type: 'cpu' | 'gpu' | 'all' - type of resources to query
            group_by: 'user' | 'account' - field to group by
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'label', 'usage_hours', 'job_count'

        Examples:
            >>> # CPU usage by user (replaces pie_user_cpu)
            >>> queries.usage_by_group('cpu', 'user', start_date, end_date)
            >>> # GPU usage by account (replaces pie_group_gpu)
            >>> queries.usage_by_group('gpu', 'account', start_date, end_date)
        """
        # Determine queues and hours field (machine-specific)
        if resource_type == 'cpu':
            queues = QueryConfig.get_cpu_queues(self.machine)
            hours_field = JobCharge.cpu_hours
        elif resource_type == 'gpu':
            queues = QueryConfig.get_gpu_queues(self.machine)
            hours_field = JobCharge.gpu_hours
        else:  # 'all'
            queues = QueryConfig.get_cpu_queues(self.machine) + QueryConfig.get_gpu_queues(self.machine)
            # For 'all', sum both cpu_hours and gpu_hours
            hours_field = func.coalesce(JobCharge.cpu_hours, 0) + func.coalesce(JobCharge.gpu_hours, 0)

        # Determine group field
        group_field = Job.user if group_by == 'user' else Job.account

        # Build query
        query = self.session.query(
            group_field.label("label"),
            func.sum(hours_field).label("usage_hours"),
            func.count(Job.id).label("job_count")
        ).join(JobCharge, Job.id == JobCharge.id).filter(Job.queue.in_(queues))

        query = self._apply_date_filter(query, start, end)
        results = query.group_by(group_field).order_by(func.sum(hours_field).desc()).all()

        return [
            {
                "label": row[0],
                "usage_hours": row[1] or 0.0,
                "job_count": row[2] or 0,
            }
            for row in results
        ]

    def job_waits_by_resource(
        self,
        resource_type: str,
        range_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job wait statistics grouped by resource count ranges.

        Generic factory method replacing wait time queries.

        Args:
            resource_type: 'cpu' | 'gpu' | 'all' - type of resources to filter
            range_type: 'gpu' | 'node' | 'core' | 'memory' - resource to group by
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'range_label', 'avg_wait_hours', 'job_count'

        Examples:
            >>> # GPU job waits (replaces gpu_job_waits_by_gpu_ranges)
            >>> queries.job_waits_by_resource('gpu', 'gpu', start_date, end_date)
            >>> # CPU job waits by node (replaces cpu_job_waits_by_node_ranges)
            >>> queries.job_waits_by_resource('cpu', 'node', start_date, end_date)
        """
        # Determine queues (machine-specific)
        if resource_type == 'cpu':
            queues = QueryConfig.get_cpu_queues(self.machine)
        elif resource_type == 'gpu':
            queues = QueryConfig.get_gpu_queues(self.machine)
        else:  # 'all'
            queues = QueryConfig.get_cpu_queues(self.machine) + QueryConfig.get_gpu_queues(self.machine)

        # Determine ranges and field
        if range_type == 'gpu':
            ranges = QueryConfig.GPU_RANGES
            overflow = QueryConfig.GPU_OVERFLOW
            field = Job.numgpus
        elif range_type == 'node':
            ranges = QueryConfig.NODE_RANGES
            overflow = QueryConfig.NODE_OVERFLOW
            field = Job.numnodes
        elif range_type == 'core':
            ranges = QueryConfig.CORE_RANGES
            overflow = QueryConfig.CORE_OVERFLOW
            field = Job.numcpus
        else:  # 'memory'
            ranges = QueryConfig.MEMORY_RANGES
            overflow = QueryConfig.MEMORY_OVERFLOW
            # Convert bytes to GB for memory ranges
            field = Job.reqmem / (1024**3)

        # Build range case and wait time calculation
        range_case = self._build_range_case(ranges, overflow, field)
        wait_time_hours = (func.julianday(Job.start) - func.julianday(Job.eligible)) * 24

        # Build subquery
        subquery = self.session.query(
            Job.id,
            range_case,
            wait_time_hours.label("wait_hours")
        ).filter(Job.queue.in_(queues))

        subquery = self._apply_date_filter(subquery, start, end)
        subquery = subquery.subquery()

        # Aggregate by range
        query = self.session.query(
            subquery.c.range_label,
            func.avg(subquery.c.wait_hours).label("avg_wait_hours"),
            func.count(subquery.c.id).label("job_count")
        ).group_by(subquery.c.range_label)

        # Apply custom ordering
        order_expr = self._build_range_ordering(ranges, overflow, subquery.c.range_label)
        results = query.order_by(order_expr).all()

        return [
            {
                "range_label": row[0],
                "avg_wait_hours": row[1] or 0.0,
                "job_count": row[2],
            }
            for row in results
        ]

    def job_sizes_by_resource(
        self,
        resource_type: str,
        range_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job size statistics grouped by resource count ranges.

        Generic factory method replacing job size queries.

        Args:
            resource_type: 'cpu' | 'gpu' | 'all' - type of resources to filter
            range_type: 'gpu' | 'node' | 'core' | 'memory' - resource to group by
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'range_label', 'job_count', 'user_count', 'hours'

        Examples:
            >>> # GPU job sizes (replaces gpu_job_sizes_by_gpu_ranges)
            >>> queries.job_sizes_by_resource('gpu', 'gpu', start_date, end_date)
            >>> # CPU job sizes by node (replaces cpu_job_sizes_by_node_ranges)
            >>> queries.job_sizes_by_resource('cpu', 'node', start_date, end_date)
        """
        # Determine queues and hours field (machine-specific)
        if resource_type == 'cpu':
            queues = QueryConfig.get_cpu_queues(self.machine)
            hours_field = JobCharge.cpu_hours
        elif resource_type == 'gpu':
            queues = QueryConfig.get_gpu_queues(self.machine)
            hours_field = JobCharge.gpu_hours
        else:  # 'all'
            queues = QueryConfig.get_cpu_queues(self.machine) + QueryConfig.get_gpu_queues(self.machine)
            hours_field = func.coalesce(JobCharge.cpu_hours, 0) + func.coalesce(JobCharge.gpu_hours, 0)

        # Determine ranges and field
        if range_type == 'gpu':
            ranges = QueryConfig.GPU_RANGES
            overflow = QueryConfig.GPU_OVERFLOW
            field = Job.numgpus
        elif range_type == 'node':
            ranges = QueryConfig.NODE_RANGES
            overflow = QueryConfig.NODE_OVERFLOW
            field = Job.numnodes
        elif range_type == 'core':
            ranges = QueryConfig.CORE_RANGES
            overflow = QueryConfig.CORE_OVERFLOW
            field = Job.numcpus
        else:  # 'memory'
            ranges = QueryConfig.MEMORY_RANGES
            overflow = QueryConfig.MEMORY_OVERFLOW
            field = Job.reqmem / (1024**3)

        # Build range case
        range_case = self._build_range_case(ranges, overflow, field)

        # Build subquery
        subquery = self.session.query(
            Job.id,
            Job.user,
            hours_field.label("hours_field"),
            range_case
        ).join(JobCharge, Job.id == JobCharge.id).filter(Job.queue.in_(queues))

        subquery = self._apply_date_filter(subquery, start, end)
        subquery = subquery.subquery()

        # Aggregate by range
        query = self.session.query(
            subquery.c.range_label,
            func.count(subquery.c.id).label("job_count"),
            func.count(func.distinct(subquery.c.user)).label("user_count"),
            func.sum(subquery.c.hours_field).label("hours")
        ).group_by(subquery.c.range_label)

        # Apply custom ordering
        order_expr = self._build_range_ordering(ranges, overflow, subquery.c.range_label)
        results = query.order_by(order_expr).all()

        return [
            {
                "range_label": row[0],
                "job_count": row[1],
                "user_count": row[2],
                "hours": row[3] or 0.0,
            }
            for row in results
        ]

    def job_durations(
        self,
        resource_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get job duration statistics by period.

        Generic factory method replacing duration queries.

        Args:
            resource_type: 'cpu' | 'gpu' | 'all' - type of resources to filter
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day' or 'month')

        Returns:
            List of dicts with keys: 'date', '<30s', '30s-30m', '30-60m', '1-5h', '5-12h', '12-18h', '>18h'

        Examples:
            >>> # GPU job durations by day
            >>> queries.job_durations('gpu', start_date, end_date)
            >>> # CPU job durations by month
            >>> queries.job_durations('cpu', start_date, end_date, period='month')
        """
        from .query_builders import ResourceTypeResolver, PeriodGrouper

        # Resolve resource type to queues and hours field (machine-specific)
        queues, hours_field = ResourceTypeResolver.resolve(resource_type, self.machine, JobCharge)

        # Get duration buckets
        duration_buckets = QueryConfig.get_duration_buckets()

        # Get period grouping function
        period_func = PeriodGrouper.get_period_func(period, Job.end)

        # Build query
        query = self.session.query(
            period_func.label("job_date"),
            *[
                func.sum(case((bucket, hours_field), else_=0)).label(label)
                for label, bucket in duration_buckets.items()
            ]
        ).join(JobCharge, Job.id == JobCharge.id).filter(Job.queue.in_(queues))

        query = self._apply_date_filter(query, start, end)
        results = query.group_by("job_date").order_by("job_date").all()

        return [
            {
                "date": row[0],
                **{label: row[i+1] or 0.0 for i, label in enumerate(duration_buckets.keys())}
            }
            for row in results
        ]

    def job_memory_per_rank(
        self,
        resource_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get job memory-per-rank histogram by period.

        Calculates memory per rank as memory_bytes / (mpiprocs * ompthreads * numnodes).
        Filters out jobs where mpiprocs, ompthreads, numnodes, or memory is 0 or NULL.

        Args:
            resource_type: 'cpu' | 'gpu' - type of resources to filter
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day' or 'month')

        Returns:
            List of dicts with date + histogram bucket columns

        Examples:
            >>> # CPU job memory-per-rank by day
            >>> queries.job_memory_per_rank('cpu', start_date, end_date)
            >>> # GPU job memory-per-rank by month
            >>> queries.job_memory_per_rank('gpu', start_date, end_date, period='month')
        """
        from .query_builders import ResourceTypeResolver, PeriodGrouper

        # Resolve resource type to queues and hours field (machine-specific)
        queues, hours_field = ResourceTypeResolver.resolve(resource_type, self.machine, JobCharge)

        # Get memory-per-rank buckets
        memory_buckets = QueryConfig.get_memory_per_rank_buckets()

        # Get period grouping function
        period_func = PeriodGrouper.get_period_func(period, Job.end)

        # Build query with CASE statements for each bucket
        query = self.session.query(
            period_func.label("job_date"),
            *[
                func.sum(case((bucket, hours_field), else_=0)).label(label)
                for label, bucket in memory_buckets.items()
            ]
        ).join(JobCharge, Job.id == JobCharge.id).filter(
            Job.queue.in_(queues),
            Job.mpiprocs.isnot(None),   # Filter NULL
            Job.mpiprocs > 0,           # Filter zero (prevents division by zero)
            Job.ompthreads.isnot(None), # Filter NULL
            Job.ompthreads > 0,         # Filter zero (prevents division by zero)
            Job.numnodes.isnot(None),   # Filter NULL
            Job.numnodes > 0,           # Filter zero (prevents division by zero)
            Job.memory.isnot(None)      # Filter NULL memory
        )

        query = self._apply_date_filter(query, start, end)
        results = query.group_by("job_date").order_by("job_date").all()

        return [
            {
                "date": row[0],
                **{label: row[i+1] or 0.0 for i, label in enumerate(memory_buckets.keys())}
            }
            for row in results
        ]

    def memory_job_waits(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job wait statistics grouped by memory requirement ranges.

        Convenience method wrapping job_waits_by_resource with memory range type.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'range_label', 'avg_wait_hours', 'job_count'

        Examples:
            >>> queries.memory_job_waits(start_date, end_date)
        """
        return self.job_waits_by_resource(
            resource_type='all',
            range_type='memory',
            start=start,
            end=end
        )

    def memory_job_sizes(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job size statistics grouped by memory requirement ranges.

        Convenience method wrapping job_sizes_by_resource with memory range type.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'range_label', 'job_count', 'user_count', 'hours'

        Examples:
            >>> queries.memory_job_sizes(start_date, end_date)
        """
        return self.job_sizes_by_resource(
            resource_type='all',
            range_type='memory',
            start=start,
            end=end
        )

    def _build_date_filter(self, start: Optional[date], end: Optional[date]) -> List:
        """Build date filter conditions for usage_history queries.

        Args:
            start: Optional start date (inclusive)
            end: Optional end date (inclusive)

        Returns:
            List of SQLAlchemy filter conditions
        """
        date_filter = []
        if start:
            date_filter.append(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            date_filter.append(Job.end <= datetime.combine(end, datetime.max.time()))
        return date_filter

    def _usage_history_total_users(self, period_col, date_filter: List):
        """Build subquery for total unique users per period.

        Args:
            period_col: SQLAlchemy expression for period grouping
            date_filter: List of filter conditions

        Returns:
            SQLAlchemy subquery for total users
        """
        return self.session.query(
            period_col.label("period"),
            func.count(func.distinct(Job.user)).label("total_users")
        ).filter(*date_filter).group_by("period").subquery()

    def _usage_history_total_projects(self, period_col, date_filter: List):
        """Build subquery for total unique projects per period.

        Args:
            period_col: SQLAlchemy expression for period grouping
            date_filter: List of filter conditions

        Returns:
            SQLAlchemy subquery for total projects
        """
        return self.session.query(
            period_col.label("period"),
            func.count(func.distinct(Job.account)).label("total_projects")
        ).filter(*date_filter).group_by("period").subquery()

    def _usage_history_resource_stats(
        self, resource_type: str, period_col, date_filter: List
    ):
        """Build subquery for CPU or GPU stats per period.

        Args:
            resource_type: 'cpu' or 'gpu'
            period_col: SQLAlchemy expression for period grouping
            date_filter: List of filter conditions

        Returns:
            SQLAlchemy subquery for resource stats (users, projects, jobs, hours)
        """
        from .query_builders import ResourceTypeResolver

        queues, hours_field = ResourceTypeResolver.resolve(
            resource_type, self.machine, JobCharge
        )

        prefix = resource_type.lower()

        return self.session.query(
            period_col.label("period"),
            func.count(func.distinct(Job.user)).label(f"{prefix}_users"),
            func.count(func.distinct(Job.account)).label(f"{prefix}_projects"),
            func.count(Job.id).label(f"{prefix}_jobs"),
            func.sum(hours_field).label(f"{prefix}_hours")
        ).join(
            JobCharge, Job.id == JobCharge.id
        ).filter(
            Job.queue.in_(queues), *date_filter
        ).group_by("period").subquery()

    def _join_usage_history_results(self, users_sq, projects_sq, cpu_sq, gpu_sq):
        """Join subqueries and format usage history results.

        Args:
            users_sq: Total users subquery
            projects_sq: Total projects subquery
            cpu_sq: CPU stats subquery
            gpu_sq: GPU stats subquery

        Returns:
            List of formatted result dictionaries with usage history data
        """
        query = self.session.query(
            users_sq.c.period,
            users_sq.c.total_users,
            projects_sq.c.total_projects,
            cpu_sq.c.cpu_users,
            cpu_sq.c.cpu_projects,
            cpu_sq.c.cpu_jobs,
            cpu_sq.c.cpu_hours,
            gpu_sq.c.gpu_users,
            gpu_sq.c.gpu_projects,
            gpu_sq.c.gpu_jobs,
            gpu_sq.c.gpu_hours
        ).join(
            projects_sq, users_sq.c.period == projects_sq.c.period
        ).outerjoin(
            cpu_sq, users_sq.c.period == cpu_sq.c.period
        ).outerjoin(
            gpu_sq, users_sq.c.period == gpu_sq.c.period
        ).order_by(users_sq.c.period)

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

    def usage_history(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get usage history by time period.

        This method coordinates 4 subqueries to gather comprehensive usage
        statistics per period (day, month, quarter, year):
        1. Total unique users across all queues
        2. Total unique projects across all queues
        3. CPU queue statistics (users, projects, jobs, hours)
        4. GPU queue statistics (users, projects, jobs, hours)

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            List of dicts with usage history statistics for each period.
            Each dict contains: Date, #-Users, #-Proj, #-CPU-Users, #-CPU-Proj,
            #-CPU-Jobs, #-CPU-Hrs, #-GPU-Users, #-GPU-Proj, #-GPU-Jobs, #-GPU-Hrs
        """
        from .query_builders import PeriodGrouper

        # Get period grouping expression
        period_col = PeriodGrouper.get_period_func(period, Job.end)

        # Build date filter once
        date_filter = self._build_date_filter(start, end)

        # Build 4 subqueries
        total_users_sq = self._usage_history_total_users(period_col, date_filter)
        total_projects_sq = self._usage_history_total_projects(period_col, date_filter)
        cpu_stats_sq = self._usage_history_resource_stats('cpu', period_col, date_filter)
        gpu_stats_sq = self._usage_history_resource_stats('gpu', period_col, date_filter)

        # Join and format results
        return self._join_usage_history_results(
            total_users_sq, total_projects_sq, cpu_stats_sq, gpu_stats_sq
        )

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

        Aggregates job counts and resource usage using pre-computed charging hours
        from the job_charges table.

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
        query = self.session.query(JobCharge).filter(
            and_(
                JobCharge.account == account,
                JobCharge.end >= datetime.combine(start, datetime.min.time()),
                JobCharge.end <= datetime.combine(end, datetime.max.time()),
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

        Aggregates job counts and resource usage using pre-computed charging hours
        from the job_charges table.

        Args:
            user: Username to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            Dict with aggregated metrics similar to usage_summary
        """
        query = self.session.query(JobCharge).filter(
            and_(
                JobCharge.user == user,
                JobCharge.end >= datetime.combine(start, datetime.min.time()),
                JobCharge.end <= datetime.combine(end, datetime.max.time()),
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

    def jobs_by_entity_period(
        self,
        primary_entity: str = "user",
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of jobs grouped by period and entity pair.

        Args:
            primary_entity: Primary grouping entity ('user' or 'account').
                           'user' gives Period > User > Account ordering.
                           'account' gives Period > Account > User ordering.
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            List of dicts with 'period', 'user', 'account', and 'job_count' keys,
            ordered by period, then primary_entity, then secondary_entity.
        """
        from .query_builders import PeriodGrouper

        period_func = PeriodGrouper.get_period_func(period, Job.end)

        if primary_entity == "user":
            order_fields = [Job.user, Job.account]
        else:  # "account"
            order_fields = [Job.account, Job.user]

        query = self.session.query(
            period_func.label("period"),
            Job.user,
            Job.account,
            func.count(Job.id).label("job_count")
        )

        query = self._apply_date_filter(query, start, end)

        results = query.group_by("period", Job.user, Job.account).order_by(
            "period", *order_fields
        ).all()

        return [
            {"period": row[0], "user": row[1], "account": row[2], "job_count": row[3]}
            for row in results
        ]

    def jobs_per_user_account_by_period(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of jobs per user per account by period in a date range.

        Delegates to jobs_by_entity_period() for backward compatibility.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            A list of dicts with 'period', 'user', 'account', and 'job_count' keys.
        """
        return self.jobs_by_entity_period(
            primary_entity="user",
            start=start,
            end=end,
            period=period
        )

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
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            A list of dicts with 'period' and 'project_count' keys.
        """
        from .query_builders import PeriodGrouper

        # Get period function
        period_func = PeriodGrouper.get_period_func(period, Job.end)

        query = self.session.query(
            period_func.label("period"),
            func.count(func.distinct(Job.account)).label("project_count")
        )

        query = self._apply_date_filter(query, start, end)

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
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            A list of dicts with 'period' and 'user_count' keys.
        """
        from .query_builders import PeriodGrouper

        # Get period function
        period_func = PeriodGrouper.get_period_func(period, Job.end)

        query = self.session.query(
            period_func.label("period"),
            func.count(func.distinct(Job.user)).label("user_count")
        )

        query = self._apply_date_filter(query, start, end)

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

    @classmethod
    def multi_machine_query(
        cls,
        machines: List[str],
        method_name: str,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Execute a query across multiple machines and aggregate results.

        This class method allows running the same query against multiple machine
        databases (casper, derecho) and combining the results with machine labels.

        Args:
            machines: List of machine names to query (e.g., ['casper', 'derecho'])
            method_name: Name of the JobQueries method to call
            **kwargs: Additional keyword arguments to pass to the query method

        Returns:
            List of result dictionaries, each tagged with a 'machine' field

        Example:
            >>> results = JobQueries.multi_machine_query(
            ...     machines=['casper', 'derecho'],
            ...     method_name='usage_by_group',
            ...     resource_type='cpu',
            ...     group_by='user',
            ...     start=date(2025, 11, 1),
            ...     end=date(2025, 11, 30)
            ... )
            >>> # Results contain data from both machines with 'machine' field
        """
        from .database import get_session

        all_results = []

        for machine in machines:
            session = get_session(machine)
            try:
                queries = cls(session, machine=machine)
                method = getattr(queries, method_name)
                results = method(**kwargs)

                # Tag each result with the machine name
                for row in results:
                    row['machine'] = machine

                all_results.extend(results)
            finally:
                session.close()

        return all_results


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
