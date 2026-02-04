"""Query building utilities for period grouping and aggregation.

This module provides helper classes that eliminate code duplication in
common query patterns like period-based grouping and resource type resolution.
"""

from typing import Tuple, Dict, Any, List
from sqlalchemy import func, cast, Integer, String


class PeriodGrouper:
    """Handles period-based grouping (day/month/quarter/year) for queries.

    This class provides utilities for:
    - Generating SQLAlchemy period grouping functions
    - Aggregating monthly data into quarterly summaries
    - Counting distinct entities by quarter

    Examples:
        >>> # Get period function for day grouping
        >>> period_func = PeriodGrouper.get_period_func('day', Job.end)

        >>> # Aggregate monthly job counts to quarterly
        >>> monthly = [
        ...     {'period': '2025-01', 'job_count': 100},
        ...     {'period': '2025-02', 'job_count': 150},
        ...     {'period': '2025-03', 'job_count': 200}
        ... ]
        >>> quarterly = PeriodGrouper.aggregate_quarters(monthly, 'job_count')
        >>> # Returns: [{'period': '2025-Q1', 'job_count': 450}]
    """

    # Period format strings for strftime
    PERIODS = {
        'day': '%Y-%m-%d',
        'month': '%Y-%m',
        'year': '%Y',
    }

    @staticmethod
    def get_period_func(period: str, date_column):
        """Get SQLAlchemy function for period grouping.

        Args:
            period: Grouping period ('day', 'month', 'quarter', or 'year')
            date_column: SQLAlchemy column to group by (e.g., Job.end)

        Returns:
            SQLAlchemy function expression for grouping

        Raises:
            ValueError: If period is not 'day', 'month', 'quarter', or 'year'

        Examples:
            >>> from qhist_db.models import Job
            >>> # Day grouping
            >>> func_day = PeriodGrouper.get_period_func('day', Job.end)
            >>> # Month grouping
            >>> func_month = PeriodGrouper.get_period_func('month', Job.end)
            >>> # Quarter grouping (returns expression like '2025-Q1')
            >>> func_quarter = PeriodGrouper.get_period_func('quarter', Job.end)
        """
        if period in PeriodGrouper.PERIODS:
            return func.strftime(PeriodGrouper.PERIODS[period], date_column)
        elif period == 'quarter':
            # Generate YYYY-Q# string using SQL expression
            # Formula: year + '-Q' + ((month - 1) // 3 + 1)
            quarter_num = (cast(func.strftime('%m', date_column), Integer) - 1) / 3 + 1
            # Note: / operator in SQLAlchemy with Integer cast typically results in integer division in SQLite
            # but to be safe and consistent with tests we rely on implicit or explicit behavior
            # The test showed (cast(...) - 1) // 3 + 1 works in Python-SQLAlchemy-SQLite mapping
            quarter_num = (cast(func.strftime('%m', date_column), Integer) - 1) // 3 + 1
            return func.strftime('%Y', date_column) + '-Q' + cast(quarter_num, String)
        else:
            raise ValueError(
                f"Invalid period: {period}. Must be 'day', 'month', 'quarter', or 'year'."
            )

    @staticmethod
    def aggregate_quarters(
        monthly_data: List[Dict],
        count_field: str,
        grouping_fields: List[str] = None
    ) -> List[Dict]:
        """Aggregate monthly data into quarters by summing counts.

        Converts monthly periods (YYYY-MM) to quarterly periods (YYYY-Q#)
        and sums the specified count field. Supports optional grouping by
        additional fields (e.g., user, account).

        Args:
            monthly_data: List of dicts with 'period' field in YYYY-MM format
            count_field: Name of field to sum (e.g., 'job_count')
            grouping_fields: Additional fields to group by (e.g., ['user', 'account'])
                           Default is None (no additional grouping).

        Returns:
            List of quarterly aggregated dicts with 'period' and count field.
            Results are sorted by period.

        Examples:
            >>> # Simple aggregation
            >>> monthly = [
            ...     {'period': '2025-01', 'job_count': 10},
            ...     {'period': '2025-02', 'job_count': 15},
            ...     {'period': '2025-03', 'job_count': 20},
            ... ]
            >>> PeriodGrouper.aggregate_quarters(monthly, 'job_count')
            [{'period': '2025-Q1', 'job_count': 45}]

            >>> # With grouping fields
            >>> monthly = [
            ...     {'period': '2025-01', 'user': 'alice', 'job_count': 10},
            ...     {'period': '2025-02', 'user': 'alice', 'job_count': 15},
            ... ]
            >>> PeriodGrouper.aggregate_quarters(
            ...     monthly, 'job_count', grouping_fields=['user']
            ... )
            [{'period': '2025-Q1', 'user': 'alice', 'job_count': 25}]
        """
        if not grouping_fields:
            grouping_fields = []

        quarterly = {}

        for row in monthly_data:
            if 'period' not in row:
                continue

            # Parse period and create quarter key
            year, month = row['period'].split('-')
            quarter = (int(month) - 1) // 3 + 1
            q_key = f"{year}-Q{quarter}"

            # Build composite key for grouping
            group_key = tuple([q_key] + [row.get(f) for f in grouping_fields])

            if group_key not in quarterly:
                result = {'period': q_key}
                for field in grouping_fields:
                    result[field] = row[field]
                result[count_field] = 0
                quarterly[group_key] = result

            quarterly[group_key][count_field] += row[count_field]

        return sorted(quarterly.values(), key=lambda x: x['period'])

    @staticmethod
    def aggregate_quarters_distinct(
        monthly_data: List[Tuple],
        entity_field: str
    ) -> List[Dict]:
        """Aggregate monthly distinct entities into quarterly counts.

        Used for counting unique users or projects per quarter.
        Unlike aggregate_quarters, this handles distinct entity counting
        by maintaining sets of entities per quarter.

        Args:
            monthly_data: List of (month_str, entity) tuples where
                         month_str is in YYYY-MM format
            entity_field: Name for the count field in output
                         (e.g., 'user_count', 'project_count')

        Returns:
            List of dicts with 'period' and entity count field.
            Results are sorted by period.

        Examples:
            >>> # Count unique users per quarter
            >>> monthly = [
            ...     ('2025-01', 'alice'),
            ...     ('2025-02', 'alice'),  # same user, shouldn't double count
            ...     ('2025-02', 'bob'),
            ...     ('2025-04', 'charlie'),
            ... ]
            >>> PeriodGrouper.aggregate_quarters_distinct(monthly, 'user_count')
            [
                {'period': '2025-Q1', 'user_count': 2},  # alice, bob
                {'period': '2025-Q2', 'user_count': 1}   # charlie
            ]
        """
        quarterly_sets = {}

        for month_str, entity in monthly_data:
            if not entity or not month_str:
                continue

            year, month = map(int, month_str.split('-'))
            quarter = (month - 1) // 3 + 1
            q_key = f"{year}-Q{quarter}"

            if q_key not in quarterly_sets:
                quarterly_sets[q_key] = set()
            quarterly_sets[q_key].add(entity)

        results = [
            {'period': key, entity_field: len(entities)}
            for key, entities in quarterly_sets.items()
        ]
        return sorted(results, key=lambda x: x['period'])


class ResourceTypeResolver:
    """Resolves resource types to queues and hour fields.

    This class handles the mapping from resource type strings ('cpu', 'gpu', 'all')
    to machine-specific queue names and appropriate charging hour fields.

    Examples:
        >>> from qhist_db.models import JobCharged
        >>> # Resolve CPU resources for Derecho
        >>> queues, hours = ResourceTypeResolver.resolve('cpu', 'derecho', JobCharged)
        >>> # queues = ['cpu', 'cpudev']
        >>> # hours = JobCharged.cpu_hours
    """

    @staticmethod
    def resolve(resource_type: str, machine: str, ChargeTable) -> Tuple:
        """Resolve resource type to queue IDs and hours field.

        Args:
            resource_type: Type of resources ('cpu', 'gpu', or 'all')
            machine: Machine name for queue lookup ('casper' or 'derecho')
            ChargeTable: Charge model class (JobCharge) for field access

        Returns:
            Tuple of (queue_ids, hours_field_expression)
            - queue_ids: List of queue IDs to filter by
            - hours_field_expression: SQLAlchemy expression for charging hours

        Raises:
            ValueError: If resource_type is not 'cpu', 'gpu', or 'all'

        Examples:
            >>> from qhist_db.models import JobCharge
            >>> # CPU resources
            >>> queues, hours = ResourceTypeResolver.resolve(
            ...     'cpu', 'derecho', JobCharge
            ... )
            >>> # queues = [queue_id_1, queue_id_2]
            >>> # hours = JobCharge.cpu_hours

            >>> # GPU resources
            >>> queues, hours = ResourceTypeResolver.resolve(
            ...     'gpu', 'derecho', JobCharge
            ... )
            >>> # queues = [queue_id_1, queue_id_2]
            >>> # hours = JobCharge.gpu_hours
        """
        from .queries import QueryConfig

        if resource_type == 'cpu':
            queues = QueryConfig.get_cpu_queues(machine)
            hours_field = ChargeTable.cpu_hours
        elif resource_type == 'gpu':
            queues = QueryConfig.get_gpu_queues(machine)
            hours_field = ChargeTable.gpu_hours
        elif resource_type == 'all':
            queues = QueryConfig.get_cpu_queues(machine) + QueryConfig.get_gpu_queues(machine)
            # For 'all', sum both cpu_hours and gpu_hours
            hours_field = func.coalesce(ChargeTable.cpu_hours, 0) + func.coalesce(
                ChargeTable.gpu_hours, 0
            )
        else:
            raise ValueError(
                f"Invalid resource_type: {resource_type}. " f"Must be 'cpu', 'gpu', or 'all'."
            )

        return queues, hours_field
