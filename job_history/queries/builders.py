"""Query building utilities for period grouping and aggregation.

This module provides helper classes that eliminate code duplication in
common query patterns like period-based grouping and resource type resolution.
"""

from typing import Tuple, Dict, Any, List
from sqlalchemy import func, cast, Integer, String, or_
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement


# ---------------------------------------------------------------------------
# Dialect-agnostic SQL expressions (compiled differently per database)
# ---------------------------------------------------------------------------

class _PeriodFunc(FunctionElement):
    """Date period formatting that compiles per dialect.

    SQLite:     strftime(sqlite_fmt, col)
    PostgreSQL: to_char(col, pg_fmt)
    """
    inherit_cache = True
    name = 'strftime'  # used by generic string representation

    def __init__(self, date_column, sqlite_fmt, pg_fmt):
        self.date_column = date_column
        self.sqlite_fmt = sqlite_fmt
        self.pg_fmt = pg_fmt
        super().__init__(date_column)


@compiles(_PeriodFunc)
def _compile_period_func_default(element, compiler, **kw):
    """Default (SQLite): strftime(fmt, col)"""
    return compiler.process(
        func.strftime(element.sqlite_fmt, element.date_column), **kw
    )


@compiles(_PeriodFunc, 'postgresql')
def _compile_period_func_pg(element, compiler, **kw):
    """PostgreSQL: to_char(col, fmt)"""
    return compiler.process(
        func.to_char(element.date_column, element.pg_fmt), **kw
    )


class _QuarterFunc(FunctionElement):
    """Quarter period extraction (YYYY-Q#) that compiles per dialect."""
    inherit_cache = True
    name = 'quarter_func'

    def __init__(self, date_column):
        self.date_column = date_column
        super().__init__(date_column)


@compiles(_QuarterFunc)
def _compile_quarter_default(element, compiler, **kw):
    """Default (SQLite): strftime('%Y', col) + '-Q' + CAST(...)"""
    date_col = element.date_column
    quarter_num = (cast(func.strftime('%m', date_col), Integer) - 1) // 3 + 1
    expr = func.strftime('%Y', date_col) + '-Q' + cast(quarter_num, String)
    return compiler.process(expr, **kw)


@compiles(_QuarterFunc, 'postgresql')
def _compile_quarter_pg(element, compiler, **kw):
    """PostgreSQL: to_char(col, 'YYYY') + '-Q' + CAST(...)"""
    date_col = element.date_column
    # EXTRACT returns float in PG; cast to Integer for integer division
    month_int = cast(func.extract('month', date_col), Integer)
    quarter_num = (month_int - 1) // 3 + 1
    expr = func.to_char(date_col, 'YYYY') + '-Q' + cast(quarter_num, String)
    return compiler.process(expr, **kw)


# ---------------------------------------------------------------------------
# Shell-glob → SQL pattern translation
# ---------------------------------------------------------------------------
#
# Deliberately NOT a @compiles FunctionElement like the classes above. Those
# rewrite the *operator* per dialect; a glob has to rewrite the *bound value*
# (glob → POSIX regex on PostgreSQL). A bindparam's value is not part of the
# compiled-SQL cache key, so two different globs would share one cache entry
# and a compiler-side value rewrite would silently serve the wrong pattern.
# Translating in Python at query-build time keeps each pattern its own
# correctly-cached bindparam.
#
# This is a deliberate near-duplicate of fs_scans.core.query_builder
# (glob_to_posix_regex / with_name_patterns). The two packages have no
# cross-imports and different substrates (raw SQL text there, ORM here), so
# they are kept independent. Two intentional divergences:
#   * glob_to_sql_like() escapes literal % and _ (fs_scans does not).
#   * The case-insensitive path uses one portable ColumnOperators.ilike()
#     instead of a hand-rolled LIKE/ILIKE dialect branch.

# Regex metacharacters to escape when translating a shell glob to a POSIX
# regex for PostgreSQL. ``*`` and ``?`` are handled separately as wildcards.
_REGEX_META = set(r".\+()[]{}^$|")

# LIKE/ILIKE escape character. ``!`` rather than the conventional backslash on
# purpose: SQLAlchemy's PostgreSQL compiler doubles backslashes in rendered
# literals while ``dialect._backslash_escapes`` is True (it is, until a live
# connection reports standard_conforming_strings=on), so ``ESCAPE '\'`` renders
# as ``ESCAPE '\\'`` when compiled offline. ``!`` renders byte-identically on
# both dialects, connected or not, which keeps the compiled-SQL tests
# trustworthy. ``!`` is never special in LIKE; a literal ``!`` is escaped below.
LIKE_ESCAPE = "!"


def glob_to_posix_regex(pattern: str) -> str:
    """Translate a shell-style glob into an anchored POSIX regex.

    Mirrors the wildcard subset SQLite ``GLOB`` supports: ``*`` → ``.*`` and
    ``?`` → ``.``. All other regex metacharacters are escaped, so ``[...]``
    character classes become literals.

    Note the resulting asymmetry: SQLite ``GLOB`` *does* honour ``[abc]``
    character classes, PostgreSQL (via this function) does not. A pattern using
    ``[...]`` therefore matches differently on the two backends. Inherited from
    ``fs_scans`` and kept for parity — ``[`` is rare enough in job names not to
    warrant a hand-written character-class translator.
    """
    out = ["^"]
    for ch in pattern:
        if ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        elif ch in _REGEX_META:
            out.append("\\" + ch)
        else:
            out.append(ch)
    out.append("$")
    return "".join(out)


def glob_to_sql_like(pattern: str) -> str:
    """Translate a shell-style glob into a LIKE pattern with escaped literals.

    ``*`` → ``%``, ``?`` → ``_``; a *literal* ``%``, ``_`` or ``!`` in the input
    is prefixed with :data:`LIKE_ESCAPE`. Must be paired with
    ``escape=LIKE_ESCAPE`` on the ``.like()`` / ``.ilike()`` call.

    The escaping is why this is not the bare
    ``pattern.replace("*", "%").replace("?", "_")`` that
    ``fs_scans.core.query_builder.with_name_patterns`` uses. Job names contain
    underscores constantly (``wrf_cycle_01``, ``cesm_b1850``); without the
    escape, ``-N 'cesm_b*' -i`` would also match ``cesmXb1850``.
    """
    out = []
    for ch in pattern:
        if ch == "*":
            out.append("%")
        elif ch == "?":
            out.append("_")
        elif ch in ("%", "_", LIKE_ESCAPE):
            out.append(LIKE_ESCAPE + ch)
        else:
            out.append(ch)
    return "".join(out)


def glob_match_clause(column, patterns, *, dialect: str, ignore_case: bool = False):
    """Build an OR'd shell-glob match against *column*.

    Returns a SQLAlchemy boolean expression, or ``None`` when *patterns* is
    empty or contains only empty strings (the caller then applies no filter).

    Three cases, not four. The case-*insensitive* path needs no dialect branch:
    ``ColumnOperators.ilike()`` already compiles to native ``ILIKE`` on
    PostgreSQL and ``lower(col) LIKE lower(?)`` on SQLite. Only the
    case-*sensitive* path has to branch, because there is no portable
    case-sensitive pattern operator — SQLite ``LIKE`` is case-insensitive for
    ASCII, so SQLite needs ``GLOB`` while PostgreSQL needs POSIX regex ``~``.

    NULL handling: all three operators yield NULL against a NULL column value,
    so rows with a NULL *column* are excluded by every branch. That is the
    intended semantics ("a job with no recorded name matches no name pattern")
    and needs no explicit guard — but it does mean a pattern can never be used
    to *find* unnamed rows.

    Args:
        column: SQLAlchemy column to match (e.g. ``Job.name``).
        patterns: Iterable of shell-glob patterns, OR'd together.
        dialect: ``session.get_bind().dialect.name`` — ``"postgresql"`` selects
            the regex branch, anything else the GLOB branch.
        ignore_case: Case-insensitive matching (portable LIKE/ILIKE path).
    """
    clauses = []
    for pattern in patterns:
        if not pattern:
            continue
        if ignore_case:
            clauses.append(
                column.ilike(glob_to_sql_like(pattern), escape=LIKE_ESCAPE)
            )
        elif dialect == "postgresql":
            # is_comparison=True types the operator as Boolean so it composes
            # inside or_() without a NullType coercion warning.
            clauses.append(
                column.op("~", is_comparison=True)(glob_to_posix_regex(pattern))
            )
        else:
            clauses.append(column.op("GLOB", is_comparison=True)(pattern))
    if not clauses:
        return None
    return or_(*clauses)


# ---------------------------------------------------------------------------

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

    _SQLITE_FORMATS = {
        'day': '%Y-%m-%d',
        'month': '%Y-%m',
        'year': '%Y',
    }
    _PG_FORMATS = {
        'day': 'YYYY-MM-DD',
        'month': 'YYYY-MM',
        'year': 'YYYY',
    }

    @staticmethod
    def get_period_func(period: str, date_column):
        """Get a dialect-agnostic SQLAlchemy expression for period grouping.

        The returned expression compiles to the correct SQL function for the
        connected database: strftime() for SQLite, to_char() for PostgreSQL.
        The dialect is resolved at query-execution time, not at build time,
        so this works correctly regardless of the configured backend.

        Args:
            period: Grouping period ('day', 'month', 'quarter', or 'year')
            date_column: SQLAlchemy column to group by (e.g., Job.end)

        Returns:
            SQLAlchemy expression for grouping (compiles per dialect)

        Raises:
            ValueError: If period is not 'day', 'month', 'quarter', or 'year'

        Examples:
            >>> from job_history.models import Job
            >>> func_day = PeriodGrouper.get_period_func('day', Job.end)
            >>> func_month = PeriodGrouper.get_period_func('month', Job.end)
            >>> func_quarter = PeriodGrouper.get_period_func('quarter', Job.end)
        """
        if period in PeriodGrouper._SQLITE_FORMATS:
            return _PeriodFunc(
                date_column,
                PeriodGrouper._SQLITE_FORMATS[period],
                PeriodGrouper._PG_FORMATS[period],
            )
        elif period == 'quarter':
            return _QuarterFunc(date_column)
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
        >>> from job_history.models import JobCharge
        >>> # Resolve CPU resources for Derecho
        >>> queues, hours = ResourceTypeResolver.resolve('cpu', 'derecho', JobCharge)
        >>> # queues = ['cpu', 'cpudev']
        >>> # hours = JobCharge.cpu_hours
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
            >>> from job_history.models import JobCharge
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
        from .jobs import QueryConfig

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
