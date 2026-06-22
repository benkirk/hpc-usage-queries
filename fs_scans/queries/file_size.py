"""File size histogram analysis for filesystem scans.

This module provides functionality to analyze file size distributions,
using either pre-computed histograms or approximating from directory stats.
"""

from collections import defaultdict
from datetime import datetime

from sqlalchemy import text

from ..cli.common import format_size
from ..core.models import SIZE_BUCKETS
from ..queries.query_engine import (
    get_scan_date,
    resolve_scope,
    _anc_predicate,
    _recursive_descendants_cte,
)


def query_size_histogram_fast(
    session,
    owner_uid: int | None = None,
):
    """Query pre-computed size histogram from ORM tables (fast path).

    This function uses the SizeHistogram ORM table populated during import
    for instant query performance.

    Args:
        session: SQLAlchemy database session
        owner_uid: Optional owner UID filter

    Returns:
        Dictionary with histogram data structure compatible with HistogramData
    """
    # Get scan date for reference
    scan_date = get_scan_date(session)
    if not scan_date:
        scan_date = datetime.now()

    # Build query with optional owner filter
    owner_filter = ""
    params = {}
    if owner_uid is not None:
        owner_filter = "WHERE owner_uid = :owner_uid"
        params["owner_uid"] = owner_uid

    query = f"""
        SELECT bucket_index, owner_uid, file_count, total_size
        FROM size_histogram
        {owner_filter}
        ORDER BY bucket_index, owner_uid
    """

    results = session.execute(text(query), params).fetchall()

    # Structure: {bucket_label: {owner_uid: (file_count, total_size)}}
    histogram_data = defaultdict(dict)

    for bucket_idx, uid, file_count, total_size in results:
        # Map bucket index to label
        if 0 <= bucket_idx < len(SIZE_BUCKETS):
            bucket_label = SIZE_BUCKETS[bucket_idx][0]
            histogram_data[bucket_label][uid] = (file_count, total_size)

    return dict(histogram_data)


def _size_bucket_case_sql(col_avg: str = "COALESCE(s.total_size_nr, 0) / s.file_count_nr"):
    """Build a SQL ``CASE`` mapping a directory's average file size to its
    :data:`SIZE_BUCKETS` index, plus the bound threshold params.

    Mirrors :func:`classify_size_bucket` on the integer-floored average
    (``total_size_nr // file_count_nr``): bucket ``i`` is the first whose
    ``max_size`` the average is under; averages past the last bound fall in the
    final bucket. Dividing (rather than comparing ``total_size_nr`` to
    ``max_size * file_count_nr``) avoids BIGINT overflow on directories with
    very large file counts. Doing this in SQL lets the caller ``GROUP BY`` the
    bucket instead of streaming every directory row into Python.
    """
    last_idx = len(SIZE_BUCKETS) - 1
    whens, params = [], {}
    for i, (_, _min, _max) in enumerate(SIZE_BUCKETS):
        if _max is None:
            continue
        key = f"size_cut_{i}"
        params[key] = _max
        whens.append(f"            WHEN {col_avg} < :{key} THEN {i}")
    case = "CASE\n" + "\n".join(whens) + f"\n            ELSE {last_idx} END"
    return case, params


def compute_size_histogram_from_directory_stats(
    session,
    scan_date: datetime,
    path_prefixes: list[str] | None = None,
    min_depth: int | None = None,
    max_depth: int | None = None,
    owner_uid: int | None = None,
):
    """Compute approximate size histogram from directory_stats (fallback for path filters).

    This approximates file size distribution by assuming uniform file sizes within
    each directory (using average file size). This works well for homogeneous data
    but is less accurate for mixed directories.

    Args:
        session: SQLAlchemy database session
        scan_date: Scan date for reference
        path_prefixes: Optional list of path prefixes to filter
        min_depth: Optional minimum depth filter
        max_depth: Optional maximum depth filter
        owner_uid: Optional owner UID filter

    Returns:
        Dictionary with histogram data structure compatible with HistogramData
    """
    # Build query to fetch all directories with their non-recursive stats
    conditions = []
    params = {}

    if min_depth is not None:
        conditions.append("d.depth >= :min_depth")
        params["min_depth"] = min_depth

    if max_depth is not None:
        conditions.append("d.depth <= :max_depth")
        params["max_depth"] = max_depth

    if owner_uid is not None:
        conditions.append("s.owner_uid = :owner_uid")
        params["owner_uid"] = owner_uid

    # Handle path prefixes: prefer the fast anc_d{k} lineage predicate, fall
    # back to the recursive descendants CTE for deep scopes / older .db files.
    cte_clause = ""
    join_clause = ""
    if path_prefixes:
        resolved, use_fast = resolve_scope(session, path_prefixes)
        if resolved is None:
            # No valid paths found, return empty histogram
            return {}
        if use_fast:
            conditions.append(_anc_predicate(resolved, params, alias="s"))
        else:
            cte_clause, join_clause = _recursive_descendants_cte(
                [rid for rid, _ in resolved], params
            )

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # The directories join is only needed for depth filters or the recursive
    # CTE; the fast path streams directly from directory_stats.
    if min_depth is not None or max_depth is not None or cte_clause:
        from_clause = (
            "FROM directories d\n        JOIN directory_stats s USING (dir_id)\n"
            f"        {join_clause}"
        )
    else:
        from_clause = "FROM directory_stats s"

    # Bucket each directory by its average file size and SUM server-side: a
    # sub-path scope can cover millions of directories, so aggregate to a
    # per-(bucket, owner) rollup in SQL and stream ~10 buckets x owners rows
    # rather than every directory row (which blows the statement_timeout on
    # large scopes). The CASE mirrors classify_size_bucket on the floored avg.
    bucket_case, bucket_params = _size_bucket_case_sql()
    params.update(bucket_params)
    query = f"""
        {cte_clause}
        SELECT
            {bucket_case} AS bucket_index,
            s.owner_uid AS owner_uid,
            SUM(s.file_count_nr) AS file_count,
            SUM(COALESCE(s.total_size_nr, 0)) AS total_size
        {from_clause}
        WHERE {where_clause} AND s.file_count_nr > 0
        GROUP BY 1, 2
    """

    # Final format: {bucket_label: {owner_uid: (file_count, total_size)}}.
    # int() coerces Postgres SUM()'s Decimal so downstream float arithmetic in
    # the webapp chart never hits float += Decimal.
    final_histogram: dict = defaultdict(dict)
    result_proxy = session.execute(text(query), params)
    batch_size = 10000
    while True:
        batch = result_proxy.fetchmany(batch_size)
        if not batch:
            break
        for row in batch:
            bucket_label = SIZE_BUCKETS[int(row[0])][0]
            final_histogram[bucket_label][row[1]] = (int(row[2] or 0), int(row[3] or 0))

    return dict(final_histogram)
