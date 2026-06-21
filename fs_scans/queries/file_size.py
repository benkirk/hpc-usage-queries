"""File size histogram analysis for filesystem scans.

This module provides functionality to analyze file size distributions,
using either pre-computed histograms or approximating from directory stats.
"""

from collections import defaultdict
from datetime import datetime

from sqlalchemy import text

from ..cli.common import format_size
from ..core.models import SIZE_BUCKETS, classify_size_bucket
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

    query = f"""
        {cte_clause}
        SELECT
            s.total_size_nr,
            s.file_count_nr,
            s.owner_uid
        {from_clause}
        WHERE {where_clause} AND s.file_count_nr > 0
    """

    # Structure: {bucket_label: {owner_uid: (file_count, total_size)}}
    histogram_data = defaultdict(lambda: defaultdict(lambda: [0, 0]))  # [file_count, total_size]

    # Stream results in batches to avoid loading all into memory
    result_proxy = session.execute(text(query), params)

    batch_size = 10000
    while True:
        batch = result_proxy.fetchmany(batch_size)
        if not batch:
            break

        for row in batch:
            total_size_nr = row[0] or 0
            file_count_nr = row[1] or 0
            uid = row[2]

            if file_count_nr == 0:
                continue

            # Approximate: assume uniform file sizes within directory
            avg_file_size = total_size_nr // file_count_nr

            # Classify into size bucket
            bucket_idx = classify_size_bucket(avg_file_size)
            bucket_label = SIZE_BUCKETS[bucket_idx][0]

            # Accumulate all files in this directory to the bucket
            histogram_data[bucket_label][uid][0] += file_count_nr
            histogram_data[bucket_label][uid][1] += total_size_nr

    # Convert to final format: {bucket_label: {owner_uid: (file_count, total_size)}}
    final_histogram = {}
    for bucket_label, owner_data in histogram_data.items():
        final_histogram[bucket_label] = {
            uid: (counts[0], counts[1])
            for uid, counts in owner_data.items()
        }

    return final_histogram
