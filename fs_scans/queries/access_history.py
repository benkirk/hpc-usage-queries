"""Access history analysis for filesystem scans.

This module provides functionality to analyze file access patterns over time,
generating histograms of data volume and file counts by access age.
"""

from datetime import datetime, timedelta
from collections import defaultdict
from typing import Any

from ..cli.common import format_size
from ..core.models import ATIME_BUCKETS


class AccessHistogram:
    """Builds and formats access history histogram data."""

    # Time bucket definitions (in days from scan date) - now uses 10 buckets from importer
    BUCKETS = ATIME_BUCKETS

    def __init__(self, scan_date: datetime):
        """Initialize histogram with scan date.

        Args:
            scan_date: Date of the filesystem scan
        """
        self.scan_date = scan_date
        self.buckets = {label: {"data": 0, "files": 0, "owners": defaultdict(lambda: {"data": 0, "files": 0})}
                       for label, _ in self.BUCKETS}
        self.total_data = 0
        self.total_files = 0

    def add_directory(self, size_nr: int, files_nr: int, atime_nr: datetime | None, owner_uid: int | None):
        """Add a directory's statistics to the appropriate time bucket.

        Args:
            size_nr: Non-recursive data size in bytes
            files_nr: Non-recursive file count
            atime_nr: Non-recursive access time (max atime of files in this dir)
            owner_uid: Owner UID (for single-owner dirs), or None for multi-owner
        """
        if size_nr == 0:
            return  # Skip empty directories

        self.total_data += size_nr
        self.total_files += files_nr

        # Determine which bucket this directory belongs to
        bucket_label = self._get_bucket(atime_nr)

        bucket = self.buckets[bucket_label]
        bucket["data"] += size_nr
        bucket["files"] += files_nr

        # Track by owner if single-owner directory
        if owner_uid is not None and owner_uid >= 0:
            bucket["owners"][owner_uid]["data"] += size_nr
            bucket["owners"][owner_uid]["files"] += files_nr

    def add_rollup(self, bucket_index: int, owner_uid: int | None, data: int, files: int):
        """Add a pre-aggregated ``(bucket, owner)`` rollup.

        The SQL-side equivalent of summing many :meth:`add_directory` calls for
        the same bucket+owner: :func:`compute_access_history` aggregates with
        ``GROUP BY`` so a large sub-path scope streams ~10 buckets x owners
        rows instead of every directory row. ``bucket_index`` is the position in
        :data:`ATIME_BUCKETS` (the DB CASE assigns it; see
        :func:`_atime_bucket_case_sql`).
        """
        if not data:
            return
        label = self.BUCKETS[int(bucket_index)][0]
        self.total_data += data
        self.total_files += files
        bucket = self.buckets[label]
        bucket["data"] += data
        bucket["files"] += files
        if owner_uid is not None and owner_uid >= 0:
            bucket["owners"][owner_uid]["data"] += data
            bucket["owners"][owner_uid]["files"] += files

    def _get_bucket(self, atime: datetime | None) -> str:
        """Determine which time bucket an access time falls into.

        Args:
            atime: Access time to categorize

        Returns:
            Bucket label (e.g., "< 1 Month", "1 Year")
        """
        if atime is None:
            return "5+ Years"  # No atime = very old

        # Calculate days since access (relative to scan date, not current time)
        # This is important because the database contains a static snapshot
        days_old = (self.scan_date - atime).days

        # Find appropriate bucket
        for label, threshold in self.BUCKETS:
            if threshold is None:
                return label  # Last bucket (5+ years)
            if days_old < threshold:
                return label

        return "5+ Years"  # Fallback

    def format_output(self, directory: str, username_map: dict[int, str], top_n: int = 10) -> str:
        """Format histogram as a readable text report.

        Args:
            directory: Directory path being analyzed
            username_map: Mapping from UID to username
            top_n: Number of top users to show per bucket

        Returns:
            Formatted histogram report
        """
        from ..cli.common import console

        lines = []
        lines.append("-" * 80)
        lines.append(f"[bold]Directory:[/bold] {directory}")
        lines.append(f"[bold]Scan date:[/bold] {self.scan_date.strftime('%Y-%m-%d')}")
        lines.append(f"[bold]Total Files:[/bold] {self._format_count(self.total_files)}")
        lines.append(f"[bold]Total Data:[/bold] {format_size(self.total_data)}")
        lines.append("")

        # Summary table - headers centered above columns
        lines.append(f"{'Last Accessed':<20} {'Data':^33} {'# Files':^25}")
        lines.append("-" * 80)

        for label, _ in self.BUCKETS:
            bucket = self.buckets[label]
            data_pct = (bucket["data"] / self.total_data * 100) if self.total_data > 0 else 0
            files_pct = (bucket["files"] / self.total_files * 100) if self.total_files > 0 else 0

            # Use dim/muted color for percentages
            data_str = f"{format_size(bucket['data']):>15} [dim]({data_pct:5.2f}%)[/dim]"
            files_str = f"{self._format_count(bucket['files']):>15} [dim]({files_pct:5.2f}%)[/dim]"

            lines.append(f"{label:<20} {data_str:<33} {files_str}")

        lines.append("")

        # Per-user breakdown - headers centered above columns
        lines.append(f"{'User Data Accessed':<20} {'Data':^33} {'# Files':^25}")
        lines.append("-" * 80)

        first_bucket = True
        for label, _ in self.BUCKETS:
            bucket = self.buckets[label]

            # Skip buckets with no data
            if bucket["data"] == 0:
                continue

            # Add blank line between buckets (except before first)
            if not first_bucket:
                lines.append("")
            first_bucket = False

            # Show bucket total on the label line
            bucket_data_str = f"{format_size(bucket['data']):>15}"
            bucket_files_str = f"{self._format_count(bucket['files']):>15}"
            lines.append(f"{label + ':':<20} {bucket_data_str:<33} {bucket_files_str}")

            # Skip showing users if no owner data
            if not bucket["owners"]:
                continue

            # Sort owners by data size descending
            sorted_owners = sorted(
                bucket["owners"].items(),
                key=lambda x: x[1]["data"],
                reverse=True
            )[:top_n]

            for idx, (uid, stats) in enumerate(sorted_owners, 1):
                username = username_map.get(uid, str(uid))

                # Calculate percentage within this bucket
                data_pct = (stats["data"] / bucket["data"] * 100) if bucket["data"] > 0 else 0
                files_pct = (stats["files"] / bucket["files"] * 100) if bucket["files"] > 0 else 0

                # Use dim/muted color for percentages
                data_str = f"{format_size(stats['data']):>15} [dim]({data_pct:5.2f}%)[/dim]"
                files_str = f"{self._format_count(stats['files']):>15} [dim]({files_pct:5.2f}%)[/dim]"

                lines.append(f"  {idx:2d}. {username:<14} {data_str:<33} {files_str}")

            if len(bucket["owners"]) > top_n:
                lines.append(f"  [...{len(bucket['owners']) - top_n} more users...]")

        lines.append("-" * 80)

        return "\n".join(lines)

    @staticmethod
    def _format_count(count: int) -> str:
        """Format file count with appropriate units.

        Args:
            count: Number of files

        Returns:
            Formatted string (e.g., "1.2 M", "543.2 K")
        """
        if count >= 1_000_000:
            return f"{count / 1_000_000:.2f} M"
        elif count >= 1_000:
            return f"{count / 1_000:.2f} K"
        else:
            return f"{count:,}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the histogram to a plain (JSON-friendly) dict.

        Owner sub-dicts keep integer UID keys in-process; the JSON exporter
        stringifies them on dump. Use :meth:`from_dict` to round-trip.
        """
        return {
            "scan_date": self.scan_date,
            "bucket_labels": [label for label, _ in self.BUCKETS],
            "total_data": self.total_data,
            "total_files": self.total_files,
            "buckets": {
                label: {
                    "data": bucket["data"],
                    "files": bucket["files"],
                    "owners": {
                        uid: {"data": stats["data"], "files": stats["files"]}
                        for uid, stats in bucket["owners"].items()
                    },
                }
                for label, bucket in self.buckets.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AccessHistogram":
        """Reconstruct an AccessHistogram from :meth:`to_dict` output."""
        hist = cls(data["scan_date"])
        hist.total_data = data["total_data"]
        hist.total_files = data["total_files"]
        for label, bucket in data["buckets"].items():
            if label not in hist.buckets:
                continue
            target = hist.buckets[label]
            target["data"] = bucket["data"]
            target["files"] = bucket["files"]
            for uid, stats in bucket["owners"].items():
                target["owners"][int(uid)]["data"] += stats["data"]
                target["owners"][int(uid)]["files"] += stats["files"]
        return hist


def _atime_bucket_case_sql(scan_date: datetime, col: str = "s.max_atime_nr"):
    """Build a SQL ``CASE`` mapping a directory's atime to its
    :data:`ATIME_BUCKETS` index, plus the bound cutoff params.

    Mirrors :meth:`AccessHistogram._get_bucket`: bucket ``i`` is the first whose
    day-threshold the age falls under (``days_old < threshold`` ⟺
    ``atime > scan_date - threshold``). NULL atime and ages past the last
    threshold fall in the final bucket. Doing this in SQL lets
    :func:`compute_access_history` ``GROUP BY`` the bucket instead of streaming
    every directory row into Python — the whole point of the slow-path
    optimization for large sub-path scopes.
    """
    last_idx = len(ATIME_BUCKETS) - 1
    whens, params = [], {}
    for i, (_, threshold) in enumerate(ATIME_BUCKETS):
        if threshold is None:
            continue
        key = f"atime_cut_{i}"
        # Bind as a string, matching DirectoryQueryBuilder.with_accessed_before
        # (proven on both sqlite + postgres; avoids the deprecated datetime
        # adapter). max_atime_nr stores microseconds, but these cutoffs never
        # sit on a sub-second boundary so lexicographic comparison is exact.
        params[key] = (scan_date - timedelta(days=threshold)).strftime("%Y-%m-%d %H:%M:%S")
        whens.append(f"            WHEN {col} > :{key} THEN {i}")
    case = (
        f"CASE WHEN {col} IS NULL THEN {last_idx}\n"
        + "\n".join(whens)
        + f"\n            ELSE {last_idx} END"
    )
    return case, params


def compute_access_history(
    session,
    scan_date: datetime,
    path_prefixes: list[str] | None = None,
    min_depth: int | None = None,
    max_depth: int | None = None,
) -> AccessHistogram:
    """Compute access history histogram from database using streaming.

    Args:
        session: SQLAlchemy database session
        scan_date: Date of the filesystem scan
        path_prefixes: Optional list of path prefixes to filter
        min_depth: Optional minimum depth filter
        max_depth: Optional maximum depth filter

    Returns:
        AccessHistogram with aggregated data
    """
    from sqlalchemy import text
    from ..queries.query_engine import (
        resolve_scope,
        _anc_predicate,
        _recursive_descendants_cte,
    )

    histogram = AccessHistogram(scan_date)

    # Build query to fetch all directories with their non-recursive stats
    conditions = []
    params = {}

    if min_depth is not None:
        conditions.append("d.depth >= :min_depth")
        params["min_depth"] = min_depth

    if max_depth is not None:
        conditions.append("d.depth <= :max_depth")
        params["max_depth"] = max_depth

    # Handle path prefixes: prefer the fast anc_d{k} lineage predicate, fall
    # back to the recursive descendants CTE for deep scopes / older .db files.
    cte_clause = ""
    join_clause = ""
    if path_prefixes:
        resolved, use_fast = resolve_scope(session, path_prefixes)
        if resolved is None:
            return histogram  # No valid paths found
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

    # Bucket + sum server-side: a sub-path scope can cover millions of
    # directories, so aggregate to a per-(bucket, owner) rollup in SQL and
    # stream ~10 buckets x owners rows rather than every directory row (which
    # blows the statement_timeout on large scopes). ``total_size_nr > 0``
    # mirrors add_directory()'s "skip empty directories".
    bucket_case, bucket_params = _atime_bucket_case_sql(scan_date)
    params.update(bucket_params)
    query = f"""
        {cte_clause}
        SELECT
            {bucket_case} AS bucket_index,
            s.owner_uid AS owner_uid,
            SUM(s.total_size_nr) AS total_size,
            SUM(s.file_count_nr) AS file_count
        {from_clause}
        WHERE {where_clause} AND s.total_size_nr > 0
        GROUP BY 1, 2
    """

    # The result is already small (bucketed), but stream in batches anyway.
    result_proxy = session.execute(text(query), params)
    batch_size = 10000
    while True:
        batch = result_proxy.fetchmany(batch_size)
        if not batch:
            break
        for row in batch:
            histogram.add_rollup(row[0], row[1], row[2] or 0, row[3] or 0)

    return histogram


def query_access_histogram_fast(
    session,
    owner_uid: int | None = None,
) -> AccessHistogram:
    """Query pre-computed access histogram from ORM tables (fast path).

    This function uses the AccessHistogram ORM table populated during import
    for instant query performance.

    Args:
        session: SQLAlchemy database session
        owner_uid: Optional owner UID filter

    Returns:
        AccessHistogram with data from ORM tables
    """
    from sqlalchemy import text
    from ..queries.query_engine import get_scan_date

    # Get scan date for histogram initialization
    scan_date = get_scan_date(session)
    if not scan_date:
        # Fallback to current date if scan date not found
        scan_date = datetime.now()

    histogram = AccessHistogram(scan_date)

    # Build query with optional owner filter
    owner_filter = ""
    params = {}
    if owner_uid is not None:
        owner_filter = "WHERE owner_uid = :owner_uid"
        params["owner_uid"] = owner_uid

    query = f"""
        SELECT bucket_index, owner_uid, file_count, total_size
        FROM access_histogram
        {owner_filter}
        ORDER BY bucket_index, owner_uid
    """

    results = session.execute(text(query), params).fetchall()

    # Populate histogram from ORM data
    for bucket_idx, uid, file_count, total_size in results:
        # Map bucket index to label
        if 0 <= bucket_idx < len(ATIME_BUCKETS):
            bucket_label = ATIME_BUCKETS[bucket_idx][0]

            # Add to appropriate bucket
            bucket = histogram.buckets[bucket_label]
            bucket["data"] += total_size
            bucket["files"] += file_count

            # Track by owner
            if uid is not None and uid >= 0:
                bucket["owners"][uid]["data"] += total_size
                bucket["owners"][uid]["files"] += file_count

            # Update totals
            histogram.total_data += total_size
            histogram.total_files += file_count

    return histogram
