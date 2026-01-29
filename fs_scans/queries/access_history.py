"""Access history analysis for filesystem scans.

This module provides functionality to analyze file access patterns over time,
generating histograms of data volume and file counts by access age.
"""

from datetime import datetime, timedelta
from collections import defaultdict
from typing import Any

from ..cli.common import format_size


class AccessHistogram:
    """Builds and formats access history histogram data."""

    # Time bucket definitions (in days from scan date)
    BUCKETS = [
        ("< 1 Month", 30),
        ("1 Month", 30 * 6),  # 1-6 months
        ("6 Months", 365),    # 6-12 months
        ("1 Year", 365 * 3),  # 1-3 years
        ("3 Years", 365 * 5), # 3-5 years
        ("5+ Years", None),   # 5+ years
    ]

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
    from ..queries.query_engine import resolve_path_to_id

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

    # Handle path prefixes
    cte_clause = ""
    join_clause = ""
    if path_prefixes:
        ancestor_ids = []
        for prefix in path_prefixes:
            ancestor_id = resolve_path_to_id(session, prefix)
            if ancestor_id is not None:
                idx = len(ancestor_ids)
                ancestor_ids.append(ancestor_id)
                params[f"ancestor_id_{idx}"] = ancestor_id

        if not ancestor_ids:
            return histogram  # No valid paths found

        ancestor_params = ", ".join(f":ancestor_id_{i}" for i in range(len(ancestor_ids)))
        cte_clause = f"""
            WITH RECURSIVE
            ancestors AS (
                SELECT dir_id FROM directories WHERE dir_id IN ({ancestor_params})
            ),
            descendants AS (
                SELECT dir_id FROM ancestors
                UNION ALL
                SELECT d.dir_id FROM directories d
                JOIN descendants p ON d.parent_id = p.dir_id
            )
        """
        join_clause = "JOIN descendants USING (dir_id)"

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
        {cte_clause}
        SELECT
            s.total_size_nr,
            s.file_count_nr,
            s.max_atime_nr,
            s.owner_uid
        FROM directories d
        JOIN directory_stats s USING (dir_id)
        {join_clause}
        WHERE {where_clause}
    """

    # Stream results in batches to avoid loading all into memory
    result_proxy = session.execute(text(query), params)

    # Process results incrementally using iterator
    batch_size = 10000
    while True:
        batch = result_proxy.fetchmany(batch_size)
        if not batch:
            break

        for row in batch:
            size_nr = row[0] or 0
            files_nr = row[1] or 0
            atime_nr = row[2]
            owner_uid = row[3]

            # Convert string datetime if needed
            if isinstance(atime_nr, str):
                try:
                    atime_nr = datetime.strptime(atime_nr.split()[0], "%Y-%m-%d")
                except (ValueError, AttributeError):
                    atime_nr = None

            histogram.add_directory(size_nr, files_nr, atime_nr, owner_uid)

    return histogram
