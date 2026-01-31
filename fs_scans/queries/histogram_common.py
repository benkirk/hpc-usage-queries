"""Shared histogram utilities for access-time and file-size distributions.

This module provides common infrastructure for querying and displaying
histogram data from pre-computed ORM tables (AccessHistogram, SizeHistogram).
"""

from collections import defaultdict
from datetime import datetime

from sqlalchemy import text

from ..cli.common import format_size
from ..core.database import get_session
from ..core.models import ATIME_BUCKETS, SIZE_BUCKETS
from .query_engine import (
    get_scan_date,
    resolve_usernames_across_databases,
)


class HistogramData:
    """Generic histogram data structure for display.

    Stores histogram bucket data aggregated across filesystems and owners.
    """

    def __init__(self, bucket_labels: list[str], scan_date: datetime | None = None):
        """Initialize histogram with bucket labels.

        Args:
            bucket_labels: List of bucket label strings (e.g., "< 1 Month", "1-3 Months")
            scan_date: Reference scan date for display
        """
        self.scan_date = scan_date
        self.bucket_labels = bucket_labels

        # Structure: {bucket_label: {"data": int, "files": int, "owners": {uid: {"data": int, "files": int}}}}
        self.buckets = {
            label: {"data": 0, "files": 0, "owners": defaultdict(lambda: {"data": 0, "files": 0})}
            for label in bucket_labels
        }
        self.total_data = 0
        self.total_files = 0

    def add_bucket_data(
        self,
        bucket_label: str,
        owner_uid: int | None,
        file_count: int,
        total_size: int,
    ):
        """Add data to a specific bucket.

        Args:
            bucket_label: Bucket label to update
            owner_uid: Owner UID (or None for aggregate data)
            file_count: Number of files to add
            total_size: Total size in bytes to add
        """
        if bucket_label not in self.buckets:
            return  # Ignore invalid bucket labels

        bucket = self.buckets[bucket_label]
        bucket["data"] += total_size
        bucket["files"] += file_count

        # Track by owner if provided
        if owner_uid is not None and owner_uid >= 0:
            bucket["owners"][owner_uid]["data"] += total_size
            bucket["owners"][owner_uid]["files"] += file_count

        # Update totals
        self.total_data += total_size
        self.total_files += file_count

    def format_output(
        self,
        title: str,
        directory: str,
        username_map: dict[int, str],
        top_n: int = 10,
    ) -> str:
        """Format histogram as a readable text report.

        Args:
            title: Histogram title (e.g., "Access Time Distribution", "File Size Distribution")
            directory: Directory path being analyzed
            username_map: Mapping from UID to username
            top_n: Number of top users to show per bucket

        Returns:
            Formatted histogram report
        """
        lines = []
        lines.append("=" * 80)
        lines.append(f"[bold]{title}[/bold]")
        if self.scan_date:
            lines.append(f"[bold]Scan date:[/bold] {self.scan_date.strftime('%Y-%m-%d')}")
        lines.append(f"[bold]Directory:[/bold] {directory}")
        lines.append(f"[bold]Total Files:[/bold] {self._format_count(self.total_files)}")
        lines.append(f"[bold]Total Data:[/bold] {format_size(self.total_data)}")
        lines.append("")

        # Summary table - headers centered above columns
        lines.append(f"{'Bucket':<20} {'Data':^33} {'# Files':^25}")
        lines.append("=" * 80)

        for label in self.bucket_labels:
            bucket = self.buckets[label]
            data_pct = (bucket["data"] / self.total_data * 100) if self.total_data > 0 else 0
            files_pct = (bucket["files"] / self.total_files * 100) if self.total_files > 0 else 0

            # Use dim/muted color for percentages
            data_str = f"{format_size(bucket['data']):>15} [dim]({data_pct:5.2f}%)[/dim]"
            files_str = f"{self._format_count(bucket['files']):>15} [dim]({files_pct:5.2f}%)[/dim]"

            lines.append(f"{label:<20} {data_str:<33} {files_str}")

        lines.append("")

        # Per-user breakdown - headers centered above columns
        lines.append(f"{'User Data per Bucket':<20} {'Data':^33} {'# Files':^25}")
        lines.append("=" * 80)

        first_bucket = True
        for label in self.bucket_labels:
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
                reverse=True,
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

        lines.append("=" * 80)

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
            return f"{count / 1_000_000:.1f} M"
        elif count >= 1_000:
            return f"{count / 1_000:.1f} K"
        else:
            return f"{count:,}"


def query_histogram_orm(
    session,
    histogram_type: str,
    owner_uid: int | None = None,
) -> dict[str, dict[int, tuple[int, int]]] | None:
    """Query histogram data from ORM tables.

    Args:
        session: SQLAlchemy session
        histogram_type: "access" or "size"
        owner_uid: Optional owner UID filter

    Returns:
        Dictionary mapping bucket_label to {owner_uid: (file_count, total_size)}
        Returns None if histogram table doesn't exist
    """
    # Determine which table and bucket definitions to use
    if histogram_type == "access":
        table_name = "access_histogram"
        bucket_defs = ATIME_BUCKETS
    elif histogram_type == "size":
        table_name = "size_histogram"
        bucket_defs = SIZE_BUCKETS
    else:
        raise ValueError(f"Invalid histogram_type: {histogram_type}")

    # Check if table exists
    try:
        table_check = session.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name = :table_name"),
            {"table_name": table_name}
        ).fetchone()

        if not table_check:
            return None  # Table doesn't exist
    except Exception:
        return None

    # Build query with optional owner filter
    owner_filter = ""
    params = {}
    if owner_uid is not None:
        owner_filter = "WHERE owner_uid = :owner_uid"
        params["owner_uid"] = owner_uid

    query = f"""
        SELECT bucket_index, owner_uid, file_count, total_size
        FROM {table_name}
        {owner_filter}
        ORDER BY bucket_index, owner_uid
    """

    try:
        results = session.execute(text(query), params).fetchall()
    except Exception:
        return None  # Query failed

    # Structure: {bucket_label: {owner_uid: (file_count, total_size)}}
    histogram_data = defaultdict(dict)

    for bucket_idx, uid, file_count, total_size in results:
        # Map bucket index to label
        if 0 <= bucket_idx < len(bucket_defs):
            bucket_label = bucket_defs[bucket_idx][0]
            histogram_data[bucket_label][uid] = (file_count, total_size)

    return dict(histogram_data)


def aggregate_histograms_across_databases(
    filesystems: list[str],
    histogram_type: str,
    owner_uid: int | None = None,
) -> tuple[HistogramData, dict[int, str]]:
    """Aggregate histogram data across multiple databases.

    Args:
        filesystems: List of filesystem names to query
        histogram_type: "access" or "size"
        owner_uid: Optional owner UID filter

    Returns:
        Tuple of (HistogramData, username_map)
    """
    # Determine bucket labels based on histogram type
    if histogram_type == "access":
        bucket_labels = [label for label, _ in ATIME_BUCKETS]
    elif histogram_type == "size":
        bucket_labels = [label for label, _, _ in SIZE_BUCKETS]
    else:
        raise ValueError(f"Invalid histogram_type: {histogram_type}")

    # Collect scan dates
    scan_dates = []
    for fs in filesystems:
        session = get_session(fs)
        try:
            scan_date = get_scan_date(session)
            if scan_date:
                scan_dates.append(scan_date)
        finally:
            session.close()

    reference_scan_date = max(scan_dates) if scan_dates else None

    # Create combined histogram
    combined = HistogramData(bucket_labels, reference_scan_date)
    all_uids = set()

    # Query each database and merge
    for fs in filesystems:
        session = get_session(fs)
        try:
            fs_histogram = query_histogram_orm(session, histogram_type, owner_uid)

            # Skip if histogram table doesn't exist
            if fs_histogram is None:
                continue

            # Merge into combined histogram
            for bucket_label, owner_data in fs_histogram.items():
                for uid, (file_count, total_size) in owner_data.items():
                    combined.add_bucket_data(bucket_label, uid, file_count, total_size)
                    all_uids.add(uid)

        finally:
            session.close()

    # Resolve usernames
    username_map = resolve_usernames_across_databases(all_uids, filesystems)

    return combined, username_map
