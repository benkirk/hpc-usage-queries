#!/usr/bin/env python3
"""
GPFS Scan Database Query CLI

Query directory statistics from the SQLite database.
Supports filtering by depth, owner, path prefix, and sorting.
"""

import os
import pwd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import click
from rich.table import Table
from sqlalchemy import text

from .cli_common import console, format_datetime, format_size, parse_date_arg
from .database import get_data_dir, get_data_dir_info, get_db_path, get_session, set_data_dir

import re as _re

_SIZE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "pb": 1000**5,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "pib": 1024**5,
    # Shorthand: K/M/G/T/P → binary (filesystem convention)
    "k": 1024,
    "m": 1024**2,
    "g": 1024**3,
    "t": 1024**4,
    "p": 1024**5,
}


def parse_size(value: str) -> int:
    """Parse a size string to bytes.

    Accepts plain integers (bytes), SI units (KB, MB, GB, TB, PB),
    binary units (KiB, MiB, GiB, TiB, PiB), or shorthand (K, M, G, T, P)
    where shorthand maps to binary (1024-based).

    Examples:
        "1GiB"  -> 1073741824
        "500MB" -> 500000000
        "2T"    -> 2199023255552
        "0"     -> 0
    """
    value = value.strip()
    match = _re.match(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]*)$", value)
    if not match:
        raise click.BadParameter(f"Invalid size: {value}")
    num_str, unit = match.groups()
    num = float(num_str)
    if not unit:
        return int(num)
    unit_lower = unit.lower()
    if unit_lower not in _SIZE_UNITS:
        raise click.BadParameter(f"Unknown size unit: {unit}")
    return int(num * _SIZE_UNITS[unit_lower])


_COUNT_UNITS = {
    "k": 1000,
    "m": 1000_000,
}


def parse_file_count(value: str) -> int:
    """Parse a file count string to an integer.

    Accepts plain integers or shorthand multipliers: K (×1000), M (×1000000).

    Examples:
        "1K"  -> 1000
        "500" -> 500
        "10M" -> 10000000
    """
    value = value.strip()
    match = _re.match(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]*)$", value)
    if not match:
        raise click.BadParameter(f"Invalid file count: {value}")
    num_str, unit = match.groups()
    num = float(num_str)
    if not unit:
        return int(num)
    unit_lower = unit.lower()
    if unit_lower not in _COUNT_UNITS:
        raise click.BadParameter(f"Unknown file count unit: {unit}")
    return int(num * _COUNT_UNITS[unit_lower])


# Known mount point prefixes to strip from user-provided paths
_MOUNT_POINT_PREFIXES = [
    "/glade/campaign",
    "/gpfs/csfs1",
    "/glade/derecho/scratch",
    "/lustre/desc1",
]


def normalize_path(path: str) -> str:
    """Strip known mount point prefixes from a path.

    Allows users to provide full filesystem paths (e.g., /glade/campaign/cisl)
    which will be normalized to database paths (e.g., /cisl).

    Args:
        path: User-provided path (may include mount point prefix)

    Returns:
        Normalized path with mount point prefix stripped if present
    """
    path = path.rstrip("/")
    for prefix in _MOUNT_POINT_PREFIXES:
        if path.startswith(prefix):
            # Strip prefix and ensure leading slash
            stripped = path[len(prefix):]
            return stripped if stripped.startswith("/") else "/" + stripped
    return path


def get_all_filesystems() -> list[str]:
    """Discover all available filesystem databases.

    Searches in the configured data directory (via get_data_dir()).

    Returns:
        List of filesystem names (e.g., ['asp', 'cisl', 'eol', 'hao'])
    """
    data_dir = get_data_dir()
    db_files = data_dir.glob("*.db")
    return sorted([f.stem for f in db_files])


def get_scan_date(session) -> datetime | None:
    """Get the scan timestamp from ScanMetadata.

    Returns:
        The scan_timestamp from the most recent scan metadata entry, or None if not found.
    """
    result = session.execute(
        text("SELECT scan_timestamp FROM scan_metadata ORDER BY scan_id DESC LIMIT 1")
    ).fetchone()
    if not result or not result[0]:
        return None
    # Handle both datetime objects and string formats
    val = result[0]
    if isinstance(val, datetime):
        return val
    # Parse string format "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
    try:
        return datetime.strptime(str(val).split()[0], "%Y-%m-%d")
    except ValueError:
        return None


def resolve_path_to_id(session, path: str) -> int | None:
    """
    Resolve a full path to its dir_id in a single query.

    Uses dynamic N-way joins to walk the entire path in one database round-trip
    instead of N sequential queries (one per path component).

    Args:
        session: SQLAlchemy session
        path: Full path like /gpfs/csfs1/asp/username

    Returns:
        dir_id or None if not found
    """
    # Normalize path - remove trailing slash, handle leading slash
    path = path.rstrip("/")
    if not path:
        return None

    # Split path into components
    components = [p for p in path.split("/") if p]
    if not components:
        return None

    # Build single query with N-way joins (1 round-trip instead of N)
    # SELECT dN.dir_id FROM directories d1
    # JOIN directories d2 ON d2.parent_id = d1.dir_id AND d2.name = :c2
    # ...
    # WHERE d1.parent_id IS NULL AND d1.name = :c1
    n = len(components)
    params = {f"c{i+1}": comp for i, comp in enumerate(components)}

    if n == 1:
        # Single component - simple query
        query = """
            SELECT dir_id FROM directories
            WHERE parent_id IS NULL AND name = :c1
        """
    else:
        # Build N-way join query
        joins = []
        for i in range(2, n + 1):
            joins.append(
                f"JOIN directories d{i} ON d{i}.parent_id = d{i-1}.dir_id AND d{i}.name = :c{i}"
            )

        query = f"""
            SELECT d{n}.dir_id FROM directories d1
            {' '.join(joins)}
            WHERE d1.parent_id IS NULL AND d1.name = :c1
        """

    result = session.execute(text(query), params).fetchone()
    return result[0] if result else None


def get_full_path(session, dir_id: int) -> str:
    """
    Reconstruct full path for a directory using recursive CTE.

    Args:
        session: SQLAlchemy session
        dir_id: Directory ID

    Returns:
        Full path string
    """
    result = session.execute(
        text("""
            WITH RECURSIVE path_cte AS (
                SELECT dir_id, parent_id, name, name as full_path
                FROM directories WHERE dir_id = :dir_id
                UNION ALL
                SELECT p.dir_id, p.parent_id, p.name, p.name || '/' || c.full_path
                FROM directories p
                JOIN path_cte c ON c.parent_id = p.dir_id
            )
            SELECT full_path FROM path_cte WHERE parent_id IS NULL
        """),
        {"dir_id": dir_id},
    ).fetchone()

    if result:
        return "/" + result[0]
    return f"<unknown:{dir_id}>"


def get_full_paths_batch(session, dir_ids: list[int]) -> dict[int, str]:
    """
    Compute full paths for multiple directories in a single recursive CTE.

    This is much more efficient than calling get_full_path() per directory
    when dealing with multiple results (N queries -> 1 query).

    Args:
        session: SQLAlchemy session
        dir_ids: List of directory IDs to resolve

    Returns:
        Dictionary mapping dir_id to full path string
    """
    if not dir_ids:
        return {}

    # SQLite doesn't support array parameters directly, so we build IN clause
    # with positional parameters
    placeholders = ", ".join(f":id_{i}" for i in range(len(dir_ids)))
    params = {f"id_{i}": did for i, did in enumerate(dir_ids)}

    result = session.execute(
        text(f"""
            WITH RECURSIVE path_cte AS (
                SELECT dir_id, parent_id, name, dir_id as origin_id, name as path_segment
                FROM directories WHERE dir_id IN ({placeholders})
                UNION ALL
                SELECT p.dir_id, p.parent_id, p.name, c.origin_id, p.name || '/' || c.path_segment
                FROM directories p
                JOIN path_cte c ON c.parent_id = p.dir_id
            )
            SELECT origin_id, '/' || path_segment as full_path
            FROM path_cte WHERE parent_id IS NULL
        """),
        params,
    )

    return {row[0]: row[1] for row in result}


def query_directories(
    session,
    min_depth: int | None = None,
    max_depth: int | None = None,
    single_owner: bool = False,
    owner_id: int | None = None,
    path_prefixes: list[str] | None = None,
    exclude_paths: list[str] | None = None,
    sort_by: str = "size_r",
    limit: int | None = None,
    accessed_before: datetime | None = None,
    accessed_after: datetime | None = None,
    leaves_only: bool = False,
    name_patterns: list[str] | None = None,
    name_pattern_ignorecase: bool = False,
    min_size: int | None = None,
    max_size: int | None = None,
    min_files: int | None = None,
    max_files: int | None = None,
) -> list[dict]:
    """
    Query directories with optional filters.

    Args:
        session: SQLAlchemy session
        min_depth: Minimum path depth filter
        max_depth: Maximum path depth filter
        single_owner: Only show single-owner directories
        owner_id: Filter to specific owner UID
        path_prefixes: Filter to paths under these prefixes (OR'd together)
        exclude_paths: List of paths to exclude (with descendants)
        sort_by: Sort field (size_r, size_nr, files_r, files_nr, atime_r, path)
        limit: Maximum results to return
        accessed_before: Filter to directories with max_atime_r before this date
        accessed_after: Filter to directories with max_atime_r after this date
        leaves_only: Only show directories with no subdirectories
        name_patterns: List of GLOB patterns to filter directory names (OR'd together)
        name_pattern_ignorecase: If True, name pattern matching is case-insensitive
        min_size: Minimum total_size_r in bytes
        max_size: Maximum total_size_r in bytes
        min_files: Minimum file_count_r
        max_files: Maximum file_count_r

    Returns:
        List of directory dictionaries with stats
    """
    # Build base query
    conditions = []
    params = {}

    if min_depth is not None:
        conditions.append("d.depth >= :min_depth")
        params["min_depth"] = min_depth

    if max_depth is not None:
        conditions.append("d.depth <= :max_depth")
        params["max_depth"] = max_depth

    if single_owner:
        conditions.append("s.owner_uid IS NOT NULL AND s.owner_uid != -1")

    if owner_id is not None:
        conditions.append("s.owner_uid = :owner_id")
        params["owner_id"] = owner_id

    if accessed_before is not None:
        conditions.append("s.max_atime_r < :accessed_before")
        params["accessed_before"] = accessed_before.strftime("%Y-%m-%d %H:%M:%S")

    if accessed_after is not None:
        conditions.append("s.max_atime_r > :accessed_after")
        params["accessed_after"] = accessed_after.strftime("%Y-%m-%d %H:%M:%S")

    if leaves_only:
        conditions.append(
            "NOT EXISTS (SELECT 1 FROM directories child WHERE child.parent_id = d.dir_id)"
        )

    if name_patterns:
        # Build OR clause for multiple patterns
        pattern_conditions = []
        for i, pattern in enumerate(name_patterns):
            param_name = f"name_pattern_{i}"
            if name_pattern_ignorecase:
                # Convert GLOB pattern to LIKE pattern (* -> %, ? -> _)
                # LIKE is case-insensitive by default in SQLite
                like_pattern = pattern.replace("*", "%").replace("?", "_")
                pattern_conditions.append(f"d.name LIKE :{param_name}")
                params[param_name] = like_pattern
            else:
                pattern_conditions.append(f"d.name GLOB :{param_name}")
                params[param_name] = pattern
        conditions.append(f"({' OR '.join(pattern_conditions)})")

    if min_size is not None:
        conditions.append("s.total_size_r >= :min_size")
        params["min_size"] = min_size

    if max_size is not None:
        conditions.append("s.total_size_r <= :max_size")
        params["max_size"] = max_size

    if min_files is not None:
        conditions.append("s.file_count_r >= :min_files")
        params["min_files"] = min_files

    if max_files is not None:
        conditions.append("s.file_count_r <= :max_files")
        params["max_files"] = max_files

    # Build CTEs for path filtering
    ctes = []
    use_descendants_cte = False

    # Handle path prefixes - find ancestors and filter descendants (OR'd together)
    if path_prefixes:
        ancestor_ids = []
        for prefix in path_prefixes:
            ancestor_id = resolve_path_to_id(session, prefix)
            if ancestor_id is not None:
                idx = len(ancestor_ids)
                ancestor_ids.append(ancestor_id)
                params[f"ancestor_id_{idx}"] = ancestor_id

        if not ancestor_ids:
            return []  # No valid paths found

        # Build IN clause for multiple ancestors
        ancestor_params = ", ".join(f":ancestor_id_{i}" for i in range(len(ancestor_ids)))
        ctes.append(f"""
            ancestors AS (
                SELECT dir_id FROM directories WHERE dir_id IN ({ancestor_params})
            ),
            descendants AS (
                SELECT dir_id FROM ancestors
                UNION ALL
                SELECT d.dir_id FROM directories d
                JOIN descendants p ON d.parent_id = p.dir_id
            )""")
        use_descendants_cte = True

    # NOTE: exclude_paths filtering is done post-query via path prefix matching
    # This is much more efficient than building a CTE of all descendants
    # (which would be O(excluded subtree size) vs O(results × excludes))

    # Build the query with optional CTEs
    if ctes:
        cte_clause = "WITH RECURSIVE " + ",".join(ctes)
    else:
        cte_clause = ""

    # Build SELECT clause - use descendants CTE if path_prefixes was set
    if use_descendants_cte:
        select_clause = """
            SELECT d.dir_id, d.parent_id, d.name, d.depth,
                   s.file_count_nr, s.total_size_nr, s.max_atime_nr,
                   s.file_count_r, s.total_size_r, s.max_atime_r,
                   s.owner_uid
            FROM descendants
            JOIN directories d USING (dir_id)
            JOIN directory_stats s USING (dir_id)
        """
    else:
        select_clause = """
            SELECT d.dir_id, d.parent_id, d.name, d.depth,
                   s.file_count_nr, s.total_size_nr, s.max_atime_nr,
                   s.file_count_r, s.total_size_r, s.max_atime_r,
                   s.owner_uid
            FROM directories d
            JOIN directory_stats s USING (dir_id)
        """

    base_query = cte_clause + select_clause

    # Add WHERE clause if we have conditions
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    # Add ORDER BY
    sort_map = {
        "size_r": "s.total_size_r DESC",
        "size_nr": "s.total_size_nr DESC",
        "files_r": "s.file_count_r DESC",
        "files_nr": "s.file_count_nr DESC",
        "atime_r": "s.max_atime_r DESC",
        "path": "d.depth ASC, d.name ASC",
        "depth": "d.depth DESC",
    }
    order_clause = sort_map.get(sort_by, sort_map["size_r"])
    base_query += f" ORDER BY {order_clause}"

    # Add LIMIT
    if limit:
        base_query += " LIMIT :limit"
        params["limit"] = limit

    # Execute query
    results = session.execute(text(base_query), params).fetchall()

    # Batch fetch all full paths in single query (N queries -> 1)
    dir_ids = [row[0] for row in results]
    path_map = get_full_paths_batch(session, dir_ids)

    # Normalize exclude paths for prefix matching
    normalized_excludes = None
    if exclude_paths:
        normalized_excludes = [p.rstrip("/") for p in exclude_paths]

    # Convert to dictionaries with full paths
    directories = []
    for row in results:
        dir_id = row[0]
        path = path_map.get(dir_id, f"<unknown:{dir_id}>")

        # Filter out excluded paths (path prefix matching)
        if normalized_excludes:
            excluded = False
            for excl in normalized_excludes:
                if path == excl or path.startswith(excl + "/"):
                    excluded = True
                    break
            if excluded:
                continue

        directories.append({
            "dir_id": dir_id,
            "path": path,
            "depth": row[3],
            "file_count_nr": row[4] or 0,
            "total_size_nr": row[5] or 0,
            "max_atime_nr": row[6],
            "file_count_r": row[7] or 0,
            "total_size_r": row[8] or 0,
            "max_atime_r": row[9],
            "owner_uid": row[10],
        })

    return directories


def print_results(
    directories: list[dict],
    verbose: bool = False,
    leaves_only: bool = False,
    username_map: dict[int, str] | None = None,
    show_total: bool = False,
) -> None:
    """Print directory results in a formatted table."""
    if not directories:
        console.print("[yellow]No directories found matching criteria.[/yellow]")
        return

    if username_map is None:
        username_map = {}

    table = Table(title=f"Directory Statistics ({len(directories)} results)")
    table.add_column("Directory", style="cyan", no_wrap=False)
    if verbose:
        table.add_column("Depth", justify="right")

    if leaves_only:
        # Simplified columns for leaf directories (R and NR are identical)
        table.add_column("Size", justify="right")
        table.add_column("Files", justify="right")
        table.add_column("Atime", justify="right")
    else:
        table.add_column("Size\n", justify="right")
        table.add_column("Size\n(NR)", justify="right")
        table.add_column("Files\n", justify="right")
        table.add_column("Files\n(NR)", justify="right")
        table.add_column("Atime\n", justify="right")
        table.add_column("Atime\n(NR)", justify="right")
    table.add_column("Owner", justify="right")

    # Track totals for summary row
    total_size_r = 0
    total_size_nr = 0
    total_files_r = 0
    total_files_nr = 0

    for i, d in enumerate(directories):
        uid = d["owner_uid"]
        if uid is not None and uid != -1:
            owner_display = username_map.get(uid, str(uid))
            owner_str = f"[green]{owner_display}[/green]"
        elif uid is None:
            owner_str = "[yellow]multiple[/yellow]"
        else:
            owner_str = "[dim]-[/dim]"

        row = [d["path"]]
        if verbose:
            row.append(str(d["depth"]))

        if leaves_only:
            row.extend([
                format_size(d["total_size_r"]),
                f"{d['file_count_r']:,}",
                format_datetime(d["max_atime_r"]),
                owner_str,
            ])
        else:
            row.extend([
                format_size(d["total_size_r"]),
                format_size(d["total_size_nr"]),
                f"{d['file_count_r']:,}",
                f"{d['file_count_nr']:,}",
                format_datetime(d["max_atime_r"]),
                format_datetime(d["max_atime_nr"]),
                owner_str,
            ])

        # Add separator line before totals row
        end_section = (i == len(directories) - 1) and len(directories) > 1 and show_total
        table.add_row(*row, end_section=end_section)

        # Accumulate totals
        total_size_r += d["total_size_r"]
        total_size_nr += d["total_size_nr"]
        total_files_r += d["file_count_r"]
        total_files_nr += d["file_count_nr"]

    # Add totals row if more than one directory and --show-total is enabled
    if len(directories) > 1 and show_total:
        row = ["[bold]Total:[/bold]"]
        if verbose:
            row.append("")  # Empty depth column

        if leaves_only:
            row.extend([
                f"[bold]{format_size(total_size_r)}[/bold]",
                f"[bold]{total_files_r:,}[/bold]",
                "",  # Empty atime
                "",  # Empty owner
            ])
        else:
            row.extend([
                f"[bold]{format_size(total_size_r)}[/bold]",
                f"[bold]{format_size(total_size_nr)}[/bold]",
                f"[bold]{total_files_r:,}[/bold]",
                f"[bold]{total_files_nr:,}[/bold]",
                "",  # Empty atime (R)
                "",  # Empty atime (NR)
                "",  # Empty owner
            ])
        table.add_row(*row)

    console.print(table)


def write_tsv(directories: list[dict], output_path: Path) -> None:
    """Write results to TSV file."""
    with open(output_path, "w") as f:
        # Header
        f.write(
            "directory\tdepth\t"
            "total_size_r\ttotal_size_nr\t"
            "file_count_r\tfile_count_nr\t"
            "max_atime_r\tmax_atime_nr\t"
            "owner_uid\n"
        )

        for d in directories:
            f.write(
                f"{d['path']}\t{d['depth']}\t"
                f"{d['total_size_r']}\t{d['total_size_nr']}\t"
                f"{d['file_count_r']}\t{d['file_count_nr']}\t"
                f"{format_datetime(d['max_atime_r'])}\t"
                f"{format_datetime(d['max_atime_nr'])}\t"
                f"{d['owner_uid']}\n"
            )

    console.print(f"[green]Results written to {output_path}[/green]")


def get_summary(session) -> dict:
    """Get summary statistics from the database."""
    result = session.execute(
        text("""
            SELECT
                COUNT(*) as dir_count,
                SUM(file_count_r) as total_files,
                MAX(total_size_r) as max_size,
                MAX(depth) as max_depth
            FROM directories d
            JOIN directory_stats s USING (dir_id)
            WHERE d.parent_id IS NULL
        """)
    ).fetchone()

    total_dirs = session.execute(
        text("SELECT COUNT(*) FROM directories")
    ).fetchone()[0]

    return {
        "total_directories": total_dirs,
        "root_directories": result[0],
        "total_files": result[1] or 0,
        "total_size": result[2] or 0,
        "max_depth": result[3] or 0,
    }


def query_owner_summary(
    session,
    min_depth: int | None = None,
    max_depth: int | None = None,
    path_prefixes: list[str] | None = None,
    limit: int | None = None,
    sort_by: str = "size",
) -> list[dict]:
    """
    Query per-owner aggregated statistics.

    Uses fast path (OwnerSummary table) when no filters are applied,
    otherwise computes dynamically from directory_stats.

    Args:
        session: SQLAlchemy session
        min_depth: Minimum path depth filter
        max_depth: Maximum path depth filter
        path_prefixes: Filter to paths under these prefixes (OR'd together)
        limit: Maximum results to return
        sort_by: Sort field (size, files, dirs)

    Returns:
        List of owner summary dictionaries
    """
    has_filters = any([min_depth, max_depth, path_prefixes])

    if not has_filters:
        # Fast path: use pre-computed OwnerSummary table
        # Check if the table exists and has data
        try:
            count = session.execute(
                text("SELECT COUNT(*) FROM owner_summary")
            ).scalar()
        except Exception:
            count = 0

        if count > 0:
            sort_map = {
                "size": "total_size DESC",
                "files": "total_files DESC",
                "dirs": "directory_count DESC",
            }
            order_clause = sort_map.get(sort_by, sort_map["size"])

            query = f"""
                SELECT owner_uid, total_size, total_files, directory_count
                FROM owner_summary
                ORDER BY {order_clause}
            """
            if limit:
                query += f" LIMIT {limit}"

            results = session.execute(text(query)).fetchall()
            return [
                {
                    "owner_uid": row[0],
                    "total_size": row[1] or 0,
                    "total_files": row[2] or 0,
                    "directory_count": row[3] or 0,
                }
                for row in results
            ]

    # Dynamic path: compute from directory_stats with filters
    conditions = ["s.owner_uid IS NOT NULL AND s.owner_uid >= 0"]
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
            return []  # No valid paths found

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

    sort_map = {
        "size": "total_size DESC",
        "files": "total_files DESC",
        "dirs": "directory_count DESC",
    }
    order_clause = sort_map.get(sort_by, sort_map["size"])

    where_clause = " AND ".join(conditions)

    query = f"""
        {cte_clause}
        SELECT
            s.owner_uid,
            SUM(s.total_size_nr) as total_size,
            SUM(s.file_count_nr) as total_files,
            COUNT(*) as directory_count
        FROM directories d
        JOIN directory_stats s USING (dir_id)
        {join_clause}
        WHERE {where_clause}
        GROUP BY s.owner_uid
        ORDER BY {order_clause}
    """
    if limit:
        query += f" LIMIT {limit}"

    results = session.execute(text(query), params).fetchall()
    return [
        {
            "owner_uid": row[0],
            "total_size": row[1] or 0,
            "total_files": row[2] or 0,
            "directory_count": row[3] or 0,
        }
        for row in results
    ]


def get_username_map(session, uids: list[int]) -> dict[int, str]:
    """
    Get username mappings for a list of UIDs from the user_info table.

    Falls back to pwd.getpwuid() for UIDs not in the table.

    Args:
        session: SQLAlchemy session
        uids: List of UIDs to resolve

    Returns:
        Dictionary mapping UID to username (or str(uid) if unknown)
    """
    if not uids:
        return {}

    result = {}

    # Try to get from user_info table first
    try:
        placeholders = ", ".join(f":uid_{i}" for i in range(len(uids)))
        params = {f"uid_{i}": uid for i, uid in enumerate(uids)}

        rows = session.execute(
            text(f"SELECT uid, username FROM user_info WHERE uid IN ({placeholders})"),
            params,
        ).fetchall()

        for uid, username in rows:
            result[uid] = username if username else str(uid)
    except Exception:
        pass

    # Fall back to pwd for missing UIDs
    for uid in uids:
        if uid not in result:
            try:
                result[uid] = pwd.getpwuid(uid).pw_name
            except (KeyError, OverflowError):
                result[uid] = str(uid)

    return result


def print_owner_results(owners: list[dict], username_map: dict[int, str]) -> None:
    """Print owner summary results in a formatted table."""
    if not owners:
        console.print("[yellow]No owner data found.[/yellow]")
        return

    table = Table(title=f"Owner Summary ({len(owners)} owners)")
    table.add_column("Owner", style="cyan")
    table.add_column("UID", justify="right")
    table.add_column("Total Size", justify="right")
    table.add_column("Total Files", justify="right")
    table.add_column("Directories", justify="right")

    for o in owners:
        uid = o["owner_uid"]
        username = username_map.get(uid, str(uid))
        table.add_row(
            username,
            str(uid),
            format_size(o["total_size"]),
            f"{o['total_files']:,}",
            f"{o['directory_count']:,}",
        )

    console.print(table)


def query_single_filesystem(
    filesystem: str,
    min_depth: int | None,
    max_depth: int | None,
    single_owner: bool,
    owner_id: int | None,
    path_prefixes: list[str] | None,
    exclude_paths: list[str] | None,
    sort_by: str,
    limit: int | None,
    accessed_before: datetime | None,
    accessed_after: datetime | None,
    leaves_only: bool,
    name_patterns: list[str] | None,
    name_pattern_ignorecase: bool,
    min_size: int | None = None,
    max_size: int | None = None,
    min_files: int | None = None,
    max_files: int | None = None,
) -> list[dict]:
    """Query a single filesystem database.

    Designed for parallel execution with ThreadPoolExecutor.
    Creates and closes its own session.

    Args:
        filesystem: Filesystem name to query
        Other args: Query parameters passed to query_directories()

    Returns:
        List of directory dictionaries from this filesystem
    """
    session = get_session(filesystem)
    try:
        return query_directories(
            session,
            min_depth=min_depth,
            max_depth=max_depth,
            single_owner=single_owner,
            owner_id=owner_id,
            path_prefixes=path_prefixes,
            exclude_paths=exclude_paths,
            sort_by=sort_by,
            limit=limit,
            accessed_before=accessed_before,
            accessed_after=accessed_after,
            leaves_only=leaves_only,
            name_patterns=name_patterns,
            name_pattern_ignorecase=name_pattern_ignorecase,
            min_size=min_size,
            max_size=max_size,
            min_files=min_files,
            max_files=max_files,
        )
    finally:
        session.close()


class DynamicHelpCommand(click.Command):
    """Custom Command class that replaces the command name in help text.

    This allows the help examples to show the actual invoked command name,
    which is useful when the tool is invoked via a symlink (e.g., cs-scan).
    """
    def get_help(self, ctx):
        help_text = super().get_help(ctx)
        # Get the actual invoked command name
        prog_name = ctx.find_root().info_name
        if prog_name and prog_name != 'query-fs-scan-db':
            # Replace hardcoded command name with actual invoked name
            help_text = help_text.replace('query-fs-scan-db', prog_name)
        return help_text


@click.command(cls=DynamicHelpCommand, context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("filesystem", type=str, default="all")
@click.option(
    "-d",
    "--min-depth",
    type=int,
    help="Filter by minimum path depth",
)
@click.option(
    "--max-depth",
    type=int,
    help="Filter by maximum path depth",
)
@click.option(
    "-s",
    "--single-owner",
    is_flag=True,
    help="Only show single-owner directories",
)
@click.option(
    "-u",
    "--owner",
    "owner_id",
    type=str,
    help="Filter to specific owner (UID or username)",
)
@click.option(
    "--mine",
    is_flag=True,
    help="Filter to current user's UID (shortcut for -u $UID)",
)
@click.option(
    "--path-prefix",
    "-P",
    "path_prefixes",
    multiple=True,
    type=str,
    help="Filter to paths starting with prefix (can be repeated for OR)",
)
@click.option(
    "--exclude",
    "-E",
    "exclude_paths",
    multiple=True,
    type=str,
    help="Exclude path and descendants from results (can be repeated)",
)
@click.option(
    "-n",
    "--limit",
    type=int,
    default=50,
    show_default=True,
    help="Limit results (0 for unlimited)",
)
@click.option(
    "--sort-by",
    type=click.Choice(["size", "size_r", "size_nr", "files", "files_r", "files_nr", "atime", "atime_r", "path", "depth", "dirs"]),
    default="size",
    show_default=True,
    help="Sort results by field (with --group-by owner: size, files, dirs)",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(path_type=Path),
    help="Write TSV output to file",
)
@click.option(
    "--accessed-before",
    type=str,
    help="Filter to max_atime_r before date (YYYY-MM-DD or Nyrs/Nmo)",
)
@click.option(
    "--accessed-after",
    type=str,
    help="Filter to max_atime_r after date (YYYY-MM-DD or Nyrs/Nmo)",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Show additional columns (Depth)",
)
@click.option(
    "--leaves-only",
    is_flag=True,
    help="Only show leaf directories (no subdirectories)",
)
@click.option(
    "--data-dir",
    "data_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Override directory containing database files (or set FS_SCAN_DATA_DIR env var)",
)
@click.option(
    "--summary",
    is_flag=True,
    help="Show database summary only",
)
@click.option(
    "--show-config",
    is_flag=True,
    help="Show data directory configuration and available databases",
)
@click.option(
    "-N",
    "--name-pattern",
    "name_patterns",
    multiple=True,
    type=str,
    help="Filter by name pattern (GLOB); can repeat for OR matching",
)
@click.option(
    "-i",
    "--ignore-case",
    is_flag=True,
    help="Make --name-pattern matching case-insensitive",
)
@click.option(
    "--min-size",
    type=str,
    default="1GiB",
    show_default=True,
    help="Min total recursive size (e.g. 500MB, 2GiB, 0 to disable)",
)
@click.option(
    "--max-size",
    type=str,
    default=None,
    help="Max total recursive size (e.g. 10GiB)",
)
@click.option(
    "--min-files",
    type=str,
    default=None,
    help="Min recursive file count (e.g. 500, 10K)",
)
@click.option(
    "--max-files",
    type=str,
    default=None,
    help="Max recursive file count",
)
@click.option(
    "--group-by",
    "group_by",
    type=click.Choice(["owner"]),
    help="Group results by field (currently: owner)",
)
@click.option(
    "--show-total",
    is_flag=True,
    help="Show totals row at bottom of results",
)
def main(
    filesystem: str,
    min_depth: int | None,
    max_depth: int | None,
    single_owner: bool,
    owner_id: str | None,
    mine: bool,
    path_prefixes: tuple[str, ...],
    exclude_paths: tuple[str, ...],
    limit: int,
    sort_by: str,
    output: Path | None,
    accessed_before: str | None,
    accessed_after: str | None,
    verbose: bool,
    leaves_only: bool,
    data_dir: Path | None,
    summary: bool,
    show_config: bool,
    name_patterns: tuple[str, ...],
    ignore_case: bool,
    min_size: str,
    max_size: str | None,
    min_files: str | None,
    max_files: str | None,
    group_by: str | None,
    show_total: bool,
):
    """
    Query GPFS scan database for directory statistics.

    FILESYSTEM is the name of the filesystem (e.g., asp, cisl, eol, hao),
    or 'all' to query all available databases (default).

    \b
    Examples:
      query-fs-scan-db                       # all filesystems (default)
      query-fs-scan-db asp                   # specific filesystem
      query-fs-scan-db -d 4 --single-owner   # single-owner dirs at depth 4+
      query-fs-scan-db --accessed-before 3yrs
      query-fs-scan-db --leaves-only
      query-fs-scan-db -N "*scratch*"        # filter by name pattern
      query-fs-scan-db -N "*scratch*" -N "*tmp*"  # multiple patterns (OR)
      query-fs-scan-db -N "*tmp*" -i         # case-insensitive name filter
      query-fs-scan-db --group-by owner      # per-user summary
      query-fs-scan-db --group-by owner --sort-by files  # sort by file count
      query-fs-scan-db --group-by owner -d 4 -P /gpfs/csfs1/cisl
    """
    # Apply data directory override if provided via CLI
    if data_dir is not None:
        set_data_dir(data_dir)

    # Handle --show-config
    if show_config:
        data_path, source = get_data_dir_info()
        console.print("[bold]Configuration[/bold]")
        console.print(f"  Data directory: {data_path}")
        console.print(f"  Source: {source}")
        console.print()

        filesystems = get_all_filesystems()
        if filesystems:
            console.print("[bold]Available databases[/bold]")
            for fs in filesystems:
                db_path = get_db_path(fs)
                size_bytes = db_path.stat().st_size if db_path.exists() else 0
                size_str = format_size(size_bytes)
                console.print(f"  {fs}: {db_path} ({size_str})")
        else:
            console.print("[yellow]No database files found.[/yellow]")
        return

    # Resolve owner_id: can be UID (int) or username (string)
    resolved_owner_id: int | None = None
    if mine:
        resolved_owner_id = os.getuid()
    elif owner_id is not None:
        try:
            resolved_owner_id = int(owner_id)
        except ValueError:
            # Not an integer, try to resolve as username
            try:
                resolved_owner_id = pwd.getpwnam(owner_id).pw_uid
            except KeyError:
                console.print(f"[red]Unknown user: {owner_id}[/red]")
                raise SystemExit(1)

    # Determine which filesystems to query
    if filesystem.lower() == "all":
        filesystems = get_all_filesystems()
        if not filesystems:
            console.print("[red]No database files found.[/red]")
            console.print("Run fs-scan-to-db first to import data.")
            raise SystemExit(1)
    else:
        db_path = get_db_path(filesystem)
        if not db_path.exists():
            console.print(f"[red]Database not found: {db_path}[/red]")
            console.print("Run fs-scan-to-db first to import data.")
            raise SystemExit(1)
        filesystems = [filesystem]

    console.print(f"[bold]Filesystem Scan Database Query[/bold]")
    console.print(f"Databases: {', '.join(filesystems)}")

    # Collect and display scan dates
    scan_dates = []
    for fs in filesystems:
        session = get_session(fs)
        try:
            scan_date = get_scan_date(session)
            if scan_date:
                scan_dates.append(scan_date)
        finally:
            session.close()

    if scan_dates:
        unique_dates = sorted(set(d.date() for d in scan_dates))
        if len(unique_dates) == 1:
            console.print(f"[dim]Data from scan: {unique_dates[0]}[/dim]")
        else:
            console.print(f"[dim]Scans from {unique_dates[0]} to {unique_dates[-1]}[/dim]")

    console.print(f"Note: this information is [bold]NOT[/bold] real-time")
    console.print()

    # Parse date arguments once
    parsed_before = parse_date_arg(accessed_before) if accessed_before else None
    parsed_after = parse_date_arg(accessed_after) if accessed_after else None

    # Parse size/file-count filter arguments
    parsed_min_size = parse_size(min_size) if min_size else None
    parsed_max_size = parse_size(max_size) if max_size else None
    parsed_min_files = parse_file_count(min_files) if min_files else None
    parsed_max_files = parse_file_count(max_files) if max_files else None

    # Normalize path arguments (strip mount point prefixes)
    normalized_path_prefixes = [normalize_path(p) for p in path_prefixes] if path_prefixes else []
    normalized_exclude_paths = [normalize_path(p) for p in exclude_paths] if exclude_paths else []

    # Handle summary mode
    if summary:
        for fs in filesystems:
            session = get_session(fs)
            try:
                stats = get_summary(session)
                console.print(f"[cyan]{fs}:[/cyan]")
                console.print(f"  Total directories: {stats['total_directories']:,}")
                console.print(f"  Root directories: {stats['root_directories']:,}")
                console.print(f"  Total files (root): {stats['total_files']:,}")
                console.print(f"  Total size (root): {format_size(stats['total_size'])}")
                console.print(f"  Maximum depth: {stats['max_depth']}")
            finally:
                session.close()
        return

    # Handle --group-by owner mode
    if group_by == "owner":
        # Map sort_by to owner summary field names
        # Accept common aliases for convenience
        owner_sort_map = {
            "size": "size",
            "files": "files",
            "dirs": "dirs",
            "directories": "dirs",
        }
        owner_sort_by = owner_sort_map.get(sort_by, "size")

        # Validate sort_by is compatible with --group-by owner
        if sort_by not in owner_sort_map:
            console.print(
                f"[yellow]Warning: --sort-by '{sort_by}' not valid with --group-by owner. "
                f"Using 'size' instead. Valid options: size, files, dirs[/yellow]"
            )
            owner_sort_by = "size"

        all_owners = []
        all_uids = set()

        for fs in filesystems:
            session = get_session(fs)
            try:
                owners = query_owner_summary(
                    session,
                    min_depth=min_depth,
                    max_depth=max_depth,
                    path_prefixes=normalized_path_prefixes if normalized_path_prefixes else None,
                    limit=limit if limit > 0 else None,
                    sort_by=owner_sort_by,
                )
                all_owners.extend(owners)
                all_uids.update(o["owner_uid"] for o in owners)
            finally:
                session.close()

        # For multi-db: aggregate by owner and re-sort
        if len(filesystems) > 1:
            aggregated = {}
            for o in all_owners:
                uid = o["owner_uid"]
                if uid not in aggregated:
                    aggregated[uid] = {
                        "owner_uid": uid,
                        "total_size": 0,
                        "total_files": 0,
                        "directory_count": 0,
                    }
                aggregated[uid]["total_size"] += o["total_size"]
                aggregated[uid]["total_files"] += o["total_files"]
                aggregated[uid]["directory_count"] += o["directory_count"]

            # Sort by the requested field
            sort_key_map = {
                "size": "total_size",
                "files": "total_files",
                "dirs": "directory_count",
            }
            sort_key = sort_key_map[owner_sort_by]

            all_owners = sorted(
                aggregated.values(),
                key=lambda x: x[sort_key],
                reverse=True,
            )
            if limit > 0:
                all_owners = all_owners[:limit]

        # Get username mappings (aggregate across all databases)
        username_map = {}
        if all_uids and filesystems:
            remaining_uids = set(all_uids)
            for fs in filesystems:
                if not remaining_uids:
                    break
                session = get_session(fs)
                try:
                    found = get_username_map(session, list(remaining_uids))
                    username_map.update(found)
                    remaining_uids -= found.keys()
                finally:
                    session.close()

        print_owner_results(all_owners, username_map)
        return

    # Query directories
    multi_db = len(filesystems) > 1
    all_directories = []

    if multi_db:
        # Parallel execution for multiple filesystems
        max_workers = min(len(filesystems), 8)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    query_single_filesystem,
                    fs,
                    min_depth,
                    max_depth,
                    single_owner,
                    resolved_owner_id,
                    normalized_path_prefixes if normalized_path_prefixes else None,
                    normalized_exclude_paths if normalized_exclude_paths else None,
                    sort_by,
                    limit if limit > 0 else None,
                    parsed_before,
                    parsed_after,
                    leaves_only,
                    list(name_patterns) if name_patterns else None,
                    ignore_case,
                    parsed_min_size,
                    parsed_max_size,
                    parsed_min_files,
                    parsed_max_files,
                ): fs
                for fs in filesystems
            }
            for future in as_completed(futures):
                fs = futures[future]
                try:
                    dirs = future.result()
                    all_directories.extend(dirs)
                except Exception as e:
                    console.print(f"[red]Error querying {fs}: {e}[/red]")
    else:
        # Single filesystem - sequential execution (no thread overhead)
        session = get_session(filesystems[0])
        try:
            all_directories = query_directories(
                session,
                min_depth=min_depth,
                max_depth=max_depth,
                single_owner=single_owner,
                owner_id=resolved_owner_id,
                path_prefixes=normalized_path_prefixes if normalized_path_prefixes else None,
                exclude_paths=normalized_exclude_paths if normalized_exclude_paths else None,
                sort_by=sort_by,
                limit=limit if limit > 0 else None,
                accessed_before=parsed_before,
                accessed_after=parsed_after,
                leaves_only=leaves_only,
                name_patterns=list(name_patterns) if name_patterns else None,
                name_pattern_ignorecase=ignore_case,
                min_size=parsed_min_size,
                max_size=parsed_max_size,
                min_files=parsed_min_files,
                max_files=parsed_max_files,
            )
        finally:
            session.close()

    # For multi-db: sort combined results and apply limit
    if multi_db:
        sort_key_map = {
            "size_r": lambda d: d["total_size_r"] or 0,
            "size_nr": lambda d: d["total_size_nr"] or 0,
            "files_r": lambda d: d["file_count_r"] or 0,
            "files_nr": lambda d: d["file_count_nr"] or 0,
            "atime_r": lambda d: d["max_atime_r"] or "",
            "path": lambda d: (d["depth"], d["path"]),
            "depth": lambda d: d["depth"],
        }
        reverse = sort_by not in ("path",)  # Most sorts are descending
        all_directories.sort(key=sort_key_map.get(sort_by, sort_key_map["size_r"]), reverse=reverse)

        # Apply limit after sorting combined results
        if limit > 0:
            all_directories = all_directories[:limit]

    # Output results
    if output:
        write_tsv(all_directories, output)
    else:
        # Resolve UIDs to usernames for display (aggregate across all databases)
        unique_uids = {
            d["owner_uid"] for d in all_directories
            if d["owner_uid"] is not None and d["owner_uid"] != -1
        }
        username_map = {}
        if unique_uids:
            remaining_uids = set(unique_uids)
            for fs in filesystems:
                if not remaining_uids:
                    break
                session = get_session(fs)
                try:
                    found = get_username_map(session, list(remaining_uids))
                    username_map.update(found)
                    remaining_uids -= found.keys()
                finally:
                    session.close()
        print_results(all_directories, verbose=verbose, leaves_only=leaves_only, username_map=username_map, show_total=show_total)


if __name__ == "__main__":
    main()
