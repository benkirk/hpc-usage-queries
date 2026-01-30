"""Query engine for filesystem scan databases.

This module provides the core business logic for querying filesystem scan databases.
Separated from CLI concerns for modularity and testability.
"""

import os
import pwd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from ..core.database import get_data_dir, get_db_path, get_session
from ..core.query_builder import DirectoryQueryBuilder


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


def get_directory_counts_batch(session, dir_ids: list[int]) -> dict[int, tuple[int, int]]:
    """
    Compute directory counts for multiple directories in a single query.

    This is much more efficient than counting per directory when dealing
    with multiple results (N*2 queries -> 1 query).

    Args:
        session: SQLAlchemy session
        dir_ids: List of directory IDs to count

    Returns:
        Dictionary mapping dir_id to (ndirs_r, ndirs_nr) tuple
        - ndirs_r: Recursive descendant directory count
        - ndirs_nr: Direct child directory count (non-recursive)
    """
    if not dir_ids:
        return {}

    # SQLite doesn't support array parameters directly, so we build IN clause
    # with positional parameters
    placeholders = ", ".join(f":id_{i}" for i in range(len(dir_ids)))
    params = {f"id_{i}": did for i, did in enumerate(dir_ids)}

    result = session.execute(
        text(f"""
            WITH RECURSIVE descendant_cte AS (
                -- Base: start from each target directory
                SELECT dir_id, dir_id as origin_id
                FROM directories
                WHERE dir_id IN ({placeholders})
                UNION ALL
                -- Recursive: find all descendants
                SELECT d.dir_id, cte.origin_id
                FROM directories d
                JOIN descendant_cte cte ON d.parent_id = cte.dir_id
            )
            SELECT
                origin_id,
                COUNT(*) - 1 as ndirs_r,
                SUM(CASE WHEN d.parent_id = origin_id THEN 1 ELSE 0 END) as ndirs_nr
            FROM descendant_cte cte
            JOIN directories d ON d.dir_id = cte.dir_id
            GROUP BY origin_id
        """),
        params,
    )

    return {row[0]: (row[1], row[2]) for row in result}


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
    compute_dir_counts: bool = False,
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
        compute_dir_counts: If True, compute directory counts (ndirs_r, ndirs_nr)

    Returns:
        List of directory dictionaries with stats
    """
    # Phase 1: Resolve path_prefixes to IDs (if provided)
    ancestor_ids = []
    if path_prefixes:
        for prefix in path_prefixes:
            ancestor_id = resolve_path_to_id(session, prefix)
            if ancestor_id is not None:
                ancestor_ids.append(ancestor_id)

        if not ancestor_ids:
            return []  # No valid paths found

    # Phase 2: Build query using DirectoryQueryBuilder
    builder = DirectoryQueryBuilder()

    # Apply depth filters
    if min_depth is not None or max_depth is not None:
        builder.with_depth_range(min_depth, max_depth)

    # Apply owner filters
    if single_owner:
        builder.with_single_owner()
    if owner_id is not None:
        builder.with_owner(owner_id)

    # Apply date filters
    if accessed_before is not None:
        builder.with_accessed_before(accessed_before)
    if accessed_after is not None:
        builder.with_accessed_after(accessed_after)

    # Apply structural filters
    if leaves_only:
        builder.with_leaves_only()

    # Apply name pattern filters
    if name_patterns:
        builder.with_name_patterns(list(name_patterns), name_pattern_ignorecase)

    # Apply size and file count filters
    if min_size is not None or max_size is not None:
        builder.with_size_range(min_size, max_size)
    if min_files is not None or max_files is not None:
        builder.with_file_count_range(min_files, max_files)

    # Apply path prefix filter (using resolved IDs)
    if ancestor_ids:
        builder.with_path_prefix_ids(ancestor_ids)

    # Apply sorting and limit
    builder.with_sort(sort_by)
    if limit is not None:
        builder.with_limit(limit)

    # Phase 3: Execute query
    query_result = builder.build()
    results = session.execute(text(query_result.sql), query_result.params).fetchall()

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

    # Optionally compute directory counts in batch
    if compute_dir_counts and directories:
        dir_ids = [d["dir_id"] for d in directories]
        dir_count_map = get_directory_counts_batch(session, dir_ids)
        for d in directories:
            ndirs_r, ndirs_nr = dir_count_map.get(d["dir_id"], (0, 0))
            d["ndirs_r"] = ndirs_r
            d["ndirs_nr"] = ndirs_nr

    return directories

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

def resolve_owner_filter(owner_arg: str | None, mine_flag: bool) -> int | None:
    """Resolve owner filter argument to a UID.

    Args:
        owner_arg: Owner identifier (UID as string or username)
        mine_flag: If True, use current user's UID

    Returns:
        Resolved UID or None if no owner filter specified

    Raises:
        SystemExit: If username cannot be resolved
    """
    if mine_flag:
        return os.getuid()

    if owner_arg is not None:
        try:
            # Try parsing as integer UID
            return int(owner_arg)
        except ValueError:
            # Not an integer, try resolving as username
            try:
                return pwd.getpwnam(owner_arg).pw_uid
            except KeyError:
                from ..cli.common import console
                console.print(f"[red]Unknown user: {owner_arg}[/red]")
                raise SystemExit(1)

    return None


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


def resolve_usernames_across_databases(
    uids: set[int] | list[int],
    filesystems: list[str],
) -> dict[int, str]:
    """Resolve UIDs to usernames by searching across multiple databases.

    Efficiently searches databases in order, stopping early once all
    UIDs are resolved. This is useful when querying multiple databases
    and needing to resolve usernames from any of them.

    Args:
        uids: Set or list of UIDs to resolve
        filesystems: List of filesystem names to search

    Returns:
        Dictionary mapping UID to username (or str(uid) if unknown)
    """
    if not uids:
        return {}

    username_map = {}
    remaining_uids = set(uids)

    for fs in filesystems:
        if not remaining_uids:
            break  # All UIDs resolved, stop early

        session = get_session(fs)
        try:
            found = get_username_map(session, list(remaining_uids))
            username_map.update(found)
            remaining_uids -= found.keys()
        finally:
            session.close()

    return username_map


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
    compute_dir_counts: bool = False,
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
            compute_dir_counts=compute_dir_counts,
        )
    finally:
        session.close()

