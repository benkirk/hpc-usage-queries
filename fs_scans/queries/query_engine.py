"""Query engine for filesystem scan databases.

This module provides the core business logic for querying filesystem scan databases.
Separated from CLI concerns for modularity and testability.
"""

import grp
import os
import pwd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from sqlalchemy import inspect, text

from ..core.database import get_data_dir, get_db_path, get_session
from ..core.models import SCOPE_INDEX_MIN_DEPTH, SCOPE_INDEX_MAX_DEPTH
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


def collection_for_path(path: str) -> str | None:
    """Return the collection/schema name a full path belongs to, or None.

    Strips the mount-point prefix (via :func:`normalize_path`) and returns the
    first remaining path component — the top-level collection that maps to a
    PostgreSQL schema / SQLite ``*.db`` name. For example::

        /glade/campaign/cisl/csg -> "cisl"
        /gpfs/csfs1/aiml         -> "aiml"

    External consumers (e.g. SAM) use this to turn a project's directory
    paths into the minimal set of ``filesystems=`` collections to query,
    avoiding a fan-out across every collection. Returns ``None`` when no
    collection component can be determined (e.g. an empty or root path).

    The name is lower-cased to match the rest of the stack
    (:func:`get_all_filesystems`, :func:`filesystem_available`,
    ``pg_schema_name`` and the SQLite ``*.db`` names all use lower case).

    This is a pure, DB-free lexical helper: it does **not** check that the
    collection exists. Callers should validate the result against the
    available collections (:func:`get_all_filesystems` /
    :func:`filesystem_available`) before querying — on the SQLite backend
    ``get_session("bogus")`` would otherwise create an empty ``bogus.db``
    and silently return zero rows.

    Args:
        path: A full or already-normalized filesystem path.

    Returns:
        The lower-cased collection name, or ``None`` if it can't be derived.
    """
    normalized = normalize_path(path).strip("/")
    if not normalized:
        return None
    first = normalized.split("/", 1)[0]
    return first.lower() if first else None


def get_all_filesystems(database: str | None = None) -> list[str]:
    """Discover all available filesystem/collection databases.

    SQLite: the ``*.db`` files in the configured data directory (via
    get_data_dir()).  PostgreSQL: the collection schemas in the configured
    database (via list_pg_schemas()). ``database`` selects which CNPG database
    to introspect (postgres only); defaults to ``FsScanConfig.PG_DB_NAME``.

    Returns:
        List of filesystem/collection names (e.g., ['asp', 'cisl', 'cgd'])
    """
    from ..core.config import FsScanConfig
    from ..core.database import list_pg_schemas

    if FsScanConfig.DB_BACKEND == "postgres":
        return list_pg_schemas(database=database)

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


def resolve_path_to_id_with_depth(session, path: str) -> tuple[int, int] | None:
    """Resolve a path to its ``(dir_id, stored_depth)``, or ``None`` if absent.

    ``depth`` is read from the ``directories`` row, NOT inferred from the number
    of path components: callers pass mount-stripped paths (the facade strips
    ``/gpfs/csfs1`` etc.) while per-collection databases store the depth measured
    from the true filesystem root (e.g. ``asp`` is depth 3), so the two differ.
    The stored depth is what indexes the anc_d{k} columns, so it must be exact.
    """
    dir_id = resolve_path_to_id(session, path)
    if dir_id is None:
        return None
    depth = session.execute(
        text("SELECT depth FROM directories WHERE dir_id = :id"), {"id": dir_id}
    ).scalar()
    if depth is None:
        return None
    return dir_id, depth


# Cache of "does directory_stats carry the anc_d* scope columns?" keyed by
# engine. Existence rarely changes; reflection is cheap but the scoped queries
# fan out per-filesystem, so memoize.
_ANC_COLUMNS_CACHE: dict = {}

# Cache of each database's collection-root depth (MIN(depth)) keyed by engine.
# anc_d* levels are relative to this root; pass2c derives it the same way, so the
# two agree (the depth values are identical after consolidation to PostgreSQL).
_ROOT_DEPTH_CACHE: dict = {}


def collection_root_depth(session) -> int | None:
    """The shallowest directory depth in this database (the collection root).

    anc_d{k} levels are measured relative to this: ``level = depth - root + 1``.
    """
    bind = session.get_bind()
    key = id(bind)
    if key not in _ROOT_DEPTH_CACHE:
        _ROOT_DEPTH_CACHE[key] = session.execute(
            text("SELECT MIN(depth) FROM directories")
        ).scalar()
    return _ROOT_DEPTH_CACHE[key]


def anc_columns_exist(session) -> bool:
    """True if directory_stats declares the anc_d* scope columns.

    Older ``.db`` files (imported before this feature) lack them entirely.
    Checked via the inspector (no failed query, so it never aborts an open
    PostgreSQL transaction); the anc_d* columns are all added together, so the
    presence of ``anc_d1`` implies the whole set.
    """
    bind = session.get_bind()
    key = id(bind)
    cached = _ANC_COLUMNS_CACHE.get(key)
    if cached is None:
        try:
            cols = {c["name"] for c in inspect(bind).get_columns("directory_stats")}
            cached = "anc_d1" in cols
        except Exception:
            cached = False
        _ANC_COLUMNS_CACHE[key] = cached
    return cached


def _scopes_populated(session, resolved) -> bool:
    """True if every resolved scope's anc_d{level} self-slot is populated.

    pass2c sets ``anc_d{k} = dir_id`` on every directory at relative level k (for
    k <= SCOPE_MAX_DEPTH): a directory is its own level-k ancestor. Verifying that
    invariant for the scope rows themselves is the exact correctness precondition
    for the fast predicate — it confirms the columns are populated (not merely
    declared) without assuming any particular tree depth. A self-slot lookup is a
    single primary-key probe.
    """
    for dir_id, level in resolved:
        row = session.execute(
            text(f"SELECT 1 FROM directory_stats WHERE dir_id = :id AND anc_d{level} = :id"),
            {"id": dir_id},
        ).fetchone()
        if row is None:
            return False
    return True


def resolve_scope(session, path_prefixes: list[str]):
    """Resolve *path_prefixes* into a subtree scope.

    Returns ``(resolved, use_fast)`` where ``resolved`` is a list of
    ``(dir_id, level)`` pairs — ``level`` being the scope's depth *relative to the
    collection root* (root = 1), which is exactly the ``anc_d{level}`` column that
    indexes it — or ``None`` if no prefix resolved (the caller should return an
    empty result). ``use_fast`` is True when every relative level falls in the
    indexed band ``[SCOPE_INDEX_MIN_DEPTH, SCOPE_INDEX_MAX_DEPTH]``, the anc_d*
    columns exist, and those scope rows are actually populated — i.e. the scope
    can be answered with an ``anc_d{level}`` equality predicate instead of the
    recursive CTE.

    The gate is the *indexed* band, not the full populated range: outside it a
    PostgreSQL ``anc_d{k}`` predicate has no covering index and would seq-scan
    the whole table, which for a deep (hence thin) subtree loses to the
    self-bounding recursive walk. Columns are populated to ``SCOPE_MAX_DEPTH`` as
    headroom so the band can be widened later by reconsolidating (rebuilding the
    PG indexes) without re-importing.

    Prefixes are assumed already collapsed (the facade's ``_collapse_prefixes``
    drops any nested under another), so the per-level predicates OR together
    without double-counting.
    """
    raw = []
    for prefix in path_prefixes:
        pair = resolve_path_to_id_with_depth(session, prefix)
        if pair is not None:
            raw.append(pair)
    if not raw:
        return None, False

    # Convert absolute depth → level relative to the collection root.
    root_depth = collection_root_depth(session) or 1
    resolved = [(dir_id, depth - root_depth + 1) for dir_id, depth in raw]

    use_fast = (
        all(SCOPE_INDEX_MIN_DEPTH <= level <= SCOPE_INDEX_MAX_DEPTH for _, level in resolved)
        and anc_columns_exist(session)
        and _scopes_populated(session, resolved)
    )
    return resolved, use_fast


def _recursive_descendants_cte(ancestor_ids: list[int], params: dict) -> tuple[str, str]:
    """Build the recursive-subtree CTE + join clause for *ancestor_ids*.

    Mutates *params* with ``ancestor_id_{i}`` binds. Returns
    ``(cte_clause, join_clause)`` — the historical fallback shared by every
    scoped consumer.
    """
    for i, aid in enumerate(ancestor_ids):
        params[f"ancestor_id_{i}"] = aid
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
    return cte_clause, "JOIN descendants USING (dir_id)"


def _anc_predicate(resolved: list, params: dict, alias: str = "s") -> str:
    """Build the fast ``(alias.anc_d{level} = :scope_i OR ...)`` lineage predicate.

    *resolved* is a list of ``(dir_id, level)`` pairs (each relative level already
    verified inside the indexed band). Mutates *params* with ``scope_anc_{i}``
    binds.
    """
    preds = []
    for i, (dir_id, level) in enumerate(resolved):
        key = f"scope_anc_{i}"
        params[key] = dir_id
        preds.append(f"{alias}.anc_d{level} = :{key}")
    return "(" + " OR ".join(preds) + ")"


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
    Get directory counts for multiple directories from directory_stats.

    Args:
        session: SQLAlchemy session
        dir_ids: List of directory IDs to get counts for

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
            SELECT dir_id, dir_count_r, dir_count_nr
            FROM directory_stats
            WHERE dir_id IN ({placeholders})
        """),
        params,
    )

    return {row[0]: (row[1] or 0, row[2] or 0) for row in result}


def query_directories(
    session,
    min_depth: int | None = None,
    max_depth: int | None = None,
    single_owner: bool = False,
    owner_id: int | None = None,
    group_id: int | None = None,
    path_prefixes: list[str] | None = None,
    exclude_paths: list[str] | None = None,
    sort_by: str = "size_r",
    limit: int | None = None,
    accessed_before: datetime | None = None,
    accessed_after: datetime | None = None,
    atime_recursive: bool = True,
    leaves_only: bool = False,
    name_patterns: list[str] | None = None,
    name_pattern_ignorecase: bool = False,
    min_size: int | None = None,
    max_size: int | None = None,
    min_files: int | None = None,
    max_files: int | None = None,
    min_avg_size: int | None = None,
    max_avg_size: int | None = None,
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
        group_id: Filter to specific group GID
        path_prefixes: Filter to paths under these prefixes (OR'd together)
        exclude_paths: List of paths to exclude (with descendants)
        sort_by: Sort field (size_r, size_nr, files_r, files_nr, atime_r, path)
        limit: Maximum results to return
        accessed_before: Filter to directories last accessed before this date
        accessed_after: Filter to directories last accessed after this date
        atime_recursive: When True (default) the access-date filters compare
            against max_atime_r (newest access anywhere in the subtree); when
            False they compare against max_atime_nr (the directory's own files)
        leaves_only: Only show directories with no subdirectories
        name_patterns: List of GLOB patterns to filter directory names (OR'd together)
        name_pattern_ignorecase: If True, name pattern matching is case-insensitive
        min_size: Minimum total_size_r in bytes
        max_size: Maximum total_size_r in bytes
        min_files: Minimum file_count_r
        max_files: Maximum file_count_r
        min_avg_size: Minimum average own-file size (total_size_nr/file_count_nr),
            inclusive — the dimension the file-size histogram buckets by
        max_avg_size: Maximum average own-file size, exclusive
        compute_dir_counts: If True, compute directory counts (ndirs_r, ndirs_nr)

    Returns:
        List of directory dictionaries with stats
    """
    # Phase 1: Resolve path_prefixes to IDs (if provided)
    scope_resolved = None
    scope_use_fast = False
    if path_prefixes:
        scope_resolved, scope_use_fast = resolve_scope(session, path_prefixes)
        if scope_resolved is None:
            return []  # No valid paths found

    # Phase 2: Build query using DirectoryQueryBuilder (dialect-aware so name
    # pattern matching uses GLOB on sqlite and regex `~`/ILIKE on postgresql).
    builder = DirectoryQueryBuilder(dialect=session.get_bind().dialect.name)

    # Apply depth filters
    if min_depth is not None or max_depth is not None:
        builder.with_depth_range(min_depth, max_depth)

    # Apply owner filters
    if single_owner:
        builder.with_single_owner()
    if owner_id is not None:
        builder.with_owner(owner_id)
    if group_id is not None:
        builder.with_group(group_id)

    # Apply date filters
    if accessed_before is not None:
        builder.with_accessed_before(accessed_before, recursive=atime_recursive)
    if accessed_after is not None:
        builder.with_accessed_after(accessed_after, recursive=atime_recursive)

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
    if min_avg_size is not None or max_avg_size is not None:
        builder.with_avg_file_size_range(min_avg_size, max_avg_size)

    # Apply path prefix filter: fast anc_d{k} lineage predicate when possible,
    # else the recursive descendants CTE (deep scopes / older .db files).
    if scope_resolved:
        if scope_use_fast:
            builder.with_path_prefix_anc(scope_resolved)
        else:
            builder.with_path_prefix_ids([rid for rid, _ in scope_resolved])

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
            "dir_count_nr": row[7] or 0,
            "file_count_r": row[8] or 0,
            "total_size_r": row[9] or 0,
            "max_atime_r": row[10],
            "dir_count_r": row[11] or 0,
            "owner_uid": row[12],
            "owner_gid": row[13],
        })

    # Optionally add directory counts for backward compatibility
    # (now they're always available, but we add separate keys if requested)
    if compute_dir_counts and directories:
        for d in directories:
            d["ndirs_r"] = d["dir_count_r"]
            d["ndirs_nr"] = d["dir_count_nr"]

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

    # Handle path prefixes: prefer the fast anc_d{k} lineage predicate, fall
    # back to the recursive descendants CTE for deep scopes / older .db files.
    cte_clause = ""
    join_clause = ""
    if path_prefixes:
        resolved, use_fast = resolve_scope(session, path_prefixes)
        if resolved is None:
            return []  # No valid paths found
        if use_fast:
            conditions.append(_anc_predicate(resolved, params, alias="s"))
        else:
            cte_clause, join_clause = _recursive_descendants_cte(
                [rid for rid, _ in resolved], params
            )

    sort_map = {
        "size": "total_size DESC",
        "files": "total_files DESC",
        "dirs": "directory_count DESC",
    }
    order_clause = sort_map.get(sort_by, sort_map["size"])

    where_clause = " AND ".join(conditions)

    # The directories join is only needed for depth filters or the recursive
    # CTE; the fast aggregate reads everything it needs from directory_stats.
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
            s.owner_uid,
            SUM(s.total_size_nr) as total_size,
            SUM(s.file_count_nr) as total_files,
            COUNT(*) as directory_count
        {from_clause}
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


def get_username_map(session, uids: list[int], fallback: bool = True) -> dict[int, str]:
    """
    Get username mappings for a list of UIDs from the user_info table.

    Args:
        session: SQLAlchemy session
        uids: List of UIDs to resolve
        fallback: When True (default), UIDs absent from this database's
            user_info table fall back to pwd.getpwuid() and finally to
            str(uid), so every requested UID is present in the result. When
            False, only UIDs with a real (non-empty) username in user_info are
            returned — used by resolve_usernames_across_databases() so that a
            str(uid) placeholder in one database does not stop the search from
            consulting the others.

    Returns:
        Dictionary mapping UID to username. With fallback=True every requested
        UID is present (real name, pwd lookup, or str(uid)); with
        fallback=False only real user_info hits are included.
    """
    if not uids:
        return {}

    real = {}  # UIDs with a non-empty username in this database's user_info

    try:
        placeholders = ", ".join(f":uid_{i}" for i in range(len(uids)))
        params = {f"uid_{i}": uid for i, uid in enumerate(uids)}

        rows = session.execute(
            text(f"SELECT uid, username FROM user_info WHERE uid IN ({placeholders})"),
            params,
        ).fetchall()

        for uid, username in rows:
            if username:
                real[uid] = username
    except Exception:
        pass

    if not fallback:
        return real

    # Fall back to pwd then str(uid) for UIDs without a real username here.
    result = dict(real)
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
    database: str | None = None,
) -> dict[int, str]:
    """Resolve UIDs to usernames by searching across multiple databases.

    Searches databases in order, stopping early once every UID has a real
    user_info match. A UID's str(uid) placeholder is *not* treated as resolved
    (that would stop the search at the first database — e.g. a path-filtered
    query against /<collection> still fans out across every database, but only
    the owning collection's user_info knows its users), so the str(uid) /
    pwd.getpwuid() last resort is applied once, after all databases are tried.

    Args:
        uids: Set or list of UIDs to resolve
        filesystems: List of filesystem names to search

    Returns:
        Dictionary mapping UID to username (or str(uid) if unknown in any db)
    """
    if not uids:
        return {}

    username_map = {}
    remaining_uids = set(uids)

    for fs in filesystems:
        if not remaining_uids:
            break  # All UIDs resolved to real names, stop early

        session = get_session(fs, database=database)
        try:
            found = get_username_map(session, list(remaining_uids), fallback=False)
        finally:
            session.close()

        username_map.update(found)
        remaining_uids -= found.keys()

    # Last resort for UIDs absent from every database's user_info.
    for uid in remaining_uids:
        try:
            username_map[uid] = pwd.getpwuid(uid).pw_name
        except (KeyError, OverflowError):
            username_map[uid] = str(uid)

    return username_map


def resolve_group_filter(group_arg: str | None, mine_flag: bool) -> int | None:
    """Resolve group filter argument to a GID.

    Args:
        group_arg: Group identifier (GID as string or groupname)
        mine_flag: If True, use current user's primary GID

    Returns:
        Resolved GID or None if no group filter specified

    Raises:
        SystemExit: If groupname cannot be resolved
    """
    if mine_flag:
        return os.getgid()

    if group_arg is not None:
        try:
            # Try parsing as integer GID
            return int(group_arg)
        except ValueError:
            # Not an integer, try resolving as groupname
            try:
                return grp.getgrnam(group_arg).gr_gid
            except KeyError:
                from ..cli.common import console
                console.print(f"[red]Unknown group: {group_arg}[/red]")
                raise SystemExit(1)

    return None


def get_groupname_map(session, gids: list[int], fallback: bool = True) -> dict[int, str]:
    """
    Get groupname mappings for a list of GIDs from the group_info table.

    Args:
        session: SQLAlchemy session
        gids: List of GIDs to resolve
        fallback: When True (default), GIDs absent from this database's
            group_info table fall back to grp.getgrgid() and finally to
            str(gid). When False, only GIDs with a real (non-empty) groupname
            in group_info are returned — used by
            resolve_groupnames_across_databases() so a str(gid) placeholder in
            one database does not stop the search from consulting the others.

    Returns:
        Dictionary mapping GID to groupname. With fallback=True every requested
        GID is present; with fallback=False only real group_info hits.
    """
    if not gids:
        return {}

    real = {}  # GIDs with a non-empty groupname in this database's group_info

    try:
        placeholders = ", ".join(f":gid_{i}" for i in range(len(gids)))
        params = {f"gid_{i}": gid for i, gid in enumerate(gids)}

        rows = session.execute(
            text(f"SELECT gid, groupname FROM group_info WHERE gid IN ({placeholders})"),
            params,
        ).fetchall()

        for gid, groupname in rows:
            if groupname:
                real[gid] = groupname
    except Exception:
        pass

    if not fallback:
        return real

    # Fall back to grp then str(gid) for GIDs without a real groupname here.
    result = dict(real)
    for gid in gids:
        if gid not in result:
            try:
                result[gid] = grp.getgrgid(gid).gr_name
            except (KeyError, OverflowError):
                result[gid] = str(gid)

    return result


def resolve_groupnames_across_databases(
    gids: set[int] | list[int],
    filesystems: list[str],
    database: str | None = None,
) -> dict[int, str]:
    """Resolve GIDs to groupnames by searching across multiple databases.

    Searches databases in order, stopping early once every GID has a real
    group_info match. A GID's str(gid) placeholder is *not* treated as resolved
    (that would stop the search at the first database), so the str(gid) /
    grp.getgrgid() last resort is applied once, after all databases are tried.

    Args:
        gids: Set or list of GIDs to resolve
        filesystems: List of filesystem names to search

    Returns:
        Dictionary mapping GID to groupname (or str(gid) if unknown in any db)
    """
    if not gids:
        return {}

    groupname_map = {}
    remaining_gids = set(gids)

    for fs in filesystems:
        if not remaining_gids:
            break  # All GIDs resolved to real names, stop early

        session = get_session(fs, database=database)
        try:
            found = get_groupname_map(session, list(remaining_gids), fallback=False)
        finally:
            session.close()

        groupname_map.update(found)
        remaining_gids -= found.keys()

    # Last resort for GIDs absent from every database's group_info.
    for gid in remaining_gids:
        try:
            groupname_map[gid] = grp.getgrgid(gid).gr_name
        except (KeyError, OverflowError):
            groupname_map[gid] = str(gid)

    return groupname_map


def query_group_summary(
    session,
    min_depth: int | None = None,
    max_depth: int | None = None,
    path_prefixes: list[str] | None = None,
    limit: int | None = None,
    sort_by: str = "size",
) -> list[dict]:
    """
    Query per-group aggregated statistics.

    Uses fast path (GroupSummary table) when no filters are applied,
    otherwise computes dynamically from directory_stats.

    Args:
        session: SQLAlchemy session
        min_depth: Minimum path depth filter
        max_depth: Maximum path depth filter
        path_prefixes: Filter to paths under these prefixes (OR'd together)
        limit: Maximum results to return
        sort_by: Sort field (size, files, dirs)

    Returns:
        List of group summary dictionaries
    """
    has_filters = any([min_depth, max_depth, path_prefixes])

    if not has_filters:
        # Fast path: use pre-computed GroupSummary table
        # Check if the table exists and has data
        try:
            count = session.execute(
                text("SELECT COUNT(*) FROM group_summary")
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
                SELECT owner_gid, total_size, total_files, directory_count
                FROM group_summary
                ORDER BY {order_clause}
            """
            if limit:
                query += f" LIMIT {limit}"

            results = session.execute(text(query)).fetchall()
            return [
                {
                    "owner_gid": row[0],
                    "total_size": row[1] or 0,
                    "total_files": row[2] or 0,
                    "directory_count": row[3] or 0,
                }
                for row in results
            ]

    # Dynamic path: compute from directory_stats with filters
    conditions = ["s.owner_gid IS NOT NULL AND s.owner_gid >= 0"]
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
            return []  # No valid paths found
        if use_fast:
            conditions.append(_anc_predicate(resolved, params, alias="s"))
        else:
            cte_clause, join_clause = _recursive_descendants_cte(
                [rid for rid, _ in resolved], params
            )

    sort_map = {
        "size": "total_size DESC",
        "files": "total_files DESC",
        "dirs": "directory_count DESC",
    }
    order_clause = sort_map.get(sort_by, sort_map["size"])

    where_clause = " AND ".join(conditions)

    # The directories join is only needed for depth filters or the recursive
    # CTE; the fast aggregate reads everything it needs from directory_stats.
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
            s.owner_gid,
            SUM(s.total_size_nr) as total_size,
            SUM(s.file_count_nr) as total_files,
            COUNT(*) as directory_count
        {from_clause}
        WHERE {where_clause}
        GROUP BY s.owner_gid
        ORDER BY {order_clause}
    """
    if limit:
        query += f" LIMIT {limit}"

    results = session.execute(text(query), params).fetchall()
    return [
        {
            "owner_gid": row[0],
            "total_size": row[1] or 0,
            "total_files": row[2] or 0,
            "directory_count": row[3] or 0,
        }
        for row in results
    ]


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
    group_id: int | None = None,
    atime_recursive: bool = True,
    min_avg_size: int | None = None,
    max_avg_size: int | None = None,
    database: str | None = None,
) -> list[dict]:
    """Query a single filesystem database.

    Designed for parallel execution with ThreadPoolExecutor.
    Creates and closes its own session.

    Args:
        filesystem: Filesystem name to query
        database: PostgreSQL database name (defaults to PG_DB_NAME); see get_engine.
        Other args: Query parameters passed to query_directories()

    Returns:
        List of directory dictionaries from this filesystem
    """
    session = get_session(filesystem, database=database)
    try:
        return query_directories(
            session,
            min_depth=min_depth,
            max_depth=max_depth,
            single_owner=single_owner,
            owner_id=owner_id,
            group_id=group_id,
            path_prefixes=path_prefixes,
            exclude_paths=exclude_paths,
            sort_by=sort_by,
            limit=limit,
            accessed_before=accessed_before,
            accessed_after=accessed_after,
            atime_recursive=atime_recursive,
            leaves_only=leaves_only,
            name_patterns=name_patterns,
            name_pattern_ignorecase=name_pattern_ignorecase,
            min_size=min_size,
            max_size=max_size,
            min_files=min_files,
            max_files=max_files,
            min_avg_size=min_avg_size,
            max_avg_size=max_avg_size,
            compute_dir_counts=compute_dir_counts,
        )
    finally:
        session.close()

