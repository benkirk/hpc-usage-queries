#!/usr/bin/env python3
"""
GPFS Scan Database Query CLI

Query directory statistics from the SQLite database.
Supports filtering by depth, owner, path prefix, and sorting.
"""

import re
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

from .database import get_db_path, get_session, DATA_DIR
from .models import Directory, DirectoryStats

console = Console()


def get_all_filesystems() -> list[str]:
    """Discover all available filesystem databases.

    Returns:
        List of filesystem names (e.g., ['asp', 'cisl', 'eol', 'hao'])
    """
    db_files = DATA_DIR.glob("*.db")
    return sorted([f.stem for f in db_files])


def format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    if size_bytes is None:
        return "N/A"
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} EB"


def format_datetime(dt: datetime | str | None) -> str:
    """Format datetime for display."""
    if dt is None:
        return "N/A"
    if isinstance(dt, str):
        # SQLite may return datetime as string - just return date portion
        return dt[:10] if len(dt) >= 10 else dt
    return dt.strftime("%Y-%m-%d")


def parse_date_arg(value: str) -> datetime:
    """
    Parse date argument - absolute (YYYY-MM-DD) or relative (3yrs, 6mo).

    Args:
        value: Date string like "2024-01-15", "3yrs", or "18mo"

    Returns:
        datetime object

    Raises:
        click.BadParameter: If format is invalid
    """
    # Try absolute date first
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        pass

    # Parse relative date (e.g., "3yrs", "6mo")
    match = re.match(r"^(\d+)(yrs?|mo)$", value.lower())
    if match:
        num = int(match.group(1))
        unit = match.group(2)
        now = datetime.now()
        if unit.startswith("yr"):
            return now.replace(year=now.year - num)
        elif unit == "mo":
            # Handle month subtraction (wrap years if needed)
            new_month = now.month - num
            new_year = now.year
            while new_month <= 0:
                new_month += 12
                new_year -= 1
            # Handle day overflow (e.g., Jan 31 - 1 month)
            day = min(now.day, 28)  # Safe for all months
            return now.replace(year=new_year, month=new_month, day=day)

    raise click.BadParameter(f"Invalid date format: {value}. Use YYYY-MM-DD or Nyrs/Nmo")


def resolve_path_to_id(session, path: str) -> int | None:
    """
    Resolve a full path to its dir_id.

    Uses a recursive CTE to find the directory matching the given path.

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

    # Find by walking down the tree
    current_id = None

    for component in components:
        if current_id is None:
            # First component - find root
            result = session.execute(
                text("""
                    SELECT dir_id FROM directories
                    WHERE parent_id IS NULL AND name = :name
                """),
                {"name": component},
            ).fetchone()
        else:
            result = session.execute(
                text("""
                    SELECT dir_id FROM directories
                    WHERE parent_id = :parent_id AND name = :name
                """),
                {"parent_id": current_id, "name": component},
            ).fetchone()

        if result is None:
            return None
        current_id = result[0]

    return current_id


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


def query_directories(
    session,
    min_depth: int | None = None,
    max_depth: int | None = None,
    single_owner: bool = False,
    owner_id: int | None = None,
    path_prefix: str | None = None,
    sort_by: str = "size_r",
    limit: int | None = None,
    accessed_before: datetime | None = None,
    accessed_after: datetime | None = None,
    leaves_only: bool = False,
) -> list[dict]:
    """
    Query directories with optional filters.

    Args:
        session: SQLAlchemy session
        min_depth: Minimum path depth filter
        max_depth: Maximum path depth filter
        single_owner: Only show single-owner directories
        owner_id: Filter to specific owner UID
        path_prefix: Filter to paths under this prefix
        sort_by: Sort field (size_r, size_nr, files_r, files_nr, atime_r, path)
        limit: Maximum results to return
        accessed_before: Filter to directories with max_atime_r before this date
        accessed_after: Filter to directories with max_atime_r after this date
        leaves_only: Only show directories with no subdirectories

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

    # Handle path prefix - find ancestor and filter descendants
    if path_prefix:
        ancestor_id = resolve_path_to_id(session, path_prefix)
        if ancestor_id is None:
            return []  # Path not found

        # Use recursive CTE to find descendants
        base_query = f"""
            WITH RECURSIVE descendants AS (
                SELECT dir_id FROM directories WHERE dir_id = :ancestor_id
                UNION ALL
                SELECT d.dir_id FROM directories d
                JOIN descendants p ON d.parent_id = p.dir_id
            )
            SELECT d.dir_id, d.parent_id, d.name, d.depth,
                   s.file_count_nr, s.total_size_nr, s.max_atime_nr,
                   s.file_count_r, s.total_size_r, s.max_atime_r,
                   s.owner_uid
            FROM descendants
            JOIN directories d USING (dir_id)
            JOIN directory_stats s USING (dir_id)
        """
        params["ancestor_id"] = ancestor_id
    else:
        base_query = """
            SELECT d.dir_id, d.parent_id, d.name, d.depth,
                   s.file_count_nr, s.total_size_nr, s.max_atime_nr,
                   s.file_count_r, s.total_size_r, s.max_atime_r,
                   s.owner_uid
            FROM directories d
            JOIN directory_stats s USING (dir_id)
        """

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

    # Convert to dictionaries with full paths
    directories = []
    for row in results:
        dir_id = row[0]
        directories.append({
            "dir_id": dir_id,
            "path": get_full_path(session, dir_id),
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
    directories: list[dict], verbose: bool = False, leaves_only: bool = False
) -> None:
    """Print directory results in a formatted table."""
    if not directories:
        console.print("[yellow]No directories found matching criteria.[/yellow]")
        return

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
        table.add_column("Size\n(R)", justify="right")
        table.add_column("Size\n(NR)", justify="right")
        table.add_column("Files\n(R)", justify="right")
        table.add_column("Files\n(NR)", justify="right")
        table.add_column("Atime\n(R)", justify="right")
        table.add_column("Atime\n(NR)", justify="right")
    table.add_column("Owner", justify="right")

    for d in directories:
        owner_str = (
            f"[green]{d['owner_uid']}[/green]"
            if d["owner_uid"] is not None and d["owner_uid"] != -1
            else "[yellow]multiple[/yellow]" if d["owner_uid"] is None else "[dim]-[/dim]"
        )

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


@click.command()
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
    "--owner-id",
    type=int,
    help="Filter to specific owner UID",
)
@click.option(
    "--path-prefix",
    "-P",
    type=str,
    help="Filter to paths starting with prefix",
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
    type=click.Choice(["size_r", "size_nr", "files_r", "files_nr", "atime_r", "path", "depth"]),
    default="size_r",
    show_default=True,
    help="Sort results by field",
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
    "--summary",
    is_flag=True,
    help="Show database summary only",
)
def main(
    filesystem: str,
    min_depth: int | None,
    max_depth: int | None,
    single_owner: bool,
    owner_id: int | None,
    path_prefix: str | None,
    limit: int,
    sort_by: str,
    output: Path | None,
    accessed_before: str | None,
    accessed_after: str | None,
    verbose: bool,
    leaves_only: bool,
    summary: bool,
):
    """
    Query GPFS scan database for directory statistics.

    FILESYSTEM is the name of the filesystem (e.g., asp, cisl, eol, hao),
    or 'all' to query all available databases (default).

    \b
    Examples:
        # Query all filesystems (default)
        query-fs-scan-db

        # Query a specific filesystem
        query-fs-scan-db asp

        # Show only single-owner directories at depth 4+
        query-fs-scan-db -d 4 --single-owner

        # Filter by access time (files not accessed in 3+ years)
        query-fs-scan-db --accessed-before 3yrs

        # Show only leaf directories
        query-fs-scan-db --leaves-only
    """
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

    console.print(f"[bold]GPFS Scan Database Query[/bold]")
    console.print(f"Databases: {', '.join(filesystems)}")
    console.print()

    # Parse date arguments once
    parsed_before = parse_date_arg(accessed_before) if accessed_before else None
    parsed_after = parse_date_arg(accessed_after) if accessed_after else None

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

    # Query directories
    multi_db = len(filesystems) > 1
    all_directories = []

    for fs in filesystems:
        session = get_session(fs)
        try:
            dirs = query_directories(
                session,
                min_depth=min_depth,
                max_depth=max_depth,
                single_owner=single_owner,
                owner_id=owner_id,
                path_prefix=path_prefix,
                sort_by=sort_by,
                limit=limit if limit > 0 else None,
                accessed_before=parsed_before,
                accessed_after=parsed_after,
                leaves_only=leaves_only,
            )
            all_directories.extend(dirs)
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
        print_results(all_directories, verbose=verbose, leaves_only=leaves_only)


if __name__ == "__main__":
    main()
