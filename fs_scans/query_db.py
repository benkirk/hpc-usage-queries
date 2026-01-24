#!/usr/bin/env python3
"""
GPFS Scan Database Query CLI

Query directory statistics from the SQLite database.
Supports filtering by depth, owner, path prefix, and sorting.
"""

from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

from .database import get_db_path, get_session
from .models import Directory, DirectoryStats

console = Console()


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
    #return dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y-%m-%d")


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


def print_results(directories: list[dict], show_recursive: bool = True) -> None:
    """Print directory results in a formatted table."""
    if not directories:
        console.print("[yellow]No directories found matching criteria.[/yellow]")
        return

    table = Table(title=f"Directory Statistics ({len(directories)} results)")
    table.add_column("Directory", style="cyan", no_wrap=False)
    table.add_column("Depth", justify="right")

    if show_recursive:
        table.add_column("Files (R)", justify="right")
        table.add_column("Size (R)", justify="right")
        table.add_column("Max Atime (R)", justify="right")
    else:
        table.add_column("Files", justify="right")
        table.add_column("Size", justify="right")
        table.add_column("Max Atime", justify="right")

    table.add_column("Owner", justify="right")

    for d in directories:
        owner_str = (
            f"[green]{d['owner_uid']}[/green]"
            if d["owner_uid"] is not None and d["owner_uid"] != -1
            else "[yellow]multiple[/yellow]" if d["owner_uid"] is None else "[dim]-[/dim]"
        )

        if show_recursive:
            table.add_row(
                d["path"],
                str(d["depth"]),
                f"{d['file_count_r']:,}",
                format_size(d["total_size_r"]),
                format_datetime(d["max_atime_r"]),
                owner_str,
            )
        else:
            table.add_row(
                d["path"],
                str(d["depth"]),
                f"{d['file_count_nr']:,}",
                format_size(d["total_size_nr"]),
                format_datetime(d["max_atime_nr"]),
                owner_str,
            )

    console.print(table)


def write_tsv(directories: list[dict], output_path: Path) -> None:
    """Write results to TSV file."""
    with open(output_path, "w") as f:
        # Header
        f.write(
            "directory\tdepth\t"
            "file_count_nr\ttotal_size_nr\tmax_atime_nr\t"
            "file_count_r\ttotal_size_r\tmax_atime_r\t"
            "owner_uid\n"
        )

        for d in directories:
            f.write(
                f"{d['path']}\t{d['depth']}\t"
                f"{d['file_count_nr']}\t{d['total_size_nr']}\t"
                f"{format_datetime(d['max_atime_nr'])}\t"
                f"{d['file_count_r']}\t{d['total_size_r']}\t"
                f"{format_datetime(d['max_atime_r'])}\t"
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
@click.argument("filesystem", type=str)
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
    "--non-recursive",
    is_flag=True,
    help="Show non-recursive stats instead of recursive",
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
    non_recursive: bool,
    summary: bool,
):
    """
    Query GPFS scan database for directory statistics.

    FILESYSTEM is the name of the filesystem (e.g., asp, cisl, eol, hao).

    \b
    Examples:
        # Show top 50 directories by recursive size
        python -m fs_scans.query_db asp

        # Filter to a specific path prefix
        python -m fs_scans.query_db asp --path-prefix /gpfs/csfs1/asp/username

        # Show only single-owner directories at depth 4+
        python -m fs_scans.query_db asp -d 4 --single-owner

        # Export all directories to TSV
        python -m fs_scans.query_db asp --limit 0 -o asp_dirs.tsv
    """
    # Check database exists
    db_path = get_db_path(filesystem)
    if not db_path.exists():
        console.print(f"[red]Database not found: {db_path}[/red]")
        console.print("Run scan_to_db.py first to import data.")
        raise SystemExit(1)

    console.print(f"[bold]GPFS Scan Database Query[/bold]")
    console.print(f"Database: {db_path}")
    console.print()

    session = get_session(filesystem)

    try:
        if summary:
            # Show summary only
            stats = get_summary(session)
            console.print(f"Total directories: {stats['total_directories']:,}")
            console.print(f"Root directories: {stats['root_directories']:,}")
            console.print(f"Total files (root): {stats['total_files']:,}")
            console.print(f"Total size (root): {format_size(stats['total_size'])}")
            console.print(f"Maximum depth: {stats['max_depth']}")
            return

        # Query directories
        directories = query_directories(
            session,
            min_depth=min_depth,
            max_depth=max_depth,
            single_owner=single_owner,
            owner_id=owner_id,
            path_prefix=path_prefix,
            sort_by=sort_by,
            limit=limit if limit > 0 else None,
        )

        # Output results
        if output:
            write_tsv(directories, output)
        else:
            print_results(directories, show_recursive=not non_recursive)

    finally:
        session.close()


if __name__ == "__main__":
    main()
