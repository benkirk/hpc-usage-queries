#!/usr/bin/env python3
"""
GPFS Scan Database Importer - Multi-Pass Algorithm

Imports GPFS policy scan log files into a SQLite database using a two-pass
algorithm that normalizes directory paths and accumulates statistics.

Pass 1: Directory Discovery (2 phases)
    Phase 1a: Scan log file, collect directory paths
    Phase 1b: Sort by depth, insert into database, build path_to_id lookup

Pass 2: Statistics Accumulation
    - Re-scan log file to accumulate file statistics
    - Batch updates to database for efficiency

The GPFS scan file explicitly lists all directories as separate lines,
so no deduplication or parent directory discovery is needed.
"""

import lzma
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TextIO

import click
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from sqlalchemy import text

from .database import (
    drop_tables,
    extract_filesystem_from_filename,
    get_session,
    init_db,
)
from .models import Directory, DirectoryStats
from .parse_gpfs_scan import LINE_PATTERN, FIELD_PATTERNS

console = Console()


def open_input_file(filepath: Path) -> TextIO:
    """Open input file, handling xz compression if needed."""
    if filepath.suffix == ".xz":
        return lzma.open(filepath, "rt", encoding="utf-8", errors="replace")
    return open(filepath, "r", encoding="utf-8", errors="replace")


def parse_line(line: str) -> dict | None:
    """
    Parse a single log line and extract relevant fields.

    Returns dict with: path, size, user_id, is_dir, atime
    Returns None if line is not a data line.
    """
    match = LINE_PATTERN.match(line)
    if not match:
        return None

    fields_str, path = match.groups()

    # Extract permissions to check if file or directory
    perm_match = FIELD_PATTERNS["permissions"].search(fields_str)
    if not perm_match:
        return None

    permissions = perm_match.group(1)
    is_dir = permissions.startswith("d")

    # Extract other fields
    size_match = FIELD_PATTERNS["size"].search(fields_str)
    user_match = FIELD_PATTERNS["user_id"].search(fields_str)
    atime_match = FIELD_PATTERNS["atime"].search(fields_str)

    if not all([size_match, user_match]):
        return None

    # Parse atime
    atime = None
    if atime_match:
        try:
            atime = datetime.strptime(atime_match.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    return {
        "path": path,
        "size": int(size_match.group(1)),
        "user_id": int(user_match.group(1)),
        "is_dir": is_dir,
        "atime": atime,
    }


def pass1_discover_directories(
    input_file: Path,
    session,
    progress_interval: int = 1_000_000,
) -> dict[str, int]:
    """
    First pass: identify all directories and build hierarchy.

    Since the GPFS scan file explicitly lists all directories, we simply:
    1. Collect directory paths during scan
    2. Sort by depth (ensuring parents exist before children)
    3. Insert and build path_to_id in one pass

    Phase 1a: Scan file, collect directory lines as (path, depth)
    Phase 1b: Sort by depth, insert, and build path_to_id directly

    Args:
        input_file: Path to the log file
        session: SQLAlchemy session
        progress_interval: Report progress every N lines

    Returns:
        Dictionary mapping full paths to dir_id
    """
    console.print("[bold]Pass 1:[/bold] Discovering directories...")

    # Phase 1a: Collect directories from file
    console.print("  [bold]Phase 1a:[/bold] Scanning for directories...")
    dir_entries = []  # (path, depth)
    line_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.fields[dirs]} directories"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"[green]Scanning {input_file.name}...",
            total=None,
            dirs=0,
        )

        with open_input_file(input_file) as f:
            for line in f:
                line_count += 1

                parsed = parse_line(line.rstrip("\n"))
                if not parsed or not parsed["is_dir"]:
                    continue

                path = parsed["path"]
                dir_entries.append((path, path.count("/")))

                if line_count % progress_interval == 0:
                    progress.update(task, dirs=len(dir_entries))

        progress.update(task, dirs=len(dir_entries))

    console.print(f"    Lines scanned: {line_count:,}")
    console.print(f"    Found {len(dir_entries):,} directories")

    # Phase 1b: Sort by depth and insert
    console.print("  [bold]Phase 1b:[/bold] Inserting into database...")
    dir_entries.sort(key=lambda x: x[1])  # sort by depth
    path_to_id = {}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            "[green]Inserting directories...",
            total=len(dir_entries),
        )

        batch_size = 1000
        batch_count = 0

        for path, depth in dir_entries:
            parent_path = os.path.dirname(path)
            parent_id = path_to_id.get(parent_path)  # None for top-level

            entry = Directory(
                parent_id=parent_id,
                name=os.path.basename(path),
                depth=depth,
            )
            session.add(entry)
            session.flush()
            path_to_id[path] = entry.dir_id
            session.add(DirectoryStats(dir_id=entry.dir_id))

            batch_count += 1
            if batch_count % batch_size == 0:
                session.commit()
                progress.update(task, advance=batch_size)

        session.commit()
        progress.update(task, completed=len(dir_entries))

    console.print(f"    Inserted {len(path_to_id):,} directories")
    del dir_entries  # Free memory before Pass 2

    return path_to_id


def flush_updates(session, pending_updates: dict) -> None:
    """
    Apply accumulated deltas to database.

    Args:
        session: SQLAlchemy session
        pending_updates: Dictionary of dir_id -> update data
    """
    for dir_id, upd in pending_updates.items():
        # Determine owner_uid: single uid or NULL for multiple
        if len(upd["uids"]) == 0:
            owner_val = -1  # No files
        elif len(upd["uids"]) == 1:
            owner_val = list(upd["uids"])[0]
        else:
            owner_val = None  # Multiple owners

        session.execute(
            text("""
                UPDATE directory_stats SET
                    file_count_nr = file_count_nr + :nr_count,
                    total_size_nr = total_size_nr + :nr_size,
                    max_atime_nr = CASE
                        WHEN max_atime_nr IS NULL THEN :nr_atime
                        WHEN :nr_atime IS NULL THEN max_atime_nr
                        WHEN :nr_atime > max_atime_nr THEN :nr_atime
                        ELSE max_atime_nr
                    END,
                    file_count_r = file_count_r + :r_count,
                    total_size_r = total_size_r + :r_size,
                    max_atime_r = CASE
                        WHEN max_atime_r IS NULL THEN :r_atime
                        WHEN :r_atime IS NULL THEN max_atime_r
                        WHEN :r_atime > max_atime_r THEN :r_atime
                        ELSE max_atime_r
                    END,
                    owner_uid = CASE
                        WHEN owner_uid = -1 THEN :owner
                        WHEN :owner IS NULL THEN NULL
                        WHEN owner_uid IS NULL THEN NULL
                        WHEN owner_uid != :owner THEN NULL
                        ELSE owner_uid
                    END
                WHERE dir_id = :dir_id
            """),
            {
                "dir_id": dir_id,
                "nr_count": upd["nr_count"],
                "nr_size": upd["nr_size"],
                "nr_atime": upd["nr_atime"],
                "r_count": upd["r_count"],
                "r_size": upd["r_size"],
                "r_atime": upd["r_atime"],
                "owner": owner_val,
            },
        )

    session.commit()


def pass2_accumulate_stats(
    input_file: Path,
    session,
    path_to_id: dict[str, int],
    batch_size: int = 10000,
    progress_interval: int = 1_000_000,
) -> None:
    """
    Second pass: accumulate file statistics into directory_stats.

    Args:
        input_file: Path to the log file
        session: SQLAlchemy session
        path_to_id: Dictionary mapping full paths to dir_id
        batch_size: Number of directories to accumulate before flushing
        progress_interval: Report progress every N lines
    """
    console.print("\n[bold]Pass 2:[/bold] Accumulating statistics...")

    def make_update():
        return {
            "nr_count": 0,
            "nr_size": 0,
            "nr_atime": None,
            "r_count": 0,
            "r_size": 0,
            "r_atime": None,
            "uids": set(),
        }

    pending_updates = defaultdict(make_update)
    line_count = 0
    file_count = 0
    flush_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.fields[files]} files"),
        TextColumn("[yellow]{task.fields[flushes]} flushes"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"[green]Processing {input_file.name}...",
            total=None,
            files=0,
            flushes=0,
        )

        with open_input_file(input_file) as f:
            for line in f:
                line_count += 1

                parsed = parse_line(line.rstrip("\n"))
                if not parsed or parsed["is_dir"]:
                    continue

                file_count += 1
                parent = os.path.dirname(parsed["path"])
                parent_id = path_to_id.get(parent)
                if not parent_id:
                    continue

                size = parsed["size"]
                atime = parsed["atime"]
                user_id = parsed["user_id"]

                # Non-recursive: direct parent only
                upd = pending_updates[parent_id]
                upd["nr_count"] += 1
                upd["nr_size"] += size
                if atime:
                    upd["nr_atime"] = (
                        max(upd["nr_atime"], atime) if upd["nr_atime"] else atime
                    )

                # Recursive: all ancestors
                current = parent
                while current and current != "/":
                    dir_id = path_to_id.get(current)
                    if dir_id:
                        upd = pending_updates[dir_id]
                        upd["r_count"] += 1
                        upd["r_size"] += size
                        if atime:
                            upd["r_atime"] = (
                                max(upd["r_atime"], atime) if upd["r_atime"] else atime
                            )
                        upd["uids"].add(user_id)
                    current = os.path.dirname(current)

                # Flush batch periodically
                if len(pending_updates) >= batch_size:
                    flush_updates(session, pending_updates)
                    flush_count += 1
                    pending_updates.clear()

                if line_count % progress_interval == 0:
                    progress.update(task, files=file_count, flushes=flush_count)

        # Final flush
        if pending_updates:
            flush_updates(session, pending_updates)
            flush_count += 1

        progress.update(task, files=file_count, flushes=flush_count)

    console.print(f"  Lines processed: {line_count:,}")
    console.print(f"  Files counted: {file_count:,}")
    console.print(f"  Database flushes: {flush_count:,}")


@click.command()
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path),
    help="Override database path (default: auto from filename)",
)
@click.option(
    "--filesystem",
    "-f",
    type=str,
    help="Override filesystem name (default: extracted from filename)",
)
@click.option(
    "--batch-size",
    type=int,
    default=10000,
    show_default=True,
    help="Batch size for DB updates",
)
@click.option(
    "--progress-interval",
    "-p",
    type=int,
    default=1_000_000,
    show_default=True,
    help="Report progress every N lines",
)
@click.option(
    "--replace",
    is_flag=True,
    help="Drop and recreate tables before import",
)
@click.option(
    "--echo",
    is_flag=True,
    help="Echo SQL statements (for debugging)",
)
def main(
    input_file: Path,
    db_path: Path | None,
    filesystem: str | None,
    batch_size: int,
    progress_interval: int,
    replace: bool,
    echo: bool,
):
    """
    Import GPFS policy scan log files into SQLite database.

    Uses a two-pass algorithm:
      1. First pass discovers all directories and builds the hierarchy
      2. Second pass accumulates file statistics

    INPUT_FILE can be a plain text log file or an xz-compressed file (.xz).

    Database is stored in fs_scans/<filesystem>.db by default.
    """
    console.print("[bold]GPFS Scan Database Importer[/bold]")
    console.print(f"Input: {input_file}")
    console.print()

    # Determine filesystem name
    if filesystem is None:
        filesystem = extract_filesystem_from_filename(input_file.name)
        if filesystem is None:
            console.print(
                "[red]Could not extract filesystem name from filename.[/red]"
            )
            console.print("Use --filesystem to specify it manually.")
            sys.exit(1)

    console.print(f"Filesystem: {filesystem}")

    # Initialize database
    if replace:
        console.print("[yellow]Dropping existing tables...[/yellow]")
        drop_tables(filesystem, echo=echo)

    engine = init_db(filesystem, echo=echo)
    session = get_session(filesystem, engine=engine)

    console.print(f"Database: {engine.url}")
    console.print()

    try:
        # Pass 1: Discover directories
        path_to_id = pass1_discover_directories(
            input_file, session, progress_interval
        )

        # Pass 2: Accumulate statistics
        pass2_accumulate_stats(
            input_file, session, path_to_id, batch_size, progress_interval
        )

        console.print("\n[green bold]Import complete![/green bold]")

    except Exception as e:
        console.print(f"\n[red]Error during import: {e}[/red]")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
