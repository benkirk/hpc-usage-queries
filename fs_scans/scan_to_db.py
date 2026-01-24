#!/usr/bin/env python3
"""
GPFS Scan Database Importer - Multi-Pass Algorithm

Imports GPFS policy scan log files into a SQLite database using a multi-pass
algorithm that normalizes directory paths and accumulates statistics.

Pass 1: Directory Discovery (2 phases)
    Phase 1a: Scan log file, collect directory paths
    Phase 1b: Sort by depth, insert into database, build path_to_id lookup

Pass 2: Statistics Accumulation (2 phases)
    Phase 2a: Re-scan log file, accumulate non-recursive stats only
    Phase 2b: Bottom-up SQL aggregation to compute recursive stats

The GPFS scan file explicitly lists all directories as separate lines,
so no deduplication or parent directory discovery is needed.
"""

import multiprocessing as mp
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from queue import Empty
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
from sqlalchemy import insert, select, text

from .database import (
    drop_tables,
    extract_filesystem_from_filename,
    get_session,
    init_db,
)
from .models import Directory, DirectoryStats
from .parse_gpfs_scan import FIELD_PATTERNS

# Extended LINE_PATTERN that captures inode and fileset_id for unique identification
# Format: <thread> inode fileset_id snapshot  fields -- /path
LINE_PATTERN = re.compile(
    r"^<\d+>\s+(\d+)\s+(\d+)\s+\d+\s+"  # <thread> inode fileset_id snapshot
    r"(.+?)\s+--\s+(.+)$"  # fields -- path
)

console = Console()


def create_progress_bar(
    extra_columns: list[TextColumn] | None = None,
    show_rate: bool = True,
) -> Progress:
    """
    Create a standardized progress bar.

    Args:
        extra_columns: Additional TextColumn instances to display
        show_rate: Whether to show items/sec rate column

    Returns:
        Configured Progress instance
    """
    columns = [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
    ]
    if extra_columns:
        columns.extend(extra_columns)
    if show_rate:
        columns.append(TextColumn("[magenta]{task.fields[rate]} items/sec"))
    columns.append(TimeElapsedColumn())
    return Progress(*columns, console=console)


def open_input_file(filepath: Path) -> TextIO:
    """Open input file for reading."""
    return open(filepath, "r", encoding="utf-8", errors="replace")


def parse_line(line: str) -> dict | None:
    """
    Parse a single log line and extract relevant fields.

    Returns dict with: inode, fileset_id, path, size, user_id, is_dir, atime
    Returns None if line is not a data line.
    """
    match = LINE_PATTERN.match(line)
    if not match:
        return None

    inode, fileset_id, fields_str, path = match.groups()

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
        "inode": int(inode),
        "fileset_id": int(fileset_id),
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
    num_workers: int = 1,
) -> tuple[dict[str, int], dict]:
    """
    First pass: identify all directories and build hierarchy.

    Uses SQLite staging table to avoid holding all directories in memory:
    Phase 1a: Stream directories to SQLite staging table (parallelizable)
    Phase 1b: SELECT from staging ORDER BY depth, insert to directories table

    Args:
        input_file: Path to the log file
        session: SQLAlchemy session
        progress_interval: Report progress every N lines
        num_workers: Number of worker processes for parsing (Phase 1a only)

    Returns:
        Tuple of:
        - Dictionary mapping full paths to dir_id
        - Metadata dict with total_lines, dir_count, inferred file_count
    """
    if num_workers > 1:
        console.print(f"[bold]Pass 1:[/bold] Discovering directories (parallel, {num_workers} workers)...")
    else:
        console.print("[bold]Pass 1:[/bold] Discovering directories...")

    # Create staging table
    session.execute(
        text("""
        CREATE TABLE IF NOT EXISTS staging_dirs (
            inode INTEGER NOT NULL,
            fileset_id INTEGER NOT NULL,
            depth INTEGER NOT NULL,
            path TEXT NOT NULL,
            PRIMARY KEY (fileset_id, inode)
        )
    """)
    )
    session.execute(
        text("CREATE INDEX IF NOT EXISTS idx_staging_depth ON staging_dirs(depth)")
    )
    session.commit()

    # Phase 1a: Stream directories to staging table
    console.print("  [bold]Phase 1a:[/bold] Scanning for directories...")
    line_count = 0
    dir_count = 0
    BATCH_SIZE = 10000
    CHUNK_SIZE = 5000
    batch = []
    start_time = time.time()

    with create_progress_bar(
        extra_columns=[TextColumn("[cyan]{task.fields[dirs]} directories")]
    ) as progress:
        task = progress.add_task(
            f"[green]Scanning {input_file.name}...",
            total=None,
            dirs="0",
            rate="0",
        )

        def update_progress():
            elapsed = time.time() - start_time
            rate = int(line_count / elapsed) if elapsed > 0 else 0
            progress.update(task, dirs=f"{dir_count:,}", rate=f"{rate:,}")

        def process_parsed_dirs(parsed_list):
            """Process a list of parsed directory entries."""
            nonlocal dir_count
            for parsed in parsed_list:
                path = parsed["path"]
                batch.append(
                    {
                        "inode": parsed["inode"],
                        "fileset_id": parsed["fileset_id"],
                        "depth": path.count("/"),
                        "path": path,
                    }
                )
                dir_count += 1

        def flush_batch():
            """Flush batch to staging table."""
            if batch:
                session.execute(
                    text("""
                    INSERT OR IGNORE INTO staging_dirs (inode, fileset_id, depth, path)
                    VALUES (:inode, :fileset_id, :depth, :path)
                """),
                    batch,
                )
                session.commit()
                batch.clear()

        if num_workers > 1:
            # Parallel Phase 1a
            input_queue = mp.Queue(maxsize=num_workers * 2)
            output_queue = mp.Queue()

            workers = [
                mp.Process(
                    target=worker_parse_lines,
                    args=(input_queue, output_queue, "dirs"),
                )
                for _ in range(num_workers)
            ]
            for w in workers:
                w.start()

            chunks_sent = 0
            chunks_received = 0
            chunk = []

            with open_input_file(input_file) as f:
                for line in f:
                    line_count += 1
                    chunk.append(line)

                    if len(chunk) >= CHUNK_SIZE:
                        input_queue.put(chunk)
                        chunks_sent += 1
                        chunk = []

                        # Process available results (non-blocking)
                        while True:
                            try:
                                results = output_queue.get_nowait()
                                chunks_received += 1
                                process_parsed_dirs(results)

                                if len(batch) >= BATCH_SIZE:
                                    flush_batch()
                                    update_progress()
                            except Empty:
                                break

                    if line_count % progress_interval == 0:
                        update_progress()

                # Send remaining chunk
                if chunk:
                    input_queue.put(chunk)
                    chunks_sent += 1

            # Send sentinel to workers
            for _ in range(num_workers):
                input_queue.put(None)

            # Wait for remaining results
            while chunks_received < chunks_sent:
                try:
                    results = output_queue.get(timeout=5)
                    chunks_received += 1
                    process_parsed_dirs(results)

                    if len(batch) >= BATCH_SIZE:
                        flush_batch()
                except Empty:
                    continue

            # Wait for workers to finish
            for w in workers:
                w.join()

        else:
            # Single-threaded Phase 1a
            with open_input_file(input_file) as f:
                for line in f:
                    line_count += 1

                    parsed = parse_line(line.rstrip("\n"))
                    if not parsed or not parsed["is_dir"]:
                        continue

                    process_parsed_dirs([parsed])

                    if len(batch) >= BATCH_SIZE:
                        flush_batch()
                        update_progress()

                    if line_count % progress_interval == 0:
                        update_progress()

        # Final flush
        flush_batch()
        update_progress()

    console.print(f"    Lines scanned: {line_count:,}")
    console.print(f"    Found {dir_count:,} directories")

    # Estimate file count (excluding headers and directories)
    estimated_files = max(0, line_count - dir_count - 50)
    console.print(f"    Inferred ~{estimated_files:,} files")

    # Phase 1b: Read from staging ordered by depth, insert to directories table
    # Optimized: Process level-by-level using bulk inserts
    console.print("  [bold]Phase 1b:[/bold] Inserting into database (bulk optimized)...")
    path_to_id = {}

    # Get distinct depths to process level-by-level
    depths = [
        r[0]
        for r in session.execute(
            text("SELECT DISTINCT depth FROM staging_dirs ORDER BY depth")
        )
    ]

    with create_progress_bar(show_rate=False) as progress:
        task = progress.add_task(
            "[green]Inserting directories...",
            total=dir_count,
        )

        insert_batch_size = 10000

        for depth in depths:
            # Fetch paths for this depth
            paths = [
                r[0]
                for r in session.execute(
                    text("SELECT path FROM staging_dirs WHERE depth = :d"), {"d": depth}
                )
            ]

            if not paths:
                continue

            # 1. Prepare and Bulk Insert Directories
            dir_inserts = []
            for p in paths:
                parent_path = os.path.dirname(p)
                parent_id = path_to_id.get(parent_path)
                dir_inserts.append(
                    {
                        "parent_id": parent_id,
                        "name": os.path.basename(p),
                        "depth": depth,
                    }
                )

            for i in range(0, len(dir_inserts), insert_batch_size):
                session.execute(insert(Directory), dir_inserts[i : i + insert_batch_size])
            session.commit()

            # 2. Retrieve assigned IDs and update map
            # Fetch (parent_id, name) -> dir_id for the current depth
            rows = session.execute(
                select(Directory.dir_id, Directory.parent_id, Directory.name).where(
                    Directory.depth == depth
                )
            ).all()

            # Create a lookup for matching (parent_id, name) -> dir_id
            # Note: parent_id can be None for root directories
            lookup = {(r.parent_id, r.name): r.dir_id for r in rows}

            # Update path_to_id and prepare stats
            stats_inserts = []
            for p in paths:
                parent_path = os.path.dirname(p)
                parent_id = path_to_id.get(parent_path)
                name = os.path.basename(p)

                dir_id = lookup.get((parent_id, name))
                if dir_id:
                    path_to_id[p] = dir_id
                    stats_inserts.append({"dir_id": dir_id})
                else:
                    # Should not happen given database consistency
                    console.print(f"[red]Warning: Could not find ID for {p}[/red]")

            # 3. Bulk Insert Stats
            if stats_inserts:
                for i in range(0, len(stats_inserts), insert_batch_size):
                    session.execute(
                        insert(DirectoryStats), stats_inserts[i : i + insert_batch_size]
                    )
                session.commit()

            progress.update(task, advance=len(paths))

    console.print(f"    Inserted {len(path_to_id):,} directories")

    # Cleanup staging table
    session.execute(text("DROP TABLE IF EXISTS staging_dirs"))
    session.commit()

    # Return path_to_id and metadata for Phase 2
    metadata = {
        "total_lines": line_count,
        "dir_count": dir_count,
        "estimated_files": estimated_files,
    }

    return path_to_id, metadata


def make_empty_update() -> dict:
    """Create an empty update dictionary for non-recursive stats accumulation."""
    return {
        "nr_count": 0,
        "nr_size": 0,
        "nr_atime": None,
        "first_uid": None,  # None = not set, -999 = multiple owners, else = single UID
    }


def accumulate_file_stats_nr(
    parsed: dict,
    path_to_id: dict[str, int],
    pending_updates: dict,
) -> bool:
    """
    Accumulate non-recursive stats for a single file into pending_updates.

    Only updates the direct parent directory. Recursive stats are computed
    later via bottom-up SQL aggregation in pass2b_aggregate_recursive_stats().

    Args:
        parsed: Parsed file entry dict
        path_to_id: Directory path to ID mapping
        pending_updates: Accumulator dictionary (modified in place)

    Returns:
        True if file was processed, False if parent not found
    """
    parent = os.path.dirname(parsed["path"])
    parent_id = path_to_id.get(parent)
    if not parent_id:
        return False

    size = parsed["size"]
    atime = parsed["atime"]
    user_id = parsed["user_id"]

    # Non-recursive: direct parent only
    upd = pending_updates[parent_id]
    upd["nr_count"] += 1
    upd["nr_size"] += size
    if atime:
        upd["nr_atime"] = max(upd["nr_atime"], atime) if upd["nr_atime"] else atime

    # Simplified UID tracking: track first_uid, mark as -999 if multiple
    if upd["first_uid"] is None:
        upd["first_uid"] = user_id
    elif upd["first_uid"] != user_id and upd["first_uid"] != -999:
        upd["first_uid"] = -999  # Sentinel for "multiple owners"

    return True


def flush_nr_updates(session, pending_updates: dict) -> None:
    """
    Apply accumulated non-recursive deltas to database using bulk execution.

    Args:
        session: SQLAlchemy session
        pending_updates: Dictionary of dir_id -> update data (nr_* fields only)
    """
    if not pending_updates:
        return

    # Prepare batch parameters
    params_batch = []
    for dir_id, upd in pending_updates.items():
        # Determine owner_uid: single uid or NULL for multiple
        first_uid = upd["first_uid"]
        if first_uid is None:
            owner_val = -1  # No files seen
        elif first_uid == -999:
            owner_val = None  # Multiple owners
        else:
            owner_val = first_uid

        params_batch.append(
            {
                "dir_id": dir_id,
                "nr_count": upd["nr_count"],
                "nr_size": upd["nr_size"],
                "nr_atime": upd["nr_atime"],
                "owner": owner_val,
            }
        )

    # Execute bulk update
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
                owner_uid = CASE
                    WHEN owner_uid = -1 THEN :owner
                    WHEN :owner IS NULL THEN NULL
                    WHEN owner_uid IS NULL THEN NULL
                    WHEN owner_uid != :owner THEN NULL
                    ELSE owner_uid
                END
            WHERE dir_id = :dir_id
        """),
        params_batch,
    )

    session.commit()


def pass2a_nonrecursive_stats(
    input_file: Path,
    session,
    path_to_id: dict[str, int],
    batch_size: int = 10000,
    progress_interval: int = 1_000_000,
    total_lines: int | None = None,
    num_workers: int = 1,
) -> None:
    """
    Phase 2a: accumulate non-recursive file statistics into directory_stats.

    Only updates non-recursive stats (file_count_nr, total_size_nr, max_atime_nr).
    Recursive stats are computed in pass2b_aggregate_recursive_stats().

    Args:
        input_file: Path to the log file
        session: SQLAlchemy session
        path_to_id: Dictionary mapping full paths to dir_id
        batch_size: Number of directories to accumulate before flushing
        progress_interval: Report progress every N lines
        total_lines: Total line count from Phase 1 (for determinate progress bar)
        num_workers: Number of worker processes (1 = single-threaded)
    """
    if num_workers > 1:
        console.print(f"\n[bold]Pass 2:[/bold] Accumulating statistics (parallel, {num_workers} workers)...")
    else:
        console.print("\n[bold]Pass 2:[/bold] Accumulating statistics...")

    console.print("  [bold]Phase 2a:[/bold] Accumulating non-recursive stats...")

    pending_updates = defaultdict(make_empty_update)
    line_count = 0
    file_count = 0
    flush_count = 0
    start_time = time.time()
    CHUNK_SIZE = 5000

    def do_flush():
        nonlocal flush_count
        if pending_updates:
            flush_nr_updates(session, pending_updates)
            flush_count += 1
            pending_updates.clear()

    def process_parsed_file(parsed):
        nonlocal file_count
        file_count += 1
        accumulate_file_stats_nr(parsed, path_to_id, pending_updates)

    def update_progress_bar(progress, task):
        elapsed = time.time() - start_time
        rate = int(line_count / elapsed) if elapsed > 0 else 0
        progress.update(
            task,
            completed=line_count,
            files=f"{file_count:,}",
            rate=f"{rate:,}",
        )

    with create_progress_bar(
        extra_columns=[
            TextColumn("[cyan]{task.fields[files]} files"),
        ]
    ) as progress:
        task = progress.add_task(
            f"[green]Processing {input_file.name}...",
            total=total_lines,
            files="0",
            rate="0",
        )

        if num_workers > 1:
            # Parallel mode
            input_queue = mp.Queue(maxsize=num_workers * 2)
            output_queue = mp.Queue()

            workers = [
                mp.Process(
                    target=worker_parse_lines,
                    args=(input_queue, output_queue, "files"),
                )
                for _ in range(num_workers)
            ]
            for w in workers:
                w.start()

            chunks_sent = 0
            chunks_received = 0
            chunk = []

            with open_input_file(input_file) as f:
                for line in f:
                    line_count += 1
                    chunk.append(line)

                    if len(chunk) >= CHUNK_SIZE:
                        input_queue.put(chunk)
                        chunks_sent += 1
                        chunk = []

                        # Process available results (non-blocking)
                        while True:
                            try:
                                results = output_queue.get_nowait()
                                chunks_received += 1
                                for parsed in results:
                                    process_parsed_file(parsed)

                                if len(pending_updates) >= batch_size:
                                    do_flush()
                            except Empty:
                                break

                    if line_count % progress_interval == 0:
                        update_progress_bar(progress, task)

                # Send remaining chunk
                if chunk:
                    input_queue.put(chunk)
                    chunks_sent += 1

            # Send sentinel to workers
            for _ in range(num_workers):
                input_queue.put(None)

            # Wait for remaining results
            while chunks_received < chunks_sent:
                try:
                    results = output_queue.get(timeout=5)
                    chunks_received += 1
                    for parsed in results:
                        process_parsed_file(parsed)

                    if len(pending_updates) >= batch_size:
                        do_flush()
                except Empty:
                    continue

            # Wait for workers to finish
            for w in workers:
                w.join()

        else:
            # Single-threaded mode
            with open_input_file(input_file) as f:
                for line in f:
                    line_count += 1

                    parsed = parse_line(line.rstrip("\n"))
                    if not parsed or parsed["is_dir"]:
                        continue

                    process_parsed_file(parsed)

                    if len(pending_updates) >= batch_size:
                        do_flush()

                    if line_count % progress_interval == 0:
                        update_progress_bar(progress, task)

        # Final flush
        do_flush()
        update_progress_bar(progress, task)

    console.print(f"    Lines processed: {line_count:,}")
    console.print(f"    Files counted: {file_count:,}")
    console.print(f"    Database flushes: {flush_count:,}")


def pass2b_aggregate_recursive_stats(session) -> None:
    """
    Phase 2b: compute recursive stats via bottom-up SQL aggregation.

    Processes directories by depth, from deepest to shallowest.
    Each directory's recursive stats = its non-recursive stats + sum of children's recursive stats.

    Optimized to use SQLite 'UPDATE FROM' (requires SQLite 3.33+).
    """
    console.print("  [bold]Phase 2b:[/bold] Computing recursive statistics...")

    # Get max depth
    max_depth = session.execute(text("SELECT MAX(depth) FROM directories")).scalar() or 0

    console.print(f"    Max directory depth: {max_depth}")

    with create_progress_bar(show_rate=False) as progress:
        task = progress.add_task(
            "[green]Aggregating by depth...",
            total=max_depth,
        )

        # Process from leaves (max_depth) down to root (depth=1)
        for depth in range(max_depth, 0, -1):
            # 1. Initialize recursive stats with non-recursive stats for this level
            #    (This covers leaf nodes and prepares parents for accumulation)
            session.execute(
                text("""
                UPDATE directory_stats
                SET
                    file_count_r = file_count_nr,
                    total_size_r = total_size_nr,
                    max_atime_r = max_atime_nr
                WHERE dir_id IN (SELECT dir_id FROM directories WHERE depth = :depth)
                """),
                {"depth": depth},
            )

            # 2. Accumulate stats from children (depth + 1) using UPDATE FROM
            #    (Only updates parents that actually have children)
            session.execute(
                text("""
                WITH child_agg AS (
                    SELECT
                        d.parent_id,
                        SUM(s.file_count_r) as sum_files,
                        SUM(s.total_size_r) as sum_size,
                        MAX(s.max_atime_r) as max_atime,
                        -- Owner Aggregation:
                        -- Check if any child has a NULL owner (conflict)
                        MAX(CASE WHEN s.owner_uid IS NULL THEN 1 ELSE 0 END) as has_conflict,
                        -- Count distinct valid owners (ignoring -1/no-files)
                        COUNT(DISTINCT CASE WHEN s.owner_uid >= 0 THEN s.owner_uid END) as distinct_valid_owners,
                        -- Get the potential common owner (if count is 1)
                        MAX(CASE WHEN s.owner_uid >= 0 THEN s.owner_uid END) as common_owner
                    FROM directories d
                    JOIN directory_stats s ON d.dir_id = s.dir_id
                    WHERE d.depth = :child_depth
                    GROUP BY d.parent_id
                )
                UPDATE directory_stats
                SET
                    file_count_r = file_count_r + agg.sum_files,
                    total_size_r = total_size_r + agg.sum_size,
                    max_atime_r = MAX(COALESCE(max_atime_r, 0), COALESCE(agg.max_atime, 0)),
                    owner_uid = CASE
                        -- Already conflicted -> stay conflicted
                        WHEN owner_uid IS NULL THEN NULL

                        -- Direct files exist (owner_uid >= 0) -> check for conflict with children
                        WHEN owner_uid >= 0 THEN
                             CASE
                                WHEN agg.has_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_owners > 0 AND agg.common_owner != owner_uid THEN NULL
                                ELSE owner_uid
                             END

                        -- No direct files (-1) -> inherit from children
                        ELSE -- owner_uid == -1
                             CASE
                                WHEN agg.has_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_owners > 1 THEN NULL
                                WHEN agg.distinct_valid_owners = 1 THEN agg.common_owner
                                ELSE -1 -- Still no owner seen
                             END
                    END
                FROM child_agg AS agg
                WHERE directory_stats.dir_id = agg.parent_id
                """),
                {"child_depth": depth + 1},
            )

            session.commit()
            progress.update(task, advance=1)

    console.print(f"    Processed {max_depth} depth levels")


def worker_parse_lines(
    input_queue: mp.Queue,
    output_queue: mp.Queue,
    filter_type: str = "files",
) -> None:
    """
    Worker process: parse lines from input queue.

    Reads chunks of lines from input_queue, parses them, and sends results
    to output_queue. Exits when it receives None sentinel.

    Args:
        input_queue: Queue to receive line chunks from
        output_queue: Queue to send parsed results to
        filter_type: "files" (default), "dirs", or "all"
    """
    while True:
        try:
            chunk = input_queue.get(timeout=1)
        except Empty:
            continue

        if chunk is None:  # Sentinel to stop worker
            break

        results = []
        for line in chunk:
            parsed = parse_line(line.rstrip("\n"))
            if parsed:
                if filter_type == "all":
                    results.append(parsed)
                elif filter_type == "dirs" and parsed["is_dir"]:
                    results.append(parsed)
                elif filter_type == "files" and not parsed["is_dir"]:
                    results.append(parsed)

        output_queue.put(results)


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
    default=50000,
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
    "--workers",
    "-w",
    type=int,
    default=1,
    show_default=True,
    help="Number of worker processes for parsing (default: 1, single-threaded)",
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
    workers: int,
    echo: bool,
):
    """
    Import GPFS policy scan log files into SQLite database.

    Uses a multi-pass algorithm:
      1. Pass 1: Discover directories and build the hierarchy
      2. Pass 2a: Accumulate non-recursive file statistics
      3. Pass 2b: Compute recursive stats via bottom-up SQL aggregation

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

    overall_start = time.time()
    try:
        # Pass 1: Discover directories
        path_to_id, metadata = pass1_discover_directories(
            input_file, session, progress_interval, num_workers=workers
        )

        # Pass 2a: Accumulate non-recursive stats
        pass2a_nonrecursive_stats(
            input_file,
            session,
            path_to_id,
            batch_size,
            progress_interval,
            total_lines=metadata["total_lines"],
            num_workers=workers,
        )

        # Pass 2b: Compute recursive stats via bottom-up aggregation
        pass2b_aggregate_recursive_stats(session)

        overall_duration = time.time() - overall_start

        # Get DB file size
        db_file = Path(f"fs_scans/{filesystem}.db")
        size_str = "unknown"
        if db_file.exists():
            size_bytes = db_file.stat().st_size
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if size_bytes < 1024:
                    size_str = f"{size_bytes:.2f} {unit}"
                    break
                size_bytes /= 1024

        console.print(f"\n[green bold]Import complete![/green bold]")
        console.print(f"[bold]Total runtime:[/bold] {overall_duration:.2f} seconds")
        console.print(f"[bold]Database size:[/bold] {size_str}")

    except Exception as e:
        console.print(f"\n[red]Error during import: {e}[/red]")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
