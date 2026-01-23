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
from sqlalchemy import text

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


def open_input_file(filepath: Path) -> TextIO:
    """Open input file, handling xz compression if needed."""
    if filepath.suffix == ".xz":
        return lzma.open(filepath, "rt", encoding="utf-8", errors="replace")
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
) -> tuple[dict[str, int], dict]:
    """
    First pass: identify all directories and build hierarchy.

    Uses SQLite staging table to avoid holding all directories in memory:
    Phase 1a: Stream directories to SQLite staging table
    Phase 1b: SELECT from staging ORDER BY depth, insert to directories table

    Args:
        input_file: Path to the log file
        session: SQLAlchemy session
        progress_interval: Report progress every N lines

    Returns:
        Tuple of:
        - Dictionary mapping full paths to dir_id
        - Metadata dict with total_lines, dir_count, inferred file_count
    """
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
    batch = []

    start_time = time.time()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.fields[dirs]} directories"),
        TextColumn("[magenta]{task.fields[rate]} items/sec"),
        TimeElapsedColumn(),
        console=console,
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

        with open_input_file(input_file) as f:
            for line in f:
                line_count += 1

                parsed = parse_line(line.rstrip("\n"))
                if not parsed or not parsed["is_dir"]:
                    continue

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

                if len(batch) >= BATCH_SIZE:
                    session.execute(
                        text("""
                        INSERT OR IGNORE INTO staging_dirs (inode, fileset_id, depth, path)
                        VALUES (:inode, :fileset_id, :depth, :path)
                    """),
                        batch,
                    )
                    session.commit()
                    batch.clear()
                    update_progress()

                if line_count % progress_interval == 0:
                    update_progress()

        # Flush remaining batch
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

        update_progress()

    console.print(f"    Lines scanned: {line_count:,}")
    console.print(f"    Found {dir_count:,} directories")

    # Estimate file count (excluding headers and directories)
    # Headers are typically < 50 lines
    estimated_files = max(0, line_count - dir_count - 50)
    console.print(f"    Inferred ~{estimated_files:,} files")

    # Phase 1b: Read from staging ordered by depth, insert to directories table
    console.print("  [bold]Phase 1b:[/bold] Inserting into database...")
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
            total=dir_count,
        )

        batch_size = 1000
        batch_count = 0

        # Stream from staging table ordered by depth
        cursor = session.execute(
            text("SELECT inode, fileset_id, depth, path FROM staging_dirs ORDER BY depth")
        )

        for row in cursor:
            inode, fileset_id, depth, path = row
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
        progress.update(task, completed=dir_count)

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
    total_lines: int | None = None,
) -> None:
    """
    Second pass: accumulate file statistics into directory_stats.

    Args:
        input_file: Path to the log file
        session: SQLAlchemy session
        path_to_id: Dictionary mapping full paths to dir_id
        batch_size: Number of directories to accumulate before flushing
        progress_interval: Report progress every N lines
        total_lines: Total line count from Phase 1 (for determinate progress bar)
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
    start_time = time.time()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.fields[files]} files"),
        TextColumn("[yellow]{task.fields[flushes]} flushes"),
        TextColumn("[magenta]{task.fields[rate]} items/sec"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"[green]Processing {input_file.name}...",
            total=total_lines,  # Now determinate if total_lines is known
            files="0",
            flushes="0",
            rate="0",
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
                    elapsed = time.time() - start_time
                    rate = int(line_count / elapsed) if elapsed > 0 else 0
                    progress.update(
                        task,
                        completed=line_count,
                        files=f"{file_count:,}",
                        flushes=f"{flush_count:,}",
                        rate=f"{rate:,}",
                    )

        # Final flush
        if pending_updates:
            flush_updates(session, pending_updates)
            flush_count += 1

        elapsed = time.time() - start_time
        rate = int(line_count / elapsed) if elapsed > 0 else 0
        progress.update(
            task,
            completed=line_count,
            files=f"{file_count:,}",
            flushes=f"{flush_count:,}",
            rate=f"{rate:,}",
        )

    console.print(f"  Lines processed: {line_count:,}")
    console.print(f"  Files counted: {file_count:,}")
    console.print(f"  Database flushes: {flush_count:,}")


def worker_parse_lines(input_queue: mp.Queue, output_queue: mp.Queue) -> None:
    """
    Worker process: parse lines from input queue.

    Reads chunks of lines from input_queue, parses them, and sends results
    to output_queue. Exits when it receives None sentinel.
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
            if parsed and not parsed["is_dir"]:  # Only files
                results.append(parsed)

        output_queue.put(results)


def pass2_accumulate_stats_parallel(
    input_file: Path,
    session,
    path_to_id: dict[str, int],
    batch_size: int = 10000,
    progress_interval: int = 1_000_000,
    total_lines: int | None = None,
    num_workers: int = 1,
) -> None:
    """
    Second pass with parallel parsing: accumulate file statistics into directory_stats.

    Uses multiprocessing to parallelize line parsing (CPU-bound regex work).
    Main process handles DB writes (SQLite single-writer constraint).

    Args:
        input_file: Path to the log file
        session: SQLAlchemy session
        path_to_id: Dictionary mapping full paths to dir_id
        batch_size: Number of directories to accumulate before flushing
        progress_interval: Report progress every N lines
        total_lines: Total line count from Phase 1 (for determinate progress bar)
        num_workers: Number of worker processes for parsing
    """
    if num_workers <= 1:
        # Fall back to single-threaded implementation
        return pass2_accumulate_stats(
            input_file, session, path_to_id, batch_size, progress_interval, total_lines
        )

    console.print(f"\n[bold]Pass 2:[/bold] Accumulating statistics (parallel, {num_workers} workers)...")

    if input_file.suffix == ".xz":
        console.print("  [yellow]Note: xz decompression may bottleneck parallel parsing[/yellow]")

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
    start_time = time.time()

    # Set up queues and workers
    CHUNK_SIZE = 5000  # Lines per chunk sent to workers
    input_queue = mp.Queue(maxsize=num_workers * 2)
    output_queue = mp.Queue()

    workers = [
        mp.Process(target=worker_parse_lines, args=(input_queue, output_queue))
        for _ in range(num_workers)
    ]
    for w in workers:
        w.start()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.fields[files]} files"),
        TextColumn("[yellow]{task.fields[flushes]} flushes"),
        TextColumn("[magenta]{task.fields[rate]} items/sec"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"[green]Processing {input_file.name}...",
            total=total_lines,
            files="0",
            flushes="0",
            rate="0",
        )

        # Producer: read file and send chunks to workers
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

                    # Process any available results (non-blocking)
                    while True:
                        try:
                            results = output_queue.get_nowait()
                            chunks_received += 1

                            for parsed in results:
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

                        except Empty:
                            break

                if line_count % progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = int(line_count / elapsed) if elapsed > 0 else 0
                    progress.update(
                        task,
                        completed=line_count,
                        files=f"{file_count:,}",
                        flushes=f"{flush_count:,}",
                        rate=f"{rate:,}",
                    )

            # Send remaining chunk
            if chunk:
                input_queue.put(chunk)
                chunks_sent += 1

        # Send sentinel to workers to signal completion
        for _ in range(num_workers):
            input_queue.put(None)

        # Wait for all results
        while chunks_received < chunks_sent:
            try:
                results = output_queue.get(timeout=5)
                chunks_received += 1

                for parsed in results:
                    file_count += 1
                    parent = os.path.dirname(parsed["path"])
                    parent_id = path_to_id.get(parent)
                    if not parent_id:
                        continue

                    size = parsed["size"]
                    atime = parsed["atime"]
                    user_id = parsed["user_id"]

                    upd = pending_updates[parent_id]
                    upd["nr_count"] += 1
                    upd["nr_size"] += size
                    if atime:
                        upd["nr_atime"] = (
                            max(upd["nr_atime"], atime) if upd["nr_atime"] else atime
                        )

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

                    if len(pending_updates) >= batch_size:
                        flush_updates(session, pending_updates)
                        flush_count += 1
                        pending_updates.clear()

            except Empty:
                continue

        # Wait for workers to finish
        for w in workers:
            w.join()

        # Final flush
        if pending_updates:
            flush_updates(session, pending_updates)
            flush_count += 1

        elapsed = time.time() - start_time
        rate = int(line_count / elapsed) if elapsed > 0 else 0
        progress.update(
            task,
            completed=line_count,
            files=f"{file_count:,}",
            flushes=f"{flush_count:,}",
            rate=f"{rate:,}",
        )

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
        path_to_id, metadata = pass1_discover_directories(
            input_file, session, progress_interval
        )

        # Pass 2: Accumulate statistics (with known total for progress bar)
        pass2_accumulate_stats_parallel(
            input_file,
            session,
            path_to_id,
            batch_size,
            progress_interval,
            total_lines=metadata["total_lines"],
            num_workers=workers,
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
