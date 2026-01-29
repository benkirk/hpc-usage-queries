"""Parser-agnostic filesystem scan importer.

This module provides the multi-pass import algorithm that works with any
filesystem parser (GPFS, Lustre, POSIX, etc.).

Multi-Pass Algorithm:
    Pass 1: Directory Discovery
        Phase 1a: Stream directories to SQLite staging table (parallelizable)
        Phase 1b: Sort by depth, insert into database, build path_to_id lookup

    Pass 2: Statistics Accumulation
        Phase 2a: Re-scan log file, accumulate non-recursive stats only
        Phase 2b: Bottom-up SQL aggregation to compute recursive stats

    Pass 3: Summary Tables
        - Resolve UIDs to usernames
        - Pre-aggregate per-owner statistics
        - Record scan metadata
"""

import multiprocessing as mp
import os
import pwd
import sys
import time
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Generator, TextIO

from rich.progress import TextColumn
from sqlalchemy import func, insert, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..cli.common import console, create_progress_bar, format_size
from ..core.database import (
    extract_filesystem_from_filename,
    extract_scan_timestamp,
    get_db_path,
    get_session,
    init_db,
    set_data_dir,
    drop_tables,
)
from ..core.models import Directory, DirectoryStats, OwnerSummary, ScanMetadata, UserInfo
from ..parsers.base import FilesystemParser


def open_input_file(filepath: Path) -> TextIO:
    """Open input file for reading with a large buffer."""
    # Use 8MB buffer to minimize syscalls
    return open(filepath, "r", encoding="utf-8", errors="replace", buffering=8 * 1024 * 1024)


def _worker_parse_chunk(args: tuple[list[str], str, FilesystemParser]) -> tuple[Any, int]:
    """
    Worker function to parse a chunk of lines using the provided parser.

    Args:
        args: Tuple of (lines_chunk, filter_type, parser)

    Returns:
        Tuple of (results, count of lines processed)
        - If filter_type="files": results is dict[parent_path, stats] (Aggregated)
        - If filter_type="dirs"/"all": results is list[ParsedEntry] (Raw)
    """
    chunk, filter_type, parser = args

    if filter_type == "files":
        # Map-Reduce Optimization: Aggregate stats locally in worker
        # This reduces IPC traffic and main thread load by ~1000x
        results = {}

        for line in chunk:
            parsed = parser.parse_line(line.rstrip("\n"))
            if parsed and not parsed.is_dir:
                parent = os.path.dirname(parsed.path)
                if parent not in results:
                    results[parent] = {
                        "nr_count": 0,
                        "nr_size": 0,
                        "nr_atime": None,
                        "first_uid": None
                    }
                stats = results[parent]

                # Accumulate count and size
                stats["nr_count"] += 1
                stats["nr_size"] += parsed.allocated

                # Accumulate atime
                if parsed.atime:
                    cur_max = stats["nr_atime"]
                    stats["nr_atime"] = max(cur_max, parsed.atime) if cur_max else parsed.atime

                # Accumulate UID (Single pass logic)
                # None = init, -999 = multiple/conflict, else = single UID
                p_uid = parsed.uid
                s_uid = stats["first_uid"]

                if s_uid is None:
                    stats["first_uid"] = p_uid
                elif s_uid != -999 and s_uid != p_uid:
                    stats["first_uid"] = -999

        # Optimize IPC payload: dict of tuples
        # (nr_count, nr_size, nr_atime, first_uid)
        final_results = {
            k: (v["nr_count"], v["nr_size"], v["nr_atime"], v["first_uid"])
            for k, v in results.items()
        }
        return final_results, len(chunk)

    else:
        # Standard behavior for Pass 1 (Dirs)
        results = []
        for line in chunk:
            parsed = parser.parse_line(line.rstrip("\n"))
            if parsed:
                if filter_type == "all":
                    results.append(parsed)
                elif filter_type == "dirs" and parsed.is_dir:
                    results.append(parsed)
                elif filter_type == "files" and not parsed.is_dir:
                    results.append(parsed)  # Fallback if ever needed
        return results, len(chunk)


def chunk_file_generator(filepath: Path, chunk_bytes: int) -> Generator[list[str], None, None]:
    """Yield chunks of lines from the input file using byte-size hints."""
    with open_input_file(filepath) as f:
        while True:
            lines = f.readlines(chunk_bytes)
            if not lines:
                break
            yield lines


def run_parallel_file_processing(
    input_file: Path,
    parser: FilesystemParser,
    num_workers: int,
    chunk_bytes: int,
    filter_type: str,
    process_results_fn: Callable[[Any], None],
    progress_callback: Callable[[int], None] | None = None,
    flush_callback: Callable[[], None] | None = None,
    should_flush_fn: Callable[[], bool] | None = None,
) -> int:
    """
    Generic parallel file processor for Phase 1a and Phase 2a.

    Uses multiprocessing.Pool to distribute parsing work.

    Args:
        input_file: Path to the log file
        parser: Parser instance to use for parsing
        num_workers: Number of worker processes
        chunk_bytes: Approx bytes per chunk (passed to readlines)
        filter_type: "dirs" or "files"
        process_results_fn: Function to process parsed results
        progress_callback: Optional callback receiving estimated line count
        flush_callback: Optional callback to flush accumulated data
        should_flush_fn: Optional function that returns True if flush needed

    Returns:
        Total line count
    """
    total_lines = 0

    # Generator for pool arguments
    def args_generator():
        for chunk in chunk_file_generator(input_file, chunk_bytes):
            yield (chunk, filter_type, parser)

    # Use a Pool to manage workers automatically
    with mp.Pool(processes=num_workers) as pool:
        # imap_unordered allows processing results as soon as they are ready
        for results, lines_in_chunk in pool.imap_unordered(_worker_parse_chunk, args_generator(), chunksize=1):
            total_lines += lines_in_chunk

            if results:
                process_results_fn(results)

            if should_flush_fn and should_flush_fn() and flush_callback:
                flush_callback()

            if progress_callback:
                progress_callback(total_lines)

    return total_lines


def configure_sqlite_pragmas(session):
    """
    Configure SQLite for maximum insertion performance.
    Risky if system crashes during import, but fine for a rebuildable cache.
    """
    session.execute(text("PRAGMA synchronous = OFF"))
    session.execute(text("PRAGMA journal_mode = MEMORY"))
    session.execute(text("PRAGMA temp_store = MEMORY"))
    session.execute(text("PRAGMA cache_size = -64000"))  # 64MB cache
    session.execute(text("PRAGMA mmap_size = 30000000000"))  # Memory map large DBs
    session.execute(text("PRAGMA busy_timeout = 30000"))  # 30s timeout for lock contention
    session.execute(text("PRAGMA locking_mode = EXCLUSIVE"))  # Faster single-writer mode


def finalize_sqlite_pragmas(session):
    """
    Finalize SQLite after import for optimal query performance.
    Should be called after all inserts are complete.
    """
    session.execute(text("PRAGMA optimize"))  # Optimize index statistics
    session.commit()


# TEMPORARY: Import pass functions from scan_to_db until fully extracted
# TODO: Refactor these to be parser-agnostic and move here
from ..scan_to_db import (
    pass1_discover_directories as _pass1_discover_directories,
    pass2a_nonrecursive_stats as _pass2a_nonrecursive_stats,
    pass2b_aggregate_recursive_stats,
    pass3_populate_summary_tables as _pass3_populate_summary_tables,
)


def run_import(
    input_file: Path,
    parser: FilesystemParser,
    filesystem: str | None = None,
    db_path: Path | None = None,
    data_dir: Path | None = None,
    batch_size: int = 10000,
    progress_interval: int = 1_000_000,
    replace: bool = False,
    workers: int = 1,
    echo: bool = False,
) -> None:
    """
    Run multi-pass import using the provided parser.

    This is the main entry point for the import process.

    Args:
        input_file: Path to the filesystem scan log file
        parser: Parser instance for the specific log format
        filesystem: Filesystem name (auto-detected from filename if None)
        db_path: Explicit database path (overrides other settings)
        data_dir: Data directory override
        batch_size: Batch size for database operations
        progress_interval: Progress report interval (lines)
        replace: If True, drop existing tables before import
        workers: Number of parallel workers for parsing
        echo: If True, enable SQL echo for debugging
    """
    console.print(f"[bold]Filesystem Scan Importer ({parser.format_name.upper()})[/bold]")
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

    # Apply data directory override if provided
    if data_dir is not None:
        set_data_dir(data_dir)

    # Resolve database path
    resolved_db_path = get_db_path(filesystem, db_path)

    # Initialize database
    if replace:
        console.print("[yellow]Dropping existing tables...[/yellow]")
        drop_tables(filesystem, echo=echo, db_path=resolved_db_path)

    engine = init_db(filesystem, echo=echo, db_path=resolved_db_path)
    session = get_session(filesystem, engine=engine)
    configure_sqlite_pragmas(session)

    console.print(f"Database: {engine.url}")
    console.print()

    overall_start = time.time()
    try:
        # Pass 1: Discover directories
        # TODO: Make pass1 parser-agnostic (currently GPFS-specific)
        path_to_id, metadata = _pass1_discover_directories(
            input_file, session, progress_interval, num_workers=workers
        )

        # Pass 2a: Accumulate non-recursive stats
        # TODO: Make pass2a parser-agnostic (currently GPFS-specific)
        _pass2a_nonrecursive_stats(
            input_file,
            session,
            path_to_id,
            batch_size,
            progress_interval,
            total_lines=metadata["total_lines"],
            num_workers=workers,
        )

        # Pass 2b: Compute recursive stats via bottom-up aggregation
        # (This pass is already parser-agnostic - pure SQL)
        pass2b_aggregate_recursive_stats(session)

        # Pass 3: Populate summary tables
        # TODO: Make pass3 parser-agnostic (currently GPFS-specific)
        _pass3_populate_summary_tables(session, input_file, filesystem, metadata)

        # Finalize database
        finalize_sqlite_pragmas(session)

        overall_duration = time.time() - overall_start

        # Get DB file size
        size_str = "unknown"
        if resolved_db_path.exists():
            size_bytes = resolved_db_path.stat().st_size
            size_str = format_size(size_bytes)

        console.print(f"\n[green bold]Import complete![/green bold]")
        console.print(f"[bold]Total runtime:[/bold] {overall_duration:.2f} seconds")
        console.print(f"[bold]Database size:[/bold] {size_str}")

    except Exception as e:
        console.print(f"\n[red]Error during import: {e}[/red]")
        session.rollback()
        raise
    finally:
        session.close()
