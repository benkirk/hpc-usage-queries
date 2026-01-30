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
from ..core.models import (
    AccessHistogram,
    Directory,
    DirectoryStats,
    OwnerSummary,
    ScanMetadata,
    SizeHistogram,
    UserInfo,
)
from ..parsers.base import FilesystemParser


# Histogram Bucket Definitions

# Access Time Histogram (10 buckets)
# Tracks file distribution by last access time relative to scan date
ATIME_BUCKETS = [
    ("< 1 Month", 30),           # 0-30 days
    ("1-3 Months", 90),          # 30-90 days
    ("3-6 Months", 180),         # 90-180 days
    ("6-12 Months", 365),        # 180-365 days
    ("1-2 Years", 730),          # 1-2 years
    ("2-3 Years", 1095),         # 2-3 years
    ("3-4 Years", 1460),         # 3-4 years
    ("5-6 Years", 2190),         # 5-6 years
    ("6-7 Years", 2555),         # 6-7 years
    ("7+ Years", None),          # 7+ years
]

# Size Histogram (10 buckets)
# Logarithmic scale covering practical file size ranges
SIZE_BUCKETS = [
    ("0 - 1 KiB", 0, 1024),
    ("1 KiB - 10 KiB", 1024, 10 * 1024),
    ("10 KiB - 100 KiB", 10 * 1024, 100 * 1024),
    ("100 KiB - 1 MiB", 100 * 1024, 1024 * 1024),
    ("1 MiB - 10 MiB", 1024 * 1024, 10 * 1024 * 1024),
    ("10 MiB - 100 MiB", 10 * 1024 * 1024, 100 * 1024 * 1024),
    ("100 MiB - 1 GiB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
    ("1 GiB - 10 GiB", 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024),
    ("10 GiB - 100 GiB", 10 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024),
    ("100 GiB+", 100 * 1024 * 1024 * 1024, None),
]


def classify_atime_bucket(atime: datetime | None, scan_date: datetime) -> int:
    """Classify file's access time into histogram bucket.

    Args:
        atime: File's last access time
        scan_date: Scan timestamp (extracted from filename)

    Returns:
        Bucket index (0-9)
    """
    if atime is None:
        return len(ATIME_BUCKETS) - 1  # Default to oldest bucket

    days_old = (scan_date - atime).days

    for idx, (_, max_days) in enumerate(ATIME_BUCKETS):
        if max_days is None:  # Last bucket (7+ years)
            return idx
        if days_old < max_days:
            return idx

    return len(ATIME_BUCKETS) - 1  # Fallback to oldest bucket


def classify_size_bucket(size_bytes: int) -> int:
    """Classify file size into histogram bucket.

    Args:
        size_bytes: File size in bytes (allocated size)

    Returns:
        Bucket index (0-9)
    """
    for idx, (_, min_size, max_size) in enumerate(SIZE_BUCKETS):
        if max_size is None:  # Last bucket (100 GiB+)
            if size_bytes >= min_size:
                return idx
        elif min_size <= size_bytes < max_size:
            return idx

    return len(SIZE_BUCKETS) - 1  # Fallback to largest bucket


def open_input_file(filepath: Path) -> TextIO:
    """Open input file for reading with a large buffer."""
    # Use 8MB buffer to minimize syscalls
    return open(filepath, "r", encoding="utf-8", errors="replace", buffering=8 * 1024 * 1024)


def _worker_parse_chunk(args: tuple[list[str], str, FilesystemParser, datetime | None]) -> tuple[Any, Any, int]:
    """
    Worker function to parse a chunk of lines using the provided parser.

    Args:
        args: Tuple of (lines_chunk, filter_type, parser, scan_date)

    Returns:
        Tuple of (dir_results, hist_results, count of lines processed)
        - If filter_type="files":
            dir_results is dict[parent_path, stats_tuple]
            where stats_tuple = (nr_count, nr_size, nr_atime, nr_dirs, first_uid, first_gid)
            hist_results is dict[uid, histograms]
        - If filter_type="dirs"/"all": dir_results is list[ParsedEntry], hist_results is None
    """
    chunk, filter_type, parser, scan_date = args

    if filter_type == "files":
        # Map-Reduce Optimization: Aggregate stats locally in worker
        # This reduces IPC traffic and main thread load by ~1000x
        results = {}
        hist_results = {}

        for line in chunk:
            parsed = parser.parse_line(line.rstrip("\n"))
            if parsed:
                parent = os.path.dirname(parsed.path)
                if parent not in results:
                    results[parent] = {
                        "nr_count": 0,
                        "nr_size": 0,
                        "nr_atime": None,
                        "nr_dirs": 0,
                        "first_uid": None,
                        "first_gid": None
                    }
                stats = results[parent]

                if parsed.is_dir:
                    # Track directory count for parent
                    stats["nr_dirs"] += 1
                else:
                    # Track file stats
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

                    # Accumulate GID (Single pass logic)
                    # None = init, -999 = multiple/conflict, else = single GID
                    p_gid = parsed.gid
                    s_gid = stats["first_gid"]

                    if s_gid is None:
                        stats["first_gid"] = p_gid
                    elif s_gid != -999 and s_gid != p_gid:
                        stats["first_gid"] = -999

                    # NEW: Track histograms per UID (files only)
                    uid = parsed.uid
                    if uid not in hist_results:
                        hist_results[uid] = {
                            "atime_hist": [0] * 10,  # file counts per atime bucket
                            "size_hist": [0] * 10,   # file counts per size bucket
                            "atime_size": [0] * 10,  # total bytes per atime bucket
                            "size_size": [0] * 10,   # total bytes per size bucket
                        }

                    hist = hist_results[uid]

                    # Classify and update histograms
                    atime_bucket = classify_atime_bucket(parsed.atime, scan_date) if scan_date else 9
                    hist["atime_hist"][atime_bucket] += 1
                    hist["atime_size"][atime_bucket] += parsed.allocated

                    size_bucket = classify_size_bucket(parsed.allocated)
                    hist["size_hist"][size_bucket] += 1
                    hist["size_size"][size_bucket] += parsed.allocated

        # Optimize IPC payload: convert dicts to tuples
        # Directory stats: (nr_count, nr_size, nr_atime, nr_dirs, first_uid, first_gid)
        final_results = {
            k: (v["nr_count"], v["nr_size"], v["nr_atime"], v["nr_dirs"], v["first_uid"], v["first_gid"])
            for k, v in results.items()
        }

        # Histogram stats: (atime_hist, size_hist, atime_size, size_size)
        final_hist_results = {
            uid: (hist["atime_hist"], hist["size_hist"],
                  hist["atime_size"], hist["size_size"])
            for uid, hist in hist_results.items()
        }

        return final_results, final_hist_results, len(chunk)

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
        return results, None, len(chunk)


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
    scan_date: datetime | None = None,
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
        scan_date: Scan timestamp (needed for histogram classification in filter_type="files")

    Returns:
        Total line count
    """
    total_lines = 0

    # Generator for pool arguments
    def args_generator():
        for chunk in chunk_file_generator(input_file, chunk_bytes):
            yield (chunk, filter_type, parser, scan_date)

    # Use a Pool to manage workers automatically
    with mp.Pool(processes=num_workers) as pool:
        # imap_unordered allows processing results as soon as they are ready
        for dir_results, hist_results, lines_in_chunk in pool.imap_unordered(_worker_parse_chunk, args_generator(), chunksize=1):
            total_lines += lines_in_chunk

            if dir_results or hist_results:
                process_results_fn((dir_results, hist_results))

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


def make_empty_update() -> dict:
    """Create an empty update dictionary for non-recursive stats accumulation."""
    return {
        "nr_count": 0,
        "nr_size": 0,
        "nr_atime": None,
        "nr_dirs": 0,
        "first_uid": None,  # None = not set, -999 = multiple owners, else = single UID
        "first_gid": None,  # None = not set, -999 = multiple groups, else = single GID
    }


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

        # Determine owner_gid: single gid or NULL for multiple
        first_gid = upd["first_gid"]
        if first_gid is None:
            group_val = -1  # No files seen
        elif first_gid == -999:
            group_val = None  # Multiple groups
        else:
            group_val = first_gid

        params_batch.append(
            {
                "dir_id": dir_id,
                "nr_count": upd["nr_count"],
                "nr_size": upd["nr_size"],
                "nr_atime": upd["nr_atime"],
                "nr_dirs": upd["nr_dirs"],
                "owner": owner_val,
                "group": group_val,
            }
        )

    # Execute bulk update
    session.execute(
        text("""
            UPDATE directory_stats SET
                file_count_nr = file_count_nr + :nr_count,
                total_size_nr = total_size_nr + :nr_size,
                dir_count_nr = dir_count_nr + :nr_dirs,
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
                END,
                owner_gid = CASE
                    WHEN owner_gid = -1 THEN :group
                    WHEN :group IS NULL THEN NULL
                    WHEN owner_gid IS NULL THEN NULL
                    WHEN owner_gid != :group THEN NULL
                    ELSE owner_gid
                END
            WHERE dir_id = :dir_id
        """),
        params_batch,
    )

    session.commit()


def flush_histograms(session, pending_histograms: dict) -> None:
    """
    Bulk insert accumulated histograms to database.

    Args:
        session: SQLAlchemy session
        pending_histograms: Dictionary of uid -> histogram data
    """
    if not pending_histograms:
        return

    atime_inserts = []
    size_inserts = []

    for uid, hist in pending_histograms.items():
        # Access time histogram (10 rows per UID)
        for bucket_idx in range(10):
            if hist["atime_hist"][bucket_idx] > 0:  # skip empty buckets
                atime_inserts.append({
                    "owner_uid": uid,
                    "bucket_index": bucket_idx,
                    "file_count": hist["atime_hist"][bucket_idx],
                    "total_size": hist["atime_size"][bucket_idx],
                })

        # Size histogram (10 rows per UID)
        for bucket_idx in range(10):
            if hist["size_hist"][bucket_idx] > 0:  # skip empty buckets
                size_inserts.append({
                    "owner_uid": uid,
                    "bucket_index": bucket_idx,
                    "file_count": hist["size_hist"][bucket_idx],
                    "total_size": hist["size_size"][bucket_idx],
                })

    # Bulk insert using executemany for performance
    if atime_inserts:
        session.execute(
            AccessHistogram.__table__.insert(),
            atime_inserts
        )

    if size_inserts:
        session.execute(
            SizeHistogram.__table__.insert(),
            size_inserts
        )

    session.commit()


def pass1_discover_directories(
    input_file: Path,
    parser: FilesystemParser,
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
        parser: Filesystem parser instance
        session: SQLAlchemy session
        progress_interval: Report progress every N lines
        num_workers: Number of worker processes for parsing (Phase 1a only)

    Returns:
        Tuple of:
        - Dictionary mapping full paths to dir_id
        - Metadata dict with total_lines, dir_count, inferred file_count
    """
    console.print(f"[bold]Pass 1:[/bold] Discovering directories ({num_workers} workers)...")

    # Create staging table (parser-agnostic version without inode/fileset_id)
    session.execute(
        text("""
        CREATE TABLE IF NOT EXISTS staging_dirs (
            depth INTEGER NOT NULL,
            path TEXT NOT NULL PRIMARY KEY
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
    CHUNK_BYTES = 32 * 1024 * 1024  # 32MB chunks for efficient reading
    batch: list[dict] = []
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

        def update_progress(estimated_lines: int | None = None):
            nonlocal line_count
            if estimated_lines is not None:
                line_count = estimated_lines
            elapsed = time.time() - start_time
            rate = int(line_count / elapsed) if elapsed > 0 else 0
            progress.update(task, dirs=f"{dir_count:,}", rate=f"{rate:,}")

        def process_parsed_dirs(results):
            """Process a batch of parsed directory entries from worker."""
            nonlocal dir_count
            if results is None or not results[0]:
                return

            dir_results, _ = results  # Extract dir_results from tuple
            for parsed in dir_results:
                batch.append(
                    {
                        "depth": parsed.path.count("/"),
                        "path": parsed.path,
                    }
                )
                dir_count += 1

        def flush_batch():
            """Flush batch to staging table."""
            nonlocal batch
            if batch:
                session.execute(
                    text("""
                    INSERT OR IGNORE INTO staging_dirs (depth, path)
                    VALUES (:depth, :path)
                """),
                    batch,
                )
                session.commit()
                batch = []  # Fresh allocation to release memory

        # Parallel Phase 1a
        line_count = run_parallel_file_processing(
            input_file=input_file,
            parser=parser,
            num_workers=num_workers,
            chunk_bytes=CHUNK_BYTES,
            filter_type="dirs",
            process_results_fn=process_parsed_dirs,
            progress_callback=update_progress,
            flush_callback=flush_batch,
            should_flush_fn=lambda: len(batch) >= BATCH_SIZE,
        )

        # Final flush
        flush_batch()
        update_progress()

    console.print(f"    Lines scanned: {line_count:,}")
    console.print(f"    Found {dir_count:,} directories")

    # Estimate file count (excluding headers and directories)
    estimated_files = max(0, line_count - dir_count - 50)
    console.print(f"    Inferred ~{estimated_files:,} files")

    # Phase 1b: Read from staging ordered by depth, insert to directories table
    # Optimized: Single-pass insertion with sequential ID tracking (no redundant SELECT)
    console.print("  [bold]Phase 1b:[/bold] Inserting into database (single-pass)...")
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

        insert_batch_size = 25000

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

            # 1. Prepare insertion data - single pass building both inserts and path list
            # Keep paths in insertion order to match with sequential IDs later
            ordered_paths = []
            dir_inserts = []

            for p in paths:
                parent_path, _, name = p.rpartition('/')
                if not name:  # Root case "/gpfs" -> name="gpfs"
                    name = p

                parent_id = path_to_id.get(parent_path)
                ordered_paths.append(p)
                dir_inserts.append({
                    "parent_id": parent_id,
                    "name": name,
                    "depth": depth,
                })

            # 2. Get max dir_id before insert to track sequential assignment
            max_id_before = session.execute(
                text("SELECT COALESCE(MAX(dir_id), 0) FROM directories")
            ).scalar()

            # 3. Bulk insert directories
            for i in range(0, len(dir_inserts), insert_batch_size):
                session.execute(insert(Directory), dir_inserts[i : i + insert_batch_size])
            session.commit()

            # 4. Assign IDs sequentially (SQLite autoincrement guarantees order)
            # IDs are max_id_before + 1, max_id_before + 2, ... max_id_before + N
            stats_inserts = []
            for idx, p in enumerate(ordered_paths):
                dir_id = max_id_before + idx + 1
                path_to_id[p] = dir_id
                stats_inserts.append({"dir_id": dir_id})

            # 5. Bulk Insert Stats
            if stats_inserts:
                for i in range(0, len(stats_inserts), insert_batch_size):
                    stmt = sqlite_insert(DirectoryStats).values(
                        stats_inserts[i : i + insert_batch_size]
                    ).on_conflict_do_nothing(index_elements=['dir_id'])
                    session.execute(stmt)
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


def pass2a_nonrecursive_stats(
    input_file: Path,
    parser: FilesystemParser,
    session,
    path_to_id: dict[str, int],
    scan_date: datetime | None = None,
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
        parser: Filesystem parser instance
        session: SQLAlchemy session
        path_to_id: Dictionary mapping full paths to dir_id
        scan_date: Scan timestamp (for histogram classification)
        batch_size: Number of directories to accumulate before flushing
        progress_interval: Report progress every N lines
        total_lines: Total line count from Phase 1 (for determinate progress bar)
        num_workers: Number of worker processes
    """
    console.print(f"\n[bold]Pass 2:[/bold] Accumulating statistics ({num_workers} workers)...")

    console.print("  [bold]Phase 2a:[/bold] Accumulating non-recursive stats...")

    pending_updates = defaultdict(make_empty_update)
    pending_histograms = defaultdict(lambda: {
        "atime_hist": [0] * 10,
        "size_hist": [0] * 10,
        "atime_size": [0] * 10,
        "size_size": [0] * 10,
    })
    line_count = 0
    file_count = 0
    flush_count = 0
    start_time = time.time()
    CHUNK_BYTES = 32 * 1024 * 1024  # 32MB chunks for efficient reading

    def do_flush():
        nonlocal flush_count, pending_updates
        if pending_updates:
            flush_nr_updates(session, pending_updates)
            flush_count += 1
            pending_updates = defaultdict(make_empty_update)  # Fresh allocation

    def process_results_batch(results):
        """
        Process a batch of results from worker.
        results is a tuple of (dir_results, hist_results) from worker or None
        """
        nonlocal file_count

        if results is None or (not results[0] and not results[1]):
            return

        dir_results, hist_results = results

        # Process directory results (always expect dict from worker)
        if isinstance(dir_results, dict):
            # Optimized Aggregated Dictionary from Worker
            # Value is tuple: (nr_count, nr_size, nr_atime, nr_dirs, first_uid, first_gid)
            for parent_path, stats_tuple in dir_results.items():
                parent_id = path_to_id.get(parent_path)
                if not parent_id:
                    continue

                nr_count, nr_size, nr_atime, nr_dirs, first_uid, first_gid = stats_tuple

                # Update file count for progress tracking
                file_count += nr_count

                # Merge worker stats into pending_updates
                upd = pending_updates[parent_id]
                upd["nr_count"] += nr_count
                upd["nr_size"] += nr_size
                upd["nr_dirs"] += nr_dirs

                # Merge max atime
                if nr_atime:
                    upd["nr_atime"] = max(upd["nr_atime"], nr_atime) if upd["nr_atime"] else nr_atime

                # Merge UID logic
                w_uid = first_uid
                m_uid = upd["first_uid"]

                if m_uid == -999:
                    pass
                elif w_uid == -999:
                    upd["first_uid"] = -999
                elif w_uid is not None:
                    if m_uid is None:
                        upd["first_uid"] = w_uid
                    elif m_uid != w_uid:
                        upd["first_uid"] = -999

                # Merge GID logic (identical to UID)
                w_gid = first_gid
                m_gid = upd["first_gid"]

                if m_gid == -999:
                    pass
                elif w_gid == -999:
                    upd["first_gid"] = -999
                elif w_gid is not None:
                    if m_gid is None:
                        upd["first_gid"] = w_gid
                    elif m_gid != w_gid:
                        upd["first_gid"] = -999

        # Merge histogram results from worker
        if hist_results:
            for uid, (atime_hist, size_hist, atime_size, size_size) in hist_results.items():
                main_hist = pending_histograms[uid]
                for i in range(10):
                    main_hist["atime_hist"][i] += atime_hist[i]
                    main_hist["atime_size"][i] += atime_size[i]
                    main_hist["size_hist"][i] += size_hist[i]
                    main_hist["size_size"][i] += size_size[i]

    def update_progress_bar(estimated_lines: int | None = None):
        nonlocal line_count
        if estimated_lines is not None:
            line_count = estimated_lines
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

        # Parallel mode with reader thread
        line_count = run_parallel_file_processing(
            input_file=input_file,
            parser=parser,
            num_workers=num_workers,
            chunk_bytes=CHUNK_BYTES,
            filter_type="files",
            process_results_fn=process_results_batch,
            progress_callback=update_progress_bar,
            flush_callback=do_flush,
            should_flush_fn=lambda: len(pending_updates) >= batch_size,
            scan_date=scan_date,
        )

        # Final flush for directory stats
        do_flush()
        update_progress_bar()

    # Flush histograms to database
    console.print("  [bold]Flushing histograms to database...[/bold]")
    flush_histograms(session, pending_histograms)
    console.print(f"    Stored histograms for {len(pending_histograms):,} users")

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
                    max_atime_r = max_atime_nr,
                    dir_count_r = dir_count_nr
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
                        SUM(s.dir_count_r) as sum_dirs,
                        MAX(s.max_atime_r) as max_atime,
                        -- Owner UID Aggregation:
                        -- Check if any child has a NULL owner (conflict)
                        MAX(CASE WHEN s.owner_uid IS NULL THEN 1 ELSE 0 END) as has_uid_conflict,
                        -- Count distinct valid owners (ignoring -1/no-files)
                        COUNT(DISTINCT CASE WHEN s.owner_uid >= 0 THEN s.owner_uid END) as distinct_valid_owners,
                        -- Get the potential common owner (if count is 1)
                        MAX(CASE WHEN s.owner_uid >= 0 THEN s.owner_uid END) as common_owner,
                        -- Owner GID Aggregation:
                        -- Check if any child has a NULL group (conflict)
                        MAX(CASE WHEN s.owner_gid IS NULL THEN 1 ELSE 0 END) as has_gid_conflict,
                        -- Count distinct valid groups (ignoring -1/no-files)
                        COUNT(DISTINCT CASE WHEN s.owner_gid >= 0 THEN s.owner_gid END) as distinct_valid_groups,
                        -- Get the potential common group (if count is 1)
                        MAX(CASE WHEN s.owner_gid >= 0 THEN s.owner_gid END) as common_group
                    FROM directories d
                    JOIN directory_stats s ON d.dir_id = s.dir_id
                    WHERE d.depth = :child_depth
                    GROUP BY d.parent_id
                )
                UPDATE directory_stats
                SET
                    file_count_r = file_count_r + agg.sum_files,
                    total_size_r = total_size_r + agg.sum_size,
                    dir_count_r = dir_count_r + agg.sum_dirs,
                    max_atime_r = MAX(COALESCE(max_atime_r, 0), COALESCE(agg.max_atime, 0)),
                    owner_uid = CASE
                        -- Already conflicted -> stay conflicted
                        WHEN owner_uid IS NULL THEN NULL

                        -- Direct files exist (owner_uid >= 0) -> check for conflict with children
                        WHEN owner_uid >= 0 THEN
                             CASE
                                WHEN agg.has_uid_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_owners > 0 AND agg.common_owner != owner_uid THEN NULL
                                ELSE owner_uid
                             END

                        -- No direct files (-1) -> inherit from children
                        ELSE -- owner_uid == -1
                             CASE
                                WHEN agg.has_uid_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_owners > 1 THEN NULL
                                WHEN agg.distinct_valid_owners = 1 THEN agg.common_owner
                                ELSE -1 -- Still no owner seen
                             END
                    END,
                    owner_gid = CASE
                        -- Already conflicted -> stay conflicted
                        WHEN owner_gid IS NULL THEN NULL

                        -- Direct files exist (owner_gid >= 0) -> check for conflict with children
                        WHEN owner_gid >= 0 THEN
                             CASE
                                WHEN agg.has_gid_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_groups > 0 AND agg.common_group != owner_gid THEN NULL
                                ELSE owner_gid
                             END

                        -- No direct files (-1) -> inherit from children
                        ELSE -- owner_gid == -1
                             CASE
                                WHEN agg.has_gid_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_groups > 1 THEN NULL
                                WHEN agg.distinct_valid_groups = 1 THEN agg.common_group
                                ELSE -1 -- Still no group seen
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


def pass3_populate_summary_tables(
    session,
    input_file: Path,
    filesystem: str,
    metadata: dict,
) -> None:
    """
    Phase 3: Populate summary tables after main processing completes.

    Phase 3a: Populate UserInfo - resolve UIDs to usernames
    Phase 3b: Compute OwnerSummary - pre-aggregate per-owner statistics
    Phase 3c: Record ScanMetadata - store scan provenance info
    """
    console.print("\n[bold]Pass 3:[/bold] Populating summary tables...")

    # Phase 3a: Populate UserInfo
    console.print("  [bold]Phase 3a:[/bold] Resolving user information...")

    @lru_cache(maxsize=10000)
    def resolve_uid(uid: int) -> tuple[str | None, str | None]:
        """Resolve UID to username and full name (GECOS)."""
        try:
            pw = pwd.getpwuid(uid)
            # GECOS field may contain comma-separated values; first is typically full name
            gecos = pw.pw_gecos.split(",")[0] if pw.pw_gecos else None
            return pw.pw_name, gecos
        except (KeyError, OverflowError):
            return None, None

    # Get all distinct UIDs from directory_stats (excluding -1 and NULL)
    uids = session.execute(
        text("""
            SELECT DISTINCT owner_uid FROM directory_stats
            WHERE owner_uid IS NOT NULL AND owner_uid >= 0
        """)
    ).fetchall()

    user_count = 0
    if uids:
        user_inserts = []
        for (uid,) in uids:
            username, full_name = resolve_uid(uid)
            user_inserts.append({
                "uid": uid,
                "username": username,
                "full_name": full_name,
            })
            user_count += 1

        # Bulk upsert
        for item in user_inserts:
            session.execute(
                text("""
                    INSERT OR REPLACE INTO user_info (uid, username, full_name)
                    VALUES (:uid, :username, :full_name)
                """),
                item,
            )
        session.commit()

    console.print(f"    Resolved {user_count} unique UIDs")

    # Phase 3b: Compute OwnerSummary
    console.print("  [bold]Phase 3b:[/bold] Computing owner summaries...")

    # Clear existing summaries and recompute
    session.execute(text("DELETE FROM owner_summary"))
    session.execute(
        text("""
            INSERT INTO owner_summary (owner_uid, total_size, total_files, directory_count)
            SELECT
                owner_uid,
                SUM(total_size_nr) as total_size,
                SUM(file_count_nr) as total_files,
                COUNT(*) as directory_count
            FROM directory_stats
            WHERE owner_uid IS NOT NULL AND owner_uid >= 0
            GROUP BY owner_uid
        """)
    )
    session.commit()

    owner_count = session.execute(
        text("SELECT COUNT(*) FROM owner_summary")
    ).scalar()
    console.print(f"    Computed summaries for {owner_count} owners")

    # Phase 3c: Record ScanMetadata
    console.print("  [bold]Phase 3c:[/bold] Recording scan metadata...")

    scan_timestamp = extract_scan_timestamp(input_file.name)
    import_timestamp = datetime.now()

    # Get aggregate totals from root directories
    totals = session.execute(
        text("""
            SELECT
                COUNT(*) as dir_count,
                COALESCE(SUM(s.file_count_r), 0) as total_files,
                COALESCE(SUM(s.total_size_r), 0) as total_size
            FROM directories d
            JOIN directory_stats s USING (dir_id)
            WHERE d.parent_id IS NULL
        """)
    ).fetchone()

    total_directories = metadata.get("dir_count", 0)
    total_files = totals[1] if totals else 0
    total_size = totals[2] if totals else 0

    session.execute(
        text("""
            INSERT INTO scan_metadata
                (source_file, scan_timestamp, import_timestamp, filesystem,
                 total_directories, total_files, total_size)
            VALUES
                (:source_file, :scan_timestamp, :import_timestamp, :filesystem,
                 :total_directories, :total_files, :total_size)
        """),
        {
            "source_file": input_file.name,
            "scan_timestamp": scan_timestamp,
            "import_timestamp": import_timestamp,
            "filesystem": filesystem,
            "total_directories": total_directories,
            "total_files": total_files,
            "total_size": total_size,
        },
    )
    session.commit()

    console.print(f"    Recorded metadata for {input_file.name}")


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

    # Extract scan date for histogram classification
    scan_date = extract_scan_timestamp(input_file.name)
    if scan_date:
        console.print(f"Scan date: {scan_date.strftime('%Y-%m-%d')}")
    else:
        console.print("[yellow]Warning: Could not extract scan date from filename, histogram classification may be inaccurate[/yellow]")
    console.print()

    try:
        # Pass 1: Discover directories (now parser-agnostic)
        path_to_id, metadata = pass1_discover_directories(
            input_file, parser, session, progress_interval, num_workers=workers
        )

        # Pass 2a: Accumulate non-recursive stats (now parser-agnostic)
        pass2a_nonrecursive_stats(
            input_file,
            parser,
            session,
            path_to_id,
            scan_date=scan_date,
            batch_size=batch_size,
            progress_interval=progress_interval,
            total_lines=metadata["total_lines"],
            num_workers=workers,
        )

        # Pass 2b: Compute recursive stats via bottom-up aggregation (pure SQL)
        pass2b_aggregate_recursive_stats(session)

        # Pass 3: Populate summary tables (parser-agnostic)
        pass3_populate_summary_tables(session, input_file, filesystem, metadata)

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
