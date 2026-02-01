import grp
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
    GroupInfo,
    GroupSummary,
    OwnerSummary,
    ScanMetadata,
    SizeHistogram,
    UserInfo,
    classify_atime_bucket,
    classify_size_bucket,
)
from ..parsers.base import FilesystemParser



class DirStatsAccumulator:
    """Memory-efficient accumulator for directory statistics using __slots__."""
    __slots__ = ('nr_count', 'nr_size', 'nr_atime', 'nr_dirs', 'first_uid', 'first_gid')

    def __init__(self):
        self.nr_count = 0
        self.nr_size = 0
        self.nr_atime = None
        self.nr_dirs = 0
        self.first_uid = None
        self.first_gid = None


class HistAccumulator:
    """Memory-efficient accumulator for histogram statistics using __slots__."""
    __slots__ = ('atime_hist', 'size_hist', 'atime_size', 'size_size')

    def __init__(self):
        self.atime_hist = [0] * 10
        self.size_hist = [0] * 10
        self.atime_size = [0] * 10
        self.size_size = [0] * 10


def _worker_parse_chunk(args: tuple[list[str], str, FilesystemParser, datetime | None]) -> tuple[Any, Any, int]:
    """
    Worker function to parse a chunk of lines using the provided parser.

    Args:
        args: Tuple of (lines_chunk, filter_type, parser, scan_date)

    Returns:
        Tuple of (dir_results, hist_results, count of lines processed)
        - If filter_type="files":
            dir_results is dict[parent_path, DirStatsAccumulator]
            hist_results is dict[uid, HistAccumulator]
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
                    results[parent] = DirStatsAccumulator()
                stats = results[parent]

                if parsed.is_dir:
                    # Track directory count for parent
                    stats.nr_dirs += 1
                else:
                    # Track file stats
                    # Accumulate count and size
                    stats.nr_count += 1
                    stats.nr_size += parsed.allocated

                    # Accumulate atime
                    if parsed.atime:
                        cur_max = stats.nr_atime
                        stats.nr_atime = max(cur_max, parsed.atime) if cur_max else parsed.atime

                    # Accumulate UID (Single pass logic)
                    # None = init, -999 = multiple/conflict, else = single UID
                    p_uid = parsed.uid
                    s_uid = stats.first_uid

                    if s_uid is None:
                        stats.first_uid = p_uid
                    elif s_uid != -999 and s_uid != p_uid:
                        stats.first_uid = -999

                    # Accumulate GID (Single pass logic)
                    # None = init, -999 = multiple/conflict, else = single GID
                    p_gid = parsed.gid
                    s_gid = stats.first_gid

                    if s_gid is None:
                        stats.first_gid = p_gid
                    elif s_gid != -999 and s_gid != p_gid:
                        stats.first_gid = -999

                    # NEW: Track histograms per UID (files only)
                    uid = parsed.uid
                    if uid not in hist_results:
                        hist_results[uid] = HistAccumulator()

                    hist = hist_results[uid]

                    # Classify and update histograms
                    atime_bucket = classify_atime_bucket(parsed.atime, scan_date) if scan_date else 9
                    hist.atime_hist[atime_bucket] += 1
                    hist.atime_size[atime_bucket] += parsed.allocated

                    size_bucket = classify_size_bucket(parsed.allocated)
                    hist.size_hist[size_bucket] += 1
                    hist.size_size[size_bucket] += parsed.allocated

        return results, hist_results, len(chunk)

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


def open_input_file(filepath: Path) -> TextIO:
    """Open input file for reading with a large buffer."""
    # Use 8MB buffer to minimize syscalls
    return open(filepath, "r", encoding="utf-8", errors="replace", buffering=8 * 1024 * 1024)


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
