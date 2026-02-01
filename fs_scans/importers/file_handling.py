from .common_imports import *
from ..parsers.base import FilesystemParser


def open_input_file(filepath: Path) -> TextIO:
    """Open input file for reading with a large buffer."""
    # Use big buffer to minimize syscalls
    return open(filepath, "r", encoding="utf-8", errors="replace", buffering=64 * 1024 * 1024)


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
    worker_parse_chunk: Callable[[Any], Any],
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
        worker_parse_chunk:
        process_results_fn: Function to process parsed results
        progress_callback: Optional callback receiving estimated line count
        flush_callback: Optional callback to flush accumulated data
        should_flush_fn: Optional function that returns True if flush needed
        scan_date: Scan timestamp (needed for histogram classification)

    Returns:
        Total line count
    """
    total_lines = 0

    # Generator for pool arguments
    def args_generator():
        for chunk in chunk_file_generator(input_file, chunk_bytes):
            yield (chunk, parser, scan_date)

    # Use a Pool to manage workers automatically
    with mp.Pool(processes=num_workers) as pool:
        # imap_unordered allows processing results as soon as they are ready
        for dir_results, hist_results, lines_in_chunk in pool.imap_unordered(worker_parse_chunk, args_generator(), chunksize=1):
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
