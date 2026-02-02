"""Parser-agnostic filesystem scan importer.

This module provides the multi-pass import algorithm that works with any
filesystem parser (GPFS, Lustre, POSIX, etc.).

Parser Requirements:
    - Parsers should provide inode and fileset_id fields for efficient deduplication
    - For most filesystems: fileset_id=0, inode=actual inode number
    - For GPFS: fileset_id and inode both vary per entry
    - If not provided, defaults to 0 (may cause import issues)

Multi-Pass Algorithm:
    Pass 1: Directory Discovery
        Phase 1a: Stream directories to SQLite staging table (parallelizable)
                  Uses (fileset_id, inode) primary key for fast deduplication
        Phase 1b: Sort by depth, insert into database, build path_to_id lookup

    Pass 2: Statistics Accumulation
        Phase 2a: Re-scan log file, accumulate non-recursive stats only
        Phase 2b: Bottom-up SQL aggregation to compute recursive stats

    Pass 3: Summary Tables
        - Resolve UIDs to usernames and GIDs to groupnames
        - Pre-aggregate per-owner and per-group statistics
        - Record scan metadata
"""

from .common_imports import *
from .file_handling import *
from .pass1 import *
from .pass2a import *
from .pass2b import *
from .pass3 import *
from .add_table_indexing import *


def run_import(
    input_file: Path,
    parser: FilesystemParser,
    filesystem: str | None = None,
    db_path: Path | None = None,
    data_dir: Path | None = None,
    batch_size: int = 10000,
    progress_interval: int = 1_000_000,
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

        # add directory indexing *after* insertions but *before* recursive stats
        # since we search on directories
        add_directories_indexing(session)
        add_directory_stats_nr_indexing(session)

        # Pass 2b: Compute recursive stats via bottom-up aggregation (pure SQL)
        pass2b_aggregate_recursive_stats(session)

        # add all other directory_stats indexing *after* recursive stats
        add_directory_stats_indexing(session)

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
