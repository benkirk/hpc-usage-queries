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
from .file_handling import *


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

    # Create staging table with inode-based primary key for faster deduplication
    # Drop existing staging_dirs from previous run (if any) before Phase 1a starts
    # This way any cleanup delay happens upfront rather than after Phase 1b
    session.execute(text("DROP TABLE IF EXISTS staging_dirs"))
    session.execute(
        text("""
        CREATE TEMPORARY TABLE IF NOT EXISTS staging_dirs (
            inode INTEGER NOT NULL,
            fileset_id INTEGER NOT NULL,
            depth INTEGER NOT NULL,
            path TEXT NOT NULL,
            PRIMARY KEY (fileset_id, inode)
        )
    """)
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
                        "inode": parsed.inode if parsed.inode is not None else 0,
                        "fileset_id": parsed.fileset_id if parsed.fileset_id is not None else 0,
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
                    INSERT OR IGNORE INTO staging_dirs (inode, fileset_id, depth, path)
                    VALUES (:inode, :fileset_id, :depth, :path)
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

    # index after
    session.execute(
        text("CREATE INDEX idx_staging_depth ON staging_dirs(depth)")
    )
    session.commit()


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

    # Explicitly drop staging table now that Phase 1b is complete (immediate cleanup)
    session.execute(text("DROP TABLE IF EXISTS staging_dirs"))
    session.commit()

    # Return path_to_id and metadata for Phase 2
    metadata = {
        "total_lines": line_count,
        "dir_count": dir_count,
        "estimated_files": estimated_files,
    }

    return path_to_id, metadata
