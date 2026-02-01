from .common_imports import *
from ..parsers.base import FilesystemParser
from .file_handling import *
from itertools import groupby


def _worker_parse_chunk(args: tuple[list[str], str, FilesystemParser, datetime | None]) -> tuple[Any, Any, int]:
    """
    Worker function to parse a chunk of lines using the provided parser.

    Args:
        args: Tuple of (lines_chunk, parser, scan_date)

    Returns:
        Tuple of (dir_results, None, count of lines processed)
        - dir_results is list[ParsedEntry], hist_results is None
    """
    chunk, parser, scan_date = args

    results = []
    for line in chunk:
        parsed = parser.parse_line(line.rstrip("\n"))
        if parsed and parsed.is_dir:
            results.append(parsed)

    return results, None, len(chunk)




def pass1_discover_directories(
    input_file: Path,
    parser: FilesystemParser,
    session,
    progress_interval: int = 1_000_000,
    num_workers: int = 1,
) -> tuple[dict[str, int], dict]:
    """
    First pass: identify all directories and build hierarchy.

    Phase 1a: Scan file in parallel, build in-memory path→depth dict
    Phase 1b: Sort by depth, insert to directories table, reuse dict for path→dir_id

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

    # Phase 1a: Scan for directories, build in-memory dict
    console.print("  [bold]Phase 1a:[/bold] Scanning for directories...")

    # In-memory structure replaces staging_dirs table
    path_to_depth = {}  # {path: depth} - will become path_to_id later

    line_count = 0
    CHUNK_BYTES = 32 * 1024 * 1024  # 32MB chunks for efficient reading
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
            progress.update(task, dirs=f"{len(path_to_depth):,}", rate=f"{rate:,}")

        def process_parsed_dirs(results):
            """Merge parsed directories into in-memory dict."""
            if results is None or not results[0]:
                return

            dir_results, _ = results  # Extract dir_results from tuple
            for parsed in dir_results:
                # Dict key automatically handles deduplication
                path_to_depth[parsed.path] = parsed.path.count('/')

        # Parallel Phase 1a - no flush needed, everything stays in memory
        line_count = run_parallel_file_processing(
            input_file=input_file,
            parser=parser,
            num_workers=num_workers,
            chunk_bytes=CHUNK_BYTES,
            worker_parse_chunk=_worker_parse_chunk,
            process_results_fn=process_parsed_dirs,
            progress_callback=update_progress,
            flush_callback=None,  # No flush needed!
            should_flush_fn=None,
        )

        update_progress()


    console.print(f"    Lines scanned: {line_count:,}")
    console.print(f"    Found {len(path_to_depth):,} unique directories")

    # Estimate file count (excluding headers and directories)
    estimated_files = max(0, line_count - len(path_to_depth) - 50)
    console.print(f"    Inferred ~{estimated_files:,} files")

    # Phase 1b: Insert directories depth-by-depth, reusing path_to_depth dict
    console.print("  [bold]Phase 1b:[/bold] Inserting into database...")

    # Sort paths by depth (O(N log N) - more efficient than O(N*D) dict scans)
    sorted_paths = sorted(path_to_depth.keys(), key=lambda p: path_to_depth[p])

    with create_progress_bar(show_rate=False) as progress:
        task = progress.add_task(
            "[green]Inserting directories...",
            total=len(path_to_depth),
        )

        insert_batch_size = 25_000

        # Group by depth using groupby (requires sorted input)
        for depth, paths_iter in groupby(sorted_paths, key=lambda p: path_to_depth[p]):
            paths_at_depth = list(paths_iter)

            # Prepare insertion data
            dir_inserts = []
            for p in paths_at_depth:
                parent_path, _, name = p.rpartition('/')
                if not name:  # Root case
                    name = p

                # Parent lookup: parent already has dir_id (processed earlier)
                parent_id = path_to_depth.get(parent_path) if parent_path else None

                dir_inserts.append({
                    "parent_id": parent_id,
                    "name": name,
                    "depth": depth,
                })

            # Get max dir_id before insert
            max_id_before = session.execute(
                text("SELECT COALESCE(MAX(dir_id), 0) FROM directories")
            ).scalar()

            # Bulk insert directories
            for i in range(0, len(dir_inserts), insert_batch_size):
                session.execute(
                    insert(Directory),
                    dir_inserts[i : i + insert_batch_size]
                )
            session.commit()

            # Assign IDs sequentially and OVERWRITE dict in place (safe now!)
            stats_inserts = []
            for idx, p in enumerate(paths_at_depth):
                dir_id = max_id_before + idx + 1
                path_to_depth[p] = dir_id  # Overwrites depth with dir_id
                stats_inserts.append({"dir_id": dir_id})

            # Bulk insert stats
            if stats_inserts:
                for i in range(0, len(stats_inserts), insert_batch_size):
                    stmt = sqlite_insert(DirectoryStats).values(
                        stats_inserts[i : i + insert_batch_size]
                    ).on_conflict_do_nothing(index_elements=['dir_id'])
                    session.execute(stmt)
                session.commit()

            progress.update(task, advance=len(paths_at_depth))

    console.print(f"    Inserted {len(path_to_depth):,} directories")

    # path_to_depth is now actually path_to_id (depths overwritten with dir_ids)
    path_to_id = path_to_depth

    # Return metadata
    metadata = {
        "total_lines": line_count,
        "dir_count": len(path_to_id),
        "estimated_files": estimated_files,
    }

    return path_to_id, metadata
