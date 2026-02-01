from .common_imports import *
from ..parsers.base import FilesystemParser
from .file_handling import *


def _worker_parse_chunk(args: tuple[list[str], str, FilesystemParser, datetime | None]) -> tuple[Any, Any, int]:
    """
    Worker function to parse a chunk of lines using the provided parser.

    Args:
        args: Tuple of (lines_chunk, parser, scan_date)

    Returns:
        Tuple of (dir_results, hist_results, count of lines processed)
        - dir_results is dict[parent_path, DirStatsAccumulator]
        - hist_results is dict[uid, HistAccumulator]
    """
    chunk, parser, scan_date = args

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


def flush_nr_updates(session, pending_updates: dict) -> None:
    """
    Apply accumulated non-recursive deltas to database using bulk execution.

    Args:
        session: SQLAlchemy session
        pending_updates: Dictionary of dir_id -> DirStatsAccumulator object
    """
    if not pending_updates:
        return

    # Prepare batch parameters
    params_batch = []
    for dir_id, upd in pending_updates.items():
        # Determine owner_uid: single uid or NULL for multiple
        first_uid = upd.first_uid
        if first_uid is None:
            owner_val = -1  # No files seen
        elif first_uid == -999:
            owner_val = None  # Multiple owners
        else:
            owner_val = first_uid

        # Determine owner_gid: single gid or NULL for multiple
        first_gid = upd.first_gid
        if first_gid is None:
            group_val = -1  # No files seen
        elif first_gid == -999:
            group_val = None  # Multiple groups
        else:
            group_val = first_gid

        params_batch.append(
            {
                "dir_id": dir_id,
                "nr_count": upd.nr_count,
                "nr_size": upd.nr_size,
                "nr_atime": upd.nr_atime,
                "nr_dirs": upd.nr_dirs,
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
        pending_histograms: Dictionary of uid -> HistAccumulator object
    """
    if not pending_histograms:
        return

    atime_inserts = []
    size_inserts = []

    for uid, hist in pending_histograms.items():
        # Access time histogram (10 rows per UID)
        for bucket_idx in range(10):
            if hist.atime_hist[bucket_idx] > 0:  # skip empty buckets
                atime_inserts.append({
                    "owner_uid": uid,
                    "bucket_index": bucket_idx,
                    "file_count": hist.atime_hist[bucket_idx],
                    "total_size": hist.atime_size[bucket_idx],
                })

        # Size histogram (10 rows per UID)
        for bucket_idx in range(10):
            if hist.size_hist[bucket_idx] > 0:  # skip empty buckets
                size_inserts.append({
                    "owner_uid": uid,
                    "bucket_index": bucket_idx,
                    "file_count": hist.size_hist[bucket_idx],
                    "total_size": hist.size_size[bucket_idx],
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

    pending_updates = defaultdict(DirStatsAccumulator)
    pending_histograms = defaultdict(HistAccumulator)
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
            pending_updates = defaultdict(DirStatsAccumulator)  # Fresh allocation

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
            # Performance Optimization: Alias for speed in tight loop
            # Avoids repeated LOAD_DEREF of closure variables
            local_path_to_id = path_to_id
            local_updates = pending_updates

            # Optimized Aggregated Dictionary from Worker (DirStatsAccumulator objects)
            for parent_path, w_stats in dir_results.items():
                parent_id = local_path_to_id.get(parent_path)
                if parent_id is None:
                    continue

                # Merge into main accumulator
                upd = local_updates[parent_id]

                # Update file count for progress tracking
                file_count += w_stats.nr_count

                upd.nr_count += w_stats.nr_count
                upd.nr_size += w_stats.nr_size
                upd.nr_dirs += w_stats.nr_dirs

                # Merge max atime
                if w_stats.nr_atime:
                    upd.nr_atime = max(upd.nr_atime, w_stats.nr_atime) if upd.nr_atime else w_stats.nr_atime

                # Merge UID logic
                w_uid = w_stats.first_uid
                m_uid = upd.first_uid

                if m_uid == -999:
                    pass
                elif w_uid == -999:
                    upd.first_uid = -999
                elif w_uid is not None:
                    if m_uid is None:
                        upd.first_uid = w_uid
                    elif m_uid != w_uid:
                        upd.first_uid = -999

                # Merge GID logic (identical to UID)
                w_gid = w_stats.first_gid
                m_gid = upd.first_gid

                if m_gid == -999:
                    pass
                elif w_gid == -999:
                    upd.first_gid = -999
                elif w_gid is not None:
                    if m_gid is None:
                        upd.first_gid = w_gid
                    elif m_gid != w_gid:
                        upd.first_gid = -999

        # Merge histogram results from worker
        if hist_results:
            local_histograms = pending_histograms
            for uid, w_hist in hist_results.items():
                main_hist = local_histograms[uid]
                for i in range(10):
                    main_hist.atime_hist[i] += w_hist.atime_hist[i]
                    main_hist.atime_size[i] += w_hist.atime_size[i]
                    main_hist.size_hist[i] += w_hist.size_hist[i]
                    main_hist.size_size[i] += w_hist.size_size[i]

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
            worker_parse_chunk=_worker_parse_chunk,
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
