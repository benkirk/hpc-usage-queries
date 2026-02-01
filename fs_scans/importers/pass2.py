from .common_imports import *
from ..parsers.base import FilesystemParser
from .file_handling import *



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
                if not parent_id:
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
