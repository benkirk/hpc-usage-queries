"""Analyze subcommand for fs-scans CLI."""

from pathlib import Path

import click

from ..cli.common import (
    console,
    make_dynamic_help_command,
)
from ..core.database import get_data_dir_info, get_db_path, get_session, set_data_dir
from ..queries.query_engine import (
    get_all_filesystems,
    get_scan_date,
    normalize_path,
    resolve_owner_filter,
    resolve_usernames_across_databases,
)
from ..queries.access_history import compute_access_history, query_access_histogram_fast
from ..queries.histogram_common import aggregate_histograms_across_databases


# Create DynamicHelpCommand for this tool
DynamicHelpCommand = make_dynamic_help_command('fs-scans analyze')


@click.command(cls=DynamicHelpCommand, context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("filesystem", type=str, default="all")
@click.option(
    "--access-history",
    is_flag=True,
    help="Generate access history histogram showing data by age",
)
@click.option(
    "--file-size",
    is_flag=True,
    help="Generate file size distribution histogram",
)
@click.option(
    "--owner",
    "-u",
    "owner_id",
    type=str,
    help="Filter to specific owner (UID or username)",
)
@click.option(
    "--mine",
    is_flag=True,
    help="Filter to current user's UID",
)
@click.option(
    "--path-prefix",
    "-P",
    "path_prefixes",
    multiple=True,
    type=str,
    help="Filter to paths starting with prefix (can be repeated for OR)",
)
@click.option(
    "-d",
    "--min-depth",
    type=int,
    help="Filter by minimum path depth",
)
@click.option(
    "--max-depth",
    type=int,
    help="Filter by maximum path depth",
)
@click.option(
    "--data-dir",
    "data_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Override directory containing database files (or set FS_SCAN_DATA_DIR env var)",
)
@click.option(
    "--show-config",
    is_flag=True,
    help="Show data directory configuration and available databases",
)
@click.option(
    "--top-n",
    type=int,
    default=10,
    show_default=True,
    help="Number of top users to show per time bucket",
)
def analyze_cmd(
    filesystem: str,
    access_history: bool,
    file_size: bool,
    owner_id: str | None,
    mine: bool,
    path_prefixes: tuple[str, ...],
    min_depth: int | None,
    max_depth: int | None,
    data_dir: Path | None,
    show_config: bool,
    top_n: int,
):
    """Analyze filesystem usage patterns.

    FILESYSTEM is the name of the filesystem (e.g., asp, cisl, eol, hao),
    or 'all' to analyze all available databases (default).

    \b
    Examples:
      fs-scans analyze --access-history                 # All filesystems
      fs-scans analyze asp --access-history             # Specific filesystem
      fs-scans analyze --access-history --owner jsmith  # Filter to user
      fs-scans analyze --access-history --mine          # Current user only
      fs-scans analyze --access-history -P /cisl        # Filter to path (slower)
      fs-scans analyze --file-size                      # File size distribution
      fs-scans analyze --file-size --owner jdoe         # Size by user
    """
    # Apply data directory override if provided via CLI
    if data_dir is not None:
        set_data_dir(data_dir)

    # Handle --show-config
    if show_config:
        data_path, source = get_data_dir_info()
        console.print("[bold]Configuration[/bold]")
        console.print(f"  Data directory: {data_path}")
        console.print(f"  Source: {source}")
        console.print()

        filesystems = get_all_filesystems()
        if filesystems:
            console.print("[bold]Available databases[/bold]")
            for fs in filesystems:
                db_path = get_db_path(fs)
                size_bytes = db_path.stat().st_size if db_path.exists() else 0
                from ..cli.common import format_size
                size_str = format_size(size_bytes)
                console.print(f"  {fs}: {db_path} ({size_str})")
        else:
            console.print("[yellow]No database files found.[/yellow]")
        return

    # Check that at least one analysis option is specified
    if not access_history and not file_size:
        console.print("[yellow]No analysis option specified.[/yellow]")
        console.print("Try: fs-scans analyze --access-history")
        console.print("     fs-scans analyze --file-size")
        console.print("Use --help for more options")
        return

    # Determine which filesystems to analyze
    if filesystem.lower() == "all":
        filesystems = get_all_filesystems()
        if not filesystems:
            console.print("[red]No database files found.[/red]")
            console.print("Run fs-scans import first to import data.")
            raise SystemExit(1)
    else:
        db_path = get_db_path(filesystem)
        if not db_path.exists():
            console.print(f"[red]Database not found: {db_path}[/red]")
            console.print("Run fs-scans import first to import data.")
            raise SystemExit(1)
        filesystems = [filesystem]

    # Resolve owner filter
    resolved_owner_uid = resolve_owner_filter(owner_id, mine)

    # Normalize path arguments (strip mount point prefixes)
    normalized_path_prefixes = [normalize_path(p) for p in path_prefixes] if path_prefixes else None

    # Determine if we can use ORM fast path (no path/depth filters)
    use_orm_histogram = (
        not normalized_path_prefixes and
        not min_depth and
        not max_depth
    )

    # Process access history - combine all filesystems into single histogram
    if access_history:
        # Determine directory for display
        if normalized_path_prefixes:
            display_dir = normalized_path_prefixes[0] if len(normalized_path_prefixes) == 1 else "Multiple paths"
        elif len(filesystems) == 1:
            display_dir = f"/{filesystems[0]}"
        else:
            display_dir = "All filesystems"

        # Collect scan dates and use the most recent one for bucketing
        scan_dates = []
        for fs in filesystems:
            session = get_session(fs)
            try:
                scan_date = get_scan_date(session)
                if scan_date:
                    scan_dates.append(scan_date)
            finally:
                session.close()

        if not scan_dates:
            console.print("[yellow]Warning: No scan dates found in any database[/yellow]")
            return

        # Use the most recent scan date for histogram bucketing
        reference_scan_date = max(scan_dates)

        # Show scan date info
        if len(filesystems) > 1:
            unique_dates = sorted(set(d.date() for d in scan_dates))
            if len(unique_dates) == 1:
                console.print(f"[dim]Scan date: {unique_dates[0]}[/dim]")
            else:
                console.print(f"[dim]Scan dates range from {unique_dates[0]} to {unique_dates[-1]}[/dim]")
                console.print(f"[dim]Using {reference_scan_date.date()} as reference for age calculations[/dim]")

        if use_orm_histogram:
            # Fast path: Use pre-computed histogram ORM tables
            combined_histogram, username_map = aggregate_histograms_across_databases(
                filesystems=filesystems,
                histogram_type="access",
                owner_uid=resolved_owner_uid,
            )

            # Display results using HistogramData format_output
            output = combined_histogram.format_output(
                title="Access Time Distribution",
                directory=display_dir,
                username_map=username_map,
                top_n=top_n,
            )
            console.print(output)
            console.print()

        else:
            # Slow path: Compute from directory_stats (needed for path filters)
            console.print("[yellow]Note: Path filtering requires on-the-fly computation (slower)[/yellow]")
            console.print()

            # Create a single combined histogram
            from ..queries.access_history import AccessHistogram
            combined_histogram = AccessHistogram(reference_scan_date)

            # Process each filesystem and merge into combined histogram
            all_uids = set()
            for fs in filesystems:
                session = get_session(fs)
                try:
                    # Get scan date for this filesystem
                    scan_date = get_scan_date(session)
                    if not scan_date:
                        console.print(f"[yellow]Warning: No scan date found for {fs}, skipping[/yellow]")
                        continue

                    # Compute access history for this filesystem
                    fs_histogram = compute_access_history(
                        session,
                        scan_date,
                        path_prefixes=normalized_path_prefixes,
                        min_depth=min_depth,
                        max_depth=max_depth,
                    )

                    # Merge into combined histogram
                    combined_histogram.total_data += fs_histogram.total_data
                    combined_histogram.total_files += fs_histogram.total_files

                    for bucket_label in combined_histogram.buckets.keys():
                        fs_bucket = fs_histogram.buckets[bucket_label]
                        combined_bucket = combined_histogram.buckets[bucket_label]

                        combined_bucket["data"] += fs_bucket["data"]
                        combined_bucket["files"] += fs_bucket["files"]

                        # Merge owner stats
                        for uid, stats in fs_bucket["owners"].items():
                            combined_bucket["owners"][uid]["data"] += stats["data"]
                            combined_bucket["owners"][uid]["files"] += stats["files"]
                            all_uids.add(uid)

                finally:
                    session.close()

            # Resolve usernames from all databases
            username_map = resolve_usernames_across_databases(all_uids, filesystems)

            # Display combined results
            output = combined_histogram.format_output(display_dir, username_map, top_n)
            console.print(output)
            console.print()

    # Process file size histogram
    if file_size:
        # Determine directory for display
        if normalized_path_prefixes:
            display_dir = normalized_path_prefixes[0] if len(normalized_path_prefixes) == 1 else "Multiple paths"
        elif len(filesystems) == 1:
            display_dir = f"/{filesystems[0]}"
        else:
            display_dir = "All filesystems"

        # Collect scan dates
        scan_dates = []
        for fs in filesystems:
            session = get_session(fs)
            try:
                scan_date = get_scan_date(session)
                if scan_date:
                    scan_dates.append(scan_date)
            finally:
                session.close()

        if not scan_dates:
            console.print("[yellow]Warning: No scan dates found in any database[/yellow]")
            return

        # Use the most recent scan date for reference
        reference_scan_date = max(scan_dates)

        # Show scan date info
        if len(filesystems) > 1:
            unique_dates = sorted(set(d.date() for d in scan_dates))
            if len(unique_dates) == 1:
                console.print(f"[dim]Scan date: {unique_dates[0]}[/dim]")
            else:
                console.print(f"[dim]Scan dates range from {unique_dates[0]} to {unique_dates[-1]}[/dim]")

        if use_orm_histogram:
            # Fast path: Use pre-computed histogram ORM tables
            combined_histogram, username_map = aggregate_histograms_across_databases(
                filesystems=filesystems,
                histogram_type="size",
                owner_uid=resolved_owner_uid,
            )

            # Display results using HistogramData format_output
            output = combined_histogram.format_output(
                title="File Size Distribution",
                directory=display_dir,
                username_map=username_map,
                top_n=top_n,
            )
            console.print(output)
            console.print()

        else:
            # Slow path: Approximate from directory_stats (needed for path filters)
            console.print("[yellow]Note: Size distribution is approximate for path-filtered queries[/yellow]")
            console.print()

            from ..importers.importer import SIZE_BUCKETS
            from ..queries.histogram_common import HistogramData
            from ..queries.file_size import compute_size_histogram_from_directory_stats

            # Get bucket labels
            bucket_labels = [label for label, _, _ in SIZE_BUCKETS]

            # Create combined histogram
            combined_histogram = HistogramData(bucket_labels, reference_scan_date)

            # Process each filesystem and merge
            all_uids = set()
            for fs in filesystems:
                session = get_session(fs)
                try:
                    # Get scan date for this filesystem
                    scan_date = get_scan_date(session)
                    if not scan_date:
                        console.print(f"[yellow]Warning: No scan date found for {fs}, skipping[/yellow]")
                        continue

                    # Compute size histogram for this filesystem
                    fs_histogram = compute_size_histogram_from_directory_stats(
                        session,
                        scan_date,
                        path_prefixes=normalized_path_prefixes,
                        min_depth=min_depth,
                        max_depth=max_depth,
                        owner_uid=resolved_owner_uid,
                    )

                    # Merge into combined histogram
                    for bucket_label, owner_data in fs_histogram.items():
                        for uid, (file_count, total_size) in owner_data.items():
                            combined_histogram.add_bucket_data(bucket_label, uid, file_count, total_size)
                            if uid is not None and uid >= 0:
                                all_uids.add(uid)

                finally:
                    session.close()

            # Resolve usernames from all databases
            username_map = resolve_usernames_across_databases(all_uids, filesystems)

            # Display combined results
            output = combined_histogram.format_output(
                title="File Size Distribution (Approximate)",
                directory=display_dir,
                username_map=username_map,
                top_n=top_n,
            )
            console.print(output)
            console.print()
