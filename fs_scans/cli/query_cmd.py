"""Query subcommand for fs-scans CLI.

This module provides the query interface for the unified fs-scans CLI.
"""

import os
from pathlib import Path

import click

from ..cli.common import (
    console,
    make_dynamic_help_command,
    parse_date_arg,
)
from ..core.database import get_data_dir_info, set_data_dir
from ..queries.query_engine import (
    get_all_filesystems,
    get_scan_date,
    get_username_map,
    normalize_path,
    query_owner_summary,
    query_single_filesystem,
)
from ..queries.display import (
    print_owner_results,
    print_results,
    write_tsv,
)



# Create DynamicHelpCommand for this tool
DynamicHelpCommand = make_dynamic_help_command('fs-scans query')

@click.command(cls=DynamicHelpCommand, context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("filesystem", type=str, default="all")
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
    "-s",
    "--single-owner",
    is_flag=True,
    help="Only show single-owner directories",
)
@click.option(
    "-u",
    "--owner",
    "owner_id",
    type=str,
    help="Filter to specific owner (UID or username)",
)
@click.option(
    "--mine",
    is_flag=True,
    help="Filter to current user's UID (shortcut for -u $UID)",
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
    "--exclude",
    "-E",
    "exclude_paths",
    multiple=True,
    type=str,
    help="Exclude path and descendants from results (can be repeated)",
)
@click.option(
    "-n",
    "--limit",
    type=int,
    default=50,
    show_default=True,
    help="Limit results (0 for unlimited)",
)
@click.option(
    "--sort-by",
    type=click.Choice(["size", "size_r", "size_nr", "files", "files_r", "files_nr", "atime", "atime_r", "path", "depth", "dirs"]),
    default="size",
    show_default=True,
    help="Sort results by field (with --group-by owner: size, files, dirs)",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(path_type=Path),
    help="Write TSV output to file",
)
@click.option(
    "--accessed-before",
    type=str,
    help="Filter to max_atime_r before date (YYYY-MM-DD or Nyrs/Nmo)",
)
@click.option(
    "--accessed-after",
    type=str,
    help="Filter to max_atime_r after date (YYYY-MM-DD or Nyrs/Nmo)",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Show additional columns (Depth)",
)
@click.option(
    "--leaves-only",
    is_flag=True,
    help="Only show leaf directories (no subdirectories)",
)
@click.option(
    "--data-dir",
    "data_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Override directory containing database files (or set FS_SCAN_DATA_DIR env var)",
)
@click.option(
    "--summary",
    is_flag=True,
    help="Show database summary only",
)
@click.option(
    "--show-config",
    is_flag=True,
    help="Show data directory configuration and available databases",
)
@click.option(
    "-N",
    "--name-pattern",
    "name_patterns",
    multiple=True,
    type=str,
    help="Filter by name pattern (GLOB); can repeat for OR matching",
)
@click.option(
    "-i",
    "--ignore-case",
    is_flag=True,
    help="Make --name-pattern matching case-insensitive",
)
@click.option(
    "--min-size",
    type=str,
    default="1GiB",
    show_default=True,
    help="Min total recursive size (e.g. 500MB, 2GiB, 0 to disable)",
)
@click.option(
    "--max-size",
    type=str,
    default=None,
    help="Max total recursive size (e.g. 10GiB)",
)
@click.option(
    "--min-files",
    type=str,
    default=None,
    help="Min recursive file count (e.g. 500, 10K)",
)
@click.option(
    "--max-files",
    type=str,
    default=None,
    help="Max recursive file count",
)
@click.option(
    "--group-by",
    "group_by",
    type=click.Choice(["owner"]),
    help="Group results by field (currently: owner)",
)
@click.option(
    "--show-total",
    is_flag=True,
    help="Show totals row at bottom of results",
)
@click.option(
    "--dir-counts",
    is_flag=True,
    help="Show directory counts (Dirs and Dirs(NR) columns)",
)
def query_cmd(
    filesystem: str,
    min_depth: int | None,
    max_depth: int | None,
    single_owner: bool,
    owner_id: str | None,
    mine: bool,
    path_prefixes: tuple[str, ...],
    exclude_paths: tuple[str, ...],
    limit: int,
    sort_by: str,
    output: Path | None,
    accessed_before: str | None,
    accessed_after: str | None,
    verbose: bool,
    leaves_only: bool,
    data_dir: Path | None,
    summary: bool,
    show_config: bool,
    name_patterns: tuple[str, ...],
    ignore_case: bool,
    min_size: str,
    max_size: str | None,
    min_files: str | None,
    max_files: str | None,
    group_by: str | None,
    show_total: bool,
    dir_counts: bool,
):
    """
    Query GPFS scan database for directory statistics.

    FILESYSTEM is the name of the filesystem (e.g., asp, cisl, eol, hao),
    or 'all' to query all available databases (default).

    \b
    Examples:
      query-fs-scan-db                       # all filesystems (default)
      query-fs-scan-db asp                   # specific filesystem
      query-fs-scan-db -d 4 --single-owner   # single-owner dirs at depth 4+
      query-fs-scan-db --accessed-before 3yrs
      query-fs-scan-db --leaves-only
      query-fs-scan-db -N "*scratch*"        # filter by name pattern
      query-fs-scan-db -N "*scratch*" -N "*tmp*"  # multiple patterns (OR)
      query-fs-scan-db -N "*tmp*" -i         # case-insensitive name filter
      query-fs-scan-db --group-by owner      # per-user summary
      query-fs-scan-db --group-by owner --sort-by files  # sort by file count
      query-fs-scan-db --group-by owner -d 4 -P /gpfs/csfs1/cisl
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
                size_str = format_size(size_bytes)
                console.print(f"  {fs}: {db_path} ({size_str})")
        else:
            console.print("[yellow]No database files found.[/yellow]")
        return

    # Resolve owner_id: can be UID (int) or username (string)
    resolved_owner_id: int | None = None
    if mine:
        resolved_owner_id = os.getuid()
    elif owner_id is not None:
        try:
            resolved_owner_id = int(owner_id)
        except ValueError:
            # Not an integer, try to resolve as username
            try:
                resolved_owner_id = pwd.getpwnam(owner_id).pw_uid
            except KeyError:
                console.print(f"[red]Unknown user: {owner_id}[/red]")
                raise SystemExit(1)

    # Determine which filesystems to query
    if filesystem.lower() == "all":
        filesystems = get_all_filesystems()
        if not filesystems:
            console.print("[red]No database files found.[/red]")
            console.print("Run fs-scan-to-db first to import data.")
            raise SystemExit(1)
    else:
        db_path = get_db_path(filesystem)
        if not db_path.exists():
            console.print(f"[red]Database not found: {db_path}[/red]")
            console.print("Run fs-scan-to-db first to import data.")
            raise SystemExit(1)
        filesystems = [filesystem]

    console.print(f"[bold]Filesystem Scan Database Query[/bold]")
    console.print(f"Databases: {', '.join(filesystems)}")

    # Collect and display scan dates
    scan_dates = []
    for fs in filesystems:
        session = get_session(fs)
        try:
            scan_date = get_scan_date(session)
            if scan_date:
                scan_dates.append(scan_date)
        finally:
            session.close()

    if scan_dates:
        unique_dates = sorted(set(d.date() for d in scan_dates))
        if len(unique_dates) == 1:
            console.print(f"[dim]Data from scan: {unique_dates[0]}[/dim]")
        else:
            console.print(f"[dim]Scans from {unique_dates[0]} to {unique_dates[-1]}[/dim]")

    console.print(f"Note: this information is [bold]NOT[/bold] real-time")
    console.print()

    # Parse date arguments once
    parsed_before = parse_date_arg(accessed_before) if accessed_before else None
    parsed_after = parse_date_arg(accessed_after) if accessed_after else None

    # Parse size/file-count filter arguments
    parsed_min_size = parse_size(min_size) if min_size else None
    parsed_max_size = parse_size(max_size) if max_size else None
    parsed_min_files = parse_file_count(min_files) if min_files else None
    parsed_max_files = parse_file_count(max_files) if max_files else None

    # Normalize path arguments (strip mount point prefixes)
    normalized_path_prefixes = [normalize_path(p) for p in path_prefixes] if path_prefixes else []
    normalized_exclude_paths = [normalize_path(p) for p in exclude_paths] if exclude_paths else []

    # Handle summary mode
    if summary:
        for fs in filesystems:
            session = get_session(fs)
            try:
                stats = get_summary(session)
                console.print(f"[cyan]{fs}:[/cyan]")
                console.print(f"  Total directories: {stats['total_directories']:,}")
                console.print(f"  Root directories: {stats['root_directories']:,}")
                console.print(f"  Total files (root): {stats['total_files']:,}")
                console.print(f"  Total size (root): {format_size(stats['total_size'])}")
                console.print(f"  Maximum depth: {stats['max_depth']}")
            finally:
                session.close()
        return

    # Handle --group-by owner mode
    if group_by == "owner":
        if dir_counts:
            console.print("[yellow]Warning: --dir-counts ignored with --group-by owner[/yellow]")

        # Map sort_by to owner summary field names
        # Accept common aliases for convenience
        owner_sort_map = {
            "size": "size",
            "files": "files",
            "dirs": "dirs",
            "directories": "dirs",
        }
        owner_sort_by = owner_sort_map.get(sort_by, "size")

        # Validate sort_by is compatible with --group-by owner
        if sort_by not in owner_sort_map:
            console.print(
                f"[yellow]Warning: --sort-by '{sort_by}' not valid with --group-by owner. "
                f"Using 'size' instead. Valid options: size, files, dirs[/yellow]"
            )
            owner_sort_by = "size"

        all_owners = []
        all_uids = set()

        for fs in filesystems:
            session = get_session(fs)
            try:
                owners = query_owner_summary(
                    session,
                    min_depth=min_depth,
                    max_depth=max_depth,
                    path_prefixes=normalized_path_prefixes if normalized_path_prefixes else None,
                    limit=limit if limit > 0 else None,
                    sort_by=owner_sort_by,
                )
                # Tag each owner result with filesystem name
                for owner in owners:
                    owner["filesystem"] = fs

                all_owners.extend(owners)
                all_uids.update(o["owner_uid"] for o in owners)
            finally:
                session.close()

        # For multi-db: sort by metric, then owner, then filesystem (don't aggregate)
        if len(filesystems) > 1:
            # Sort to group owners together while preserving per-filesystem breakdown
            sort_key_map = {
                "size": lambda o: (-o["total_size"], o["owner_uid"], o["filesystem"]),
                "files": lambda o: (-o["total_files"], o["owner_uid"], o["filesystem"]),
                "dirs": lambda o: (-o["directory_count"], o["owner_uid"], o["filesystem"]),
            }
            sort_key = sort_key_map[owner_sort_by]
            all_owners.sort(key=sort_key)

            # Apply limit to final sorted list
            if limit > 0:
                all_owners = all_owners[:limit]

        # Get username mappings (aggregate across all databases)
        username_map = {}
        if all_uids and filesystems:
            remaining_uids = set(all_uids)
            for fs in filesystems:
                if not remaining_uids:
                    break
                session = get_session(fs)
                try:
                    found = get_username_map(session, list(remaining_uids))
                    username_map.update(found)
                    remaining_uids -= found.keys()
                finally:
                    session.close()

        # Show filesystem column when querying multiple databases
        show_filesystem = len(filesystems) > 1
        print_owner_results(all_owners, username_map, show_filesystem=show_filesystem)
        return

    # Query directories
    multi_db = len(filesystems) > 1
    all_directories = []

    if multi_db:
        # Parallel execution for multiple filesystems
        max_workers = min(len(filesystems), 8)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    query_single_filesystem,
                    fs,
                    min_depth,
                    max_depth,
                    single_owner,
                    resolved_owner_id,
                    normalized_path_prefixes if normalized_path_prefixes else None,
                    normalized_exclude_paths if normalized_exclude_paths else None,
                    sort_by,
                    limit if limit > 0 else None,
                    parsed_before,
                    parsed_after,
                    leaves_only,
                    list(name_patterns) if name_patterns else None,
                    ignore_case,
                    parsed_min_size,
                    parsed_max_size,
                    parsed_min_files,
                    parsed_max_files,
                    dir_counts,
                ): fs
                for fs in filesystems
            }
            for future in as_completed(futures):
                fs = futures[future]
                try:
                    dirs = future.result()
                    all_directories.extend(dirs)
                except Exception as e:
                    console.print(f"[red]Error querying {fs}: {e}[/red]")
    else:
        # Single filesystem - sequential execution (no thread overhead)
        session = get_session(filesystems[0])
        try:
            all_directories = query_directories(
                session,
                min_depth=min_depth,
                max_depth=max_depth,
                single_owner=single_owner,
                owner_id=resolved_owner_id,
                path_prefixes=normalized_path_prefixes if normalized_path_prefixes else None,
                exclude_paths=normalized_exclude_paths if normalized_exclude_paths else None,
                sort_by=sort_by,
                limit=limit if limit > 0 else None,
                accessed_before=parsed_before,
                accessed_after=parsed_after,
                leaves_only=leaves_only,
                name_patterns=list(name_patterns) if name_patterns else None,
                name_pattern_ignorecase=ignore_case,
                min_size=parsed_min_size,
                max_size=parsed_max_size,
                min_files=parsed_min_files,
                max_files=parsed_max_files,
                compute_dir_counts=dir_counts,
            )
        finally:
            session.close()

    # For multi-db: sort combined results and apply limit
    if multi_db:
        sort_key_map = {
            "size_r": lambda d: d["total_size_r"] or 0,
            "size_nr": lambda d: d["total_size_nr"] or 0,
            "files_r": lambda d: d["file_count_r"] or 0,
            "files_nr": lambda d: d["file_count_nr"] or 0,
            "atime_r": lambda d: d["max_atime_r"] or "",
            "path": lambda d: (d["depth"], d["path"]),
            "depth": lambda d: d["depth"],
        }
        reverse = sort_by not in ("path",)  # Most sorts are descending
        all_directories.sort(key=sort_key_map.get(sort_by, sort_key_map["size_r"]), reverse=reverse)

        # Apply limit after sorting combined results
        if limit > 0:
            all_directories = all_directories[:limit]

    # Output results
    if output:
        write_tsv(all_directories, output, include_dir_counts=dir_counts)
    else:
        # Resolve UIDs to usernames for display (aggregate across all databases)
        unique_uids = {
            d["owner_uid"] for d in all_directories
            if d["owner_uid"] is not None and d["owner_uid"] != -1
        }
        username_map = {}
        if unique_uids:
            remaining_uids = set(unique_uids)
            for fs in filesystems:
                if not remaining_uids:
                    break
                session = get_session(fs)
                try:
                    found = get_username_map(session, list(remaining_uids))
                    username_map.update(found)
                    remaining_uids -= found.keys()
                finally:
                    session.close()
        print_results(all_directories, verbose=verbose, leaves_only=leaves_only, username_map=username_map, show_total=show_total, show_dir_counts=dir_counts)


if __name__ == "__main__":
    main()
