"""Query subcommand for fs-scans CLI.

Thin Click adapter over :class:`fs_scans.queries.FsScanQueries`: it parses and
validates options, calls the facade (the single source of truth shared with
importing Python apps), wraps the result in a ``kind=`` envelope, and emits it
through the selected :class:`~fs_scans.cli.core.Exporter`.
"""

from pathlib import Path

import click

from ..cli.common import (
    console,
    format_size,
    make_dynamic_help_command,
    parse_date_arg,
    parse_file_count,
    parse_size,
    render_show_config,
)
from ..cli.core import (
    ExporterRegistry,
    build_directories,
    build_group_summary,
    build_owner_summary,
)
from ..cli.core.output import TSVFileExporter
from ..core.database import filesystem_available, get_db_url, set_data_dir
from ..queries.facade import FsScanQueries
from ..queries.query_engine import (
    get_all_filesystems,
    resolve_group_filter,
    resolve_owner_filter,
)

# Valid --sort-by values when grouping by owner/group.
_ENTITY_SORT_CHOICES = {"size", "files", "dirs", "directories"}


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
    "-g",
    "--group",
    "group_id",
    type=str,
    help="Filter to specific group (GID or groupname)",
)
@click.option(
    "--mygroup",
    is_flag=True,
    help="Filter to current user's primary GID (shortcut for -g $GID)",
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
    type=click.Choice(["size", "size_r", "size_nr", "files", "files_r", "files_nr", "dirs", "dirs_r", "dirs_nr", "atime", "atime_r", "path", "depth"]),
    default="size",
    show_default=True,
    help="Sort results by field (with --group-by owner/group: size, files, dirs)",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(path_type=Path),
    help="Write TSV output to file",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["rich", "json"]),
    default="rich",
    show_default=True,
    help="Output format for stdout (ignored when -o/--output is given)",
)
@click.option(
    "--accessed-before",
    type=str,
    help="Filter to last access before date (YYYY-MM-DD or Nyrs/Nmo)",
)
@click.option(
    "--accessed-after",
    type=str,
    help="Filter to last access after date (YYYY-MM-DD or Nyrs/Nmo)",
)
@click.option(
    "--atime-non-recursive",
    "atime_non_recursive",
    is_flag=True,
    help="Apply --accessed-before/--accessed-after to the directory's own "
         "files (max_atime_nr) instead of the whole subtree (max_atime_r)",
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
    "--min-avg-size",
    type=str,
    default=None,
    help="Min average own-file size (total_size_nr/file_count_nr; e.g. 1KiB, "
         "1MiB) — the dimension the file-size histogram buckets by",
)
@click.option(
    "--max-avg-size",
    type=str,
    default=None,
    help="Max average own-file size, exclusive (e.g. 10KiB)",
)
@click.option(
    "--group-by",
    "group_by",
    type=click.Choice(["owner", "group"]),
    help="Group results by field (owner or group)",
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
    group_id: str | None,
    mygroup: bool,
    path_prefixes: tuple[str, ...],
    exclude_paths: tuple[str, ...],
    limit: int,
    sort_by: str,
    output: Path | None,
    output_format: str,
    accessed_before: str | None,
    accessed_after: str | None,
    atime_non_recursive: bool,
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
    min_avg_size: str | None,
    max_avg_size: str | None,
    group_by: str | None,
    show_total: bool,
    dir_counts: bool,
):
    """Query GPFS scan database for directory statistics.

    FILESYSTEM is the name of the filesystem (e.g., asp, cisl, eol, hao),
    or 'all' to query all available databases (default).

    \b
    Examples:
      fs-scans query                          # All filesystems (default)
      fs-scans query asp                      # Specific filesystem
      fs-scans query -d 4 --single-owner      # Single-owner dirs at depth 4+
      fs-scans query --accessed-before 3yrs   # Old data (newest access in subtree)
      fs-scans query --accessed-before 3yrs --atime-non-recursive  # Stale own-files
      fs-scans query --min-avg-size 1KiB --max-avg-size 10KiB  # Dirs in a size band
      fs-scans query --leaves-only            # Leaf directories only
      fs-scans query -N "*scratch*"           # Filter by name pattern
      fs-scans query -N "*scratch*" -N "*tmp*"  # Multiple patterns (OR)
      fs-scans query -N "*tmp*" -i            # Case-insensitive pattern
      fs-scans query --group-by owner         # Per-user summary (aggregated)
      fs-scans query --group-by owner -v     # Per-user per-filesystem breakdown
      fs-scans query --group-by owner --sort-by files  # Sort by file count
      fs-scans query --group-by owner -d 4 -P /gpfs/csfs1/cisl
      fs-scans query -g cseg                  # Directories owned by a group
      fs-scans query --mygroup                # Directories owned by your group
      fs-scans query --group-by group         # Per-group summary (aggregated)
      fs-scans query --group-by group -v     # Per-group per-filesystem breakdown
      fs-scans query --group-by owner --format json     # JSON envelope to stdout
    """
    # Apply data directory override if provided via CLI
    if data_dir is not None:
        set_data_dir(data_dir)

    # Handle --show-config
    if show_config:
        render_show_config()
        return

    # JSON output goes to stdout, so suppress the human-oriented header lines.
    quiet = output_format == "json" and output is None

    # Resolve owner_id: can be UID (int) or username (string)
    resolved_owner_id = resolve_owner_filter(owner_id, mine)
    # Resolve group_id: can be GID (int) or groupname (string)
    resolved_group_id = resolve_group_filter(group_id, mygroup)

    # Determine which filesystems to query
    if filesystem.lower() == "all":
        filesystems = get_all_filesystems()
        if not filesystems:
            console.print("[red]No database files found.[/red]")
            console.print("Run fs-scan-to-db first to import data.")
            raise SystemExit(1)
    else:
        if not filesystem_available(filesystem):
            console.print(f"[red]Database not found: {get_db_url(filesystem)}[/red]")
            console.print("Run fs-scan-to-db first to import data.")
            raise SystemExit(1)
        filesystems = [filesystem]

    queries = FsScanQueries(filesystems=filesystems)

    if not quiet:
        console.print(f"[bold]Filesystem Scan Database Query[/bold]")
        console.print(f"Databases: {', '.join(filesystems)}")

        # Collect and display scan dates
        scan_dates = queries.scan_dates()
        if scan_dates:
            unique_dates = sorted(set(d.date() for d in scan_dates))
            if len(unique_dates) == 1:
                console.print(f"[dim]Data from scan: {unique_dates[0]}[/dim]")
            else:
                console.print(f"[dim]Scans from {unique_dates[0]} to {unique_dates[-1]}[/dim]")

        console.print(f"Note: this information is [bold]NOT[/bold] real-time")
        console.print()

    # Parse size/file-count filter arguments
    parsed_before = parse_date_arg(accessed_before) if accessed_before else None
    parsed_after = parse_date_arg(accessed_after) if accessed_after else None
    parsed_min_size = parse_size(min_size) if min_size else None
    parsed_max_size = parse_size(max_size) if max_size else None
    parsed_min_files = parse_file_count(min_files) if min_files else None
    parsed_max_files = parse_file_count(max_files) if max_files else None
    parsed_min_avg_size = parse_size(min_avg_size) if min_avg_size else None
    parsed_max_avg_size = parse_size(max_avg_size) if max_avg_size else None

    # Handle summary mode
    if summary:
        for stats in queries.summary():
            console.print(f"[cyan]{stats['filesystem']}:[/cyan]")
            console.print(f"  Total directories: {stats['total_directories']:,}")
            console.print(f"  Root directories: {stats['root_directories']:,}")
            console.print(f"  Total files (root): {stats['total_files']:,}")
            console.print(f"  Total size (root): {format_size(stats['total_size'])}")
            console.print(f"  Maximum depth: {stats['max_depth']}")
        return

    # Handle --group-by owner/group modes
    if group_by in ("owner", "group"):
        if dir_counts and not quiet:
            console.print(f"[yellow]Warning: --dir-counts ignored with --group-by {group_by}[/yellow]")
        if sort_by not in _ENTITY_SORT_CHOICES and not quiet:
            console.print(
                f"[yellow]Warning: --sort-by '{sort_by}' not valid with --group-by {group_by}. "
                f"Using 'size' instead. Valid options: size, files, dirs[/yellow]"
            )

        method = queries.owner_summary if group_by == "owner" else queries.group_summary
        rows = method(
            breakdown=verbose,
            min_depth=min_depth,
            max_depth=max_depth,
            path_prefixes=path_prefixes or None,
            limit=limit,
            sort_by=sort_by,
        )

        show_filesystem = len(filesystems) > 1 and verbose
        if group_by == "owner":
            ids = {r["owner_uid"] for r in rows}
            name_map = queries.resolve_usernames(ids)
            envelope = build_owner_summary(
                rows, filesystems=filesystems, name_map=name_map,
                show_filesystem=show_filesystem,
            )
        else:
            ids = {r["owner_gid"] for r in rows}
            name_map = queries.resolve_groupnames(ids)
            envelope = build_group_summary(
                rows, filesystems=filesystems, name_map=name_map,
                show_filesystem=show_filesystem,
            )

        ExporterRegistry.resolve(output_format).emit(envelope)
        return

    # Default: directory listing
    directories = queries.list_directories(
        min_depth=min_depth,
        max_depth=max_depth,
        single_owner=single_owner,
        owner_id=resolved_owner_id,
        group_id=resolved_group_id,
        path_prefixes=path_prefixes or None,
        exclude_paths=exclude_paths or None,
        sort_by=sort_by,
        limit=limit,
        accessed_before=parsed_before,
        accessed_after=parsed_after,
        atime_recursive=not atime_non_recursive,
        leaves_only=leaves_only,
        name_patterns=list(name_patterns) if name_patterns else None,
        name_pattern_ignorecase=ignore_case,
        min_size=parsed_min_size,
        max_size=parsed_max_size,
        min_files=parsed_min_files,
        max_files=parsed_max_files,
        min_avg_size=parsed_min_avg_size,
        max_avg_size=parsed_max_avg_size,
        compute_dir_counts=dir_counts,
    )

    # Resolve UIDs to usernames for display (aggregate across all databases)
    unique_uids = {
        d["owner_uid"] for d in directories
        if d["owner_uid"] is not None and d["owner_uid"] != -1
    }
    username_map = queries.resolve_usernames(unique_uids)

    envelope = build_directories(
        directories,
        filesystems=filesystems,
        username_map=username_map,
        verbose=verbose,
        leaves_only=leaves_only,
        show_total=show_total,
        show_dir_counts=dir_counts,
    )

    if output:
        TSVFileExporter(output).emit(envelope)
    else:
        ExporterRegistry.resolve(output_format).emit(envelope)
