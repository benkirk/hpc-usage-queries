"""Analyze subcommand for fs-scans CLI.

Thin Click adapter over :class:`fs_scans.queries.FsScanQueries`: it parses
options, calls the facade (the single source of truth shared with importing
Python apps), and renders the resulting histogram through the selected
:class:`~fs_scans.cli.core.Exporter`.
"""

from pathlib import Path

import click

from ..cli.common import (
    console,
    make_dynamic_help_command,
    render_show_config,
)
from ..cli.core import (
    ExporterRegistry,
    build_access_history,
    build_file_size,
)
from ..core.database import filesystem_available, get_db_url, set_data_dir
from ..queries.facade import FsScanQueries
from ..queries.query_engine import (
    get_all_filesystems,
    resolve_owner_filter,
)


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
    "--format",
    "output_format",
    type=click.Choice(["rich", "json"]),
    default="rich",
    show_default=True,
    help="Output format for stdout",
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
    output_format: str,
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
      fs-scans analyze --access-history --format json   # JSON envelope to stdout
    """
    # Apply data directory override if provided via CLI
    if data_dir is not None:
        set_data_dir(data_dir)

    # Handle --show-config
    if show_config:
        render_show_config()
        return

    # Check that at least one analysis option is specified
    if not access_history and not file_size:
        console.print("[yellow]No analysis option specified.[/yellow]")
        console.print("Try: fs-scans analyze --access-history")
        console.print("     fs-scans analyze --file-size")
        console.print("Use --help for more options")
        return

    quiet = output_format == "json"

    # Determine which filesystems to analyze
    if filesystem.lower() == "all":
        filesystems = get_all_filesystems()
        if not filesystems:
            console.print("[red]No database files found.[/red]")
            console.print("Run fs-scans import first to import data.")
            raise SystemExit(1)
    else:
        if not filesystem_available(filesystem):
            console.print(f"[red]Database not found: {get_db_url(filesystem)}[/red]")
            console.print("Run fs-scans import first to import data.")
            raise SystemExit(1)
        filesystems = [filesystem]

    # Resolve owner filter
    resolved_owner_uid = resolve_owner_filter(owner_id, mine)
    prefixes = path_prefixes or None

    queries = FsScanQueries(filesystems=filesystems)

    # Process access history
    if access_history:
        hist = queries.access_history(
            owner_uid=resolved_owner_uid,
            path_prefixes=prefixes,
            min_depth=min_depth,
            max_depth=max_depth,
        )
        if hist is None:
            console.print("[yellow]Warning: No scan dates found in any database[/yellow]")
            return

        if not quiet:
            _print_scan_context(filesystems, hist, include_reference=True)
            if not hist["fast_path"]:
                console.print("[yellow]Note: Path filtering requires on-the-fly computation (slower)[/yellow]")
                console.print()

        envelope = build_access_history(hist, filesystems=filesystems, top_n=top_n)
        ExporterRegistry.resolve(output_format).emit(envelope)

    # Process file size histogram
    if file_size:
        hist = queries.file_size_histogram(
            owner_uid=resolved_owner_uid,
            path_prefixes=prefixes,
            min_depth=min_depth,
            max_depth=max_depth,
        )
        if hist is None:
            console.print("[yellow]Warning: No scan dates found in any database[/yellow]")
            return

        if not quiet:
            _print_scan_context(filesystems, hist, include_reference=False)
            if not hist["fast_path"]:
                console.print("[yellow]Note: Size distribution is approximate for path-filtered queries[/yellow]")
                console.print()

        envelope = build_file_size(hist, filesystems=filesystems, top_n=top_n)
        ExporterRegistry.resolve(output_format).emit(envelope)


def _print_scan_context(filesystems, hist, *, include_reference: bool) -> None:
    """Print the multi-filesystem scan-date context lines (rich mode only)."""
    if len(filesystems) <= 1:
        return

    scan_dates = hist["scan_dates"]
    unique_dates = sorted(set(d.date() for d in scan_dates))
    if len(unique_dates) == 1:
        console.print(f"[dim]Scan date: {unique_dates[0]}[/dim]")
    else:
        console.print(f"[dim]Scan dates range from {unique_dates[0]} to {unique_dates[-1]}[/dim]")
        if include_reference:
            console.print(
                f"[dim]Using {hist['reference_scan_date'].date()} as reference for age calculations[/dim]"
            )
