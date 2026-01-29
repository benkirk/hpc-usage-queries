"""Display and output formatting for query results.

This module handles presentation of query results including Rich tables and TSV output.
"""

from pathlib import Path

from rich.table import Table

from ..cli.common import console, format_datetime, format_size


def print_results(
    directories: list[dict],
    verbose: bool = False,
    leaves_only: bool = False,
    username_map: dict[int, str] | None = None,
    show_total: bool = False,
    show_dir_counts: bool = False,
) -> None:
    """Print directory results in a formatted table."""
    if not directories:
        console.print("[yellow]No directories found matching criteria.[/yellow]")
        return

    if username_map is None:
        username_map = {}

    table = Table(title=f"Directory Statistics ({len(directories)} results)")
    table.add_column("Directory", style="cyan", no_wrap=False)
    if verbose:
        table.add_column("Depth", justify="right")

    if leaves_only:
        # Simplified columns for leaf directories (R and NR are identical)
        table.add_column("Size", justify="right")
        table.add_column("Files", justify="right")
        if show_dir_counts:
            table.add_column("Dirs", justify="right")
        table.add_column("Atime", justify="right")
    else:
        table.add_column("Size\n", justify="right")
        table.add_column("Size\n(NR)", justify="right")
        table.add_column("Files\n", justify="right")
        table.add_column("Files\n(NR)", justify="right")
        if show_dir_counts:
            table.add_column("Dirs\n", justify="right")
            table.add_column("Dirs\n(NR)", justify="right")
        table.add_column("Atime\n", justify="right")
        table.add_column("Atime\n(NR)", justify="right")
    table.add_column("Owner", justify="right")

    # Track totals for summary row
    total_size_r = 0
    total_size_nr = 0
    total_files_r = 0
    total_files_nr = 0

    for i, d in enumerate(directories):
        uid = d["owner_uid"]
        if uid is not None and uid != -1:
            owner_display = username_map.get(uid, str(uid))
            owner_str = f"[green]{owner_display}[/green]"
        elif uid is None:
            owner_str = "[yellow]multiple[/yellow]"
        else:
            owner_str = "[dim]-[/dim]"

        row = [d["path"]]
        if verbose:
            row.append(str(d["depth"]))

        if leaves_only:
            row_data = [
                format_size(d["total_size_r"]),
                f"{d['file_count_r']:,}",
            ]
            if show_dir_counts:
                row_data.append(f"{d.get('ndirs_nr', 0):,}")
            row_data.extend([
                format_datetime(d["max_atime_r"]),
                owner_str,
            ])
            row.extend(row_data)
        else:
            row_data = [
                format_size(d["total_size_r"]),
                format_size(d["total_size_nr"]),
                f"{d['file_count_r']:,}",
                f"{d['file_count_nr']:,}",
            ]
            if show_dir_counts:
                row_data.extend([
                    f"{d.get('ndirs_r', 0):,}",
                    f"{d.get('ndirs_nr', 0):,}",
                ])
            row_data.extend([
                format_datetime(d["max_atime_r"]),
                format_datetime(d["max_atime_nr"]),
                owner_str,
            ])
            row.extend(row_data)

        # Add separator line before totals row
        end_section = (i == len(directories) - 1) and len(directories) > 1 and show_total
        table.add_row(*row, end_section=end_section)

        # Accumulate totals
        total_size_r += d["total_size_r"]
        total_size_nr += d["total_size_nr"]
        total_files_r += d["file_count_r"]
        total_files_nr += d["file_count_nr"]

    # Add totals row if more than one directory and --show-total is enabled
    if len(directories) > 1 and show_total:
        row = ["[bold]Total:[/bold]"]
        if verbose:
            row.append("")  # Empty depth column

        if leaves_only:
            row_data = [
                f"[bold]{format_size(total_size_r)}[/bold]",
                f"[bold]{total_files_r:,}[/bold]",
            ]
            if show_dir_counts:
                row_data.append("")  # Empty dir count
            row_data.extend([
                "",  # Empty atime
                "",  # Empty owner
            ])
            row.extend(row_data)
        else:
            row_data = [
                f"[bold]{format_size(total_size_r)}[/bold]",
                f"[bold]{format_size(total_size_nr)}[/bold]",
                f"[bold]{total_files_r:,}[/bold]",
                f"[bold]{total_files_nr:,}[/bold]",
            ]
            if show_dir_counts:
                row_data.extend(["", ""])  # Empty dir counts (R and NR)
            row_data.extend([
                "",  # Empty atime (R)
                "",  # Empty atime (NR)
                "",  # Empty owner
            ])
            row.extend(row_data)
        table.add_row(*row)

    console.print(table)


def write_tsv(directories: list[dict], output_path: Path, include_dir_counts: bool = False) -> None:
    """Write results to TSV file."""
    with open(output_path, "w") as f:
        # Header
        header = (
            "directory\tdepth\t"
            "total_size_r\ttotal_size_nr\t"
            "file_count_r\tfile_count_nr\t"
        )
        if include_dir_counts:
            header += "dir_count_r\tdir_count_nr\t"
        header += (
            "max_atime_r\tmax_atime_nr\t"
            "owner_uid\n"
        )
        f.write(header)

        for d in directories:
            line = (
                f"{d['path']}\t{d['depth']}\t"
                f"{d['total_size_r']}\t{d['total_size_nr']}\t"
                f"{d['file_count_r']}\t{d['file_count_nr']}\t"
            )
            if include_dir_counts:
                line += f"{d.get('ndirs_r', 0)}\t{d.get('ndirs_nr', 0)}\t"
            line += (
                f"{format_datetime(d['max_atime_r'])}\t"
                f"{format_datetime(d['max_atime_nr'])}\t"
                f"{d['owner_uid']}\n"
            )
            f.write(line)

    console.print(f"[green]Results written to {output_path}[/green]")


def print_owner_results(
    owners: list[dict],
    username_map: dict[int, str],
    show_filesystem: bool = False
) -> None:
    """Print owner summary results in a formatted table.

    Args:
        owners: List of owner summary dictionaries
        username_map: Mapping from UID to username
        show_filesystem: If True, add Filesystem column (for multi-DB queries)
    """
    if not owners:
        console.print("[yellow]No owner data found.[/yellow]")
        return

    # Adjust title based on whether showing filesystem breakdown
    if show_filesystem:
        unique_combos = len(owners)  # Each row is owner+filesystem combo
        table = Table(title=f"Owner Summary ({unique_combos} owner-filesystem combinations)")
    else:
        table = Table(title=f"Owner Summary ({len(owners)} owners)")

    table.add_column("Owner", style="cyan")

    # Add Filesystem column if showing breakdown
    if show_filesystem:
        table.add_column("Filesystem", style="blue")

    # UID column removed (redundant with username)
    table.add_column("Total Size", justify="right")
    table.add_column("Total Files", justify="right")
    table.add_column("Directories", justify="right")

    for o in owners:
        uid = o["owner_uid"]
        username = username_map.get(uid, str(uid))

        row = [username]

        # Add filesystem column value if needed
        if show_filesystem:
            row.append(o.get("filesystem", "unknown"))

        row.extend([
            format_size(o["total_size"]),
            f"{o['total_files']:,}",
            f"{o['directory_count']:,}",
        ])

        table.add_row(*row)

    console.print(table)

