#!/usr/bin/env python3
"""
GPFS Policy Scan Log Parser - Directory-Level Metrics

Parses GPFS policy scan log files and computes directory-level metrics
via streaming accumulators. Only directory summaries are stored, not file-level data.

Metrics per directory:
- Non-recursive: file_count, total_size, max_atime (direct children only)
- Recursive: file_count, total_size, max_atime (all descendants)
- Single-owner tracking: identifies directories where all contents share one owner
"""

import lzma
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TextIO

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

console = Console()


@dataclass
class DirStats:
    """Statistics for a single directory."""

    # Non-recursive (direct children only)
    file_count: int = 0
    total_size: int = 0
    max_atime: datetime | None = None

    # Recursive (all descendants)
    file_count_recursive: int = 0
    total_size_recursive: int = 0
    max_atime_recursive: datetime | None = None

    # Single-owner tracking (memory-efficient approach)
    owner_id: int = -1  # First owner encountered (-1 = not yet seen)
    single_owner: bool = True  # False once a different owner is seen


# Regex pattern for parsing data lines
# Format: <node> inode gen snapshot  key=value pairs -- /path
LINE_PATTERN = re.compile(
    r"^<\d+>\s+\d+\s+\d+\s+\d+\s+"  # <node> inode gen snapshot
    r"(.+?)\s+--\s+(.+)$"  # key=value pairs -- path
)

# Pattern to extract specific fields from the key=value section
FIELD_PATTERNS = {
    "size": re.compile(r"s=(\d+)"),
    "user_id": re.compile(r"u=(\d+)"),
    "permissions": re.compile(r"p=([^\s]+)"),
    "atime": re.compile(r"ac=(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"),
}


def parse_line(line: str) -> dict | None:
    """
    Parse a single log line and extract relevant fields.

    Returns dict with: path, size, user_id, is_file, atime
    Returns None if line is not a data line or is a directory entry.
    """
    match = LINE_PATTERN.match(line)
    if not match:
        return None

    fields_str, path = match.groups()

    # Extract permissions to check if file or directory
    perm_match = FIELD_PATTERNS["permissions"].search(fields_str)
    if not perm_match:
        return None

    permissions = perm_match.group(1)
    is_file = permissions.startswith("-")

    # Skip directory entries - we only count files
    if not is_file:
        return None

    # Extract other fields
    size_match = FIELD_PATTERNS["size"].search(fields_str)
    user_match = FIELD_PATTERNS["user_id"].search(fields_str)
    atime_match = FIELD_PATTERNS["atime"].search(fields_str)

    if not all([size_match, user_match]):
        return None

    # Parse atime
    atime = None
    if atime_match:
        try:
            atime = datetime.strptime(atime_match.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    return {
        "path": path,
        "size": int(size_match.group(1)),
        "user_id": int(user_match.group(1)),
        "atime": atime,
    }


def update_owner(dir_stats: DirStats, user_id: int) -> None:
    """Track single ownership efficiently - O(1) memory per directory."""
    if dir_stats.owner_id == -1:
        dir_stats.owner_id = user_id  # First file seen
    elif dir_stats.owner_id != user_id:
        dir_stats.single_owner = False  # Different owner found


def process_file(
    path: str,
    size: int,
    atime: datetime | None,
    user_id: int,
    stats_dict: dict[str, DirStats],
) -> None:
    """
    Process a single file entry, updating directory statistics.

    Updates:
    - Non-recursive stats for the direct parent directory
    - Recursive stats for all ancestor directories
    - Single-owner tracking for all ancestors
    """
    parent = os.path.dirname(path)

    # Non-recursive: only direct parent
    if parent not in stats_dict:
        stats_dict[parent] = DirStats()
    stats = stats_dict[parent]
    stats.file_count += 1
    stats.total_size += size
    if atime:
        stats.max_atime = max(stats.max_atime, atime) if stats.max_atime else atime

    # Recursive: all ancestors (including direct parent)
    current = parent
    while current and current != "/":
        if current not in stats_dict:
            stats_dict[current] = DirStats()
        stats = stats_dict[current]
        stats.file_count_recursive += 1
        stats.total_size_recursive += size
        if atime:
            stats.max_atime_recursive = (
                max(stats.max_atime_recursive, atime)
                if stats.max_atime_recursive
                else atime
            )
        update_owner(stats, user_id)
        current = os.path.dirname(current)


def open_input_file(filepath: Path) -> TextIO:
    """Open input file, handling xz compression if needed."""
    if filepath.suffix == ".xz":
        return lzma.open(filepath, "rt", encoding="utf-8", errors="replace")
    return open(filepath, "r", encoding="utf-8", errors="replace")


def format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} EB"


def format_datetime(dt: datetime | None) -> str:
    """Format datetime for display."""
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def get_path_depth(path: str) -> int:
    """Get the depth of a path (number of components)."""
    # Normalize and count components
    parts = [p for p in path.split("/") if p]
    return len(parts)


def parse_scan_file(
    filepath: Path,
    progress_interval: int = 1_000_000,
) -> dict[str, DirStats]:
    """
    Parse a GPFS scan log file and compute directory statistics.

    Args:
        filepath: Path to the log file (plain text or .xz compressed)
        progress_interval: Report progress every N lines

    Returns:
        Dictionary mapping directory paths to their statistics
    """
    stats_dict: dict[str, DirStats] = {}
    line_count = 0
    file_count = 0
    skipped_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.fields[files]} files"),
        TextColumn("[yellow]{task.fields[dirs]} dirs"),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"[green]Parsing {filepath.name}...",
            total=None,
            files=0,
            dirs=0,
        )

        with open_input_file(filepath) as f:
            for line in f:
                line_count += 1

                parsed = parse_line(line.rstrip("\n"))
                if parsed:
                    process_file(
                        parsed["path"],
                        parsed["size"],
                        parsed["atime"],
                        parsed["user_id"],
                        stats_dict,
                    )
                    file_count += 1
                else:
                    skipped_count += 1

                if line_count % progress_interval == 0:
                    progress.update(
                        task,
                        files=file_count,
                        dirs=len(stats_dict),
                    )

        progress.update(task, files=file_count, dirs=len(stats_dict))

    console.print(f"\n[green]Parsing complete!")
    console.print(f"  Lines processed: {line_count:,}")
    console.print(f"  Files counted: {file_count:,}")
    console.print(f"  Directories found: {len(stats_dict):,}")
    console.print(f"  Lines skipped (headers/dirs): {skipped_count:,}")

    return stats_dict


def print_directory_stats(
    stats_dict: dict[str, DirStats],
    min_depth: int = 3,
    single_owner_only: bool = False,
    owner_id: int | None = None,
    max_results: int | None = None,
    sort_by: str = "size_recursive",
) -> None:
    """Print directory statistics in a formatted table."""

    # Filter directories
    filtered_dirs = []
    for path, stats in stats_dict.items():
        # Apply depth filter
        if get_path_depth(path) < min_depth:
            continue

        # Apply single-owner filter
        if single_owner_only and not stats.single_owner:
            continue

        # Apply owner-id filter
        if owner_id is not None:
            if not stats.single_owner or stats.owner_id != owner_id:
                continue

        filtered_dirs.append((path, stats))

    # Sort directories
    sort_key_map = {
        "size_recursive": lambda x: x[1].total_size_recursive,
        "size": lambda x: x[1].total_size,
        "files_recursive": lambda x: x[1].file_count_recursive,
        "files": lambda x: x[1].file_count,
        "atime_recursive": lambda x: x[1].max_atime_recursive or datetime.min,
        "atime": lambda x: x[1].max_atime or datetime.min,
        "path": lambda x: x[0],
    }
    sort_key = sort_key_map.get(sort_by, sort_key_map["size_recursive"])
    filtered_dirs.sort(key=sort_key, reverse=(sort_by != "path"))

    # Apply max results limit
    if max_results:
        filtered_dirs = filtered_dirs[:max_results]

    # Create table
    table = Table(title=f"Directory Statistics ({len(filtered_dirs)} directories)")
    table.add_column("Directory", style="cyan", no_wrap=False)
    table.add_column("Files", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Max Atime", justify="right")
    table.add_column("Files (R)", justify="right", style="dim")
    table.add_column("Size (R)", justify="right", style="dim")
    table.add_column("Max Atime (R)", justify="right", style="dim")
    table.add_column("Owner", justify="right")

    for path, stats in filtered_dirs:
        owner_str = (
            f"[green]{stats.owner_id}[/green]"
            if stats.single_owner
            else "[yellow]multiple[/yellow]"
        )
        table.add_row(
            path,
            f"{stats.file_count:,}",
            format_size(stats.total_size),
            format_datetime(stats.max_atime),
            f"{stats.file_count_recursive:,}",
            format_size(stats.total_size_recursive),
            format_datetime(stats.max_atime_recursive),
            owner_str,
        )

    console.print(table)


def write_output(
    stats_dict: dict[str, DirStats],
    output_path: Path,
    min_depth: int = 0,
    single_owner_only: bool = False,
    owner_id: int | None = None,
) -> None:
    """Write directory statistics to a file in TSV format."""

    with open(output_path, "w") as f:
        # Write header
        f.write(
            "directory\t"
            "file_count\ttotal_size\tmax_atime\t"
            "file_count_recursive\ttotal_size_recursive\tmax_atime_recursive\t"
            "owner_id\tsingle_owner\n"
        )

        for path, stats in sorted(stats_dict.items()):
            # Apply filters
            if get_path_depth(path) < min_depth:
                continue
            if single_owner_only and not stats.single_owner:
                continue
            if owner_id is not None:
                if not stats.single_owner or stats.owner_id != owner_id:
                    continue

            f.write(
                f"{path}\t"
                f"{stats.file_count}\t{stats.total_size}\t{format_datetime(stats.max_atime)}\t"
                f"{stats.file_count_recursive}\t{stats.total_size_recursive}\t"
                f"{format_datetime(stats.max_atime_recursive)}\t"
                f"{stats.owner_id}\t{stats.single_owner}\n"
            )

    console.print(f"[green]Results written to {output_path}")


@click.command()
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Write results to file (TSV format)",
)
@click.option(
    "--min-depth", "-d",
    type=int,
    default=3,
    show_default=True,
    help="Only report directories at depth >= N",
)
@click.option(
    "--single-owner-only", "-s",
    is_flag=True,
    help="Only report single-owner directories",
)
@click.option(
    "--owner-id", "-u",
    type=int,
    help="Filter to single-owner directories owned by UID",
)
@click.option(
    "--progress-interval", "-p",
    type=int,
    default=1_000_000,
    show_default=True,
    help="Report progress every N lines",
)
@click.option(
    "--max-results", "-n",
    type=int,
    help="Limit output to N directories",
)
@click.option(
    "--sort-by",
    type=click.Choice([
        "size_recursive", "size", "files_recursive", "files",
        "atime_recursive", "atime", "path"
    ]),
    default="size_recursive",
    show_default=True,
    help="Sort results by field",
)
def main(
    input_file: Path,
    output: Path | None,
    min_depth: int,
    single_owner_only: bool,
    owner_id: int | None,
    progress_interval: int,
    max_results: int | None,
    sort_by: str,
):
    """
    Parse GPFS policy scan log files and compute directory-level metrics.

    INPUT_FILE can be a plain text log file or an xz-compressed file (.xz).

    \b
    Metrics computed per directory:
    - Non-recursive: file count, total size, max atime (direct children)
    - Recursive: file count, total size, max atime (all descendants)
    - Single-owner tracking: identifies directories owned entirely by one user
    """
    console.print(f"[bold]GPFS Scan Parser[/bold]")
    console.print(f"Input: {input_file}")
    console.print()

    # Parse the file
    stats_dict = parse_scan_file(input_file, progress_interval)

    if not stats_dict:
        console.print("[red]No directory statistics found!")
        sys.exit(1)

    # Output results
    if output:
        write_output(stats_dict, output, min_depth, single_owner_only, owner_id)
    else:
        print_directory_stats(
            stats_dict,
            min_depth=min_depth,
            single_owner_only=single_owner_only,
            owner_id=owner_id,
            max_results=max_results or 50,
            sort_by=sort_by,
        )


if __name__ == "__main__":
    main()
