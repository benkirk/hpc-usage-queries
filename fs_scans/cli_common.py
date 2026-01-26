#!/usr/bin/env python3
"""
Shared CLI utilities for fs_scans tools.

Provides common functionality used by both scan_to_db.py and query_db.py:
- Console output
- Progress bar creation
- Formatting utilities
- Date parsing
- Common CLI option decorators
"""

import re
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

# Shared console instance for all CLI output
console = Console()


def create_progress_bar(
    extra_columns: list[TextColumn] | None = None,
    show_rate: bool = True,
) -> Progress:
    """
    Create a standardized progress bar.

    Args:
        extra_columns: Additional TextColumn instances to display
        show_rate: Whether to show items/sec rate column

    Returns:
        Configured Progress instance
    """
    columns = [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
    ]
    if extra_columns:
        columns.extend(extra_columns)
    if show_rate:
        columns.append(TextColumn("[magenta]{task.fields[rate]} items/sec"))
    columns.append(TimeElapsedColumn())
    return Progress(*columns, console=console)


def format_size(size_bytes: int | None) -> str:
    """Format byte size to human-readable string."""
    if size_bytes is None:
        return "N/A"
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} EB"


def format_datetime(dt: datetime | str | None) -> str:
    """Format datetime for display."""
    if dt is None:
        return "N/A"
    if isinstance(dt, str):
        # SQLite may return datetime as string - just return date portion
        return dt[:10] if len(dt) >= 10 else dt
    return dt.strftime("%Y-%m-%d")


def parse_date_arg(value: str) -> datetime:
    """
    Parse date argument - absolute (YYYY-MM-DD) or relative (3yrs, 6mo).

    Args:
        value: Date string like "2024-01-15", "3yrs", or "18mo"

    Returns:
        datetime object

    Raises:
        click.BadParameter: If format is invalid
    """
    # Try absolute date first
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        pass

    # Parse relative date (e.g., "3yrs", "6mo")
    match = re.match(r"^(\d+)(yrs?|mo)$", value.lower())
    if match:
        num = int(match.group(1))
        unit = match.group(2)
        now = datetime.now()
        if unit.startswith("yr"):
            return now.replace(year=now.year - num)
        elif unit == "mo":
            # Handle month subtraction (wrap years if needed)
            new_month = now.month - num
            new_year = now.year
            while new_month <= 0:
                new_month += 12
                new_year -= 1
            # Handle day overflow (e.g., Jan 31 - 1 month)
            day = min(now.day, 28)  # Safe for all months
            return now.replace(year=new_year, month=new_month, day=day)

    raise click.BadParameter(f"Invalid date format: {value}. Use YYYY-MM-DD or Nyrs/Nmo")


# Common CLI option decorators
def data_dir_option():
    """Decorator for --data-dir option."""
    return click.option(
        "--data-dir",
        "data_dir",
        type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
        help="Override directory for database files (or set FS_SCAN_DATA_DIR env var)",
    )


def echo_option():
    """Decorator for --echo option."""
    return click.option(
        "--echo",
        is_flag=True,
        help="Echo SQL statements (for debugging)",
    )
