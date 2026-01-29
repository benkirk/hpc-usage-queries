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
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} EiB"


def format_datetime(dt: datetime | str | int | None) -> str:
    """Format datetime for display."""
    if dt is None or dt == 0:
        return "N/A"
    if isinstance(dt, str):
        # SQLite may return datetime as string - just return date portion
        return dt[:10] if len(dt) >= 10 else dt
    if isinstance(dt, int):
        # Unexpected integer value - treat as N/A
        return "N/A"
    return dt.strftime("%Y-%m-%d")


# Size parsing constants and utilities
_SIZE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "pb": 1000**5,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "pib": 1024**5,
    # Shorthand: K/M/G/T/P → binary (filesystem convention)
    "k": 1024,
    "m": 1024**2,
    "g": 1024**3,
    "t": 1024**4,
    "p": 1024**5,
}


def parse_size(value: str) -> int:
    """Parse a size string to bytes.

    Accepts plain integers (bytes), SI units (KB, MB, GB, TB, PB),
    binary units (KiB, MiB, GiB, TiB, PiB), or shorthand (K, M, G, T, P)
    where shorthand maps to binary (1024-based).

    Examples:
        "1GiB"  -> 1073741824
        "500MB" -> 500000000
        "2T"    -> 2199023255552
        "0"     -> 0
    """
    value = value.strip()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]*)$", value)
    if not match:
        raise click.BadParameter(f"Invalid size: {value}")
    num_str, unit = match.groups()
    num = float(num_str)
    if not unit:
        return int(num)
    unit_lower = unit.lower()
    if unit_lower not in _SIZE_UNITS:
        raise click.BadParameter(f"Unknown size unit: {unit}")
    return int(num * _SIZE_UNITS[unit_lower])


# File count parsing constants and utilities
_COUNT_UNITS = {
    "k": 1000,
    "m": 1000_000,
}


def parse_file_count(value: str) -> int:
    """Parse a file count string to an integer.

    Accepts plain integers or shorthand multipliers: K (×1000), M (×1000000).

    Examples:
        "1K"  -> 1000
        "500" -> 500
        "10M" -> 10000000
    """
    value = value.strip()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]*)$", value)
    if not match:
        raise click.BadParameter(f"Invalid file count: {value}")
    num_str, unit = match.groups()
    num = float(num_str)
    if not unit:
        return int(num)
    unit_lower = unit.lower()
    if unit_lower not in _COUNT_UNITS:
        raise click.BadParameter(f"Unknown file count unit: {unit}")
    return int(num * _COUNT_UNITS[unit_lower])


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


def make_dynamic_help_command(default_command_name: str):
    """Factory function to create a DynamicHelpCommand for a specific tool.

    Args:
        default_command_name: The default command name to replace in help text
                            (e.g., 'query-fs-scan-db' or 'fs-scan-to-db')

    Returns:
        A Click Command class that replaces the default command name with
        the actual invoked name (useful for symlinks)

    Example:
        ```python
        DynamicHelpCommand = make_dynamic_help_command('query-fs-scan-db')

        @click.command(cls=DynamicHelpCommand)
        def main():
            ...
        ```
    """

    class DynamicHelpCommand(click.Command):
        """Custom Command class that replaces the command name in help text."""

        def get_help(self, ctx):
            help_text = super().get_help(ctx)
            # Build full command path by walking up the context hierarchy
            command_path = []
            context = ctx
            while context:
                if context.info_name:
                    command_path.insert(0, context.info_name)
                context = context.parent

            # Get the actual invoked command name (full path)
            prog_name = ' '.join(command_path) if command_path else None

            if prog_name and prog_name != default_command_name:
                # Replace hardcoded command name with actual invoked name
                help_text = help_text.replace(default_command_name, prog_name)
            return help_text

    return DynamicHelpCommand


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
