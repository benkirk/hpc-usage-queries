"""Import subcommand for fs-scans CLI."""

from pathlib import Path

import click

from ..parsers import detect_parser, get_parser, list_formats
from ..importers.importer import run_import
from .common import console, make_dynamic_help_command


# Use dynamic help command to adapt help text for wrapper scripts
DynamicHelpCommand = make_dynamic_help_command("fs-scans import")


@click.command(cls=DynamicHelpCommand, context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--format",
    "-F",
    "format_name",
    type=str,
    help="Parser format (auto-detect if not specified). Available: " + ", ".join(list_formats()),
)
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path),
    help="Database file path (overrides FS_SCAN_DB and default location)",
)
@click.option(
    "--data-dir",
    "data_dir",
    type=click.Path(path_type=Path),
    help="Data directory for databases (overrides FS_SCAN_DATA_DIR)",
)
@click.option(
    "--filesystem",
    "-f",
    type=str,
    help="Filesystem name (auto-detected from filename if not specified)",
)
@click.option(
    "--batch-size",
    type=int,
    default=10000,
    show_default=True,
    help="Batch size for database operations",
)
@click.option(
    "--progress-interval",
    "-p",
    type=int,
    default=1_000_000,
    show_default=True,
    help="Report progress every N lines",
)
@click.option(
    "--replace",
    is_flag=True,
    help="Drop and recreate tables (no-op for backwards compatibility - always happens)",
)
@click.option(
    "--workers",
    "-w",
    type=int,
    default=4,
    show_default=True,
    help="Number of parallel workers for parsing (minimum 1)",
)
@click.option(
    "--echo",
    is_flag=True,
    help="Enable SQL echo for debugging",
)
def import_cmd(
    input_file: Path,
    format_name: str | None,
    db_path: Path | None,
    data_dir: Path | None,
    filesystem: str | None,
    batch_size: int,
    progress_interval: int,
    workers: int,
    echo: bool,
):
    """Import filesystem scan logs into SQLite database.

    Supports multiple scan formats: GPFS, Lustre, POSIX.
    The format is auto-detected from the filename if not specified.

    \b
    Examples:
      fs-scans import scan.log                # Import with auto-detection
      fs-scans import scan.log --format gpfs  # Import with explicit format
      fs-scans import scan.log --workers 4    # Import with parallel workers
      fs-scans import scan.log --replace      # Replace existing database

    \b
    Database location precedence:
      1. --db option (explicit file path)
      2. FS_SCAN_DB environment variable
      3. --data-dir / FS_SCAN_DATA_DIR / default + {filesystem}.db
    """
    # Detect or select parser
    if format_name:
        try:
            parser = get_parser(format_name)
            console.print(f"Using {format_name.upper()} parser")
        except ValueError as e:
            console.print(f"[red]Error: {e}[/red]")
            return 1
    else:
        parser = detect_parser(input_file)
        if not parser:
            console.print(
                f"[red]Could not auto-detect format for: {input_file.name}[/red]"
            )
            console.print(f"Available formats: {', '.join(list_formats())}")
            console.print("Use --format to specify explicitly.")
            return 1
        console.print(f"Auto-detected {parser.format_name.upper()} format")

    # Run import
    run_import(
        input_file=input_file,
        parser=parser,
        filesystem=filesystem,
        db_path=db_path,
        data_dir=data_dir,
        batch_size=batch_size,
        progress_interval=progress_interval,
        workers=workers,
        echo=echo,
    )
