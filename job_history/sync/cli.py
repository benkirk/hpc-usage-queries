"""CLI command and shared options for syncing PBS accounting logs."""

import click
from pathlib import Path
from datetime import datetime, timedelta
from typing import Callable

from ..database import get_session, init_db, get_db_url
from . import sync_pbs_logs_bulk


# ---------------------------------------------------------------------------
# Shared Click option decorators (from sync_cli/common.py)
# ---------------------------------------------------------------------------

def machine_option(allow_all: bool = True) -> Callable:
    """Machine selection option decorator.

    Args:
        allow_all: If True, include 'all' choice for syncing both machines
    """
    choices = ["casper", "derecho", "all"] if allow_all else ["casper", "derecho"]
    default = "all" if allow_all else "derecho"
    return click.option(
        "-m", "--machine",
        type=click.Choice(choices),
        default=default,
        show_default=True,
        help="Machine to sync from" if allow_all else "Machine name"
    )


def date_options() -> Callable:
    """Decorator that adds --date, --start, --end options."""
    def decorator(func):
        func = click.option(
            "-d", "--date",
            type=str,
            help="Sync jobs for a specific date (YYYY-MM-DD)"
        )(func)
        func = click.option(
            "--start",
            type=str,
            help="Start date for range (YYYY-MM-DD, default: 2024-01-01)"
        )(func)
        func = click.option(
            "--end",
            type=str,
            help="End date for range (YYYY-MM-DD, default: yesterday)"
        )(func)
        return func
    return decorator


def sync_options() -> Callable:
    """Decorator that adds common sync options."""
    def decorator(func):
        func = click.option(
            "--batch-size",
            type=int,
            default=1000,
            show_default=True,
            help="Number of records per batch insert"
        )(func)
        func = click.option(
            "--dry-run",
            is_flag=True,
            help="Fetch and parse data but don't insert into database"
        )(func)
        func = click.option(
            "-v", "--verbose",
            is_flag=True,
            help="Enable verbose output"
        )(func)
        func = click.option(
            "--force",
            is_flag=True,
            help="Force sync even for days already summarized"
        )(func)
        func = click.option(
            "--no-summary",
            is_flag=True,
            help="Skip generating daily summaries after sync"
        )(func)
        func = click.option(
            "--summary-only",
            is_flag=True,
            help="Only regenerate summaries (no data fetch)"
        )(func)
        return func
    return decorator


def validate_dates(date: str | None, start: str | None, end: str | None) -> None:
    """Validate date arguments.

    Raises:
        click.BadParameter: If dates are invalid or conflicting
    """
    if date and (start or end):
        raise click.BadParameter("Cannot use --date with --start/--end")

    for date_str, name in [(date, "--date"), (start, "--start"), (end, "--end")]:
        if date_str:
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise click.BadParameter(f"{name} must be in YYYY-MM-DD format")


def print_sync_stats(stats: dict, machine: str, verbose: bool = False) -> None:
    """Print sync statistics in consistent format."""
    click.echo(f"\nSync complete for {machine}:")
    click.echo(f"  Fetched:  {stats['fetched']:,}")
    click.echo(f"  Inserted: {stats['inserted']:,}")
    click.echo(f"  Skipped:  {stats['fetched'] - stats['inserted'] - stats['errors']:,} (duplicates)")
    click.echo(f"  Errors:   {stats['errors']:,}")

    if stats.get("days_skipped", 0) > 0:
        click.echo(f"  Days skipped: {stats['days_skipped']} (already summarized)")
    if stats.get("days_failed", 0) > 0:
        click.echo(f"  Days failed: {stats['days_failed']} (missing accounting data)")
        if verbose and stats.get("failed_days"):
            click.echo(f"    Failed dates: {', '.join(stats['failed_days'])}")
    if stats.get("days_summarized", 0) > 0:
        click.echo(f"  Days summarized: {stats['days_summarized']}")

    # Per-machine breakdown if machine='all'
    if machine == "all" and "machines" in stats:
        click.echo("\nPer-machine breakdown:")
        for m, mstats in stats["machines"].items():
            click.echo(f"\n  {m}:")
            click.echo(f"    Fetched:  {mstats['fetched']:,}")
            click.echo(f"    Inserted: {mstats['inserted']:,}")
            click.echo(f"    Errors:   {mstats['errors']:,}")


# ---------------------------------------------------------------------------
# sync Click command (from sync_cli/sync.py)
# ---------------------------------------------------------------------------

@click.command()
@machine_option(allow_all=False)
@click.option(
    "-l", "--log-path",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Path to PBS log directory (containing YYYYMMDD files)"
)
@date_options()
@sync_options()
def sync(machine, log_path, date, start, end, batch_size, dry_run, verbose, force, no_summary, summary_only):
    """Sync jobs from local PBS accounting logs.

    Parses PBS accounting log files (named YYYYMMDD) from the given directory
    and imports them into the local SQLite database.  PBS logs contain
    cpu_type and gpu_type in select strings which are not available from
    other sources.

    \b
    Date Selection (all optional):
      --date:         Sync a single date
      --start --end:  Sync a date range (defaults: 2024-01-01 to yesterday)
      (no dates):     Sync from 2024-01-01 to yesterday

    \b
    Examples:
      # Single day
      jobhist sync -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29

      # Date range
      jobhist sync -m derecho -l ./data/pbs_logs --start 2026-01-01 --end 2026-01-31

      # Dry run to preview
      jobhist sync -m casper -l ./logs -d 2026-01-29 --dry-run -v

      # Force re-sync already summarized dates
      jobhist sync -m derecho -l ./logs -d 2026-01-29 --force
    """
    # Validate dates
    validate_dates(date, start, end)

    # Validate log directory
    if not log_path.exists():
        click.echo(f"Error: PBS log directory not found: {log_path}", err=True)
        raise click.Abort()

    # Initialize database
    if verbose:
        click.echo(f"Initializing database: {get_db_url(machine)}")

    engine = init_db(machine)
    session = get_session(machine, engine)

    try:
        # Print info
        if verbose:
            if date:
                click.echo(f"Parsing {machine} logs for date: {date}")
            else:
                display_start = start or '2024-01-01'
                display_end = end or (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
                click.echo(f"Parsing {machine} logs from {display_start} to {display_end}")
            click.echo(f"Log directory: {log_path}")
            if dry_run:
                click.echo("(DRY RUN - no data will be inserted)")
            if force:
                click.echo("(FORCE - will sync even if already summarized)")

        # Run sync
        stats = sync_pbs_logs_bulk(
            session=session,
            machine=machine,
            log_dir=str(log_path),
            period=date,
            start_date=start,
            end_date=end,
            dry_run=dry_run,
            batch_size=batch_size,
            verbose=verbose,
            force=force,
            generate_summary=not no_summary,
        )

        # Print results
        print_sync_stats(stats, machine, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback
        if verbose:
            traceback.print_exc()
        raise click.Abort()
    finally:
        session.close()
