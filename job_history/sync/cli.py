"""CLI command for syncing scheduler accounting logs into the job history database."""

import click
from pathlib import Path
from datetime import datetime, timedelta
from typing import Callable

from ..database import get_session, init_db, get_db_url
from .base import MACHINE_SCHEDULERS
from .pbs import SyncPBSLogs
from .slurm import SyncSLURMLogs


# Registry: scheduler name → SyncBase subclass
SCHEDULER_REGISTRY = {
    "pbs": SyncPBSLogs,
    "slurm": SyncSLURMLogs,
}


# ---------------------------------------------------------------------------
# Shared Click option decorators
# ---------------------------------------------------------------------------

def machine_option(allow_all: bool = True) -> Callable:
    """Machine selection option decorator."""
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
            "--upsert",
            is_flag=True,
            help=(
                "Re-parse and update existing records with fresh-parsed values "
                "(recalculates charges and regenerates daily summaries). "
                "Bypasses the already-summarized day skip automatically."
            )
        )(func)
        func = click.option(
            "--resummarize",
            is_flag=True,
            help=(
                "Recompute daily summaries from current Job/JobCharge data. "
                "No log parsing — --log-path is not required."
            )
        )(func)
        func = click.option(
            "--no-summary",
            is_flag=True,
            help="Skip generating daily summaries after sync"
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
    click.echo(f"  Updated:  {stats.get('updated', 0):,}")
    click.echo(f"  Skipped:  {stats['fetched'] - stats['inserted'] - stats.get('updated', 0) - stats['errors']:,} (duplicates)")
    click.echo(f"  Errors:   {stats['errors']:,}")

    if stats.get("days_skipped", 0) > 0:
        click.echo(f"  Days skipped: {stats['days_skipped']} (already summarized)")
    if stats.get("days_failed", 0) > 0:
        click.echo(f"  Days failed: {stats['days_failed']} (missing accounting data)")
        if verbose and stats.get("failed_days"):
            click.echo(f"    Failed dates: {', '.join(stats['failed_days'])}")
    if stats.get("days_summarized", 0) > 0:
        click.echo(f"  Days summarized: {stats['days_summarized']}")

    if machine == "all" and "machines" in stats:
        click.echo("\nPer-machine breakdown:")
        for m, mstats in stats["machines"].items():
            click.echo(f"\n  {m}:")
            click.echo(f"    Fetched:  {mstats['fetched']:,}")
            click.echo(f"    Inserted: {mstats['inserted']:,}")
            click.echo(f"    Errors:   {mstats['errors']:,}")


# ---------------------------------------------------------------------------
# sync Click command
# ---------------------------------------------------------------------------

@click.command()
@machine_option(allow_all=False)
@click.option(
    "--scheduler",
    type=click.Choice(list(SCHEDULER_REGISTRY)),
    default=None,
    help=(
        "Scheduler type (default: inferred from machine via MACHINE_SCHEDULERS). "
        f"Known schedulers: {', '.join(SCHEDULER_REGISTRY)}."
    ),
)
@click.option(
    "-l", "--log-path",
    default=None,
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    help="Path to scheduler log directory (e.g. directory of YYYYMMDD files for PBS)"
)
@date_options()
@sync_options()
def sync(machine, scheduler, log_path, date, start, end, batch_size, dry_run, verbose, upsert, resummarize, no_summary):
    """Sync jobs from local scheduler accounting logs.

    Parses accounting log files from the given directory and imports them
    into the local database.  The scheduler is inferred from the machine
    name unless overridden with --scheduler.

    \b
    Date Selection (all optional):
      --date:         Sync a single date
      --start --end:  Sync a date range (defaults: 2024-01-01 to yesterday)
      (no dates):     Sync from 2024-01-01 to yesterday

    \b
    Examples:
      # Single day (PBS inferred for derecho)
      jobhist sync -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29

      # Date range
      jobhist sync -m derecho -l ./data/pbs_logs --start 2026-01-01 --end 2026-01-31

      # Dry run to preview
      jobhist sync -m casper -l ./logs -d 2026-01-29 --dry-run -v

      # Re-parse and update existing records (recalculates charges + summaries)
      jobhist sync -m derecho -l ./logs -d 2026-01-29 --upsert

      # Recompute daily summaries only (no log parsing required)
      jobhist sync -m derecho -d 2026-01-29 --resummarize
      jobhist sync -m derecho --start 2026-01-01 --end 2026-01-31 --resummarize

      # Explicit scheduler override
      jobhist sync -m derecho --scheduler pbs -l ./logs -d 2026-01-29
    """
    validate_dates(date, start, end)

    # Resolve scheduler: explicit flag > machine default
    resolved_scheduler = scheduler or MACHINE_SCHEDULERS.get(machine, "pbs")
    syncer_cls = SCHEDULER_REGISTRY.get(resolved_scheduler)
    if syncer_cls is None:
        click.echo(f"Error: unknown scheduler '{resolved_scheduler}'", err=True)
        raise click.Abort()

    if verbose:
        click.echo(f"Initializing database: {get_db_url(machine)}")
        click.echo(f"Scheduler: {syncer_cls.SCHEDULER_NAME}")

    engine = init_db(machine)
    session = get_session(machine, engine)

    try:
        if verbose:
            if resummarize:
                click.echo(f"Recomputing summaries for {machine}")
            else:
                if date:
                    click.echo(f"Parsing {machine} logs for date: {date}")
                else:
                    display_start = start or '2024-01-01'
                    display_end = end or (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
                    click.echo(f"Parsing {machine} logs from {display_start} to {display_end}")
                if log_path:
                    click.echo(f"Log directory: {log_path}")
                if dry_run:
                    click.echo("(DRY RUN - no data will be inserted)")
                if upsert:
                    click.echo("(UPSERT - existing records will be updated)")

        syncer = syncer_cls(session, machine)
        stats = syncer.sync(
            log_dir=str(log_path) if log_path else None,
            period=date,
            start_date=start,
            end_date=end,
            dry_run=dry_run,
            batch_size=batch_size,
            verbose=verbose,
            upsert=upsert,
            resummarize_only=resummarize,
            generate_summary=not no_summary,
        )

        print_sync_stats(stats, machine, verbose)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback
        if verbose:
            traceback.print_exc()
        raise click.Abort()
    finally:
        session.close()
