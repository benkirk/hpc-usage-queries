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
    """Decorator that adds --date, --start, --end, --today, --last options."""
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
        func = click.option(
            "--today",
            "today_flag",
            is_flag=True,
            help="Sync jobs for today (shorthand for --date <today>)"
        )(func)
        func = click.option(
            "--last",
            type=str,
            default=None,
            metavar="N[d]",
            help="Sync the last N days including today (e.g. --last 3d)"
        )(func)
        return func
    return decorator


def parse_last_spec(spec: str) -> int:
    """Parse --last spec: '3d' or '3' → 3.

    Raises:
        click.BadParameter: if spec is not a positive integer optionally followed by 'd'
    """
    s = spec.strip().lower().rstrip("d")
    try:
        n = int(s)
    except ValueError:
        raise click.BadParameter(f"--last must be in the form Nd (e.g., 3d), got: {spec!r}")
    if n < 1:
        raise click.BadParameter("--last N must be >= 1")
    return n


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
            "--incremental",
            is_flag=True,
            help=(
                "Insert new records only; skip existing ones. "
                "Re-summarizes the day only when new records were added. "
                "Intended for frequent intra-day syncs. "
                "Mutually exclusive with --upsert."
            )
        )(func)
        func = click.option(
            "--recalculate",
            is_flag=True,
            help=(
                "Recompute charges for all jobs in the given date range directly "
                "from the database — no log parsing required. "
                "Updates job_charges and regenerates daily summaries. "
                "Mutually exclusive with --upsert, --incremental, --resummarize."
            )
        )(func)
        func = click.option(
            "--no-summary",
            is_flag=True,
            help="Skip generating daily summaries after sync"
        )(func)
        return func
    return decorator


def validate_dates(
    date: str | None,
    start: str | None,
    end: str | None,
    today_flag: bool = False,
    last: str | None = None,
) -> None:
    """Validate date arguments.

    Raises:
        click.BadParameter: If dates are invalid or conflicting
    """
    if today_flag and (date or start or end or last):
        raise click.BadParameter("--today cannot be combined with --date, --start, --end, or --last")
    if last and (date or start or end or today_flag):
        raise click.BadParameter("--last cannot be combined with --date, --start, --end, or --today")
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
    if stats.get("recalculated", 0) > 0:
        click.echo(f"  Recalculated: {stats['recalculated']:,} (charges recomputed from DB)")

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

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
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
def sync(machine, scheduler, log_path, date, start, end, today_flag, last, batch_size, dry_run, verbose, upsert, incremental, resummarize, recalculate, no_summary):
    """Sync jobs from local scheduler accounting logs.

    Parses accounting log files from the given directory and imports them
    into the local database.  The scheduler is inferred from the machine
    name unless overridden with --scheduler.

    \b
    Date Selection (all optional):
      --date:         Sync a single date
      --today:        Sync today (shorthand for --date <today>)
      --last N[d]:    Sync the last N days including today (e.g. --last 3d)
      --start --end:  Sync a date range (defaults: 2024-01-01 to yesterday)
      (no dates):     Sync from 2024-01-01 to yesterday

    \b
    Examples:
      jobhist sync -m derecho -l ./pbs_logs -d 2026-01-29                              # single day
      jobhist sync -m derecho -l ./logs --today                                         # today
      jobhist sync -m derecho -l ./logs --last 3d                                       # last 3 days
      jobhist sync -m derecho -l ./pbs_logs --start 2026-01-01 --end 2026-01-31        # date range
      jobhist sync -m casper  -l ./logs -d 2026-01-29 --dry-run -v                     # dry run
      jobhist sync -m derecho -l ./logs -d 2026-01-29 --upsert                         # re-parse + update
      jobhist sync -m derecho -l ./logs --today --incremental                          # intra-day refresh
      jobhist sync -m derecho -l ./logs --last 3d --incremental                        # incremental last 3 days
      jobhist sync -m derecho -d 2026-01-29 --resummarize                              # re-summarize only
      jobhist sync -m derecho --start 2026-01-01 --end 2026-01-31 --resummarize
      jobhist sync -m casper -d 2026-03-21 --recalculate                               # recalculate charges from DB
      jobhist sync -m casper --start 2021-01-01 --end 2026-03-21 --recalculate         # full historical backfill
      jobhist sync -m derecho --scheduler pbs -l ./logs -d 2026-01-29                  # explicit scheduler
    """
    # Validate user-supplied flags before any resolution
    validate_dates(date, start, end, today_flag, last)

    if sum([bool(upsert), bool(incremental), bool(resummarize), bool(recalculate)]) > 1:
        click.echo("Error: --upsert, --incremental, --resummarize, and --recalculate are mutually exclusive", err=True)
        raise click.Abort()

    # Resolve --today and --last into date / start+end
    today_str = datetime.now().date().strftime("%Y-%m-%d")
    if today_flag:
        date = today_str
    if last:
        n = parse_last_spec(last)
        from datetime import date as _date, timedelta as _td
        start = (_date.today() - _td(days=n - 1)).strftime("%Y-%m-%d")
        end = today_str

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
                    if last:
                        click.echo(f"  (--last {last}: last {parse_last_spec(last)} days including today)")
                if log_path:
                    click.echo(f"Log directory: {log_path}")
                if dry_run:
                    click.echo("(DRY RUN - no data will be inserted)")
                if upsert:
                    click.echo("(UPSERT - existing records will be updated)")
                if incremental:
                    click.echo("(INCREMENTAL - insert new records only; re-summarizes only if new records found)")
                if recalculate:
                    click.echo("(RECALCULATE - recomputing charges from DB, no log parsing)")

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
            incremental=incremental,
            resummarize_only=resummarize,
            generate_summary=not no_summary,
            recalculate=recalculate,
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
