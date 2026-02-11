"""Local sync subcommand - Parse local PBS logs."""

import click
from pathlib import Path
from datetime import datetime, timedelta
from .common import machine_option, date_options, sync_options, validate_dates, print_sync_stats
from ..database import get_session, init_db, get_db_path
from ..sync import sync_pbs_logs_bulk


@click.command()
@machine_option(allow_all=False)  # Local logs don't support machine='all'
@click.option(
    "-l", "--log-path",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Path to PBS log directory (containing YYYYMMDD files)"
)
@date_options()
@sync_options()
def local(machine, log_path, date, start, end, batch_size, dry_run, verbose, force, no_summary, summary_only):
    """Sync jobs from local PBS accounting logs.

    Parses PBS accounting log files from local filesystem instead of SSH'ing
    to remote machines. Useful when log files have been copied locally or
    SSH access is unavailable.

    PBS logs contain cpu_type and gpu_type in select strings which are NOT
    available in qhist JSON output, allowing this tool to populate fields
    that remote sync cannot.

    \b
    Date Selection (all optional):
      --date:         Sync a single date
      --start --end:  Sync a date range (defaults: 2024-01-01 to yesterday)
      (no dates):     Sync from 2024-01-01 to yesterday

    \b
    Examples:
      # Single day
      qhist-db sync local -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29

      # Date range
      qhist-db sync local -m derecho -l ./data/pbs_logs --start 2026-01-01 --end 2026-01-31

      # Dry run to preview
      qhist-db sync local -m casper -l ./logs -d 2026-01-29 --dry-run -v

      # Force re-sync already summarized dates
      qhist-db sync local -m derecho -l ./logs -d 2026-01-29 --force
    """
    # Validate dates
    validate_dates(date, start, end)

    # Validate log directory
    if not log_path.exists():
        click.echo(f"Error: PBS log directory not found: {log_path}", err=True)
        raise click.Abort()

    # Initialize database
    if verbose:
        click.echo(f"Initializing database: {get_db_path(machine)}")

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
