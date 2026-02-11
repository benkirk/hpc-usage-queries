"""Remote sync subcommand - SSH to HPC machines."""

import click
from datetime import datetime, timedelta
from .common import machine_option, date_options, sync_options, validate_dates, print_sync_stats
from ..database import get_session, init_db, get_db_path, VALID_MACHINES
from ..sync import sync_jobs_bulk
from ..summary import generate_daily_summary, generate_summaries_for_range


@click.command()
@machine_option(allow_all=True)
@date_options()
@sync_options()
def remote(machine, date, start, end, batch_size, dry_run, verbose, force, no_summary, summary_only):
    """Sync jobs from remote HPC machines via SSH.

    Fetches job data using qhist command over SSH. Supports syncing individual
    machines or both (machine='all').

    \b
    Date Selection (all optional):
      --date:         Sync a single date
      --start --end:  Sync a date range (defaults: 2024-01-01 to yesterday)
      (no dates):     Sync from 2024-01-01 to yesterday

    \b
    Examples:
      # Sync all machines for yesterday
      qhist-db sync remote -m all

      # Sync derecho for specific date
      qhist-db sync remote -m derecho -d 2024-01-29

      # Sync date range with verbose output
      qhist-db sync remote -m casper --start 2024-01-01 --end 2024-01-31 -v

      # Dry run to preview
      qhist-db sync remote -m derecho -d 2024-01-29 --dry-run -v

      # Force re-sync already summarized dates
      qhist-db sync remote -m derecho --start 2024-01-01 --force
    """
    # Validate dates
    validate_dates(date, start, end)

    # Initialize database(s)
    if machine == "all":
        if verbose:
            click.echo("Initializing databases for all machines...")
        init_db(machine=None)
        session = None
    else:
        if verbose:
            click.echo(f"Initializing database: {get_db_path(machine)}")
        engine = init_db(machine)
        session = get_session(machine, engine)

    try:
        # Handle summary-only mode
        if summary_only:
            if verbose:
                click.echo(f"Regenerating summaries for {machine}")

            if machine == "all":
                # Handle all machines
                for m in sorted(VALID_MACHINES):
                    machine_session = get_session(m)
                    try:
                        if verbose:
                            click.echo(f"\nRegenerating summaries for {m}...")

                        if date:
                            day_date = datetime.strptime(date, "%Y-%m-%d").date()
                            result = generate_daily_summary(machine_session, m, day_date, replace=True)
                            click.echo(f"  {m}: {result['rows_inserted']} rows")
                        elif start and end:
                            start_dt = datetime.strptime(start, "%Y-%m-%d").date()
                            end_dt = datetime.strptime(end, "%Y-%m-%d").date()
                            result = generate_summaries_for_range(
                                machine_session, m, start_dt, end_dt,
                                replace=True, verbose=verbose
                            )
                            click.echo(f"  {m}: {result['days_processed']} days, {result['total_rows']} rows")
                        else:
                            click.echo("Error: --summary-only requires --date or --start/--end", err=True)
                            raise click.Abort()
                    finally:
                        machine_session.close()
            else:
                # Single machine
                if date:
                    day_date = datetime.strptime(date, "%Y-%m-%d").date()
                    result = generate_daily_summary(session, machine, day_date, replace=True)
                    click.echo(f"\nSummary regenerated: {result['rows_inserted']} rows")
                elif start and end:
                    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
                    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
                    result = generate_summaries_for_range(
                        session, machine, start_dt, end_dt,
                        replace=True, verbose=verbose
                    )
                    click.echo(f"\nSummaries regenerated:")
                    click.echo(f"  Days processed: {result['days_processed']}")
                    click.echo(f"  Total rows: {result['total_rows']}")
                else:
                    click.echo("Error: --summary-only requires --date or --start/--end", err=True)
                    raise click.Abort()
            return

        # Print info
        if verbose:
            if date:
                click.echo(f"Syncing {machine} for date: {date}")
            else:
                display_start = start or '2024-01-01'
                display_end = end or (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
                click.echo(f"Syncing {machine} from {display_start} to {display_end}")
            if dry_run:
                click.echo("(DRY RUN - no data will be inserted)")
            if force:
                click.echo("(FORCE - will sync even if already summarized)")

        # Run sync
        stats = sync_jobs_bulk(
            session=session,
            machine=machine,
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
        raise click.Abort()
    finally:
        if session is not None:
            session.close()
