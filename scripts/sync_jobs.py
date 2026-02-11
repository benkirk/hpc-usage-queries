#!/usr/bin/env python3
"""CLI script to sync job data from HPC machines to local database.

DEPRECATED: This script is maintained for backward compatibility.
New users should use: qhist-db sync remote

The qhist-sync entry point now delegates to the unified qhist-db CLI.
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports (cross-platform)
sys.path.insert(0, str(Path(__file__).parent.parent))

from qhist_db import init_db, get_session, get_db_path
from qhist_db.sync import sync_jobs_bulk, date_range
from qhist_db.summary import generate_summaries_for_range


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync job history data from HPC machines to local SQLite database."
    )

    parser.add_argument(
        "-m", "--machine",
        default="all",
        choices=["casper", "derecho", "all"],
        help="Machine to sync from (default: all)"
    )

    # Date options
    date_group = parser.add_argument_group("date selection (all optional)")
    date_group.add_argument(
        "-d", "--date",
        metavar="YYYY-MM-DD",
        help="Sync jobs for a specific date"
    )
    date_group.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        help="Start date for range sync (default: 2024-01-01)"
    )
    date_group.add_argument(
        "--end",
        metavar="YYYY-MM-DD",
        help="End date for range sync (default: yesterday)"
    )

    # Other options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse data but don't insert into database"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        metavar="N",
        help="Number of records per batch insert (default: 1000)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force sync even for days already summarized"
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Skip generating daily summaries after sync"
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only regenerate summaries (no data fetch)"
    )

    return parser.parse_args()


def validate_date(date_str: str) -> bool:
    """Validate date string format."""
    if not date_str:
        return True
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def main():
    """Main entry point."""
    args = parse_args()

    # Validate dates
    for date_arg, date_name in [
        (args.date, "--date"),
        (args.start, "--start"),
        (args.end, "--end"),
    ]:
        if date_arg and not validate_date(date_arg):
            print(f"Error: {date_name} must be in YYYY-MM-DD format", file=sys.stderr)
            sys.exit(1)

    # Date options are now optional - defaults will be applied in sync_jobs_bulk
    # If none specified, will sync from 2024-01-01 to yesterday

    # Initialize database(s)
    if args.machine == "all":
        if args.verbose:
            print("Initializing databases for all machines...")
        engines = init_db(machine=None)  # Initialize all machines
        session = None  # Will be created per-machine in sync_jobs_bulk
    else:
        if args.verbose:
            db_path = get_db_path(args.machine)
            print(f"Initializing database: {db_path}")
        engine = init_db(args.machine)
        session = get_session(args.machine, engine)

    try:
        # Determine sync parameters
        period = args.date
        start_date = args.start
        end_date = args.end

        # Handle summary-only mode
        if args.summary_only:
            if args.verbose:
                print(f"Regenerating summaries for {args.machine}")

            if args.machine == "all":
                # Handle all machines
                from qhist_db.database import VALID_MACHINES
                from qhist_db.summary import generate_daily_summary

                for m in sorted(VALID_MACHINES):
                    machine_session = get_session(m)
                    try:
                        if args.verbose:
                            print(f"\nRegenerating summaries for {m}...")

                        if period:
                            day_date = datetime.strptime(period, "%Y-%m-%d").date()
                            result = generate_daily_summary(machine_session, m, day_date, replace=True)
                            print(f"  {m}: {result['rows_inserted']} rows")
                        elif start_date and end_date:
                            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
                            result = generate_summaries_for_range(
                                machine_session, m, start_dt, end_dt,
                                replace=True, verbose=args.verbose
                            )
                            print(f"  {m}: {result['days_processed']} days, {result['total_rows']} rows")
                    finally:
                        machine_session.close()
            else:
                # Single machine
                if period:
                    day_date = datetime.strptime(period, "%Y-%m-%d").date()
                    from qhist_db.summary import generate_daily_summary
                    result = generate_daily_summary(session, args.machine, day_date, replace=True)
                    print(f"\nSummary regenerated: {result['rows_inserted']} rows")
                elif start_date and end_date:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
                    result = generate_summaries_for_range(
                        session, args.machine, start_dt, end_dt,
                        replace=True, verbose=args.verbose
                    )
                    print(f"\nSummaries regenerated:")
                    print(f"  Days processed: {result['days_processed']}")
                    print(f"  Total rows: {result['total_rows']}")
                else:
                    print("Error: --summary-only requires --date or --start/--end", file=sys.stderr)
                    sys.exit(1)
            return

        if args.verbose:
            if period:
                print(f"Syncing {args.machine} for date: {period}")
            else:
                # Show actual defaults that will be used
                display_start = start_date or '2024-01-01'
                display_end = end_date or (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"Syncing {args.machine} from {display_start} to {display_end}")

            if args.dry_run:
                print("(DRY RUN - no data will be inserted)")
            if args.force:
                print("(FORCE - will sync even if already summarized)")

        # Run sync
        stats = sync_jobs_bulk(
            session=session,
            machine=args.machine,
            period=period,
            start_date=start_date,
            end_date=end_date,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            verbose=args.verbose,
            force=args.force,
            generate_summary=not args.no_summary,
        )

        # Print results
        print(f"\nSync complete for {args.machine}:")
        print(f"  Fetched:  {stats['fetched']}")
        print(f"  Inserted: {stats['inserted']}")
        print(f"  Skipped:  {stats['fetched'] - stats['inserted'] - stats['errors']} (duplicates)")
        print(f"  Errors:   {stats['errors']}")
        if stats.get("days_skipped", 0) > 0:
            print(f"  Days skipped: {stats['days_skipped']} (already summarized)")
        if stats.get("days_failed", 0) > 0:
            print(f"  Days failed: {stats['days_failed']} (missing accounting data)")
            if args.verbose and stats.get("failed_days"):
                print(f"    Failed dates: {', '.join(stats['failed_days'])}")
        if stats.get("days_summarized", 0) > 0:
            print(f"  Days summarized: {stats['days_summarized']}")

        # Print per-machine breakdown if syncing all machines
        if args.machine == "all" and "machines" in stats:
            print(f"\nPer-machine breakdown:")
            for machine, machine_stats in stats["machines"].items():
                print(f"\n  {machine}:")
                print(f"    Fetched:  {machine_stats['fetched']}")
                print(f"    Inserted: {machine_stats['inserted']}")
                print(f"    Errors:   {machine_stats['errors']}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    main()
