#!/usr/bin/env python3
"""CLI script to sync job data from HPC machines to local database."""

import argparse
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(__file__).rsplit("/", 2)[0])

from qhist_db import init_db, get_session
from qhist_db.sync import sync_jobs_bulk


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync job history data from HPC machines to local SQLite database."
    )

    parser.add_argument(
        "-m", "--machine",
        required=True,
        choices=["casper", "derecho"],
        help="Machine to sync from"
    )

    # Date options (mutually exclusive groups)
    date_group = parser.add_argument_group("date selection")
    date_group.add_argument(
        "-d", "--date",
        metavar="YYYYMMDD",
        help="Sync jobs for a specific date"
    )
    date_group.add_argument(
        "--start",
        metavar="YYYYMMDD",
        help="Start date for range sync"
    )
    date_group.add_argument(
        "--end",
        metavar="YYYYMMDD",
        help="End date for range sync"
    )

    # Other options
    parser.add_argument(
        "--db-path",
        metavar="PATH",
        help="Path to SQLite database (default: data/qhist.db)"
    )
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

    return parser.parse_args()


def validate_date(date_str: str) -> bool:
    """Validate date string format."""
    if not date_str:
        return True
    try:
        datetime.strptime(date_str, "%Y%m%d")
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
            print(f"Error: {date_name} must be in YYYYMMDD format", file=sys.stderr)
            sys.exit(1)

    # Check that at least one date option is provided
    if not any([args.date, args.start, args.end]):
        print("Error: Must specify --date or --start/--end", file=sys.stderr)
        sys.exit(1)

    # Initialize database
    if args.verbose:
        print(f"Initializing database...")

    engine = init_db(args.db_path)
    session = get_session(engine)

    try:
        # Determine sync parameters
        period = args.date
        start_date = args.start
        end_date = args.end

        if args.verbose:
            if period:
                print(f"Syncing {args.machine} for date: {period}")
            else:
                print(f"Syncing {args.machine} from {start_date or 'beginning'} to {end_date or 'now'}")

            if args.dry_run:
                print("(DRY RUN - no data will be inserted)")

        # Run sync
        stats = sync_jobs_bulk(
            session=session,
            machine=args.machine,
            period=period,
            start_date=start_date,
            end_date=end_date,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )

        # Print results
        print(f"\nSync complete for {args.machine}:")
        print(f"  Fetched:  {stats['fetched']}")
        print(f"  Inserted: {stats['inserted']}")
        print(f"  Skipped:  {stats['fetched'] - stats['inserted'] - stats['errors']} (duplicates)")
        print(f"  Errors:   {stats['errors']}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
