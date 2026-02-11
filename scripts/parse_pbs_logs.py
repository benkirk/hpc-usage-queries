#!/usr/bin/env python3
"""Parse PBS accounting logs and import jobs into the database.

DEPRECATED: This script is maintained for backward compatibility.
New users should use: qhist-db sync local

This script provides an alternative to qhist-sync that works with local PBS
accounting log files. It's useful when:
- Log files have been copied locally
- SSH access is unavailable
- Processing historical logs in bulk
- Working offline

The PBS logs contain cpu_type and gpu_type in select strings which are NOT
available in qhist JSON output, allowing this tool to populate fields that
qhist-sync cannot.

The qhist-parse-logs entry point now delegates to the unified qhist-db CLI.
"""

import argparse
import sys
from pathlib import Path


def main():
    """Main entry point for PBS log parsing."""
    parser = argparse.ArgumentParser(
        description="Parse PBS accounting logs and import jobs into database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single day
  %(prog)s -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29

  # Date range
  %(prog)s -m derecho -l ./data/pbs_logs/derecho --start 2026-01-01 --end 2026-01-31

  # Dry run to preview
  %(prog)s -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29 --dry-run -v

  # Force re-sync already summarized dates
  %(prog)s -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29 --force
        """,
    )

    parser.add_argument(
        "-m", "--machine",
        required=True,
        choices=["casper", "derecho"],
        help="Machine name (determines database to use)"
    )

    parser.add_argument(
        "-l", "--log-path",
        required=True,
        type=str,
        help="Path to PBS log directory (containing YYYYMMDD files)"
    )

    # Date selection (mutually exclusive groups)
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "-d", "--date",
        type=str,
        help="Single date to sync (YYYY-MM-DD)"
    )

    # Date range
    parser.add_argument(
        "--start",
        type=str,
        help="Start date for range (YYYY-MM-DD, default: 2024-01-01 if no date specified)"
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date for range (YYYY-MM-DD, default: yesterday if no date specified)"
    )

    # Options
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of records per batch (default: 1000)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't insert records, just count them"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print progress for each day"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Sync even if date has already been summarized"
    )

    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Don't generate daily summaries after sync"
    )

    args = parser.parse_args()

    # Validate date arguments
    if not args.date and not args.start and not args.end:
        # If no dates specified, will use defaults (2024-01-01 to yesterday)
        pass
    elif args.date and (args.start or args.end):
        parser.error("Cannot use --date with --start/--end")

    # Validate log directory exists
    log_path = Path(args.log_path)
    if not log_path.exists():
        print(f"Error: PBS log directory not found: {args.log_path}", file=sys.stderr)
        return 1

    # Import here to avoid slow imports at startup
    from qhist_db.database import get_session
    from qhist_db.sync import sync_pbs_logs_bulk

    # Get database session
    session = get_session(args.machine)

    try:
        # Sync jobs from PBS logs
        stats = sync_pbs_logs_bulk(
            session=session,
            machine=args.machine,
            log_dir=args.log_path,
            period=args.date,
            start_date=args.start,
            end_date=args.end,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            verbose=args.verbose,
            force=args.force,
            generate_summary=not args.no_summary,
        )

        # Print summary
        print("\nSync Summary:")
        print(f"  Parsed:     {stats['fetched']:,} jobs")
        print(f"  Inserted:   {stats['inserted']:,} new jobs")
        print(f"  Errors:     {stats['errors']:,}")
        print(f"  Summarized: {stats['days_summarized']} days")
        print(f"  Skipped:    {stats['days_skipped']} days (already summarized)")
        print(f"  Failed:     {stats['days_failed']} days")

        if stats['failed_days']:
            print(f"\nFailed days: {', '.join(stats['failed_days'])}")

        if args.dry_run:
            print("\n(Dry run - no records were actually inserted)")

        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
