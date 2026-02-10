"""PBS accounting log scanning and job record streaming.

This module provides functions to scan local PBS accounting log files
and stream parsed job records as dictionaries ready for database insertion.
"""

import logging
from pathlib import Path
from typing import Iterator

import pbsparse

from .parsers import date_range, parse_date_string
from .pbs_parsers import parse_pbs_record

logger = logging.getLogger(__name__)


def get_log_file_path(log_dir: Path, date_str: str) -> Path:
    """Construct PBS log file path for a given date.

    PBS accounting logs are named by date in YYYYMMDD format.

    Args:
        log_dir: Base directory containing PBS logs
        date_str: Date in YYYY-MM-DD format

    Returns:
        Path to log file (e.g., log_dir/20260129)

    Examples:
        >>> get_log_file_path(Path("/data/pbs_logs"), "2026-01-29")
        PosixPath('/data/pbs_logs/20260129')
    """
    # Convert YYYY-MM-DD to YYYYMMDD
    dt = parse_date_string(date_str)
    filename = dt.strftime("%Y%m%d")
    return log_dir / filename


def fetch_jobs_from_pbs_logs(
    log_dir: str | Path,
    machine: str,
    date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Iterator[dict]:
    """Scan PBS log files and yield parsed job dictionaries.

    This function mirrors the interface of remote.fetch_jobs_ssh() for compatibility
    with existing sync infrastructure. It streams job records from local PBS
    accounting logs without loading everything into memory.

    Args:
        log_dir: Directory containing PBS log files (named YYYYMMDD)
        machine: Machine name for type inference fallback (e.g., "derecho", "casper")
        date: Single date to process (YYYY-MM-DD format)
        start_date: Start of date range (YYYY-MM-DD format)
        end_date: End of date range (YYYY-MM-DD format)

    Yields:
        Normalized job dictionaries ready for database insertion

    Raises:
        RuntimeError: If log file doesn't exist or can't be parsed

    Notes:
        - Only processes End ('E') records from PBS logs
        - Validates timestamp ordering (submit <= eligible <= start <= end)
        - Skips records with missing job_id but continues processing
    """
    log_dir = Path(log_dir)

    # Determine date range (single date takes precedence)
    if date:
        dates = [date]
    elif start_date and end_date:
        dates = list(date_range(start_date, end_date))
    else:
        raise ValueError("Must provide either 'date' or 'start_date' and 'end_date'")

    # Process each date
    for date_str in dates:
        log_path = get_log_file_path(log_dir, date_str)

        # Check if log file exists
        if not log_path.exists():
            raise RuntimeError(
                f"PBS log file not found: {log_path}\n"
                f"Expected log file for date {date_str}"
            )

        logger.info(f"Scanning PBS log: {log_path}")

        # Parse PBS records (only End records)
        try:
            records = pbsparse.get_pbs_records(str(log_path), type_filter="E")
        except Exception as e:
            raise RuntimeError(f"Failed to parse PBS log {log_path}: {e}") from e

        # Process each record
        for pbs_record in records:
            # Parse to database format
            try:
                job_dict = parse_pbs_record(pbs_record, machine)
            except Exception as e:
                logger.warning(
                    f"Failed to parse PBS record {pbs_record.id}: {e}",
                    exc_info=True
                )
                continue

            # Validate job_id
            if not job_dict.get("job_id"):
                logger.warning("Skipping record with missing job_id")
                continue

            # Validate timestamp ordering
            submit = job_dict.get("submit")
            eligible = job_dict.get("eligible")
            start = job_dict.get("start")
            end = job_dict.get("end")

            # Check ordering if all timestamps are present
            if submit and eligible and start and end:
                if not (submit <= eligible <= start <= end):
                    logger.warning(
                        f"Invalid timestamp ordering for job {job_dict['job_id']}: "
                        f"submit={submit}, eligible={eligible}, start={start}, end={end}"
                    )
                    # Still yield the record - database constraints will catch serious issues
                    # This is just a warning for potential data quality issues

            yield job_dict
