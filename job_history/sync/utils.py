"""Shared utility functions for job_history sync module."""

from datetime import datetime, timedelta
from typing import Iterator


# ---------------------------------------------------------------------------
# Date helpers (scheduler-agnostic)
# ---------------------------------------------------------------------------

def parse_date_string(date_str: str) -> datetime:
    """Parse YYYY-MM-DD string to datetime object.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        datetime object

    Raises:
        ValueError: If date_str is not in YYYY-MM-DD format
    """
    return datetime.strptime(date_str, "%Y-%m-%d")


def date_range(start_date: str, end_date: str) -> Iterator[str]:
    """Iterate through dates from start to end (inclusive).

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Yields:
        Date strings in YYYY-MM-DD format
    """
    start = parse_date_string(start_date)
    end = parse_date_string(end_date)
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def date_range_length(start_date: str, end_date: str) -> int:
    """Determine the number of days in a date range (inclusive).

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        The number of days in the range
    """
    start = parse_date_string(start_date)
    end = parse_date_string(end_date)
    return (end - start).days + 1


# ---------------------------------------------------------------------------
# Type coercion helpers
# ---------------------------------------------------------------------------

def safe_int(value, default=None):
    """Safely convert value to integer.

    Args:
        value: Value to convert
        default: Default value if conversion fails (None allows database NULL handling)

    Returns:
        Integer value or default
    """
    if value is None or value == '':
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value, default=None):
    """Safely convert value to float.

    Args:
        value: Value to convert
        default: Default value if conversion fails (None allows database NULL handling)

    Returns:
        Float value or default
    """
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Datetime normalization
# ---------------------------------------------------------------------------

def normalize_datetime_to_naive(dt):
    """Normalize datetime to naive (remove timezone).

    SQLite stores datetimes as naive, so this ensures consistent comparison
    when checking for duplicates or filtering by date.

    Args:
        dt: datetime object (may be timezone-aware or naive)

    Returns:
        Naive datetime or None if input is None
    """
    if dt and dt.tzinfo:
        return dt.replace(tzinfo=None)
    return dt


def validate_timestamp_ordering(submit, eligible, start, end):
    """Validate job timestamp ordering.

    Jobs should have timestamps in order: submit <= eligible <= start <= end.
    Missing timestamps are allowed (returns True).

    Args:
        submit: Job submission datetime
        eligible: Job eligible datetime
        start: Job start datetime
        end: Job end datetime

    Returns:
        True if ordering is valid or if any timestamp is missing
    """
    if submit and eligible and start and end:
        return submit <= eligible <= start <= end
    return True  # Don't invalidate if missing timestamps
