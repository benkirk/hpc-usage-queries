"""Field parsing and type conversion for qhist job records."""

from datetime import datetime, timedelta, timezone
from typing import Iterator
from zoneinfo import ZoneInfo


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
    """Determine the length of a date range (inclusive).

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        The number of days in the range
    """
    start = parse_date_string(start_date)
    end = parse_date_string(end_date)

    return (end-start).days
