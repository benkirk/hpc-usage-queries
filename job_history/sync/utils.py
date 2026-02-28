"""Shared utility functions for job_history module."""


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
    """Validate PBS job timestamp ordering.

    PBS jobs should have timestamps in order: submit <= eligible <= start <= end.
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
