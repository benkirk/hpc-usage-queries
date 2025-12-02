"""Field parsing and type conversion for qhist job records."""

from datetime import datetime, timedelta, timezone
from typing import Iterator
from zoneinfo import ZoneInfo

# All available fields from qhist
ALL_FIELDS = (
    "id,short_id,account,avgcpu,count,cpupercent,cputime,cputype,"
    "elapsed,eligible,end,gputype,memory,mpiprocs,name,numcpus,"
    "numgpus,numnodes,ompthreads,ptargets,queue,reqmem,resources,"
    "start,status,submit,user,vmemory,walltime"
)

# Fields that contain timestamps and need UTC conversion
TIMESTAMP_FIELDS = {"submit", "eligible", "start", "end"}

# Fields that should be integers
INTEGER_FIELDS = {
    "short_id", "elapsed", "walltime", "cputime",
    "numcpus", "numgpus", "numnodes", "mpiprocs", "ompthreads",
    "reqmem", "memory", "vmemory", "count"
}

# Fields that should be floats
FLOAT_FIELDS = {"cpupercent", "avgcpu"}


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


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse a timestamp string and convert to UTC.

    Args:
        value: Timestamp string from qhist (format varies)

    Returns:
        datetime in UTC, or None if parsing fails
    """
    if not value:
        return None

    # Mountain Time zone (handles MST/MDT automatically)
    mountain = ZoneInfo("America/Denver")

    # Try common formats
    formats = [
        "%Y-%m-%dT%H:%M:%S",      # ISO format without timezone
        "%Y-%m-%dT%H:%M:%S%z",    # ISO format with timezone
        "%Y-%m-%d %H:%M:%S",      # Space-separated
        "%Y-%m-%d %H:%M:%S%z",    # Space-separated with timezone
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                # Assume times without timezone are in Mountain Time
                # ZoneInfo handles MST/MDT transitions automatically
                dt = dt.replace(tzinfo=mountain).astimezone(timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def parse_int(value) -> int | None:
    """Safely parse an integer value.

    Args:
        value: Value to parse (string, int, or None)

    Returns:
        Integer value or None if parsing fails
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_job_id(value) -> int | None:
    """Parse a job ID, handling array job format like '6049117[28]'.

    For array jobs, extracts the base job ID before the bracket.

    Args:
        value: Job ID string (may include array index)

    Returns:
        Base job ID as integer, or None if parsing fails
    """
    if value is None or value == "":
        return None
    try:
        # Handle array job IDs like "6049117[28]"
        str_val = str(value)
        if "[" in str_val:
            str_val = str_val.split("[")[0]
        return int(str_val)
    except (ValueError, TypeError):
        return None


def parse_float(value) -> float | None:
    """Safely parse a float value.

    Args:
        value: Value to parse (string, float, or None)

    Returns:
        Float value or None if parsing fails
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_job_record(record: dict, full_job_id: str | None = None) -> dict:
    """Parse and normalize a job record from qhist JSON output.

    The qhist JSON has a nested structure that needs to be flattened
    to match our database schema.

    Args:
        record: Raw job record dictionary from qhist
        full_job_id: Full job ID from JSON key (e.g., "2712367.desched1")

    Returns:
        Normalized record with proper types matching database schema
    """
    resource_list = record.get("Resource_List", {})
    resources_used = record.get("resources_used", {})

    # Parse the select string for mpiprocs and ompthreads
    select_str = resource_list.get("select", "")
    mpiprocs = None
    ompthreads = None
    if select_str:
        for part in select_str.split(":"):
            if part.startswith("mpiprocs="):
                mpiprocs = parse_int(part.split("=")[1])
            elif part.startswith("ompthreads="):
                ompthreads = parse_int(part.split("=")[1])

    # Convert hours to seconds for time fields
    def hours_to_seconds(val):
        if val is None:
            return None
        try:
            return int(float(val) * 3600)
        except (ValueError, TypeError):
            return None

    # Convert GB to bytes for memory fields
    def gb_to_bytes(val):
        if val is None:
            return None
        try:
            return int(float(val) * 1024 * 1024 * 1024)
        except (ValueError, TypeError):
            return None

    # Get the short_id which may include array index like "6049117[28]"
    raw_short_id = record.get("short_id", "")

    # Use full_job_id from JSON key (e.g., "2712367.desched1")
    full_id = full_job_id if full_job_id else raw_short_id

    result = {
        # job_id is the scheduler's identifier (not globally unique across years)
        "job_id": full_id,
        # short_id is just the base job number for efficient queries
        "short_id": parse_job_id(raw_short_id),
        "name": record.get("jobname"),
        "user": record.get("user"),
        "account": record.get("account"),

        # Queue and status
        "queue": record.get("queue"),
        "status": record.get("Exit_status"),

        # Timestamps (ctime=submit, etime=eligible, start, end)
        "submit": parse_timestamp(record.get("ctime")),
        "eligible": parse_timestamp(record.get("etime")),
        "start": parse_timestamp(record.get("start")),
        "end": parse_timestamp(record.get("end")),

        # Time metrics (convert from hours to seconds)
        "elapsed": hours_to_seconds(resources_used.get("walltime")),
        "walltime": hours_to_seconds(resource_list.get("walltime")),
        "cputime": hours_to_seconds(resources_used.get("cput")),

        # Resource allocation
        "numcpus": parse_int(resource_list.get("ncpus")),
        "numgpus": parse_int(resource_list.get("ngpus")),
        "numnodes": parse_int(resource_list.get("nodect")),
        "mpiprocs": mpiprocs,
        "ompthreads": ompthreads,

        # Memory (convert from GB to bytes)
        "reqmem": gb_to_bytes(resource_list.get("mem")),
        "memory": gb_to_bytes(resources_used.get("mem")),
        "vmemory": gb_to_bytes(resources_used.get("vmem")),

        # Resource types (not available in JSON, set to None)
        "cputype": None,
        "gputype": None,
        "resources": resource_list.get("select"),
        "ptargets": resource_list.get("preempt_targets"),

        # Performance metrics
        "cpupercent": parse_float(resources_used.get("cpupercent")),
        "avgcpu": parse_float(resources_used.get("avgcpu")),
        "count": parse_int(record.get("run_count")),
    }

    return result
