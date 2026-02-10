"""Field parsing and type conversion for PBS accounting log records.

This module transforms pbsparse.PbsRecord objects into the same normalized
dictionary format as qhist_db.parsers.parse_job_record(), enabling complete
code reuse for database insertion, charge calculation, and summaries.
"""

from datetime import datetime, timezone


def parse_pbs_time(time_str: str) -> int | None:
    """Convert HH:MM:SS time string to seconds.

    Args:
        time_str: Time in HH:MM:SS format (e.g., "00:14:18")

    Returns:
        Total seconds as integer, or None if parsing fails

    Examples:
        >>> parse_pbs_time("00:14:18")
        858
        >>> parse_pbs_time("06:42:05")
        24125
    """
    if not time_str:
        return None
    try:
        parts = time_str.split(":")
        if len(parts) != 3:
            return None
        hours, minutes, seconds = map(int, parts)
        return hours * 3600 + minutes * 60 + seconds
    except (ValueError, AttributeError):
        return None


def parse_pbs_memory_kb(mem_str: str) -> int | None:
    """Convert memory string with 'kb' suffix to bytes.

    Args:
        mem_str: Memory string like "172600kb"

    Returns:
        Memory in bytes as integer, or None if parsing fails

    Examples:
        >>> parse_pbs_memory_kb("172600kb")
        176742400
        >>> parse_pbs_memory_kb("1024kb")
        1048576
    """
    if not mem_str:
        return None
    try:
        # Strip 'kb' suffix (case-insensitive) and convert to bytes
        val_str = mem_str.lower().rstrip("kb")
        return int(val_str) * 1024
    except (ValueError, AttributeError):
        return None


def parse_pbs_memory_gb(mem_str: str) -> int | None:
    """Convert memory string with 'gb' or 'GB' suffix to bytes.

    Args:
        mem_str: Memory string like "235gb" or "150G"

    Returns:
        Memory in bytes as integer, or None if parsing fails

    Examples:
        >>> parse_pbs_memory_gb("235gb")
        252348030976
        >>> parse_pbs_memory_gb("150G")
        161061273600
    """
    if not mem_str:
        return None
    try:
        # Strip 'gb' or 'g' suffix (case-insensitive) and convert to bytes
        val_str = mem_str.lower().rstrip("gb")
        return int(float(val_str) * 1024 * 1024 * 1024)
    except (ValueError, AttributeError):
        return None


def parse_pbs_timestamp(unix_time: int | str) -> datetime | None:
    """Convert Unix timestamp to UTC datetime.

    Args:
        unix_time: Unix epoch timestamp (integer or string)

    Returns:
        datetime in UTC timezone, or None if parsing fails

    Examples:
        >>> parse_pbs_timestamp(1769670016)
        datetime.datetime(2026, 1, 29, 0, 0, 16, tzinfo=datetime.timezone.utc)
        >>> parse_pbs_timestamp("1769670016")
        datetime.datetime(2026, 1, 29, 0, 0, 16, tzinfo=datetime.timezone.utc)
    """
    if not unix_time:
        return None
    try:
        timestamp = int(unix_time)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def parse_select_string(select_str: str) -> dict:
    """Extract mpiprocs, ompthreads, cpu_type, and gpu_type from select string.

    PBS select strings have format like:
    "1:ncpus=128:mpiprocs=128:mem=235GB:ompthreads=1:cpu_type=genoa"

    Args:
        select_str: Resource_List.select value

    Returns:
        Dictionary with optional keys: mpiprocs, ompthreads, cpu_type, gpu_type
        (only includes keys that are found in the select string)

    Examples:
        >>> parse_select_string("1:ncpus=128:mpiprocs=128:ompthreads=1:cpu_type=genoa")
        {'mpiprocs': 128, 'ompthreads': 1, 'cpu_type': 'genoa'}
    """
    result = {}
    if not select_str:
        return result

    # Split on ':' and parse key=value pairs
    for part in select_str.split(":"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)

        if key == "mpiprocs":
            try:
                result["mpiprocs"] = int(value)
            except ValueError:
                pass
        elif key == "ompthreads":
            try:
                result["ompthreads"] = int(value)
            except ValueError:
                pass
        elif key == "cpu_type":
            result["cpu_type"] = value
        elif key == "gpu_type":
            result["gpu_type"] = value

    return result


# Queue name to GPU type mapping
GPU_QUEUE_TYPES = {
    "a100": "a100",
    "h100": "h100",
    "l40": "l40",
    "nvgpu": "v100",  # Casper nvgpu uses V100
}

# Default CPU types for machines when not specified in select string
MACHINE_CPU_DEFAULTS = {
    "derecho": "milan",  # AMD Milan
    "casper": None,      # Mixed types, don't guess
}


def infer_types_from_queue(queue_name: str, machine: str) -> dict:
    """Fallback: infer CPU/GPU types from queue name when not in select string.

    Args:
        queue_name: PBS queue name (e.g., "cpu", "a100", "h100")
        machine: Machine name (e.g., "derecho", "casper")

    Returns:
        Dictionary with optional keys: cputype, gputype

    Examples:
        >>> infer_types_from_queue("a100", "derecho")
        {'gputype': 'a100'}
        >>> infer_types_from_queue("cpu", "derecho")
        {'cputype': 'milan'}
        >>> infer_types_from_queue("nvgpu", "casper")
        {'gputype': 'v100'}
    """
    result = {}

    # Check if queue name matches a GPU type
    if queue_name in GPU_QUEUE_TYPES:
        result["gputype"] = GPU_QUEUE_TYPES[queue_name]
    else:
        # For CPU-only queues, use machine default
        cpu_default = MACHINE_CPU_DEFAULTS.get(machine)
        if cpu_default:
            result["cputype"] = cpu_default

    return result


def parse_pbs_record(pbs_record, machine: str) -> dict:
    """Transform pbsparse.PbsRecord to database dictionary.

    This produces the EXACT same format as qhist_db.parsers.parse_job_record(),
    ensuring compatibility with existing sync infrastructure.

    Args:
        pbs_record: pbsparse.PbsRecord object (type 'E' End record)
        machine: Machine name for type inference fallback

    Returns:
        Normalized dictionary matching database schema

    Field mappings:
        - job_id: pbs_record.id (e.g., "4779496.desched1")
        - short_id: int(pbs_record.short_id)
        - user: pbs_record.user
        - account: pbs_record.account with quotes stripped
        - queue: pbs_record.queue
        - name: pbs_record.jobname
        - submit: parse_pbs_timestamp(ctime)
        - eligible: parse_pbs_timestamp(etime)
        - start: parse_pbs_timestamp(start)
        - end: parse_pbs_timestamp(end)
        - walltime: parse_pbs_time(Resource_List.walltime)
        - elapsed: parse_pbs_time(resources_used.walltime)
        - cputime: parse_pbs_time(resources_used.cput)
        - reqmem: parse_pbs_memory_gb(Resource_List.mem)
        - memory: parse_pbs_memory_kb(resources_used.mem)
        - vmemory: parse_pbs_memory_kb(resources_used.vmem)
        - cputype/gputype: from select string or inferred from queue
        - resources: Resource_List.select
        - ptargets: Resource_List.preempt_targets
    """
    resource_list = getattr(pbs_record, 'Resource_List', None) or {}
    resources_used = getattr(pbs_record, 'resources_used', None) or {}

    # Extract mpiprocs, ompthreads, cpu_type, gpu_type from select string
    select_str = resource_list.get("select", "")
    select_info = parse_select_string(select_str)

    # Get mpiprocs and ompthreads from select string, with fallback
    mpiprocs = select_info.get("mpiprocs")
    if mpiprocs is None:
        # Fallback to Resource_List.mpiprocs
        mpiprocs_str = resource_list.get("mpiprocs")
        if mpiprocs_str:
            try:
                mpiprocs = int(mpiprocs_str)
            except ValueError:
                pass

    ompthreads = select_info.get("ompthreads")

    # Get CPU/GPU types from select string, with queue fallback
    cputype = select_info.get("cpu_type")
    gputype = select_info.get("gpu_type")
    if not cputype and not gputype:
        # Fallback to queue-based inference
        queue_types = infer_types_from_queue(pbs_record.queue, machine)
        cputype = queue_types.get("cputype")
        gputype = queue_types.get("gputype")

    # Parse account field - remove surrounding quotes
    # PBS logs have: account="UCSD0047"
    account = pbs_record.account
    if account and account.startswith('"') and account.endswith('"'):
        account = account[1:-1]

    # Parse integer fields
    def safe_int(value):
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # Parse float fields
    def safe_float(value):
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    result = {
        # Job identification
        "job_id": pbs_record.id,
        "short_id": safe_int(pbs_record.short_id),
        "name": pbs_record.jobname,
        "user": pbs_record.user,
        "account": account,

        # Queue and status
        "queue": pbs_record.queue,
        "status": pbs_record.Exit_status,

        # Timestamps (all converted to UTC)
        "submit": parse_pbs_timestamp(pbs_record.ctime),
        "eligible": parse_pbs_timestamp(pbs_record.etime),
        "start": parse_pbs_timestamp(pbs_record.start),
        "end": parse_pbs_timestamp(pbs_record.end),

        # Time metrics (all in seconds)
        "walltime": parse_pbs_time(resource_list.get("walltime")),
        "elapsed": parse_pbs_time(resources_used.get("walltime")),
        "cputime": parse_pbs_time(resources_used.get("cput")),

        # Resource allocation
        "numcpus": safe_int(resource_list.get("ncpus")),
        "numgpus": safe_int(resource_list.get("ngpus")),
        "numnodes": safe_int(resource_list.get("nodect")),
        "mpiprocs": mpiprocs,
        "ompthreads": ompthreads,

        # Memory (all in bytes)
        "reqmem": parse_pbs_memory_gb(resource_list.get("mem")),
        "memory": parse_pbs_memory_kb(resources_used.get("mem")),
        "vmemory": parse_pbs_memory_kb(resources_used.get("vmem")),

        # Resource types (PBS logs can provide these!)
        "cputype": cputype,
        "gputype": gputype,
        "resources": select_str,
        "ptargets": resource_list.get("preempt_targets"),

        # Performance metrics
        "cpupercent": safe_float(resources_used.get("cpupercent")),
        "avgcpu": None,  # Not available in PBS logs
        "count": safe_int(pbs_record.run_count),
    }

    return result
