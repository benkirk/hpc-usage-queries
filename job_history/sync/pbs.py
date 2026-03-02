"""PBS accounting log parsing, scanning, job record streaming, and sync driver.

All PBS-specific logic lives here:
- Field parsing and type conversion
- Log file scanning and job record streaming
- SyncPBSLogs: the SyncBase subclass for PBS accounting logs
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pbsparse

from .base import SyncBase
from .utils import (
    parse_date_string, date_range,
    safe_int, safe_float, validate_timestamp_ordering,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PBS time / memory / timestamp parsers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# PBS select-string and queue-type helpers
# ---------------------------------------------------------------------------

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

    if queue_name in GPU_QUEUE_TYPES:
        result["gputype"] = GPU_QUEUE_TYPES[queue_name]
    else:
        cpu_default = MACHINE_CPU_DEFAULTS.get(machine)
        if cpu_default:
            result["cputype"] = cpu_default

    return result


# ---------------------------------------------------------------------------
# PBS record → database dict
# ---------------------------------------------------------------------------

def parse_pbs_record(pbs_record, machine: str) -> dict:
    """Transform pbsparse.PbsRecord to database dictionary.

    Args:
        pbs_record: pbsparse.PbsRecord object (type 'E' End record)
        machine: Machine name for type inference fallback

    Returns:
        Normalized dictionary matching database schema
    """
    resource_list = getattr(pbs_record, 'Resource_List', None) or {}
    resources_used = getattr(pbs_record, 'resources_used', None) or {}

    select_str = resource_list.get("select", "")
    select_info = parse_select_string(select_str)

    mpiprocs = select_info.get("mpiprocs")
    if mpiprocs is None:
        mpiprocs_str = resource_list.get("mpiprocs")
        if mpiprocs_str:
            try:
                mpiprocs = int(mpiprocs_str)
            except ValueError:
                pass

    ompthreads = select_info.get("ompthreads")

    cputype = select_info.get("cpu_type")
    gputype = select_info.get("gpu_type")
    if not cputype and not gputype:
        queue_types = infer_types_from_queue(pbs_record.queue, machine)
        cputype = queue_types.get("cputype")
        gputype = queue_types.get("gputype")

    try:
        account = pbs_record.account
        if account and account.startswith('"') and account.endswith('"'):
            account = account[1:-1]
    except AttributeError:
        account = "none"

    result = {
        "job_id": pbs_record.id,
        "short_id": safe_int(pbs_record.short_id),
        "name": pbs_record.jobname,
        "user": pbs_record.user,
        "account": account,
        "queue": pbs_record.queue,
        "status": pbs_record.Exit_status,
        "submit": parse_pbs_timestamp(pbs_record.ctime),
        "eligible": parse_pbs_timestamp(pbs_record.etime),
        "start": parse_pbs_timestamp(pbs_record.start),
        "end": parse_pbs_timestamp(pbs_record.end),
        "walltime": parse_pbs_time(resource_list.get("walltime")),
        "elapsed": parse_pbs_time(resources_used.get("walltime")),
        "numcpus": safe_int(resource_list.get("ncpus")),
        "numgpus": safe_int(resource_list.get("ngpus")),
        "numnodes": safe_int(resource_list.get("nodect")),
        "mpiprocs": mpiprocs,
        "ompthreads": ompthreads,
        "reqmem": parse_pbs_memory_gb(resource_list.get("mem")),
        "memory": parse_pbs_memory_kb(resources_used.get("mem")),
        "vmemory": parse_pbs_memory_kb(resources_used.get("vmem")),
        "priority": resource_list.get("job_priority"),
        "cputype": cputype,
        "gputype": gputype,
        "resources": select_str,
        "ptargets": resource_list.get("preempt_targets"),
        # Full PBS record stored for charging refinement and JobRecord archival
        "pbs_record_object": pbs_record,
    }

    # Fix start=0 (Unix epoch) — calculate from end - elapsed
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    if result["start"] is not None and result["end"] is not None and result["elapsed"] is not None:
        if result["start"] == epoch:
            from datetime import timedelta
            result["start"] = result["end"] - timedelta(seconds=result["elapsed"])

    # Fix eligible=0
    if result["eligible"] is not None and result["submit"] is not None:
        if result["eligible"] == epoch:
            result["eligible"] = result["submit"]

    return result


# ---------------------------------------------------------------------------
# PBS log file helpers
# ---------------------------------------------------------------------------

def _get_record_class(machine: str) -> type:
    """Return the appropriate PbsRecord subclass for the given machine.

    For Derecho, imports DerechoRecord from the job_history._vendor.ncar shim
    (which resolves to _vendor/pbs-parser-ncar/ncar.py).  The shim registers
    the module in sys.modules so that pickle deserialization works in any
    execution context (sync, query, or downstream packages).
    All other machines use PbsRecord directly.
    """
    if machine == "derecho":
        try:
            from job_history._vendor.pbs_parser_ncar.ncar import DerechoRecord
            return DerechoRecord
        except Exception as e:
            logger.debug(f"DerechoRecord unavailable ({e}); using base PbsRecord")
    return pbsparse.PbsRecord


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

    Streams job records from local PBS accounting logs without loading
    everything into memory.

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
    """
    log_dir = Path(log_dir)

    if date:
        dates = [date]
    elif start_date and end_date:
        dates = list(date_range(start_date, end_date))
    else:
        raise ValueError("Must provide either 'date' or 'start_date' and 'end_date'")

    for date_str in dates:
        log_path = get_log_file_path(log_dir, date_str)

        if not log_path.exists():
            raise RuntimeError(
                f"PBS log file not found: {log_path}\n"
                f"Expected log file for date {date_str}"
            )

        logger.info(f"Scanning PBS log: {log_path}")

        try:
            records = pbsparse.get_pbs_records(
                str(log_path),
                CustomRecord=_get_record_class(machine),
                type_filter="E",
            )
        except Exception as e:
            raise RuntimeError(f"Failed to parse PBS log {log_path}: {e}") from e

        for pbs_record in records:
            try:
                job_dict = parse_pbs_record(pbs_record, machine)
            except Exception as e:
                logger.warning(
                    f"Failed to parse PBS record {pbs_record.id}: {e}",
                    exc_info=True,
                )
                continue

            if not job_dict.get("job_id"):
                logger.warning("Skipping record with missing job_id")
                continue

            if not validate_timestamp_ordering(
                job_dict.get("submit"), job_dict.get("eligible"),
                job_dict.get("start"), job_dict.get("end"),
            ):
                logger.warning(
                    f"Invalid timestamp ordering for job {job_dict['job_id']}: "
                    f"submit={job_dict.get('submit')}, eligible={job_dict.get('eligible')}, "
                    f"start={job_dict.get('start')}, end={job_dict.get('end')}"
                )
                # Still yield — _sync_single_day re-validates and skips if needed

            yield job_dict


# ---------------------------------------------------------------------------
# PBS sync driver
# ---------------------------------------------------------------------------

class SyncPBSLogs(SyncBase):
    """Sync driver for PBS accounting logs.

    Implements fetch_records() using local YYYYMMDD-named PBS accounting files.
    All sync orchestration (date iteration, insert, charge, summarize) is
    inherited from SyncBase.
    """

    SCHEDULER_NAME = "PBS"

    def fetch_records(self, log_dir: str | Path | None, period: str) -> Iterator[dict]:
        """Yield normalized job dicts for one day from PBS accounting logs.

        Args:
            log_dir: Directory containing YYYYMMDD-named PBS log files
            period:  Date in YYYY-MM-DD format

        Yields:
            Normalized job dictionaries (includes 'pbs_record_object' key)

        Raises:
            RuntimeError: If log_dir is None, file is missing, or parse fails
        """
        if log_dir is None:
            raise RuntimeError("PBS sync requires a log directory (--log-path)")
        return fetch_jobs_from_pbs_logs(
            log_dir=log_dir, machine=self.machine, date=period
        )
