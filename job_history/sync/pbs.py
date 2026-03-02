"""PBS accounting log parsing and sync driver.

All PBS-specific logic lives in SyncPBSLogs:
- Field parsing and type conversion (static methods)
- Log file scanning and job record streaming (fetch_records)
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pbsparse

from .base import SyncBase
from .utils import parse_date_string, safe_int, safe_float

logger = logging.getLogger(__name__)


class SyncPBSLogs(SyncBase):
    """Sync driver for PBS accounting logs.

    All PBS-specific parsing lives here as static methods.
    Sync orchestration (date iteration, insert, charge, summarize) is
    inherited from SyncBase.
    """

    SCHEDULER_NAME = "PBS"

    # ------------------------------------------------------------------
    # Class-level constants
    # ------------------------------------------------------------------

    # Queue name → GPU type
    GPU_QUEUE_TYPES = {
        "a100": "a100",
        "h100": "h100",
        "l40": "l40",
        "nvgpu": "v100",  # Casper nvgpu uses V100
    }

    # Default CPU type per machine when not in select string
    MACHINE_CPU_DEFAULTS = {
        "derecho": "milan",  # AMD Milan
        "casper": None,      # Mixed types, don't guess
    }

    # ------------------------------------------------------------------
    # Time / memory / timestamp parsers
    # ------------------------------------------------------------------

    @staticmethod
    def parse_pbs_time(time_str: str) -> int | None:
        """Convert HH:MM:SS time string to seconds.

        Examples:
            >>> SyncPBSLogs.parse_pbs_time("00:14:18")
            858
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

    @staticmethod
    def parse_pbs_memory_kb(mem_str: str) -> int | None:
        """Convert memory string with 'kb' suffix to bytes.

        Examples:
            >>> SyncPBSLogs.parse_pbs_memory_kb("172600kb")
            176742400
        """
        if not mem_str:
            return None
        try:
            return int(mem_str.lower().rstrip("kb")) * 1024
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def parse_pbs_memory_gb(mem_str: str) -> int | None:
        """Convert memory string with 'gb'/'g' suffix to bytes.

        Examples:
            >>> SyncPBSLogs.parse_pbs_memory_gb("235gb")
            252348030976
        """
        if not mem_str:
            return None
        try:
            return int(float(mem_str.lower().rstrip("gb")) * 1024 * 1024 * 1024)
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def parse_pbs_timestamp(unix_time: int | str) -> datetime | None:
        """Convert Unix timestamp to UTC datetime.

        Examples:
            >>> SyncPBSLogs.parse_pbs_timestamp(1769670016)
            datetime.datetime(2026, 1, 29, 0, 0, 16, tzinfo=datetime.timezone.utc)
        """
        if not unix_time:
            return None
        try:
            return datetime.fromtimestamp(int(unix_time), tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return None

    # ------------------------------------------------------------------
    # Select-string and queue-type helpers
    # ------------------------------------------------------------------

    @staticmethod
    def parse_select_string(select_str: str) -> dict:
        """Extract mpiprocs, ompthreads, cpu_type, gpu_type from PBS select string.

        Examples:
            >>> SyncPBSLogs.parse_select_string("1:ncpus=128:mpiprocs=128:cpu_type=genoa")
            {'mpiprocs': 128, 'cpu_type': 'genoa'}
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

    @staticmethod
    def infer_types_from_queue(queue_name: str, machine: str) -> dict:
        """Fallback: infer CPU/GPU types from queue name when not in select string.

        Examples:
            >>> SyncPBSLogs.infer_types_from_queue("a100", "derecho")
            {'gputype': 'a100'}
            >>> SyncPBSLogs.infer_types_from_queue("cpu", "derecho")
            {'cputype': 'milan'}
        """
        if queue_name in SyncPBSLogs.GPU_QUEUE_TYPES:
            return {"gputype": SyncPBSLogs.GPU_QUEUE_TYPES[queue_name]}
        cpu_default = SyncPBSLogs.MACHINE_CPU_DEFAULTS.get(machine)
        return {"cputype": cpu_default} if cpu_default else {}

    # ------------------------------------------------------------------
    # PBS record → database dict
    # ------------------------------------------------------------------

    @staticmethod
    def parse_pbs_record(pbs_record, machine: str) -> dict:
        """Transform pbsparse.PbsRecord to database dictionary.

        Args:
            pbs_record: pbsparse.PbsRecord (type 'E' End record)
            machine: Machine name for type inference fallback

        Returns:
            Normalized dictionary matching database schema
        """
        resource_list = getattr(pbs_record, 'Resource_List', None) or {}
        resources_used = getattr(pbs_record, 'resources_used', None) or {}

        select_str = resource_list.get("select", "")
        select_info = SyncPBSLogs.parse_select_string(select_str)

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
            queue_types = SyncPBSLogs.infer_types_from_queue(pbs_record.queue, machine)
            cputype = queue_types.get("cputype")
            gputype = queue_types.get("gputype")

        try:
            account = pbs_record.account
            if account and account.startswith('"') and account.endswith('"'):
                account = account[1:-1]
        except AttributeError:
            account = "none"

        result = {
            "job_id":    pbs_record.id,
            "short_id":  safe_int(pbs_record.short_id),
            "name":      pbs_record.jobname,
            "user":      pbs_record.user,
            "account":   account,
            "queue":     pbs_record.queue,
            "status":    pbs_record.Exit_status,
            "submit":    SyncPBSLogs.parse_pbs_timestamp(pbs_record.ctime),
            "eligible":  SyncPBSLogs.parse_pbs_timestamp(pbs_record.etime),
            "start":     SyncPBSLogs.parse_pbs_timestamp(pbs_record.start),
            "end":       SyncPBSLogs.parse_pbs_timestamp(pbs_record.end),
            "walltime":  SyncPBSLogs.parse_pbs_time(resource_list.get("walltime")),
            "elapsed":   SyncPBSLogs.parse_pbs_time(resources_used.get("walltime")),
            "numcpus":   safe_int(resource_list.get("ncpus")),
            "numgpus":   safe_int(resource_list.get("ngpus")),
            "numnodes":  safe_int(resource_list.get("nodect")),
            "mpiprocs":  mpiprocs,
            "ompthreads": ompthreads,
            "reqmem":    SyncPBSLogs.parse_pbs_memory_gb(resource_list.get("mem")),
            "memory":    SyncPBSLogs.parse_pbs_memory_kb(resources_used.get("mem")),
            "vmemory":   SyncPBSLogs.parse_pbs_memory_kb(resources_used.get("vmem")),
            "priority":  resource_list.get("job_priority"),
            "cputype":   cputype,
            "gputype":   gputype,
            "resources": select_str,
            "ptargets":  resource_list.get("preempt_targets"),
            # Full PBS record for charging refinement and JobRecord archival
            "pbs_record_object": pbs_record,
        }

        # Fix start=0 (Unix epoch) — calculate from end - elapsed
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        if result["start"] == epoch and result["end"] is not None and result["elapsed"] is not None:
            result["start"] = result["end"] - timedelta(seconds=result["elapsed"])

        # Fix eligible=0
        if result["eligible"] == epoch and result["submit"] is not None:
            result["eligible"] = result["submit"]

        return result

    # ------------------------------------------------------------------
    # Log file helpers
    # ------------------------------------------------------------------

    @staticmethod
    def get_log_file_path(log_dir: Path, date_str: str) -> Path:
        """Construct PBS log file path (YYYYMMDD) for a given YYYY-MM-DD date.

        Examples:
            >>> SyncPBSLogs.get_log_file_path(Path("/pbs"), "2026-01-29")
            PosixPath('/pbs/20260129')
        """
        return log_dir / parse_date_string(date_str).strftime("%Y%m%d")

    @staticmethod
    def _get_record_class(machine: str) -> type:
        """Return the appropriate PbsRecord subclass for the given machine."""
        if machine == "derecho":
            try:
                from job_history._vendor.pbs_parser_ncar.ncar import DerechoRecord
                return DerechoRecord
            except Exception as e:
                logger.debug(f"DerechoRecord unavailable ({e}); using base PbsRecord")
        return pbsparse.PbsRecord

    # ------------------------------------------------------------------
    # SyncBase implementation
    # ------------------------------------------------------------------

    def fetch_records(self, log_dir: str | Path | None, period: str) -> Iterator[dict]:
        """Yield normalized job dicts for one day from PBS accounting logs.

        Args:
            log_dir: Directory containing YYYYMMDD-named PBS log files
            period:  Date in YYYY-MM-DD format

        Raises:
            RuntimeError: If log_dir is None, file is missing, or parse fails
        """
        if log_dir is None:
            raise RuntimeError("PBS sync requires a log directory (--log-path)")

        log_path = self.get_log_file_path(Path(log_dir), period)

        if not log_path.exists():
            raise RuntimeError(
                f"PBS log file not found: {log_path}\n"
                f"Expected log file for date {period}"
            )

        logger.info(f"Scanning PBS log: {log_path}")

        try:
            records = pbsparse.get_pbs_records(
                str(log_path),
                CustomRecord=self._get_record_class(self.machine),
                type_filter="E",
            )
        except Exception as e:
            raise RuntimeError(f"Failed to parse PBS log {log_path}: {e}") from e

        for pbs_record in records:
            try:
                job_dict = self.parse_pbs_record(pbs_record, self.machine)
            except Exception as e:
                logger.warning(
                    f"Failed to parse PBS record {pbs_record.id}: {e}",
                    exc_info=True,
                )
                continue

            if not job_dict.get("job_id"):
                logger.warning("Skipping record with missing job_id")
                continue

            yield job_dict
