"""Charging rules for HPC resources by machine and queue.

This module defines charging calculations for NCAR HPC clusters. Each machine
has different charging rules based on queue type and resource usage.

Charges are computed in Python during job import and stored in the job_charges table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .models import Job

# Type alias for charging function
ChargingFunc = Callable[["Job"], dict]

# ============================================================================
# Machine-specific constants
# ============================================================================

# Derecho cluster
DERECHO_CORES_PER_NODE = 128  # CPU cores per compute node
DERECHO_GPUS_PER_NODE = 4     # GPUs per GPU node

# Memory conversion
BYTES_PER_GB = 1024 * 1024 * 1024  # 1 GB in bytes

# Time conversion
SECONDS_PER_HOUR = 3600


# ============================================================================
# Helpers
# ============================================================================

def _get_pbs_record(job: "Job"):
    """Safely retrieve the PbsRecord for a job.

    pbs_record requires a live SQLAlchemy session (it lazy-loads job_record_obj
    via a relationship).  It may be unavailable for:
      - historical jobs imported before the JobRecord feature was added
      - test fixtures that use SimpleNamespace rather than a real ORM object
      - jobs accessed outside an active session (DetachedInstanceError)

    Returns:
        PbsRecord object, or None if unavailable for any reason
    """
    try:
        return job.pbs_record
    except Exception:
        return None


# ============================================================================
# Python charging functions (for ad-hoc calculations)
# ============================================================================

def derecho_charge(job: "Job") -> dict:
    """Calculate charge metrics for a Derecho job.

    Derecho tracks CPU-hours, GPU-hours, and memory-hours.
    CPU/GPU hours depend on queue type (dev vs production).

    This function is called by Job.calculate_charges() during import to populate
    the job_charges table.

    Args:
        job: Job object with elapsed, numnodes, numcpus, numgpus, memory, queue attributes

    Returns:
        Dict with cpu_hours, gpu_hours, and memory_hours
    """
    # Future expansion: pbs_record exposes the full original PBS accounting
    # record and can be used to refine charging (e.g., actual node topology,
    # exec_host breakdown, resource_list overrides).
    _pbs_record = _get_pbs_record(job)  # noqa: F841 (unused until implemented)

    elapsed = getattr(job, "elapsed", None) or 0
    numnodes = getattr(job, "numnodes", None) or 0
    numcpus = getattr(job, "numcpus", None) or 0
    numgpus = getattr(job, "numgpus", None) or 0
    memory = getattr(job, "memory", None) or 0  # in bytes
    queue = (getattr(job, "queue", None) or "").lower()

    is_gpu_queue = "gpu" in queue
    is_dev_queue = "dev" in queue

    # CPU hours: dev queues use actual CPUs, production uses cores per node
    if is_dev_queue:
        cpu_hours = elapsed * numcpus / SECONDS_PER_HOUR
    else:
        cpu_hours = elapsed * numnodes * DERECHO_CORES_PER_NODE / SECONDS_PER_HOUR

    # GPU hours: only for GPU queues; dev uses actual GPUs, production uses GPUs per node
    if is_gpu_queue:
        if is_dev_queue:
            gpu_hours = elapsed * numgpus / SECONDS_PER_HOUR
        else:
            gpu_hours = elapsed * numnodes * DERECHO_GPUS_PER_NODE / SECONDS_PER_HOUR
    else:
        gpu_hours = 0.0

    # Memory hours: GB-hours based on actual memory used
    memory_hours = elapsed * memory / (SECONDS_PER_HOUR * BYTES_PER_GB)

    return {
        "cpu_hours": cpu_hours,
        "gpu_hours": gpu_hours,
        "memory_hours": memory_hours,
    }


def casper_charge(job: "Job") -> dict:
    """Calculate charge metrics for a Casper job.

    Casper tracks CPU-hours, memory-hours, and GPU-hours (when GPUs used).

    This function is called by Job.calculate_charges() during import to populate
    the job_charges table.

    Args:
        job: Job object with elapsed, numcpus, numgpus, memory attributes

    Returns:
        Dict with cpu_hours, memory_hours, and gpu_hours
    """
    # Future expansion: pbs_record exposes the full original PBS accounting
    # record and can be used to refine charging (e.g., actual node topology,
    # exec_host breakdown, resource_list overrides).
    _pbs_record = _get_pbs_record(job)  # noqa: F841 (unused until implemented)

    elapsed = getattr(job, "elapsed", None) or 0
    numcpus = getattr(job, "numcpus", None) or 0
    numgpus = getattr(job, "numgpus", None) or 0
    memory = getattr(job, "memory", None) or 0  # in bytes

    return {
        "cpu_hours": elapsed * numcpus / SECONDS_PER_HOUR,
        "gpu_hours": elapsed * numgpus / SECONDS_PER_HOUR,
        "memory_hours": elapsed * memory / (SECONDS_PER_HOUR * BYTES_PER_GB),
    }
