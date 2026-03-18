"""Charging rules for HPC resources by machine and queue.

This module defines charging calculations for NCAR HPC clusters. Each machine
has different charging rules based on queue type and resource usage.

Charges are computed in Python during job import and stored in the job_charges table.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..database.models import Job

# ============================================================================
# Module-level constants (also available as class attributes on SystemCharging)
# ============================================================================

BYTES_PER_GB = 1024 * 1024 * 1024  # 1 GB in bytes
SECONDS_PER_HOUR = 3600

# Registry populated automatically via __init_subclass__
_REGISTRY: dict[str, type["SystemCharging"]] = {}


# ============================================================================
# Base class
# ============================================================================

class SystemCharging(ABC):
    """Abstract base class for machine-specific HPC charging calculations.

    Subclasses set MACHINE_NAME and implement calculate().  The shared helpers
    (_get_elapsed, _get_memory, _get_qos_factor, _get_memory_hours) cover the
    logic that is identical on every machine; only machine-specific formulas
    belong in the subclass.

    Usage:
        SystemCharging.charge("derecho", job)   # dispatch by machine name
        DerechoCharging.calculate(job)           # direct call
    """

    MACHINE_NAME: str = ""

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if cls.MACHINE_NAME:
            _REGISTRY[cls.MACHINE_NAME] = cls

    # ── Abstract ──────────────────────────────────────────────────────────────

    @classmethod
    @abstractmethod
    def calculate(cls, job: "Job") -> dict:
        """Calculate charge metrics for a job on this machine.

        Returns:
            Dict with cpu_hours, gpu_hours, memory_hours, qos_factor
        """

    # ── Factory dispatch ──────────────────────────────────────────────────────

    @classmethod
    def charge(cls, machine: str, job: "Job") -> dict:
        """Dispatch to the correct subclass based on machine name.

        Args:
            machine: Registered machine name (e.g. 'derecho', 'casper')
            job: Job object to calculate charges for

        Raises:
            ValueError: if machine is not registered
        """
        if machine not in _REGISTRY:
            raise ValueError(f"Unknown machine: {machine!r}. Known machines: {sorted(_REGISTRY)}")
        return _REGISTRY[machine].calculate(job)

    # ── Shared helpers ────────────────────────────────────────────────────────

    @staticmethod
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

    @staticmethod
    def _get_elapsed(job: "Job") -> float:
        return getattr(job, "elapsed", None) or 0

    @staticmethod
    def _get_memory(job: "Job") -> float:
        return getattr(job, "memory", None) or 0  # in bytes

    @staticmethod
    def _get_qos_factor(job: "Job") -> float:
        """Return the QoS multiplier based on job priority or for particular queues."""
        priority = (getattr(job, "priority", None) or "").lower()
        queue    = (getattr(job, "queue",   None) or "").lower()
        if queue == "jhublogin":
            return 0.0
        if priority == "premium":
            return 1.5
        if priority == "economy":
            return 0.7
        return 1.0  # regular or unset

    @classmethod
    def _get_memory_hours(cls, job: "Job") -> float:
        """Memory hours: GB-hours based on actual memory used."""
        return cls._get_elapsed(job) * cls._get_memory(job) / (SECONDS_PER_HOUR * BYTES_PER_GB)


# ============================================================================
# Derecho
# ============================================================================

class DerechoCharging(SystemCharging):
    """Charging rules for the Derecho cluster.

    CPU/GPU hours depend on queue type (dev vs production):
      - Production: charge by node (cores/GPUs per node * numnodes)
      - Dev:        charge by actual allocated CPUs/GPUs
    Memory hours use actual memory consumed.
    """

    MACHINE_NAME = "derecho"

    CORES_PER_NODE = 128  # CPU cores per compute node
    GPUS_PER_NODE = 4     # GPUs per GPU node

    @classmethod
    def calculate(cls, job: "Job") -> dict:
        # Future expansion: pbs_record exposes the full original PBS accounting
        # record and can be used to refine charging (e.g., actual node topology,
        # exec_host breakdown, resource_list overrides).
        _pbs_record = cls._get_pbs_record(job)  # noqa: F841 (unused until implemented)

        elapsed = cls._get_elapsed(job)
        numnodes = getattr(job, "numnodes", None) or 0
        numcpus  = getattr(job, "numcpus",  None) or 0
        numgpus  = getattr(job, "numgpus",  None) or 0
        queue    = (getattr(job, "queue",   None) or "").lower()

        is_dev_queue = "dev" in queue
        is_gpu_queue = "gpu" in queue

        # dev queues use actual CPUs/GPUs, production uses cores/GPU per node
        if is_dev_queue:
            cpu_hours = elapsed * numcpus / SECONDS_PER_HOUR
            gpu_hours = elapsed * numgpus / SECONDS_PER_HOUR
        else:
            cpu_hours = elapsed * numnodes * cls.CORES_PER_NODE / SECONDS_PER_HOUR
            gpu_hours = elapsed * numnodes * cls.GPUS_PER_NODE / SECONDS_PER_HOUR

        if not is_gpu_queue:
            gpu_hours = 0

        return {
            "cpu_hours":    cpu_hours,
            "gpu_hours":    gpu_hours,
            "memory_hours": cls._get_memory_hours(job),
            "qos_factor":   cls._get_qos_factor(job),
        }


# ============================================================================
# Casper
# ============================================================================

class CasperCharging(SystemCharging):
    """Charging rules for the Casper cluster.

    Casper charges directly by allocated CPUs and GPUs (no node-based scaling).
    Memory hours use actual memory consumed.
    """

    MACHINE_NAME = "casper"

    @classmethod
    def calculate(cls, job: "Job") -> dict:
        # Future expansion: pbs_record exposes the full original PBS accounting
        # record and can be used to refine charging (e.g., actual node topology,
        # exec_host breakdown, resource_list overrides).
        _pbs_record = cls._get_pbs_record(job)  # noqa: F841 (unused until implemented)

        elapsed = cls._get_elapsed(job)
        numcpus = getattr(job, "numcpus", None) or 0
        numgpus = getattr(job, "numgpus", None) or 0

        return {
            "cpu_hours":    elapsed * numcpus / SECONDS_PER_HOUR,
            "gpu_hours":    elapsed * numgpus / SECONDS_PER_HOUR,
            "memory_hours": cls._get_memory_hours(job),
            "qos_factor":   cls._get_qos_factor(job),
        }
