"""Charging rules for HPC resources by machine and queue.

This module defines charging calculations for NCAR HPC clusters. Each machine
has different charging rules based on queue type and resource usage.

Charges are computed in Python during job import and stored in the job_charges table.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..database.session import JOB_QOS_SEED

if TYPE_CHECKING:
    from ..database.models import Job

# ============================================================================
# Module-level constants (also available as class attributes on SystemCharging)
# ============================================================================

BYTES_PER_GB = 1024 * 1024 * 1024  # 1 GB in bytes
SECONDS_PER_HOUR = 3600

# Registry populated automatically via __init_subclass__
_REGISTRY: dict[str, type["SystemCharging"]] = {}

# Mapping of raw PBS priority strings → canonical JobQoS row name.
# Authoritative for resolving _pending_qos_name at sync time; the numeric
# factors live in the JobQoS table (seeded by _ensure_qos_seed_rows).
_PRIORITY_TO_QOS_NAME: dict = {
    "premium": "premium",
    "economy": "economy",
    "regular": "regular",
    "special": "special",
    "":        "regular",
    None:      "regular",
}

# Factors for a job with no resolved JobQoS row (SimpleNamespace fixtures,
# pre-migration rows): the canonical seed values.
_FALLBACK_QOS_FACTORS: dict = {name: factor for name, factor, _ in JOB_QOS_SEED}


@dataclass(frozen=True)
class NodeType:
    cores: int
    gpus: int


def select_chunks(select: str | None) -> list[tuple[int, int]]:
    """(node count, ngpus per node) for each ``+`` chunk of a PBS select string."""
    chunks = []
    for chunk in (select or "").split("+"):
        parts = [p for p in chunk.strip().split(":") if p]
        if not parts:
            continue
        count = int(parts.pop(0)) if parts[0].isdigit() else 1
        ngpus = 0
        for part in parts:
            key, _, value = part.partition("=")
            if key == "ngpus" and value.isdigit():
                ngpus = int(value)
        chunks.append((count, ngpus))
    return chunks


@dataclass(frozen=True)
class QueueRule:
    """A queue's charging rule; an unlisted queue is exclusive-node, priority QoS."""

    shared: bool = False     # charge allocated ncpus/ngpus, not whole nodes
    qos: str | None = None   # JobQoS name that overrides job_priority


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

    # Per-machine queue rules; a subclass extends this dict.
    # bin/update_jobs_db.py backfills jobs.qos_id from the same qos overrides.
    QUEUE_RULES: dict[str, QueueRule] = {
        "jhublogin": QueueRule(qos="uncharged"),
    }

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
        return cls.for_machine(machine).calculate(job)

    @classmethod
    def resolve_qos_name(cls, machine: str, job) -> str:
        """Canonical JobQoS row name for *job* under *machine*'s queue rules."""
        return cls.for_machine(machine)._resolve_qos_name(job)

    @classmethod
    def queue_rule(cls, queue: str | None) -> QueueRule:
        return cls.QUEUE_RULES.get((queue or "").lower(), QueueRule())

    @classmethod
    def qos_overrides(cls) -> dict[str, str]:
        """Queue -> JobQoS name for every queue whose rule overrides priority."""
        return {q: r.qos for q, r in cls.QUEUE_RULES.items() if r.qos}

    @classmethod
    def priority_overrides(cls) -> dict[str, str]:
        """PBS priority -> JobQoS name for every priority that is not the regular default."""
        return {p: q for p, q in _PRIORITY_TO_QOS_NAME.items() if p and q != "regular"}

    @classmethod
    def for_machine(cls, machine: str) -> type["SystemCharging"]:
        """The registered subclass for *machine*.

        Raises:
            ValueError: if machine is not registered
        """
        if machine not in _REGISTRY:
            raise ValueError(f"Unknown machine: {machine!r}. Known machines: {sorted(_REGISTRY)}")
        return _REGISTRY[machine]

    # ── Shared helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _get_elapsed(job: "Job") -> float:
        return getattr(job, "elapsed", None) or 0

    @staticmethod
    def _get_memory(job: "Job") -> float:
        return getattr(job, "memory", None) or 0  # in bytes

    @classmethod
    def _resolve_qos_name(cls, job) -> str:
        """Map a job's queue + priority to a canonical JobQoS row name.

        Queue takes precedence: a queue rule with a qos short-circuits the
        priority check.  Accepts either an ORM Job (or SimpleNamespace) or a
        raw record dict.
        """
        if isinstance(job, dict):
            queue = job.get("queue")
            priority = (job.get("priority") or "").lower()
        else:
            queue = getattr(job, "queue", None)
            priority = (getattr(job, "priority", None) or "").lower()
        rule_qos = cls.queue_rule(queue).qos
        if rule_qos:
            return rule_qos
        return _PRIORITY_TO_QOS_NAME.get(priority, "regular")

    @classmethod
    def _get_qos_factor(cls, job: "Job") -> float:
        """Return the QoS multiplier for *job*.

        Prefers the resolved JobQoS row (job.qos_obj.factor) when present.
        Falls back to mapping (priority, queue) against the canonical seed
        factors so SimpleNamespace test fixtures and pre-migration rows
        without qos_id still produce the right value.
        """
        qos_obj = getattr(job, "qos_obj", None)
        if qos_obj is not None:
            return qos_obj.factor
        name = cls._resolve_qos_name(job)
        return _FALLBACK_QOS_FACTORS.get(name, 1.0)

    @classmethod
    def _get_memory_hours(cls, job: "Job") -> float:
        """Memory hours: GB-hours based on actual memory used."""
        return cls._get_elapsed(job) * cls._get_memory(job) / (SECONDS_PER_HOUR * BYTES_PER_GB)


# ============================================================================
# Derecho
# ============================================================================

class DerechoCharging(SystemCharging):
    """Charging rules for the Derecho cluster.

    Exclusive-node queues charge whole nodes of the type the job requested:
    a select chunk with ngpus > 0 is a GPU node, the rest are CPU nodes.
    Shared (dev) queues charge the allocated ncpus/ngpus.  Memory hours use
    actual memory consumed.
    """

    MACHINE_NAME = "derecho"

    CPU_NODE = NodeType(cores=128, gpus=0)
    GPU_NODE = NodeType(cores=64, gpus=4)

    # pcpu/pgpu are the execution queues behind `-q preempt`; preempt pricing
    # wins over whatever job_priority the job carries.
    QUEUE_RULES = {
        **SystemCharging.QUEUE_RULES,
        "cpudev": QueueRule(shared=True),
        "gpudev": QueueRule(shared=True),
        "pcpu":   QueueRule(qos="preempt"),
        "pgpu":   QueueRule(qos="preempt"),
    }

    @classmethod
    def _gpu_nodes(cls, job: "Job", numnodes: int) -> int:
        """Nodes of the job's request that are GPU nodes; numgpus decides without a select."""
        gpu_nodes = sum(n for n, ngpus in select_chunks(getattr(job, "resources", None)) if ngpus)
        if not gpu_nodes and (getattr(job, "numgpus", None) or 0) > 0:
            gpu_nodes = numnodes
        return min(gpu_nodes, numnodes)

    @classmethod
    def calculate(cls, job: "Job") -> dict:
        elapsed  = cls._get_elapsed(job)
        numnodes = getattr(job, "numnodes", None) or 0
        numcpus  = getattr(job, "numcpus",  None) or 0
        numgpus  = getattr(job, "numgpus",  None) or 0

        if cls.queue_rule(getattr(job, "queue", None)).shared:
            cpus, gpus = numcpus, numgpus
        else:
            gpu_nodes = cls._gpu_nodes(job, numnodes)
            cpu_nodes = numnodes - gpu_nodes
            cpus = cpu_nodes * cls.CPU_NODE.cores + gpu_nodes * cls.GPU_NODE.cores
            gpus = gpu_nodes * cls.GPU_NODE.gpus

        return {
            "cpu_hours":    elapsed * cpus / SECONDS_PER_HOUR,
            "gpu_hours":    elapsed * gpus / SECONDS_PER_HOUR,
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
        elapsed = cls._get_elapsed(job)
        numcpus = getattr(job, "numcpus", None) or 0
        numgpus = getattr(job, "numgpus", None) or 0

        return {
            "cpu_hours":    elapsed * numcpus / SECONDS_PER_HOUR,
            "gpu_hours":    elapsed * numgpus / SECONDS_PER_HOUR,
            "memory_hours": cls._get_memory_hours(job),
            "qos_factor":   cls._get_qos_factor(job),
        }
