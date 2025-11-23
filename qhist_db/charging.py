"""Charging rules for HPC resources by machine and queue."""

from typing import Callable

# Type alias for charging function
ChargingFunc = Callable[[dict], float]


def derecho_charge(job: dict) -> float:
    """Calculate charge hours for a Derecho job.

    Args:
        job: Job record dict with elapsed, numnodes, numcpus, numgpus, queue

    Returns:
        Charge in appropriate units (core-hours or GPU-hours)
    """
    elapsed = job.get("elapsed") or 0
    numnodes = job.get("numnodes") or 0
    numcpus = job.get("numcpus") or 0
    numgpus = job.get("numgpus") or 0
    queue = (job.get("queue") or "").lower()

    # GPU development queue - charge by actual GPUs
    if "gpu" in queue and "dev" in queue:
        return elapsed * numgpus / 3600.0

    # CPU development queue - charge by actual CPUs
    if "dev" in queue:
        return elapsed * numcpus / 3600.0

    # GPU production queues - 4 GPUs per node
    if "gpu" in queue:
        return elapsed * numnodes * 4 / 3600.0

    # CPU production queues (default) - 128 cores per node
    return elapsed * numnodes * 128 / 3600.0


def casper_charge(job: dict) -> dict:
    """Calculate charge metrics for a Casper job.

    Casper tracks CPU-hours, memory-hours, and GPU-hours (when GPUs used).

    Args:
        job: Job record dict with elapsed, numcpus, numgpus, memory

    Returns:
        Dict with cpu_hours, memory_hours, and gpu_hours
    """
    elapsed = job.get("elapsed") or 0
    numcpus = job.get("numcpus") or 0
    numgpus = job.get("numgpus") or 0
    memory = job.get("memory") or 0  # in bytes

    return {
        "cpu_hours": elapsed * numcpus / 3600.0,
        "gpu_hours": elapsed * numgpus / 3600.0,
        "memory_hours": elapsed * memory / (3600.0 * 1024 * 1024 * 1024),  # GB-hours
    }


# SQL for creating charging views
DERECHO_VIEW_SQL = """
CREATE VIEW IF NOT EXISTS v_jobs_charged AS
SELECT *,
  CASE
    WHEN queue LIKE '%gpu%' AND queue LIKE '%dev%'
      THEN elapsed * COALESCE(numgpus, 0) / 3600.0
    WHEN queue LIKE '%dev%'
      THEN elapsed * COALESCE(numcpus, 0) / 3600.0
    WHEN queue LIKE '%gpu%'
      THEN elapsed * COALESCE(numnodes, 0) * 4 / 3600.0
    ELSE elapsed * COALESCE(numnodes, 0) * 128 / 3600.0
  END AS charge_hours
FROM jobs;
"""

CASPER_VIEW_SQL = """
CREATE VIEW IF NOT EXISTS v_jobs_charged AS
SELECT *,
  elapsed * COALESCE(numcpus, 0) / 3600.0 AS cpu_hours,
  elapsed * COALESCE(numgpus, 0) / 3600.0 AS gpu_hours,
  elapsed * COALESCE(memory, 0) / (3600.0 * 1024 * 1024 * 1024) AS memory_hours
FROM jobs;
"""

VIEW_SQL = {
    "derecho": DERECHO_VIEW_SQL,
    "casper": CASPER_VIEW_SQL,
}


def get_view_sql(machine: str) -> str:
    """Get the SQL to create the charging view for a machine.

    Args:
        machine: Machine name ('casper' or 'derecho')

    Returns:
        SQL CREATE VIEW statement
    """
    return VIEW_SQL.get(machine, DERECHO_VIEW_SQL)
