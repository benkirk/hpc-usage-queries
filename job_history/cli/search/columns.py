"""Single source of truth for ``jobhist search`` column metadata.

Each entry maps a column key → display spec (header/width/format) plus the
attribute path used to pull the value from either the ``Job`` row or its
outer-joined ``JobCharge`` row. The same registry drives:

* the row-projection inside :meth:`JobQueries.jobs_search`
* envelope construction in :func:`builders.build_search`
* ``--display`` validation in :class:`SearchCommand`

That way default/verbose/custom column lists all flow from one declaration.
"""

from typing import Any, Dict, Sequence


# Each spec: header (display), width (Rich/exporter), format (printf-style),
# source ("job.<attr>" or "charge.<attr>") — used by project_row().
COLUMNS: Dict[str, Dict[str, Any]] = {
    "job_id":         {"header": "Job ID",    "width": 22, "format": "s",   "source": "job.job_id"},
    "user":           {"header": "User",      "width": 12, "format": "s",   "source": "job.user"},
    "account":        {"header": "Project",   "width": 12, "format": "s",   "source": "job.account"},
    "queue":          {"header": "Queue",     "width": 10, "format": "s",   "source": "job.queue"},
    "status":         {"header": "Status",    "width":  8, "format": "s",   "source": "job.status"},
    "numnodes":       {"header": "Nodes",     "width":  6, "format": "d",   "source": "job.numnodes"},
    "numcpus":        {"header": "CPUs",      "width":  6, "format": "d",   "source": "job.numcpus"},
    "numgpus":        {"header": "GPUs",      "width":  6, "format": "d",   "source": "job.numgpus"},
    "start":          {"header": "Start",     "width": 19, "format": "s",   "source": "job.start"},
    "end":            {"header": "End",       "width": 19, "format": "s",   "source": "job.end"},
    "elapsed":        {"header": "Elapsed",   "width":  9, "format": "d",   "source": "job.elapsed"},
    "cpu_hours":      {"header": "CPU-h",     "width": 10, "format": ".2f", "source": "charge.cpu_hours"},
    "gpu_hours":      {"header": "GPU-h",     "width": 10, "format": ".2f", "source": "charge.gpu_hours"},
    # — verbose-only below —
    "short_id":       {"header": "Short ID",  "width": 10, "format": "d",   "source": "job.short_id"},
    "name":           {"header": "Name",      "width": 20, "format": "s",   "source": "job.name"},
    "priority":       {"header": "Pri",       "width":  6, "format": "s",   "source": "job.priority"},
    "submit":         {"header": "Submit",    "width": 19, "format": "s",   "source": "job.submit"},
    "eligible":       {"header": "Eligible",  "width": 19, "format": "s",   "source": "job.eligible"},
    "walltime":       {"header": "Walltime",  "width":  9, "format": "d",   "source": "job.walltime"},
    "mpiprocs":       {"header": "MPI",       "width":  6, "format": "d",   "source": "job.mpiprocs"},
    "ompthreads":     {"header": "OMP",       "width":  6, "format": "d",   "source": "job.ompthreads"},
    "reqmem":         {"header": "ReqMem",    "width": 14, "format": "d",   "source": "job.reqmem"},
    "memory":         {"header": "Mem",       "width": 14, "format": "d",   "source": "job.memory"},
    "vmemory":        {"header": "VMem",      "width": 14, "format": "d",   "source": "job.vmemory"},
    "cputype":        {"header": "CPU type",  "width": 12, "format": "s",   "source": "job.cputype"},
    "gputype":        {"header": "GPU type",  "width": 12, "format": "s",   "source": "job.gputype"},
    "resources":      {"header": "Resources", "width": 30, "format": "s",   "source": "job.resources"},
    "memory_hours":   {"header": "Mem-h",     "width": 10, "format": ".2f", "source": "charge.memory_hours"},
    "qos_factor":     {"header": "QoS",       "width":  6, "format": ".2f", "source": "charge.qos_factor"},
    "charge_version": {"header": "Chg ver",   "width":  7, "format": "d",   "source": "charge.charge_version"},
    # Per-job QoS-weighted charges are not stored — computed = hours × qos_factor.
    "cpu_charges":    {"header": "CPU chg",   "width": 10, "format": ".2f", "source": "computed.cpu_charges"},
    "gpu_charges":    {"header": "GPU chg",   "width": 10, "format": ".2f", "source": "computed.gpu_charges"},
    "memory_charges": {"header": "Mem chg",   "width": 10, "format": ".2f", "source": "computed.memory_charges"},
}


DEFAULT_COLUMNS = (
    "job_id", "user", "account", "queue",
    "numnodes", "numcpus", "numgpus",
    "start", "end", "elapsed",
    "cpu_hours", "gpu_hours",
)

VERBOSE_COLUMNS = tuple(COLUMNS.keys())


def project_row(job, charge, cols: Sequence[str]) -> Dict[str, Any]:
    """Project a ``(Job, JobCharge|None)`` pair into a flat dict.

    Datetimes are serialized to ISO 8601 strings so the result is JSON-ready
    and renders cleanly through the Rich exporter without per-cell coercion.
    Computed-source columns derive QoS-weighted charges on the fly.
    """
    out: Dict[str, Any] = {}
    for key in cols:
        spec = COLUMNS[key]
        kind, attr = spec["source"].split(".", 1)
        if kind == "job":
            value = getattr(job, attr, None) if job is not None else None
        elif kind == "charge":
            value = getattr(charge, attr, None) if charge is not None else None
        else:  # "computed" — derive from charge
            value = _compute_charge(charge, attr)
        if hasattr(value, "isoformat"):
            value = value.isoformat(sep=" ", timespec="seconds")
        out[key] = value
    return out


def _compute_charge(charge, attr):
    """Per-job QoS-weighted charge: hours × qos_factor.

    Mirrors how ``daily_summary`` materializes the stored ``*_charges``
    columns. Returns None when there's no JobCharge row (outer-joined miss).
    """
    if charge is None:
        return None
    qos = charge.qos_factor if charge.qos_factor is not None else 1.0
    hours_attr = {
        "cpu_charges": "cpu_hours",
        "gpu_charges": "gpu_hours",
        "memory_charges": "memory_hours",
    }.get(attr)
    if hours_attr is None:
        return None
    hours = getattr(charge, hours_attr, None)
    if hours is None:
        return None
    return hours * qos
