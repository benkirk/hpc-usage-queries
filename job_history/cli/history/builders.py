"""History envelope builders.

Pure functions that take raw query rows and the active :class:`Context`,
and return a JSON-ready envelope ``{kind, machine, start, end, group_by,
columns, rows, ...}``. The same envelope is consumed by both the Rich
table exporter and the JSON stdout exporter.
"""

from collections import defaultdict
from typing import Any, Dict, List


def _columns_jobs_per_entity(primary_entity: str, verbose: bool) -> List[Dict[str, Any]]:
    if primary_entity == "user":
        cols = [
            {"key": "period",  "header": "Period",    "width": 12, "format": "s"},
            {"key": "user",    "header": "User",      "width": 16, "format": "s"},
        ]
        if verbose:
            cols.append({"key": "account", "header": "Account", "width": 16, "format": "s"})
    else:  # account
        cols = [
            {"key": "period",  "header": "Period",    "width": 12, "format": "s"},
            {"key": "account", "header": "Account",   "width": 16, "format": "s"},
        ]
        if verbose:
            cols.append({"key": "user", "header": "User", "width": 16, "format": "s"})
    cols.append({"key": "job_count", "header": "Job Count", "width": 0, "format": ""})
    return cols


def _collapse_secondary(rows: List[Dict[str, Any]], primary_entity: str) -> List[Dict[str, Any]]:
    """Aggregate away the secondary entity column when ``--verbose`` is off.

    Mirrors the original ``_run_jobs_per_entity_report`` behaviour in
    ``job_history/cli.py``.
    """
    aggregated: Dict[tuple, int] = defaultdict(int)
    for row in rows:
        key = (row["period"], row[primary_entity])
        aggregated[key] += row["job_count"]
    return [
        {"period": period, primary_entity: entity, "job_count": count}
        for (period, entity), count in sorted(aggregated.items())
    ]


def build_jobs_per_entity(rows, *, ctx, primary_entity: str, verbose: bool) -> Dict[str, Any]:
    """Envelope for jobs-per-user / jobs-per-project."""
    if not verbose:
        rows = _collapse_secondary(rows, primary_entity)
    return {
        "kind": f"jobs_per_{primary_entity}",
        "machine": ctx.machine,
        "start": ctx.start_date,
        "end": ctx.end_date,
        "group_by": ctx.group_by,
        "primary_entity": primary_entity,
        "verbose": verbose,
        "columns": _columns_jobs_per_entity(primary_entity, verbose),
        "rows": rows,
    }


def build_unique_projects(rows, *, ctx) -> Dict[str, Any]:
    """Envelope for unique-projects."""
    return {
        "kind": "unique_projects",
        "machine": ctx.machine,
        "start": ctx.start_date,
        "end": ctx.end_date,
        "group_by": ctx.group_by,
        "columns": [
            {"key": "period",        "header": "Period",          "width": 12, "format": "s"},
            {"key": "project_count", "header": "Unique Projects", "width": 0,  "format": ""},
        ],
        "rows": rows,
    }


def build_unique_users(rows, *, ctx) -> Dict[str, Any]:
    """Envelope for unique-users."""
    return {
        "kind": "unique_users",
        "machine": ctx.machine,
        "start": ctx.start_date,
        "end": ctx.end_date,
        "group_by": ctx.group_by,
        "columns": [
            {"key": "period",     "header": "Period",       "width": 12, "format": "s"},
            {"key": "user_count", "header": "Unique Users", "width": 0,  "format": ""},
        ],
        "rows": rows,
    }


def build_daily_summary(rows, *, ctx) -> Dict[str, Any]:
    """Envelope for daily-summary."""
    return {
        "kind": "daily_summary",
        "machine": ctx.machine,
        "start": ctx.start_date,
        "end": ctx.end_date,
        "columns": [
            {"key": "date",          "header": "Date",    "width": 12, "format": "s"},
            {"key": "user",          "header": "User",    "width": 16, "format": "s"},
            {"key": "account",       "header": "Account", "width": 16, "format": "s"},
            {"key": "queue",         "header": "Queue",   "width": 12, "format": "s"},
            {"key": "job_count",     "header": "Jobs",    "width": 8,  "format": ""},
            {"key": "cpu_hours",     "header": "CPU-h",   "width": 12, "format": ".1f"},
            {"key": "gpu_hours",     "header": "GPU-h",   "width": 12, "format": ".1f"},
            {"key": "memory_hours",  "header": "Mem-h",   "width": 0,  "format": ".1f"},
        ],
        "rows": rows,
    }
