"""Envelope builder for ``jobhist search``.

Pure function: takes pre-projected dict rows from
:meth:`JobQueries.jobs_search` plus the active :class:`Context` and emits
the JSON envelope consumed by every Exporter.
"""

from typing import Any, Dict, List, Optional, Sequence

from .columns import COLUMNS


def build_search(
    rows: List[Dict[str, Any]],
    *,
    ctx,
    requested_cols: Sequence[str],
    filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the ``kind="search"`` envelope.

    ``requested_cols`` controls column ordering / visibility — the row dicts
    already contain exactly those keys (see :func:`columns.project_row`).
    """
    columns = [
        {"key": k, "header": COLUMNS[k]["header"],
         "width": COLUMNS[k]["width"], "format": COLUMNS[k]["format"]}
        for k in requested_cols
    ]
    return {
        "kind": "search",
        "machine": ctx.machine,
        "start": ctx.start_date,
        "end": ctx.end_date,
        "filters": filters or {},
        "columns": columns,
        "rows": rows,
    }
