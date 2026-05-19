"""Unified output / exporter layer for the jobhist CLI.

One ``Exporter`` ABC drives every output format. The active exporter is
chosen by ``Context.output_format`` (``rich``/``json``/``dat``/``csv``/``md``)
via the ``ExporterRegistry``.

Phase 1 lands the registry, the JSON encoder, and the two stdout
exporters (``rich`` and ``json``). The file-based exporters (``dat``,
``csv``, ``md``, ``json-file``) wrap the existing ``job_history.exporters``
module and are registered in Phase 3.

JSON envelope conventions match project_samuel's ``src/cli/core/output.py``:

    - ``date`` / ``datetime`` → ISO 8601 strings
    - ``Decimal`` → ``float``
    - ``set`` → sorted list
    - ``indent=2``, ``sort_keys=False``
    - top-level ``kind`` field identifies the envelope shape
"""

import json
import sys
from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from rich.table import Table


class _JobhistEncoder(json.JSONEncoder):
    """JSON encoder for jobhist envelopes. Mirrors SAM's ``_SAMEncoder``."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, set):
            return sorted(obj)
        return super().default(obj)


def output_json(envelope: Dict[str, Any]) -> None:
    """Write a JSON envelope to stdout with the standard formatting."""
    json.dump(envelope, sys.stdout, cls=_JobhistEncoder, indent=2, sort_keys=False)
    sys.stdout.write("\n")


class Exporter(ABC):
    """Abstract output adaptor — one implementation per ``--format`` value."""

    @abstractmethod
    def emit(self, envelope: Dict[str, Any], *, ctx, config=None) -> None:
        """Render ``envelope`` for the user.

        ``ctx`` is the active :class:`Context`. ``config`` is an optional
        ``ReportConfig`` (only resource-style commands supply one); history
        commands pass ``None``.
        """


class RichTableExporter(Exporter):
    """``--format rich`` → Rich table on ``ctx.console``."""

    def emit(self, envelope: Dict[str, Any], *, ctx, config=None) -> None:
        columns = envelope.get("columns") or []
        rows = envelope.get("rows") or []

        table = Table(show_header=True, header_style="bold cyan")
        if columns:
            for col in columns:
                table.add_column(col["header"])
            for row in rows:
                table.add_row(*[self._cell(row.get(col["key"]), col) for col in columns])
        else:
            # Fallback: rows is a list of dicts with arbitrary keys
            if rows:
                keys = list(rows[0].keys())
                for k in keys:
                    table.add_column(str(k))
                for row in rows:
                    table.add_row(*[str(row.get(k, "")) for k in keys])
        ctx.console.print(table)

    @staticmethod
    def _cell(value: Any, col: Dict[str, Any]) -> str:
        if value is None:
            return ""
        fmt = col.get("format") or ""
        if fmt and fmt != "s":
            try:
                return format(value, fmt)
            except (TypeError, ValueError):
                return str(value)
        return str(value)


class JSONStdoutExporter(Exporter):
    """``--format json`` → JSON envelope to stdout."""

    def emit(self, envelope: Dict[str, Any], *, ctx, config=None) -> None:
        output_json(envelope)


class ExporterRegistry:
    """Maps ``--format`` strings to :class:`Exporter` classes."""

    _registry: Dict[str, type] = {
        "rich": RichTableExporter,
        "json": JSONStdoutExporter,
        # dat / csv / md / json-file land in Phase 3 via register().
    }

    @classmethod
    def register(cls, fmt: str, exporter_cls: type) -> None:
        """Add or override an exporter for ``fmt``."""
        if not issubclass(exporter_cls, Exporter):
            raise TypeError(f"{exporter_cls!r} is not an Exporter subclass")
        cls._registry[fmt] = exporter_cls

    @classmethod
    def resolve(cls, fmt: str) -> Exporter:
        """Return a fresh instance of the exporter for ``fmt``."""
        if fmt not in cls._registry:
            available = ", ".join(sorted(cls._registry))
            raise ValueError(f"Unknown output format {fmt!r}. Available: {available}")
        return cls._registry[fmt]()

    @classmethod
    def available(cls):
        return sorted(cls._registry)
