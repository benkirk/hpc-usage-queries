"""File-format exporters (``dat`` / ``csv`` / ``md`` / ``json-file``).

Thin adaptors around :mod:`job_history.exporters`. Each one converts a
jobhist envelope ``{rows, columns, ...}`` back to the ``(data, columns,
filepath)`` shape the legacy exporter expects, then delegates — so the
resulting file is byte-for-byte identical to the pre-refactor output.

Resolving the output filename:
    - If a :class:`ReportConfig` was supplied (resource commands),
      ``config.get_filename(machine, start, end, ext)`` provides the
      canonical ``{prefix}_{filename_base}_{start}_{end}.{ext}`` name.
    - Otherwise (history commands using a file format), the filename
      is derived from ``envelope['kind']`` + the date window.
"""

import os
from pathlib import Path
from typing import Any, Dict, List

from job_history.exporters import (
    CSVExporter,
    DatExporter,
    JSONExporter,
    MarkdownExporter,
)

from .output import Exporter


class _ColumnView:
    """Lightweight stand-in for the dataclass ColumnSpec.

    The legacy exporters in :mod:`job_history.exporters` read ``.key``,
    ``.header``, ``.width``, and ``.format`` attributes from each column
    object. Envelope columns are plain dicts, so this view adapts them
    without forcing a re-import of the dataclass in this module.
    """

    __slots__ = ("key", "header", "width", "format")

    def __init__(self, d: Dict[str, Any]):
        self.key = d["key"]
        self.header = d["header"]
        self.width = d["width"]
        self.format = d["format"]


def _envelope_columns(envelope: Dict[str, Any]) -> List[_ColumnView]:
    return [_ColumnView(c) for c in envelope.get("columns", [])]


def _resolve_output_dir(ctx) -> Path:
    """Pick the directory for file-format output.

    Defaults to the current working directory if ``ctx.output_dir`` is
    unset — same behaviour as the legacy ``resource`` group default.
    """
    out = ctx.output_dir or Path.cwd()
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    return out


def _resolve_filename(envelope: Dict[str, Any], *, ctx, config, extension: str) -> Path:
    """Resolve the final output path for a file-format export."""
    if config is not None:
        machine = envelope.get("machine") or ctx.machine
        start = envelope.get("start") or ctx.start_date
        end = envelope.get("end") or ctx.end_date
        name = config.get_filename(machine, start, end, extension=extension)
    else:
        kind = envelope.get("kind", "report")
        start = envelope.get("start") or ctx.start_date
        end = envelope.get("end") or ctx.end_date
        name = f"{kind}_{start}_{end}.{extension}"
    return _resolve_output_dir(ctx) / name


def _announce(ctx, path: Path) -> None:
    """Tell the user where the file landed (matches legacy behaviour)."""
    try:
        ctx.console.print(f"Report saved to {path}")
    except Exception:
        # If the console can't render (e.g. in subprocess capture), fall
        # back to a plain print so the path is still surfaced.
        print(f"Report saved to {path}")


class DatFileExporter(Exporter):
    """``--format dat`` → fixed-width DAT file (byte-compatible with legacy)."""

    EXT = "dat"

    def emit(self, envelope, *, ctx, config=None):
        path = _resolve_filename(envelope, ctx=ctx, config=config, extension=self.EXT)
        DatExporter().export(
            envelope.get("rows", []),
            _envelope_columns(envelope),
            str(path),
        )
        _announce(ctx, path)


class CsvFileExporter(Exporter):
    """``--format csv`` → CSV file."""

    EXT = "csv"

    def emit(self, envelope, *, ctx, config=None):
        path = _resolve_filename(envelope, ctx=ctx, config=config, extension=self.EXT)
        CSVExporter().export(
            envelope.get("rows", []),
            _envelope_columns(envelope),
            str(path),
        )
        _announce(ctx, path)


class MarkdownFileExporter(Exporter):
    """``--format md`` → Markdown table file."""

    EXT = "md"

    def emit(self, envelope, *, ctx, config=None):
        path = _resolve_filename(envelope, ctx=ctx, config=config, extension=self.EXT)
        MarkdownExporter().export(
            envelope.get("rows", []),
            _envelope_columns(envelope),
            str(path),
        )
        _announce(ctx, path)


class JSONFileExporter(Exporter):
    """``--format json-file`` → JSON envelope to a file (parallel to JSONStdoutExporter)."""

    EXT = "json"

    def emit(self, envelope, *, ctx, config=None):
        path = _resolve_filename(envelope, ctx=ctx, config=config, extension=self.EXT)
        # Use the legacy JSONExporter for byte-for-byte parity with old behaviour.
        JSONExporter().export(
            envelope.get("rows", []),
            _envelope_columns(envelope),
            str(path),
        )
        _announce(ctx, path)


def register_file_exporters() -> None:
    """Register ``dat``/``csv``/``md``/``json-file`` with ``ExporterRegistry``.

    Called at import time of :mod:`job_history.cli.cmds.jobhist` so the
    formats are available wherever the CLI is entered.
    """
    from .output import ExporterRegistry
    ExporterRegistry.register("dat", DatFileExporter)
    ExporterRegistry.register("csv", CsvFileExporter)
    ExporterRegistry.register("md", MarkdownFileExporter)
    ExporterRegistry.register("json-file", JSONFileExporter)
