"""Pluggable output layer for the fs-scans CLI.

One :class:`Exporter` per ``--format`` value, selected via
:class:`ExporterRegistry`. ``rich`` reuses the existing presentation code in
:mod:`fs_scans.queries.display` and the histogram ``format_output`` methods so
console output is identical to the pre-refactor CLI; ``json`` emits the
``kind=``-tagged envelope to stdout.

JSON conventions match ``job_history``/project_samuel: ``date``/``datetime`` →
ISO 8601, ``Decimal`` → ``float``, ``set`` → sorted list, ``indent=2``,
``sort_keys=False``, top-level ``kind`` identifying the envelope shape.
"""

import json
import sys
from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


class _FsScanEncoder(json.JSONEncoder):
    """JSON encoder for fs-scans envelopes."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, set):
            return sorted(obj)
        return super().default(obj)


def output_json(envelope: dict) -> None:
    """Write a JSON envelope to stdout with the standard formatting."""
    json.dump(envelope, sys.stdout, cls=_FsScanEncoder, indent=2, sort_keys=False)
    sys.stdout.write("\n")


class Exporter(ABC):
    """Abstract output adaptor — one implementation per ``--format`` value."""

    @abstractmethod
    def emit(self, envelope: dict) -> None:
        """Render ``envelope`` for the user."""


class RichExporter(Exporter):
    """``--format rich`` → Rich tables / histogram text on the shared console.

    Dispatches on ``envelope['kind']`` and delegates to the existing
    presentation helpers so output matches the historical CLI exactly.
    """

    def emit(self, envelope: dict) -> None:
        from ...queries import display
        from ...cli.common import console

        kind = envelope["kind"]
        meta = envelope.get("meta", {})

        if kind == "fs_directories":
            display.print_results(
                envelope["rows"],
                verbose=meta.get("verbose", False),
                leaves_only=meta.get("leaves_only", False),
                username_map=meta.get("username_map", {}),
                show_total=meta.get("show_total", False),
                show_dir_counts=meta.get("show_dir_counts", False),
            )
        elif kind == "fs_owner_summary":
            display.print_owner_results(
                envelope["rows"],
                meta.get("name_map", {}),
                show_filesystem=meta.get("show_filesystem", False),
            )
        elif kind == "fs_group_summary":
            display.print_group_results(
                envelope["rows"],
                meta.get("name_map", {}),
                show_filesystem=meta.get("show_filesystem", False),
            )
        elif kind in ("fs_access_history", "fs_file_size"):
            console.print(self._render_histogram(envelope))
            console.print()
        else:  # pragma: no cover - guarded by builders
            raise ValueError(f"Unknown envelope kind: {kind}")

    @staticmethod
    def _render_histogram(envelope: dict) -> str:
        from ...queries.access_history import AccessHistogram
        from ...queries.histogram_common import HistogramData

        hist = envelope["histogram"]
        top_n = envelope.get("meta", {}).get("top_n", 10)
        directory = hist["directory"]
        username_map = hist["username_map"]

        if hist["renderer"] == "access_histogram":
            return AccessHistogram.from_dict(hist).format_output(
                directory, username_map, top_n
            )
        return HistogramData.from_dict(hist).format_output(
            title=hist["title"],
            directory=directory,
            username_map=username_map,
            top_n=top_n,
        )


class JSONStdoutExporter(Exporter):
    """``--format json`` → JSON envelope to stdout."""

    def emit(self, envelope: dict) -> None:
        output_json(envelope)


class TSVFileExporter(Exporter):
    """``-o FILE`` → tab-separated directory listing written to a file.

    Only the ``fs_directories`` kind is supported (matching the historical
    ``--output`` behavior of ``fs-scans query``).
    """

    def __init__(self, output_path: Path):
        self.output_path = output_path

    def emit(self, envelope: dict) -> None:
        from ...queries.display import write_tsv

        if envelope["kind"] != "fs_directories":
            raise ValueError(
                f"TSV output is only supported for directory listings, not {envelope['kind']!r}"
            )
        write_tsv(
            envelope["rows"],
            self.output_path,
            include_dir_counts=envelope.get("meta", {}).get("show_dir_counts", False),
        )


class ExporterRegistry:
    """Maps ``--format`` strings to :class:`Exporter` classes."""

    _registry: dict[str, type] = {
        "rich": RichExporter,
        "json": JSONStdoutExporter,
    }

    @classmethod
    def register(cls, fmt: str, exporter_cls: type) -> None:
        if not issubclass(exporter_cls, Exporter):
            raise TypeError(f"{exporter_cls!r} is not an Exporter subclass")
        cls._registry[fmt] = exporter_cls

    @classmethod
    def resolve(cls, fmt: str) -> Exporter:
        if fmt not in cls._registry:
            available = ", ".join(sorted(cls._registry))
            raise ValueError(f"Unknown output format {fmt!r}. Available: {available}")
        return cls._registry[fmt]()

    @classmethod
    def available(cls) -> list[str]:
        return sorted(cls._registry)
