"""CLI output layer for fs-scans: envelope builders + pluggable exporters.

Mirrors the pattern in ``job_history/cli/core`` (a ``kind=``-tagged JSON
envelope rendered through an :class:`Exporter` chosen by ``--format``) but is
wholly independent — the two modules never import one another.
"""

from .builders import (
    build_access_history,
    build_directories,
    build_file_size,
    build_group_summary,
    build_owner_summary,
)
from .output import Exporter, ExporterRegistry, output_json

__all__ = [
    "build_directories",
    "build_owner_summary",
    "build_group_summary",
    "build_access_history",
    "build_file_size",
    "Exporter",
    "ExporterRegistry",
    "output_json",
]
