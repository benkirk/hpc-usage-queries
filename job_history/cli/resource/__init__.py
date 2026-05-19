"""Resource subcommands — declarative RESOURCE_REPORTS table driven by
a single :class:`ResourceCommand` execution class.

The mix matches the chosen design: declarative registration of all
~30 reports stays in ``reports.py``; execution flows through one
command class in ``commands.py`` so exit codes, exception handling,
and exporter dispatch behave consistently across every report.
"""

from .reports import (
    ColumnSpec,
    ColumnSpecs,
    RESOURCE_REPORTS,
    ReportConfig,
)
from .commands import ResourceCommand

__all__ = [
    "ColumnSpec",
    "ColumnSpecs",
    "RESOURCE_REPORTS",
    "ReportConfig",
    "ResourceCommand",
]
