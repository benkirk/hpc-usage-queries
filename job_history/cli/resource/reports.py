"""Declarative resource-report registry.

The dataclasses (:class:`ColumnSpec`, :class:`ReportConfig`) and the
:class:`ColumnSpecs` column-set factory were moved verbatim from the
original ``job_history/cli.py``. ``RESOURCE_REPORTS`` is the single
source of truth for every resource subcommand the CLI exposes.

Adding a new resource report = appending one ``ReportConfig`` entry
here. No new command class is needed; :class:`ResourceCommand` consumes
the config at execution time.
"""

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List


@dataclass
class ColumnSpec:
    """Specification for a single output column."""
    key: str          # Dict key in query result rows
    header: str       # Column header in output
    width: int        # Column width for formatting (0 = last column, no padding)
    format: str       # Format spec: "s", ".1f", ".4f", "" (no format)


@dataclass
class ReportConfig:
    """Configuration for a single resource report."""
    command_name: str                    # CLI subcommand name
    description: str                     # Click help text
    query_method: str                    # Method name on JobQueries
    query_params: Dict[str, Any]         # kwargs passed to the query method
    filename_base: str                   # Base name for file-format outputs
    columns: List[ColumnSpec]

    def get_filename(self, machine: str, start: date, end: date,
                     extension: str = "dat") -> str:
        """Generate the standard output filename for this report.

        Examples:
            ``De_pie_user_cpu_2026-01-01_2026-01-31.dat``
            ``All_usage_history_2026-01-01_2026-01-31.csv``
        """
        if machine.lower() == "all":
            prefix = "All"
        else:
            prefix = machine[:2].capitalize()
        return f"{prefix}_{self.filename_base}_{start}_{end}.{extension}"


class ColumnSpecs:
    """Factory for the common column-set patterns used by RESOURCE_REPORTS.

    Keeping these as static methods (rather than module-level constants)
    matches the original cli.py shape and lets future configs request a
    variant with a different label without redefining the whole list.
    """

    @staticmethod
    def usage_counts(label: str = "User-ids", label_width: int = 15) -> List[ColumnSpec]:
        return [
            ColumnSpec("label", label, label_width, "s"),
            ColumnSpec("usage_hours", "Usage", 15, ".1f"),
            ColumnSpec("job_count", "Counts", 0, ""),
        ]

    @staticmethod
    def range_waits(range_label: str) -> List[ColumnSpec]:
        return [
            ColumnSpec("range_label", range_label, 20, "s"),
            ColumnSpec("avg_wait_hours", "AveWait-hrs", 12, ".4f"),
            ColumnSpec("job_count", "#-Jobs", 0, ""),
        ]

    @staticmethod
    def range_sizes(range_label: str) -> List[ColumnSpec]:
        return [
            ColumnSpec("range_label", range_label, 20, "s"),
            ColumnSpec("job_count", "#-Jobs", 12, ""),
            ColumnSpec("user_count", "#-Users", 12, ""),
            ColumnSpec("hours", "Cr-hrs", 0, ".1f"),
        ]

    @staticmethod
    def duration_buckets() -> List[ColumnSpec]:
        return [
            ColumnSpec("date", "Date", 20, "s"),
            ColumnSpec("<30s", "<30s", 12, ".1f"),
            ColumnSpec("30s-30m", "30s-30m", 12, ".1f"),
            ColumnSpec("30-60m", "30-60m", 12, ".1f"),
            ColumnSpec("1-5h", "1-5h", 12, ".1f"),
            ColumnSpec("5-12h", "5-12h", 12, ".1f"),
            ColumnSpec("12-18h", "12-18h", 12, ".1f"),
            ColumnSpec(">18h", ">18h", 0, ".1f"),
        ]

    @staticmethod
    def memory_per_rank_buckets() -> List[ColumnSpec]:
        return [
            ColumnSpec("date", "Date", 20, "s"),
            ColumnSpec("<128MB", "<128MB", 12, ".1f"),
            ColumnSpec("128MB-512MB", "128MB-512MB", 14, ".1f"),
            ColumnSpec("512MB-1GB", "512MB-1GB", 12, ".1f"),
            ColumnSpec("1-2GB", "1-2GB", 12, ".1f"),
            ColumnSpec("2-4GB", "2-4GB", 12, ".1f"),
            ColumnSpec("4-8GB", "4-8GB", 12, ".1f"),
            ColumnSpec("8-16GB", "8-16GB", 12, ".1f"),
            ColumnSpec("16-32GB", "16-32GB", 12, ".1f"),
            ColumnSpec("32-64GB", "32-64GB", 12, ".1f"),
            ColumnSpec("64-128GB", "64-128GB", 12, ".1f"),
            ColumnSpec("128-256GB", "128-256GB", 12, ".1f"),
            ColumnSpec(">256GB", ">256GB", 0, ".1f"),
        ]

    @staticmethod
    def usage_history() -> List[ColumnSpec]:
        return [
            ColumnSpec("Date", "Date", 18, "s"),
            ColumnSpec("#-Users", "#-Users", 12, ""),
            ColumnSpec("#-Proj", "#-Proj", 8, ""),
            ColumnSpec("#-CPU-Users", "#-CPU-Users", 13, ""),
            ColumnSpec("#-CPU-Proj", "#-CPU-Proj", 13, ""),
            ColumnSpec("#-CPU-Jobs", "#-CPU-Jobs", 13, ""),
            ColumnSpec("#-CPU-Hrs", "#-CPU-Hrs", 12, ".1f"),
            ColumnSpec("#-GPU-Users", "#-GPU-Users", 13, ""),
            ColumnSpec("#-GPU-Proj", "#-GPU-Proj", 13, ""),
            ColumnSpec("#-GPU-Jobs", "#-GPU-Jobs", 13, ""),
            ColumnSpec("#-GPU-Hrs", "#-GPU-Hrs", 0, ".1f"),
        ]


# Query methods that accept a ``period`` parameter; for these reports
# the resource group's ``--group-by`` value is injected into query_params
# at execution time by :class:`ResourceCommand`.
PERIODIC_QUERY_METHODS = frozenset({"usage_history", "job_durations", "job_memory_per_rank"})


RESOURCE_REPORTS: List[ReportConfig] = [
    # Pie chart reports — usage by group
    ReportConfig(
        command_name="pie-proj-cpu",
        description="CPU usage by project (account)",
        query_method="usage_by_group",
        query_params={"resource_type": "cpu", "group_by": "account"},
        filename_base="pie_proj_cpu",
        columns=ColumnSpecs.usage_counts(label="Accounts"),
    ),
    ReportConfig(
        command_name="pie-user-gpu",
        description="GPU usage by user",
        query_method="usage_by_group",
        query_params={"resource_type": "gpu", "group_by": "user"},
        filename_base="pie_user_gpu",
        columns=ColumnSpecs.usage_counts(label="User-ids"),
    ),
    ReportConfig(
        command_name="pie-user-cpu",
        description="CPU usage by user",
        query_method="usage_by_group",
        query_params={"resource_type": "cpu", "group_by": "user"},
        filename_base="pie_user_cpu",
        columns=ColumnSpecs.usage_counts(label="User-ids"),
    ),
    ReportConfig(
        command_name="pie-proj-gpu",
        description="GPU usage by project (account)",
        query_method="usage_by_group",
        query_params={"resource_type": "gpu", "group_by": "account"},
        filename_base="pie_proj_gpu",
        columns=ColumnSpecs.usage_counts(label="Accounts"),
    ),
    ReportConfig(
        command_name="pie-group-gpu",
        description="GPU usage by account",
        query_method="usage_by_group",
        query_params={"resource_type": "gpu", "group_by": "account"},
        filename_base="pie_group_gpu",
        columns=ColumnSpecs.usage_counts(label="Accounts"),
    ),
    ReportConfig(
        command_name="pie-group-cpu",
        description="CPU usage by account",
        query_method="usage_by_group",
        query_params={"resource_type": "cpu", "group_by": "account"},
        filename_base="pie_group_cpu",
        columns=ColumnSpecs.usage_counts(label="Accounts"),
    ),
    ReportConfig(
        command_name="pie-facility-cpu",
        description="CPU usage by facility (UNIV/WNA/CSL/CISL/NCAR)",
        query_method="usage_by_facility",
        query_params={"resource_type": "cpu"},
        filename_base="pie_facility_cpu",
        columns=ColumnSpecs.usage_counts(label="Facility"),
    ),
    ReportConfig(
        command_name="pie-facility-gpu",
        description="GPU usage by facility (UNIV/WNA/CSL/CISL/NCAR)",
        query_method="usage_by_facility",
        query_params={"resource_type": "gpu"},
        filename_base="pie_facility_gpu",
        columns=ColumnSpecs.usage_counts(label="Facility"),
    ),

    # Duration reports
    ReportConfig(
        command_name="gpu-job-durations",
        description="GPU job durations by period",
        query_method="job_durations",
        query_params={"resource_type": "gpu"},
        filename_base="gpu_job_durations",
        columns=ColumnSpecs.duration_buckets(),
    ),
    ReportConfig(
        command_name="cpu-job-durations",
        description="CPU job durations by period",
        query_method="job_durations",
        query_params={"resource_type": "cpu"},
        filename_base="cpu_job_durations",
        columns=ColumnSpecs.duration_buckets(),
    ),

    # Memory-per-rank histogram reports
    ReportConfig(
        command_name="cpu-job-memory-per-rank",
        description="CPU job memory-per-rank histogram by period",
        query_method="job_memory_per_rank",
        query_params={"resource_type": "cpu"},
        filename_base="cpu_job_memory_per_rank",
        columns=ColumnSpecs.memory_per_rank_buckets(),
    ),
    ReportConfig(
        command_name="gpu-job-memory-per-rank",
        description="GPU job memory-per-rank histogram by period",
        query_method="job_memory_per_rank",
        query_params={"resource_type": "gpu"},
        filename_base="gpu_job_memory_per_rank",
        columns=ColumnSpecs.memory_per_rank_buckets(),
    ),

    # Wait-time reports
    ReportConfig(
        command_name="gpu-job-waits",
        description="GPU job waits by GPU count",
        query_method="job_waits_by_resource",
        query_params={"resource_type": "gpu", "range_type": "gpu"},
        filename_base="gpu_job_waits",
        columns=ColumnSpecs.range_waits("GPUs"),
    ),
    ReportConfig(
        command_name="cpu-job-waits",
        description="CPU job waits by node count",
        query_method="job_waits_by_resource",
        query_params={"resource_type": "cpu", "range_type": "node"},
        filename_base="cpu_job_waits",
        columns=ColumnSpecs.range_waits("Nodes"),
    ),
    ReportConfig(
        command_name="job-waits",
        description="Job waits by core count",
        query_method="job_waits_by_resource",
        query_params={"resource_type": "all", "range_type": "core"},
        filename_base="bycore_job_waits",
        columns=ColumnSpecs.range_waits("Cores"),
    ),

    # Job-size reports
    ReportConfig(
        command_name="gpu-job-sizes",
        description="GPU job sizes by GPU count",
        query_method="job_sizes_by_resource",
        query_params={"resource_type": "gpu", "range_type": "gpu"},
        filename_base="gpu_job_sizes",
        columns=ColumnSpecs.range_sizes("GPUs"),
    ),
    ReportConfig(
        command_name="cpu-job-sizes",
        description="CPU job sizes by node count",
        query_method="job_sizes_by_resource",
        query_params={"resource_type": "cpu", "range_type": "node"},
        filename_base="cpu_job_sizes",
        columns=ColumnSpecs.range_sizes("Nodes"),
    ),
    ReportConfig(
        command_name="job-sizes",
        description="Job sizes by core count",
        query_method="job_sizes_by_resource",
        query_params={"resource_type": "all", "range_type": "core"},
        filename_base="bycore_job_sizes",
        columns=ColumnSpecs.range_sizes("Cores"),
    ),

    # Usage history report
    ReportConfig(
        command_name="usage-history",
        description="Usage history by period",
        query_method="usage_history",
        query_params={},
        filename_base="usage_history",
        columns=ColumnSpecs.usage_history(),
    ),

    # Memory-based reports
    ReportConfig(
        command_name="memory-job-waits",
        description="Job waits by memory requirement",
        query_method="memory_job_waits",
        query_params={},
        filename_base="memory_job_waits",
        columns=ColumnSpecs.range_waits("Memory(GB)"),
    ),
    ReportConfig(
        command_name="memory-job-sizes",
        description="Job sizes by memory requirement",
        query_method="memory_job_sizes",
        query_params={},
        filename_base="memory_job_sizes",
        columns=ColumnSpecs.range_sizes("Memory(GB)"),
    ),
]
